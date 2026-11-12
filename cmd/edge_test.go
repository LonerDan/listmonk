package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/knadh/goyesql/v2"
	"github.com/knadh/koanf/v2"
	"github.com/knadh/listmonk/internal/edge"
	"github.com/knadh/listmonk/models"
	_ "github.com/lib/pq"
)

// TestEdgeSync runs the edge sync against a fake edge and a real database.
// It requires LISTMONK_TEST_DSN to point at a disposable database, as the schema
// is reset. It's skipped otherwise.
func TestEdgeSync(t *testing.T) {
	dsn := os.Getenv("LISTMONK_TEST_DSN")
	if dsn == "" {
		t.Skip("LISTMONK_TEST_DSN not set")
	}

	// resetSchema drops the public schema. Refuse DSNs that don't look like a
	// disposable test database to avoid nuking a real instance.
	if !strings.Contains(strings.ToLower(dsn), "test") {
		t.Fatalf("refusing to run: LISTMONK_TEST_DSN (%q) must point to a database with 'test' in its name", dsn)
	}

	db, err := sqlx.Connect("postgres", dsn)
	if err != nil {
		t.Fatalf("connecting to db: %v", err)
	}
	defer db.Close()

	resetSchema(t, db)

	const (
		campUUID = "11111111-1111-1111-1111-111111111111"
		subUUID  = "22222222-2222-2222-2222-222222222222"
		linkUUID = "33333333-3333-3333-3333-333333333333"
	)
	db.MustExec(`INSERT INTO campaigns (uuid, name, subject, from_email, body, messenger)
		VALUES ($1, 'c', 's', 'a@b.c', '', 'email')`, campUUID)
	db.MustExec(`INSERT INTO subscribers (uuid, email, name) VALUES ($1, 's@example.com', 's')`, subUUID)
	db.MustExec(`INSERT INTO links (uuid, url) VALUES ($1, 'https://example.com')`, linkUUID)

	q := prepareEdgeQueries(t, db)

	// The sync cycle reads tracking settings from the DB (not koanf), so seed
	// the setting that the test asserts on.
	db.MustExec(`UPDATE settings SET value = 'true' WHERE key = 'privacy.individual_tracking'`)

	at := time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)
	fake := newFakeEdge("edge-1", []edge.Event{
		{Seq: 1, Type: "view", At: at, CampUUID: campUUID, SubUUID: subUUID},
		{Seq: 2, Type: "click", At: at, CampUUID: campUUID, SubUUID: subUUID, LinkUUID: linkUUID},
		{Seq: 3, Type: "view", At: at, CampUUID: dummyUUID, SubUUID: subUUID}, // Preview hit, must be ignored.
		{Seq: 4, Type: "view", At: at, CampUUID: "not-a-uuid"},                // Malformed, must be skipped.
	})
	srv := httptest.NewServer(fake)
	defer srv.Close()

	ko := koanf.New(".")
	s := initEdgeSync(edge.New(srv.URL, "secret", time.Second), db, q, ko)

	if err := s.sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}

	var views, clicks int
	db.Get(&views, `SELECT COUNT(*) FROM campaign_views WHERE subscriber_id IS NOT NULL AND created_at = $1`, at)
	db.Get(&clicks, `SELECT COUNT(*) FROM link_clicks WHERE campaign_id IS NOT NULL AND subscriber_id IS NOT NULL`)
	if views != 1 || clicks != 1 {
		t.Fatalf("expected 1 view and 1 click, got %d views and %d clicks", views, clicks)
	}

	var last int64
	db.Get(&last, `SELECT last_seq FROM edge_sync_state WHERE edge_id = 'edge-1'`)
	if last != 4 {
		t.Fatalf("expected sync cursor 4, got %d", last)
	}
	if fake.acked() != 4 {
		t.Fatalf("expected edge to be acked up to 4, got %d", fake.acked())
	}

	// Re-delivery of already processed events must not create duplicates.
	fake.redeliver = true
	if err := s.sync(); err != nil {
		t.Fatalf("second sync: %v", err)
	}
	db.Get(&views, `SELECT COUNT(*) FROM campaign_views`)
	db.Get(&clicks, `SELECT COUNT(*) FROM link_clicks`)
	if views != 1 || clicks != 1 {
		t.Fatalf("re-delivery created duplicates: %d views, %d clicks", views, clicks)
	}

	// After the first sync, the edge must hold the link that listmonk knows.
	if got := fake.links[linkUUID]; got != "https://example.com" {
		t.Fatalf("expected link pushed to edge, got %q", got)
	}

	// Simulate the edge losing its data: the version mismatch must trigger a
	// full re-push of listmonk's links on the next cycle.
	fake.wipe()
	if err := s.sync(); err != nil {
		t.Fatalf("sync after edge data loss: %v", err)
	}
	if got := fake.links[linkUUID]; got != "https://example.com" {
		t.Fatalf("expected link re-pushed to edge after wipe, got %q", got)
	}
	var version string
	db.Get(&version, `SELECT links_version FROM edge_sync_state WHERE edge_id = 'edge-1'`)
	if version == "" {
		t.Fatal("expected links_version to be stored after re-sync")
	}

	// A subsequent cycle must not re-push (version now matches, map untouched).
	n := len(fake.links)
	if err := s.sync(); err != nil {
		t.Fatalf("third sync: %v", err)
	}
	if len(fake.links) != n {
		t.Fatalf("expected link map unchanged, got %d links", len(fake.links))
	}
}

func resetSchema(t *testing.T, db *sqlx.DB) {
	t.Helper()
	db.MustExec(`DROP SCHEMA public CASCADE; CREATE SCHEMA public;`)

	schema, err := os.ReadFile("../schema.sql")
	if err != nil {
		t.Fatalf("reading schema: %v", err)
	}
	db.MustExec(string(schema))
}

// prepareEdgeQueries prepares the edge queries from queries/edge.sql, plus
// get-settings from queries/misc.sql which the sync cycle reads.
func prepareEdgeQueries(t *testing.T, db *sqlx.DB) *models.Queries {
	t.Helper()
	qMap := goyesql.Queries{}
	for _, f := range []string{"../queries/edge.sql", "../queries/misc.sql"} {
		partial, err := goyesql.ParseFile(f)
		if err != nil {
			t.Fatalf("parsing %s: %v", f, err)
		}
		maps.Copy(qMap, partial)
	}

	prep := func(name string) *sqlx.Stmt {
		stmt, err := db.Preparex(qMap[name].Query)
		if err != nil {
			t.Fatalf("preparing %s: %v", name, err)
		}
		return stmt
	}

	return &models.Queries{
		EdgeInsertView:   prep("edge-insert-view"),
		EdgeInsertClick:  prep("edge-insert-click"),
		EdgeGetSyncState: prep("edge-get-sync-state"),
		EdgeSetSyncState: prep("edge-set-sync-state"),
		EdgeSetLinksVer:  prep("edge-set-links-version"),
		EdgeGetLinks:     prep("edge-get-links"),
		GetSettings:      prep("get-settings"),
	}
}

// fakeEdge implements the edge's internal API with a fixed set of events and
// an in-memory link map, mirroring the SQLite-backed behaviour in the spec.
type fakeEdge struct {
	edgeID    string
	events    []edge.Event
	redeliver bool

	mu    sync.Mutex
	links map[string]string

	ackedTo int64
}

func newFakeEdge(edgeID string, events []edge.Event) *fakeEdge {
	return &fakeEdge{edgeID: edgeID, events: events, links: map[string]string{}}
}

func (f *fakeEdge) acked() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ackedTo
}

// linksVersion computes an ETag-style fingerprint of the link map: the SHA-256
// hex digest of the sorted uuid:url lines, as recommended in the spec.
func (f *fakeEdge) linksVersionLocked() string {
	lines := make([]string, 0, len(f.links))
	for uuid, url := range f.links {
		lines = append(lines, uuid+":"+url)
	}
	slices.Sort(lines)
	h := sha256.Sum256([]byte(strings.Join(lines, "\n")))
	return hex.EncodeToString(h[:])
}

// wipe simulates the edge losing its data (wiped SQLite DB).
func (f *fakeEdge) wipe() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.links = map[string]string{}
}

func (f *fakeEdge) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != "Bearer secret" {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	switch r.URL.Path {
	case "/internal/v1/config":
		w.WriteHeader(http.StatusNoContent)

	case "/internal/v1/links":
		var body struct {
			Links []edge.Link `json:"links"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		for _, l := range body.Links {
			f.links[l.UUID] = l.URL
		}
		json.NewEncoder(w).Encode(map[string]string{"links_version": f.linksVersionLocked()})

	case "/internal/v1/health":
		json.NewEncoder(w).Encode(edge.Health{EdgeID: f.edgeID, LinksVersion: f.linksVersionLocked()})

	case "/internal/v1/events":
		var after int64
		json.Unmarshal([]byte(r.URL.Query().Get("after")), &after)

		out := []edge.Event{}
		for _, ev := range f.events {
			if ev.Seq > after || f.redeliver {
				out = append(out, ev)
			}
		}
		json.NewEncoder(w).Encode(edge.EventBatch{EdgeID: f.edgeID, Events: out})

	case "/internal/v1/events/ack":
		var body struct {
			UpTo int64 `json:"up_to"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		f.ackedTo = body.UpTo
		w.WriteHeader(http.StatusNoContent)

	default:
		http.NotFound(w, r)
	}
}
