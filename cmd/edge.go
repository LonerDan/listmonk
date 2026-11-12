package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/jmoiron/sqlx"
	"github.com/jmoiron/sqlx/types"
	"github.com/knadh/koanf/v2"
	"github.com/knadh/listmonk/internal/edge"
	"github.com/knadh/listmonk/models"
)

const (
	edgeBatchSize    = 500
	edgeLinkBatch    = 500
	edgePollInterval = 30 * time.Second
	edgeHTTPTimeout  = 10 * time.Second
)

// initEdgeClient returns a client for the tracking edge's internal API, or nil
// if the tracking edge is not enabled.
func initEdgeClient(ko *koanf.Koanf) *edge.Client {
	if !ko.Bool("tracking_edge.enabled") {
		return nil
	}

	for _, k := range []string{"tracking_edge.public_url", "tracking_edge.internal_url", "tracking_edge.token"} {
		if ko.String(k) == "" {
			lo.Fatalf("%s is required when tracking_edge.enabled is true", k)
		}
	}

	return edge.New(ko.String("tracking_edge.internal_url"), ko.String("tracking_edge.token"), edgeHTTPTimeout)
}

// edgeSync pulls tracking events from the edge and records them in the database.
// The main instance only makes outbound connections to the edge.
type edgeSync struct {
	client   *edge.Client
	db       *sqlx.DB
	q        *models.Queries
	interval time.Duration

	// lastEdgeID is the edge ID seen in the last successful sync cycle. If it
	// changes (eg: the edge's database was wiped or replaced), sync is halted
	// and a prominent alert is logged, as events would otherwise be re-read
	// from seq 0 and duplicated.
	lastEdgeID string

	mu sync.Mutex
}

func initEdgeSync(client *edge.Client, db *sqlx.DB, q *models.Queries, ko *koanf.Koanf) *edgeSync {
	interval := ko.Duration("tracking_edge.poll_interval")
	if interval <= 0 {
		interval = edgePollInterval
	}

	return &edgeSync{
		client:   client,
		db:       db,
		q:        q,
		interval: interval,
	}
}

// Run polls the edge at the configured interval until ctx is cancelled.
func (e *edgeSync) Run(ctx context.Context) {
	for {
		if err := e.sync(); err != nil {
			lo.Printf("error syncing tracking edge: %v", err)
		}

		select {
		case <-ctx.Done():
			lo.Println("stopping tracking edge sync")
			return
		case <-time.After(e.interval):
		}
	}
}

// edgeSyncState is the sync cursor row for one edge.
type edgeSyncState struct {
	LastSeq      int64  `db:"last_seq"`
	LinksVersion string `db:"links_version"`
}

// checkEdgeID halts syncing if the edge's ID has changed since the last cycle.
// A changed edge ID means events may be re-read from seq 0, which is never
// something that should happen silently.
func (e *edgeSync) checkEdgeID(id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.lastEdgeID == "" {
		e.lastEdgeID = id
		return nil
	}
	if e.lastEdgeID != id {
		return fmt.Errorf("TRACKING EDGE ID CHANGED from %s to %s. This usually means the edge's "+
			"data store was wiped or replaced. Syncing is halted to avoid re-reading and "+
			"duplicating events from seq 0. Restore the previous edge ID or reset the "+
			"edge_sync_state row for the new edge manually", e.lastEdgeID, id)
	}
	return nil
}

// syncLinks re-pushes every link known to listmonk to the edge and records the
// resulting links_version fingerprint. This reconciles the edge after data loss
// (its map was wiped) or brings a new edge up to date.
func (e *edgeSync) syncLinks(edgeID string) error {
	lo.Printf("tracking edge link map out of sync (edge_id=%s), re-pushing all links", edgeID)

	rows, err := e.q.EdgeGetLinks.Queryx()
	if err != nil {
		return err
	}
	defer rows.Close()

	// Push in batches; the edge returns the fingerprint of its map after each
	// batch, and the last one is the version of the fully applied update.
	batch := make([]edge.Link, 0, edgeLinkBatch)
	var version string
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		v, err := e.client.PutLinks(batch)
		if err != nil {
			return err
		}
		version = v
		batch = batch[:0]
		return nil
	}

	for rows.Next() {
		var uuid, url string
		if err := rows.Scan(&uuid, &url); err != nil {
			return err
		}
		batch = append(batch, edge.Link{UUID: uuid, URL: url})
		if len(batch) >= edgeLinkBatch {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if err := flush(); err != nil {
		return err
	}

	if _, err := e.q.EdgeSetLinksVer.Exec(edgeID, version); err != nil {
		return fmt.Errorf("saving links version: %w", err)
	}
	lo.Printf("tracking edge link map synced (%s)", edgeID)
	return nil
}

// sync pushes the tracking config, then drains all pending events from the edge.
func (e *edgeSync) sync() error {
	// Read the tracking settings from the DB on every cycle so that changes
	// made from the admin UI are reflected without a restart.
	disable, individual, err := e.getTrackingSettings()
	if err != nil {
		return fmt.Errorf("reading tracking settings: %w", err)
	}

	if err := e.client.PutConfig(edge.Config{
		DisableTracking:    disable,
		IndividualTracking: individual,
	}); err != nil {
		return fmt.Errorf("pushing config: %w", err)
	}

	h, err := e.client.Health()
	if err != nil {
		return fmt.Errorf("fetching health: %w", err)
	}

	if err := e.checkEdgeID(h.EdgeID); err != nil {
		// Prominently flag the change: this is a data-integrity condition, not
		// a transient failure. The error is returned (and logged by Run) on
		// every cycle until it is resolved.
		lo.Printf("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n"+
			"%v\n"+
			"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!", err)
		return err
	}

	var state edgeSyncState
	if err := e.q.EdgeGetSyncState.Get(&state, h.EdgeID); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("reading sync state: %w", err)
	}

	// If the edge's link map fingerprint doesn't match what we last synced to it
	// (eg: its data store was wiped), re-push all known links before its version
	// check can register clicks against missing links.
	if h.LinksVersion != state.LinksVersion {
		if err := e.syncLinks(h.EdgeID); err != nil {
			return fmt.Errorf("syncing links to edge: %w", err)
		}
	}

	last := state.LastSeq
	for {
		batch, err := e.client.PullEvents(last, edgeBatchSize)
		if err != nil {
			return fmt.Errorf("pulling events: %w", err)
		}
		if batch.EdgeID != h.EdgeID {
			return fmt.Errorf("edge ID changed from %s to %s during sync", h.EdgeID, batch.EdgeID)
		}
		if len(batch.Events) == 0 {
			return nil
		}

		newLast, err := e.record(batch.EdgeID, last, batch.Events, individual)
		if err != nil {
			return err
		}

		// Events are durable in the DB. Ack them so the edge can prune its queue.
		if err := e.client.AckEvents(newLast); err != nil {
			return fmt.Errorf("acking events: %w", err)
		}
		last = newLast

		if len(batch.Events) < edgeBatchSize {
			return nil
		}
	}
}

// getTrackingSettings reads privacy.disable_tracking and privacy.individual_tracking
// from the settings table, so that admin UI changes take effect without a restart.
func (e *edgeSync) getTrackingSettings() (disable, individual bool, err error) {
	var s types.JSONText
	if err := e.q.GetSettings.Get(&s); err != nil {
		return false, false, err
	}

	var out map[string]any
	if err := json.Unmarshal(s, &out); err != nil {
		return false, false, err
	}

	parse := func(key string) bool {
		switch v := out[key].(type) {
		case bool:
			return v
		case string:
			return v == "true" || v == "t" || v == "1"
		}
		return false
	}
	return parse("privacy.disable_tracking"), parse("privacy.individual_tracking"), nil
}

// record inserts a batch of events and advances the sync cursor in a single
// transaction. Events with a sequence number at or below last were already
// processed and are skipped. It returns the new cursor value.
func (e *edgeSync) record(edgeID string, last int64, events []edge.Event, individual bool) (int64, error) {
	tx, err := e.db.Beginx()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	newLast := last
	for _, ev := range events {
		if ev.Seq <= last {
			continue
		}
		if ev.Seq > newLast {
			newLast = ev.Seq
		}

		if err := e.recordEvent(tx, ev, individual); err != nil {
			return 0, fmt.Errorf("recording event %d: %w", ev.Seq, err)
		}
	}

	if newLast > last {
		if _, err := tx.Stmtx(e.q.EdgeSetSyncState).Exec(edgeID, newLast); err != nil {
			return 0, fmt.Errorf("saving sync state: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return newLast, nil
}

// recordEvent writes a single view or click. Malformed events are logged and skipped
// so that they don't block the sync cursor.
func (e *edgeSync) recordEvent(tx *sqlx.Tx, ev edge.Event, individual bool) error {
	if !validUUID(ev.CampUUID) || (ev.SubUUID != "" && !validUUID(ev.SubUUID)) {
		lo.Printf("skipping tracking edge event %d: invalid campaign or subscriber UUID", ev.Seq)
		return nil
	}

	at := ev.At
	if at.IsZero() {
		at = time.Now()
	}

	// Mirror the main instance's handling of the dummy UUIDs used by template previews
	// and by individual tracking being disabled.
	sub := ev.SubUUID
	if !individual || sub == dummyUUID {
		sub = ""
	}

	switch ev.Type {
	case "view":
		if ev.CampUUID == dummyUUID {
			return nil
		}
		_, err := tx.Stmtx(e.q.EdgeInsertView).Exec(ev.CampUUID, sub, at)
		return err

	case "click":
		if !validUUID(ev.LinkUUID) {
			lo.Printf("skipping tracking edge event %d: invalid link UUID", ev.Seq)
			return nil
		}
		_, err := tx.Stmtx(e.q.EdgeInsertClick).Exec(ev.LinkUUID, ev.CampUUID, sub, at)
		return err

	default:
		lo.Printf("skipping tracking edge event %d: unknown type %q", ev.Seq, ev.Type)
		return nil
	}
}

func validUUID(s string) bool {
	_, err := uuid.FromString(s)
	return err == nil
}
