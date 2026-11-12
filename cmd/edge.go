package main

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/jmoiron/sqlx"
	"github.com/knadh/koanf/v2"
	"github.com/knadh/listmonk/internal/edge"
	"github.com/knadh/listmonk/models"
)

const (
	edgeBatchSize    = 500
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
	client     *edge.Client
	db         *sqlx.DB
	q          *models.Queries
	interval   time.Duration
	disable    bool
	individual bool
}

func initEdgeSync(client *edge.Client, db *sqlx.DB, q *models.Queries, ko *koanf.Koanf) *edgeSync {
	interval := ko.Duration("tracking_edge.poll_interval")
	if interval <= 0 {
		interval = edgePollInterval
	}

	return &edgeSync{
		client:     client,
		db:         db,
		q:          q,
		interval:   interval,
		disable:    ko.Bool("privacy.disable_tracking"),
		individual: ko.Bool("privacy.individual_tracking"),
	}
}

// Run polls the edge at the configured interval. It never returns.
func (e *edgeSync) Run() {
	for {
		if err := e.sync(); err != nil {
			lo.Printf("error syncing tracking edge: %v", err)
		}
		time.Sleep(e.interval)
	}
}

// sync pushes the tracking config, then drains all pending events from the edge.
func (e *edgeSync) sync() error {
	if err := e.client.PutConfig(edge.Config{
		DisableTracking:    e.disable,
		IndividualTracking: e.individual,
	}); err != nil {
		return fmt.Errorf("pushing config: %w", err)
	}

	h, err := e.client.Health()
	if err != nil {
		return fmt.Errorf("fetching health: %w", err)
	}

	var last int64
	if err := e.q.EdgeGetSyncState.Get(&last, h.EdgeID); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("reading sync state: %w", err)
	}

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

		newLast, err := e.record(batch.EdgeID, last, batch.Events)
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

// record inserts a batch of events and advances the sync cursor in a single
// transaction. Events with a sequence number at or below last were already
// processed and are skipped. It returns the new cursor value.
func (e *edgeSync) record(edgeID string, last int64, events []edge.Event) (int64, error) {
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

		if err := e.recordEvent(tx, ev); err != nil {
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
func (e *edgeSync) recordEvent(tx *sqlx.Tx, ev edge.Event) error {
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
	if !e.individual || sub == dummyUUID {
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
