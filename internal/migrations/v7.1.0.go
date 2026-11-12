package migrations

import (
	"log"

	"github.com/jmoiron/sqlx"
	"github.com/knadh/koanf/v2"
	"github.com/knadh/stuffbin"
)

// V7_1_0 adds the table that tracks the event sync cursor for tracking edge instances.
func V7_1_0(db *sqlx.DB, fs stuffbin.FileSystem, ko *koanf.Koanf, lo *log.Logger) error {
	if _, err := db.Exec(`
        CREATE TABLE IF NOT EXISTS edge_sync_state (
            edge_id          TEXT PRIMARY KEY,
            last_seq         BIGINT NOT NULL DEFAULT 0,
            updated_at       TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
    `); err != nil {
		return err
	}

	return nil
}
