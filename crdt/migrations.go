package crdt

import "fmt"

// ShadowTableDDL generates the CREATE TABLE statement for a CRDT shadow table.
// The DDL uses $1-style placeholders that are compatible with PostgreSQL.
// For SQLite, the caller should substitute the appropriate syntax.
func ShadowTableDDL(table string) string {
	shadow := ShadowTableName(table)
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    pk_hash     TEXT    NOT NULL,
    field_name  TEXT    NOT NULL,
    hlc_ts      BIGINT  NOT NULL,
    hlc_counter INTEGER NOT NULL DEFAULT 0,
    node_id     TEXT    NOT NULL,
    tombstone   BOOLEAN NOT NULL DEFAULT FALSE,
    crdt_state  TEXT,

    PRIMARY KEY (pk_hash, field_name, node_id)
)`, shadow)
}

// ShadowTableSyncIndex generates the CREATE INDEX statement for efficient
// sync queries (ordered by HLC timestamp).
func ShadowTableSyncIndex(table string) string {
	shadow := ShadowTableName(table)
	return fmt.Sprintf(
		`CREATE INDEX IF NOT EXISTS idx_%s_sync ON %s (hlc_ts, node_id)`,
		shadow, shadow,
	)
}

// DropShadowTableDDL generates the DROP TABLE statement for a shadow table.
func DropShadowTableDDL(table string) string {
	shadow := ShadowTableName(table)
	return fmt.Sprintf(`DROP TABLE IF EXISTS %s`, shadow)
}
