package migrate

// Schema constants for the migration tracking tables.
const (
	MigrationTable     = "grove_migrations"
	MigrationLockTable = "grove_migration_locks"
)

// MigrationTableSchema returns the CREATE TABLE SQL for the migration tracking table.
func MigrationTableSchema() string {
	return `CREATE TABLE IF NOT EXISTS ` + MigrationTable + ` (
        id          BIGSERIAL PRIMARY KEY,
        version     VARCHAR(14) NOT NULL,
        name        VARCHAR(255) NOT NULL,
        "group"     VARCHAR(255) NOT NULL,
        migrated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE(version, "group")
    )`
}

// MigrationLockTableSchema returns the CREATE TABLE SQL for the lock table.
func MigrationLockTableSchema() string {
	return `CREATE TABLE IF NOT EXISTS ` + MigrationLockTable + ` (
        id          INT PRIMARY KEY DEFAULT 1,
        locked_at   TIMESTAMPTZ,
        locked_by   VARCHAR(255),
        CONSTRAINT single_lock CHECK (id = 1)
    )`
}
