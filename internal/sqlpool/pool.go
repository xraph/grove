// Package sqlpool applies Grove's pool options to network database/sql drivers.
package sqlpool

import (
	"database/sql"

	"github.com/xraph/grove/driver"
)

// Configure bounds the pool and expires unused connections. Do not use this
// for an in-memory database: closing its last connection may erase its data.
func Configure(db *sql.DB, opts *driver.DriverOptions) {
	if opts.PoolSize > 0 {
		db.SetMaxOpenConns(opts.PoolSize)
	}
	// database/sql has no minimum pool size. Preserve the existing mapping
	// of MinConns to the idle connection cap for callers who set it.
	if opts.MinConns > 0 {
		db.SetMaxIdleConns(int(opts.MinConns))
	}
	db.SetConnMaxLifetime(opts.MaxConnLifetime)
	db.SetConnMaxIdleTime(opts.MaxConnIdleTime)
}
