package sqlitedriver

import "github.com/xraph/grove"

// Unwrap extracts the underlying *SqliteDB from a *grove.DB handle.
// This allows access to SQLite-specific query builders and features.
//
//	sdb := sqlitedriver.Unwrap(db) // returns *sqlitedriver.SqliteDB
//	sdb.NewSelect(&users).Where("email LIKE ?", "%@test.com").Scan(ctx)
//
// Panics if the driver is not a *SqliteDB.
func Unwrap(db *grove.DB) *SqliteDB {
	sdb, ok := db.Driver().(*SqliteDB)
	if !ok {
		panic("sqlitedriver: driver is not a *SqliteDB")
	}
	return sdb
}
