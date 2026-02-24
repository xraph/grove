package pgdriver

import "github.com/xraph/grove"

// Unwrap extracts the underlying *PgDB from a *grove.DB handle.
// This allows access to PostgreSQL-specific query builders and features.
//
//	pgdb := pgdriver.Unwrap(db) // returns *pgdriver.PgDB
//	pgdb.NewSelect(&users).Where("email ILIKE $1", "%@test.com").Scan(ctx)
//
// Panics if the driver is not a *PgDB.
func Unwrap(db *grove.DB) *PgDB {
	pgdb, ok := db.Driver().(*PgDB)
	if !ok {
		panic("pgdriver: driver is not a *PgDB")
	}
	return pgdb
}
