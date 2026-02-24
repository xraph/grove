package tursodriver

import "github.com/xraph/grove"

// Unwrap extracts the underlying *TursoDB from a *grove.DB handle.
// This allows access to Turso-specific query builders and features.
//
//	tdb := tursodriver.Unwrap(db) // returns *tursodriver.TursoDB
//	tdb.NewSelect(&users).Where("email LIKE ?", "%@test.com").Scan(ctx)
//
// Panics if the driver is not a *TursoDB.
func Unwrap(db *grove.DB) *TursoDB {
	tdb, ok := db.Driver().(*TursoDB)
	if !ok {
		panic("tursodriver: driver is not a *TursoDB")
	}
	return tdb
}
