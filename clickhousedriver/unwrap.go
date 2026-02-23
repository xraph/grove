package clickhousedriver

import "github.com/xraph/grove"

// Unwrap extracts the underlying *ClickHouseDB from a *grove.DB handle.
// This allows access to ClickHouse-specific query builders and features.
//
//	chdb := clickhousedriver.Unwrap(db) // returns *clickhousedriver.ClickHouseDB
//	chdb.NewSelect(&events).Final().Prewhere("date > ?", "2024-01-01").Scan(ctx)
//
// Panics if the driver is not a *ClickHouseDB.
func Unwrap(db *grove.DB) *ClickHouseDB {
	chdb, ok := db.Driver().(*ClickHouseDB)
	if !ok {
		panic("clickhousedriver: driver is not a *ClickHouseDB")
	}
	return chdb
}
