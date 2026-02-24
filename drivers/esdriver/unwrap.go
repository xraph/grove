package esdriver

import "github.com/xraph/grove"

// Unwrap extracts the underlying *ElasticDB from a *grove.DB handle.
// This allows access to Elasticsearch-specific query builders and features.
//
//	esdb := esdriver.Unwrap(db) // returns *esdriver.ElasticDB
//	esdb.NewSearch(&users).Match("name", "alice").Scan(ctx)
//
// Panics if the driver is not an *ElasticDB.
func Unwrap(db *grove.DB) *ElasticDB {
	esdb, ok := db.Driver().(*ElasticDB)
	if !ok {
		panic("esdriver: driver is not an *ElasticDB")
	}
	return esdb
}
