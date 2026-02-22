package mongodriver

import "github.com/xraph/grove"

// Unwrap extracts the underlying *MongoDB from a *grove.DB handle.
// This allows access to MongoDB-specific query builders and features.
//
//	mdb := mongodriver.Unwrap(db) // returns *mongodriver.MongoDB
//	mdb.NewFind(&users).Filter(bson.M{"role": "admin"}).Scan(ctx)
//
// Panics if the driver is not a *MongoDB.
func Unwrap(db *grove.DB) *MongoDB {
	mdb, ok := db.Driver().(*MongoDB)
	if !ok {
		panic("mongodriver: driver is not a *MongoDB")
	}
	return mdb
}
