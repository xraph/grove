package mysqldriver

import "github.com/xraph/grove"

// Unwrap extracts the underlying *MysqlDB from a *grove.DB handle.
// This allows access to MySQL-specific query builders and features.
//
//	mydb := mysqldriver.Unwrap(db) // returns *mysqldriver.MysqlDB
//	mydb.NewSelect(&users).Where("`email` LIKE ?", "%@test.com").Scan(ctx)
//
// Panics if the driver is not a *MysqlDB.
func Unwrap(db *grove.DB) *MysqlDB {
	mydb, ok := db.Driver().(*MysqlDB)
	if !ok {
		panic("mysqldriver: driver is not a *MysqlDB")
	}
	return mydb
}
