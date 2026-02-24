package esdriver

// Elasticsearch does not support multi-document ACID transactions.
//
// The ElasticDB type deliberately does NOT implement the grove txBeginner
// interface. This means grove.DB.BeginTx() will return grove.ErrNotSupported
// when used with the Elasticsearch driver.
//
// For batching multiple write operations into a single network request,
// use ElasticDB.NewBulk() instead:
//
//	esdb := esdriver.Unwrap(db)
//	result, err := esdb.NewBulk().
//	    Index("users", "1", user1).
//	    Update("users", "2", esdriver.M{"name": "updated"}).
//	    Delete("users", "3").
//	    Exec(ctx)
