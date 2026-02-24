package mongodriver

import (
	"context"

	"go.mongodb.org/mongo-driver/v2/mongo"
)

// MongoTx wraps a MongoDB session with an active transaction.
// Query builder methods on MongoTx execute within the transaction's
// session context.
type MongoTx struct {
	session *mongo.Session
	db      *MongoDB
}

// Commit commits the transaction and ends the session.
func (tx *MongoTx) Commit() error {
	ctx := context.Background()
	err := tx.session.CommitTransaction(ctx)
	tx.session.EndSession(ctx)
	return err
}

// Rollback aborts the transaction and ends the session.
func (tx *MongoTx) Rollback() error {
	ctx := context.Background()
	err := tx.session.AbortTransaction(ctx)
	tx.session.EndSession(ctx)
	return err
}

// Session returns the underlying MongoDB session.
func (tx *MongoTx) Session() *mongo.Session {
	return tx.session
}

// DB returns the parent MongoDB driver.
func (tx *MongoTx) DB() *MongoDB {
	return tx.db
}

// SessionContext returns a context with the session attached, suitable
// for passing to MongoDB operations that should participate in the
// transaction.
func (tx *MongoTx) SessionContext(ctx context.Context) context.Context {
	return mongo.NewSessionContext(ctx, tx.session)
}

// NewFind creates a FindQuery that executes within this transaction.
func (tx *MongoTx) NewFind(model ...any) *FindQuery {
	q := tx.db.NewFind(model...)
	q.session = tx.session
	return q
}

// NewInsert creates an InsertQuery that executes within this transaction.
func (tx *MongoTx) NewInsert(model any) *InsertQuery {
	q := tx.db.NewInsert(model)
	q.session = tx.session
	return q
}

// NewUpdate creates an UpdateQuery that executes within this transaction.
func (tx *MongoTx) NewUpdate(model any) *UpdateQuery {
	q := tx.db.NewUpdate(model)
	q.session = tx.session
	return q
}

// NewDelete creates a DeleteQuery that executes within this transaction.
func (tx *MongoTx) NewDelete(model any) *DeleteQuery {
	q := tx.db.NewDelete(model)
	q.session = tx.session
	return q
}
