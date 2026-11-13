package mongodriver

import (
	"context"
	"errors"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/xraph/grove/driver"
)

// MongoTx wraps a MongoDB session with an active transaction.
// Query builder methods on MongoTx execute within the transaction's
// session context.
type MongoTx struct {
	session *mongo.Session
	db      *MongoDB
	ctx     context.Context
}

// Commit honors the transaction's caller context and configured operation
// timeout, then ends the session with a separate, bounded cleanup context.
func (tx *MongoTx) Commit() (err error) {
	ctx, cancel := context.WithTimeout(tx.ctx, transactionTimeout(&tx.db.txTimeout))
	defer cancel()
	defer func() {
		cleanCtx, cleanCancel := tx.cleanupContext()
		defer cleanCancel()
		tx.session.EndSession(cleanCtx)
		err = errors.Join(err, cleanCtx.Err())
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
	return tx.session.CommitTransaction(ctx)
}

// Rollback aborts the transaction and ends the session.
func (tx *MongoTx) Rollback() error {
	ctx, cancel := tx.cleanupContext()
	defer cancel()
	err := tx.session.AbortTransaction(ctx)
	tx.session.EndSession(ctx)
	// The SDK deliberately suppresses abort command failures, including
	// timeouts. Report an expired cleanup budget to the caller.
	return errors.Join(err, ctx.Err())
}

func (tx *MongoTx) cleanupContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(tx.ctx), min(transactionTimeout(&tx.db.txTimeout), 5*time.Second))
}

func transactionTimeout(configured *time.Duration) time.Duration {
	if configured != nil && *configured > 0 {
		return *configured
	}
	return driver.DefaultDriverOptions().QueryTimeout
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
