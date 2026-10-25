package migrate

import (
	"context"
	"errors"
	"fmt"
)

// TxExecutor is an optional interface an [Executor] implements so the
// orchestrator can run a migration and its applied record in one transaction.
// The orchestrator finds it with [ExecutorAs], so a wrapper that has an Unwrap
// method keeps the transactions of the executor inside it, and executors that
// don't implement it keep working without one.
//
// The orchestrator owns the transaction, and the executor holds its state on
// the connection that has the migration lock. BeginTx returns no handle:
// after it, every statement the migration runs through the executor, and the
// RecordApplied or RemoveApplied call that follows, must go through that
// transaction on that connection until CommitTx or RollbackTx.
type TxExecutor interface {
	// BeginTx opens the transaction. It returns [ErrTxUnsupported] when this
	// executor can't hold one for the migration, and the orchestrator then runs
	// the migration without it.
	BeginTx(ctx context.Context) error

	// CommitTx commits the transaction.
	CommitTx(ctx context.Context) error

	// RollbackTx abandons the transaction. The orchestrator also calls it
	// after a failed CommitTx, so it has to tolerate a transaction that is
	// already finished.
	RollbackTx(ctx context.Context) error
}

// ErrTxUnsupported is what [TxExecutor.BeginTx] returns when the executor
// can't run the migration in a transaction. It isn't a failure: the migration
// runs plain, as it would on an executor without [TxExecutor].
var ErrTxUnsupported = errors.New("migrate: executor cannot run this migration in a transaction")

// runStep runs work, which is a migration's step and the record of it, for m.
// When m is Transactional and the executor is a [TxExecutor], work runs
// between BeginTx and CommitTx, so a record that fails to write can't leave
// the step's changes behind. Otherwise work runs as is, which is what struct
// migrations and executors without transactions have always done.
func (o *Orchestrator) runStep(ctx context.Context, m *Migration, work func(context.Context) error) error {
	tx, ok := ExecutorAs[TxExecutor](o.executor)
	if !m.Transactional || !ok {
		return work(ctx)
	}

	if err := tx.BeginTx(ctx); err != nil {
		if errors.Is(err, ErrTxUnsupported) {
			return work(ctx)
		}
		return fmt.Errorf("migrate: begin %s/%s: %w", m.Group, m.Name, err)
	}

	settled := false
	// Roll back with a context the caller can't cancel, so a cancelled
	// context doesn't leave the transaction open on the lock connection.
	abort := func(cause error) error {
		settled = true
		rbErr := tx.RollbackTx(context.WithoutCancel(ctx))
		if rbErr == nil {
			return cause
		}
		return fmt.Errorf("%w (rollback failed: %w)", cause, rbErr)
	}
	// A panic in work skips every return below, so this is what abandons the
	// transaction then. The panic keeps unwinding with its own value and
	// stack, and the deferred ReleaseLock in the caller still runs.
	defer func() {
		if !settled {
			tx.RollbackTx(context.WithoutCancel(ctx)) //nolint:errcheck // the panic in flight is the error that matters
		}
	}()

	if err := work(ctx); err != nil {
		return abort(err)
	}
	if err := tx.CommitTx(ctx); err != nil {
		return abort(fmt.Errorf("migrate: commit %s/%s: %w", m.Group, m.Name, err))
	}
	settled = true
	return nil
}
