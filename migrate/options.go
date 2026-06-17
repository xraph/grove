package migrate

import (
	"context"
	"time"
)

// DefaultLockTimeout is the default maximum time Migrate/Rollback will wait to
// acquire the migration lock before giving up. Raised from the original 30s so
// that several extensions migrating the same database in one process serialize
// and wait instead of failing the boot.
const DefaultLockTimeout = 5 * time.Minute

// OrchestratorOption configures an Orchestrator.
type OrchestratorOption func(*Orchestrator)

// WithLockTimeout sets how long to wait for the migration lock. A value of 0
// means "wait until the context deadline / cancellation".
func WithLockTimeout(d time.Duration) OrchestratorOption {
	return func(o *Orchestrator) { o.lockTimeout = d }
}

// LockInspector is an optional capability: executors that can report who holds
// the migration lock implement it, letting the orchestrator produce a
// diagnosable error when the wait budget is exhausted.
type LockInspector interface {
	LockInfo(ctx context.Context) (*LockInfo, error)
}
