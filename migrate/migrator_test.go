package migrate

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/grove/driver"
)

// retryMockExecutor implements [Executor] for testing acquireLockWithRetry.
// Only AcquireLock behaviour is configurable; other methods are no-ops.
type retryMockExecutor struct {
	acquireFn func() error
	releaseFn func(ctx context.Context) error
	calls     atomic.Int32
}

func (m *retryMockExecutor) AcquireLock(_ context.Context, _ string) error {
	m.calls.Add(1)
	return m.acquireFn()
}

func (m *retryMockExecutor) ReleaseLock(ctx context.Context) error {
	if m.releaseFn != nil {
		return m.releaseFn(ctx)
	}
	return nil
}
func (m *retryMockExecutor) Exec(context.Context, string, ...any) (driver.Result, error) {
	return nil, nil
}
func (m *retryMockExecutor) Query(context.Context, string, ...any) (driver.Rows, error) {
	return nil, nil
}
func (m *retryMockExecutor) EnsureMigrationTable(context.Context) error { return nil }
func (m *retryMockExecutor) EnsureLockTable(context.Context) error      { return nil }
func (m *retryMockExecutor) ListApplied(context.Context) ([]*AppliedMigration, error) {
	return nil, nil
}
func (m *retryMockExecutor) RecordApplied(context.Context, *Migration) error { return nil }
func (m *retryMockExecutor) RemoveApplied(context.Context, *Migration) error { return nil }

func TestAcquireLockWithRetry_SuccessOnFirstAttempt(t *testing.T) {
	mock := &retryMockExecutor{acquireFn: func() error { return nil }}
	orch := NewOrchestrator(mock)

	err := orch.acquireLockWithRetry(context.Background(), "test:1")
	require.NoError(t, err)
	assert.Equal(t, int32(1), mock.calls.Load(), "should call AcquireLock exactly once")
}

func TestAcquireLockWithRetry_SuccessAfterRetries(t *testing.T) {
	var attempt atomic.Int32
	failCount := int32(3)

	mock := &retryMockExecutor{
		acquireFn: func() error {
			n := attempt.Add(1)
			if n <= failCount {
				return fmt.Errorf("pgmigrate: %w", ErrLockHeld)
			}
			return nil
		},
	}
	orch := NewOrchestrator(mock)

	start := time.Now()
	err := orch.acquireLockWithRetry(context.Background(), "test:1")
	elapsed := time.Since(start)

	require.NoError(t, err)
	assert.Equal(t, failCount+1, mock.calls.Load(), "should retry until success")
	assert.Less(t, elapsed, 5*time.Second, "should complete quickly with backoff")
}

func TestAcquireLockWithRetry_NonLockErrorNotRetried(t *testing.T) {
	connErr := errors.New("connection refused")
	mock := &retryMockExecutor{acquireFn: func() error { return connErr }}
	orch := NewOrchestrator(mock)

	err := orch.acquireLockWithRetry(context.Background(), "test:1")
	require.Error(t, err)
	assert.ErrorIs(t, err, connErr)
	assert.Equal(t, int32(1), mock.calls.Load(), "non-lock errors should not be retried")
}

func TestAcquireLockWithRetry_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	mock := &retryMockExecutor{
		acquireFn: func() error {
			return fmt.Errorf("pgmigrate: %w", ErrLockHeld)
		},
	}
	orch := NewOrchestrator(mock)

	start := time.Now()
	err := orch.acquireLockWithRetry(ctx, "test:1")
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.Less(t, elapsed, 1*time.Second, "should return promptly on cancelled context")
}

func TestMigrate_ReleaseLockCalledWithUncancelledContext(t *testing.T) {
	// Verify that ReleaseLock receives an uncancelled context even when
	// the caller's context is cancelled during migration execution.
	var releaseCtxCancelled atomic.Bool

	ctx, cancel := context.WithCancel(context.Background())

	mock := &retryMockExecutor{
		acquireFn: func() error { return nil },
		releaseFn: func(rctx context.Context) error {
			// Record whether the context passed to ReleaseLock is cancelled.
			if rctx.Err() != nil {
				releaseCtxCancelled.Store(true)
			}
			return nil
		},
	}

	// Create a group with a migration whose Up function cancels the context,
	// simulating a shutdown signal arriving while migrations are in progress.
	g := NewGroup("test")
	g.MustRegister(&Migration{
		Version: "00000000000001",
		Name:    "cancel_context",
		Group:   "test",
		Up: func(_ context.Context, _ Executor) error {
			cancel() // simulate shutdown signal
			return nil
		},
	})
	orch := NewOrchestrator(mock, g)

	_, err := orch.Migrate(ctx)
	require.NoError(t, err)

	// The context passed to ReleaseLock must NOT be cancelled.
	assert.False(t, releaseCtxCancelled.Load(),
		"ReleaseLock should receive an uncancelled context (context.WithoutCancel)")
}
