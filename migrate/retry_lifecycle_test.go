package migrate

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLockRetryStaysThrottledAfterManyAttempts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var previous time.Time
		attempts := 0
		exec := &retryMockExecutor{acquireFn: func() error {
			attempts++
			now := time.Now()
			if attempts > 7 {
				require.GreaterOrEqual(t, now.Sub(previous), time.Second, "attempt %d must retain its capped backoff", attempts)
				require.LessOrEqual(t, now.Sub(previous), 2*time.Second)
			}
			previous = now
			if attempts == 60 {
				return nil
			}
			return ErrLockHeld
		}}
		require.NoError(t, NewOrchestrator(exec).acquireLockWithRetry(t.Context(), "test"))
		require.Equal(t, 60, attempts)
	})
}

func TestLockRetryDoesNotQueryAfterBudgetExpires(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		exec := &retryMockExecutor{acquireFn: func() error { return ErrLockHeld }}
		started := time.Now()
		err := NewOrchestrator(exec).SetLockTimeout(20*time.Millisecond).acquireLockWithRetry(t.Context(), "test")
		require.ErrorIs(t, err, ErrLockHeld)
		require.EqualValues(t, 1, exec.calls.Load(), "the wait budget must stop another database query")
		require.Equal(t, 20*time.Millisecond, time.Since(started))
	})
}

func TestCanceledLockRetryDoesNotQuery(t *testing.T) {
	exec := &retryMockExecutor{acquireFn: func() error { return ErrLockHeld }}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err := NewOrchestrator(exec).acquireLockWithRetry(ctx, "test")
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, exec.calls.Load())
}

type waitingLockInspector struct{ retryMockExecutor }

func (*waitingLockInspector) LockInfo(ctx context.Context) (*LockInfo, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestLockInspectionHasDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		exec := &waitingLockInspector{retryMockExecutor{acquireFn: func() error { return ErrLockHeld }}}
		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()
		started := time.Now()
		err := NewOrchestrator(exec).SetLockTimeout(time.Millisecond).acquireLockWithRetry(ctx, "test")
		require.ErrorIs(t, err, ErrLockHeld)
		require.LessOrEqual(t, time.Since(started), 100*time.Millisecond, "diagnostic queries must honor the caller's deadline")
	})
}

func TestLockInspectionWithoutCallerDeadlineIsBounded(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		exec := &waitingLockInspector{retryMockExecutor{acquireFn: func() error { return ErrLockHeld }}}
		started := time.Now()
		err := NewOrchestrator(exec).SetLockTimeout(time.Millisecond).acquireLockWithRetry(t.Context(), "test")
		require.ErrorIs(t, err, ErrLockHeld)
		require.Equal(t, time.Second+time.Millisecond, time.Since(started))
	})
}

type waitingLockExecutor struct {
	retryMockExecutor
	failFirst bool
}

func (e *waitingLockExecutor) AcquireLock(ctx context.Context, _ string) error {
	if e.calls.Add(1) == 1 && e.failFirst {
		return ErrLockHeld
	}
	<-ctx.Done()
	return ctx.Err()
}

func TestLockQueryHonorsWaitBudget(t *testing.T) {
	for _, failFirst := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			exec := &waitingLockExecutor{failFirst: failFirst}
			started := time.Now()
			err := NewOrchestrator(exec).SetLockTimeout(time.Second).acquireLockWithRetry(t.Context(), "test")
			if failFirst {
				require.ErrorIs(t, err, ErrLockHeld)
				require.EqualValues(t, 2, exec.calls.Load())
			} else {
				require.ErrorIs(t, err, context.DeadlineExceeded)
				require.EqualValues(t, 1, exec.calls.Load())
			}
			require.Equal(t, time.Second, time.Since(started))
		})
	}
}
