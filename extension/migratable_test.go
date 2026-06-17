package extension

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/migrate"
)

// --- fake driver for lock-timeout behavioral test ---

type fakeLTDriver struct{}

func (f *fakeLTDriver) Name() string             { return "lt_test_driver" }
func (f *fakeLTDriver) Close() error             { return nil }
func (f *fakeLTDriver) Ping(_ context.Context) error { return nil }

// --- alwaysLockedExecutor: AcquireLock always returns ErrLockHeld ---

type alwaysLockedExecutor struct{}

func (a *alwaysLockedExecutor) AcquireLock(_ context.Context, _ string) error {
	return fmt.Errorf("pgmigrate: %w", migrate.ErrLockHeld)
}

func (a *alwaysLockedExecutor) EnsureMigrationTable(_ context.Context) error { return nil }
func (a *alwaysLockedExecutor) EnsureLockTable(_ context.Context) error      { return nil }
func (a *alwaysLockedExecutor) RecordApplied(_ context.Context, _ *migrate.Migration) error {
	return nil
}
func (a *alwaysLockedExecutor) RemoveApplied(_ context.Context, _ *migrate.Migration) error {
	return nil
}
func (a *alwaysLockedExecutor) ReleaseLock(_ context.Context) error { return nil }
func (a *alwaysLockedExecutor) ListApplied(_ context.Context) ([]*migrate.AppliedMigration, error) {
	return nil, nil
}
func (a *alwaysLockedExecutor) Exec(_ context.Context, _ string, _ ...any) (driver.Result, error) {
	return nil, nil
}
func (a *alwaysLockedExecutor) Query(_ context.Context, _ string, _ ...any) (driver.Rows, error) {
	return nil, nil
}

func init() {
	migrate.RegisterExecutor("lt_test_driver", func(_ any) migrate.Executor {
		return &alwaysLockedExecutor{}
	})
}

// TestWithLockTimeout_SetsConfig verifies that WithLockTimeout stores the value in config.
func TestWithLockTimeout_SetsConfig(t *testing.T) {
	e := New(WithLockTimeout(7 * time.Second))
	if e.config.LockTimeout != 7*time.Second {
		t.Fatalf("config.LockTimeout = %v, want %v", e.config.LockTimeout, 7*time.Second)
	}
}

// TestBuildOrchestrator_AppliesLockTimeout is a behavioral test: it proves the
// timeout is actually wired into the orchestrator. With a 200ms timeout and an
// executor that always returns ErrLockHeld, Migrate should fail well under 2s.
// Without the wiring it would retry for the 5-minute DefaultLockTimeout.
func TestBuildOrchestrator_AppliesLockTimeout(t *testing.T) {
	e := New(WithLockTimeout(200 * time.Millisecond))

	orch, err := e.buildOrchestrator(&fakeLTDriver{}, []*migrate.Group{migrate.NewGroup("t")})
	if err != nil {
		t.Fatalf("buildOrchestrator: %v", err)
	}
	if orch == nil {
		t.Fatal("expected non-nil orchestrator")
	}

	start := time.Now()
	_, err = orch.Migrate(context.Background())
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected error from locked executor")
	}
	if !migrate.IsLockError(err) {
		t.Fatalf("expected lock error, got: %v", err)
	}
	// The 2s bound proves SetLockTimeout was applied (vs the 5m DefaultLockTimeout);
	// it assumes the orchestrator's backoff stays well under that bound for a 200ms timeout.
	if elapsed >= 2*time.Second {
		t.Fatalf("Migrate took %v — lock timeout not applied (want < 2s)", elapsed)
	}
}
