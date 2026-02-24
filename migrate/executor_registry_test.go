package migrate

import (
	"context"
	"testing"

	"github.com/xraph/grove/driver"
)

// mockDriver implements the driverNamer interface for testing.
type mockDriver struct {
	name string
}

func (d *mockDriver) Name() string { return d.name }

// mockExecutor is a minimal test double for the Executor interface.
type mockExecutor struct {
	name string
}

func (e *mockExecutor) Exec(_ context.Context, _ string, _ ...any) (driver.Result, error) {
	return nil, nil
}
func (e *mockExecutor) Query(_ context.Context, _ string, _ ...any) (driver.Rows, error) {
	return nil, nil
}
func (e *mockExecutor) EnsureMigrationTable(_ context.Context) error               { return nil }
func (e *mockExecutor) EnsureLockTable(_ context.Context) error                    { return nil }
func (e *mockExecutor) AcquireLock(_ context.Context, _ string) error              { return nil }
func (e *mockExecutor) ReleaseLock(_ context.Context) error                        { return nil }
func (e *mockExecutor) ListApplied(_ context.Context) ([]*AppliedMigration, error) { return nil, nil }
func (e *mockExecutor) RecordApplied(_ context.Context, _ *Migration) error        { return nil }
func (e *mockExecutor) RemoveApplied(_ context.Context, _ *Migration) error        { return nil }

func TestRegisterExecutor(t *testing.T) {
	// Clean up after test to avoid affecting other tests.
	defer func() {
		executorsMu.Lock()
		delete(executorFactories, "test-driver")
		executorsMu.Unlock()
	}()

	RegisterExecutor("test-driver", func(_ any) Executor {
		return &mockExecutor{name: "test"}
	})

	names := Executors()
	found := false
	for _, n := range names {
		if n == "test-driver" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected 'test-driver' in registered executors, got %v", names)
	}
}

func TestRegisterExecutor_Overwrite(t *testing.T) {
	defer func() {
		executorsMu.Lock()
		delete(executorFactories, "overwrite-driver")
		executorsMu.Unlock()
	}()

	RegisterExecutor("overwrite-driver", func(_ any) Executor {
		return &mockExecutor{name: "first"}
	})
	RegisterExecutor("overwrite-driver", func(_ any) Executor {
		return &mockExecutor{name: "second"}
	})

	drv := &mockDriver{name: "overwrite-driver"}
	exec, err := NewExecutorFor(drv)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	me, ok := exec.(*mockExecutor)
	if !ok {
		t.Fatal("expected *mockExecutor")
	}
	if me.name != "second" {
		t.Errorf("expected 'second' (overwritten), got '%s'", me.name)
	}
}

func TestNewExecutorFor_Success(t *testing.T) {
	defer func() {
		executorsMu.Lock()
		delete(executorFactories, "mock-pg")
		executorsMu.Unlock()
	}()

	RegisterExecutor("mock-pg", func(drv any) Executor {
		return &mockExecutor{name: drv.(*mockDriver).name}
	})

	drv := &mockDriver{name: "mock-pg"}
	exec, err := NewExecutorFor(drv)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if exec == nil {
		t.Fatal("expected non-nil executor")
	}
}

func TestNewExecutorFor_UnregisteredDriver(t *testing.T) {
	drv := &mockDriver{name: "nonexistent-driver"}
	_, err := NewExecutorFor(drv)
	if err == nil {
		t.Fatal("expected error for unregistered driver")
	}
}

func TestNewExecutorFor_NoNameMethod(t *testing.T) {
	// A value that does not implement driverNamer.
	_, err := NewExecutorFor("not-a-driver")
	if err == nil {
		t.Fatal("expected error for value without Name() method")
	}
}

func TestExecutors_Empty(t *testing.T) {
	// Save and restore state
	executorsMu.Lock()
	saved := make(map[string]ExecutorFactory, len(executorFactories))
	for k, v := range executorFactories {
		saved[k] = v
	}
	executorFactories = make(map[string]ExecutorFactory)
	executorsMu.Unlock()

	defer func() {
		executorsMu.Lock()
		executorFactories = saved
		executorsMu.Unlock()
	}()

	names := Executors()
	if len(names) != 0 {
		t.Errorf("expected empty list, got %v", names)
	}
}
