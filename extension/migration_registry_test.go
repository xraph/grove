package extension

import (
	"context"
	"strings"
	"testing"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/migrate"
)

// --- fake driver for registry tests ---

type fakeRegDriver struct{}

func (f *fakeRegDriver) Name() string                 { return "reg_test_driver" }
func (f *fakeRegDriver) Close() error                 { return nil }
func (f *fakeRegDriver) Ping(_ context.Context) error { return nil }

// --- fake drivers for rollback-reverse test ---

type fakeRegDriverA struct{}

func (f *fakeRegDriverA) Name() string                 { return "reg_test_a" }
func (f *fakeRegDriverA) Close() error                 { return nil }
func (f *fakeRegDriverA) Ping(_ context.Context) error { return nil }

type fakeRegDriverB struct{}

func (f *fakeRegDriverB) Name() string                 { return "reg_test_b" }
func (f *fakeRegDriverB) Close() error                 { return nil }
func (f *fakeRegDriverB) Ping(_ context.Context) error { return nil }

// --- recordingExecutor: succeeds every operation ---

type recordingExecutor struct{}

func (r *recordingExecutor) AcquireLock(_ context.Context, _ string) error { return nil }
func (r *recordingExecutor) ReleaseLock(_ context.Context) error           { return nil }
func (r *recordingExecutor) EnsureMigrationTable(_ context.Context) error  { return nil }
func (r *recordingExecutor) EnsureLockTable(_ context.Context) error       { return nil }
func (r *recordingExecutor) ListApplied(_ context.Context) ([]*migrate.AppliedMigration, error) {
	return nil, nil
}
func (r *recordingExecutor) RecordApplied(_ context.Context, _ *migrate.Migration) error {
	return nil
}
func (r *recordingExecutor) RemoveApplied(_ context.Context, _ *migrate.Migration) error {
	return nil
}
func (r *recordingExecutor) Exec(_ context.Context, _ string, _ ...any) (driver.Result, error) {
	return nil, nil
}
func (r *recordingExecutor) Query(_ context.Context, _ string, _ ...any) (driver.Rows, error) {
	return nil, nil
}

// --- rollbackExec: returns one applied migration so Rollback has something to do ---

type rollbackExec struct {
	group string
}

func (e *rollbackExec) AcquireLock(_ context.Context, _ string) error { return nil }
func (e *rollbackExec) ReleaseLock(_ context.Context) error           { return nil }
func (e *rollbackExec) EnsureMigrationTable(_ context.Context) error  { return nil }
func (e *rollbackExec) EnsureLockTable(_ context.Context) error       { return nil }
func (e *rollbackExec) ListApplied(_ context.Context) ([]*migrate.AppliedMigration, error) {
	return []*migrate.AppliedMigration{
		{Group: e.group, Version: "0001", Name: "m"},
	}, nil
}
func (e *rollbackExec) RecordApplied(_ context.Context, _ *migrate.Migration) error { return nil }
func (e *rollbackExec) RemoveApplied(_ context.Context, _ *migrate.Migration) error { return nil }
func (e *rollbackExec) Exec(_ context.Context, _ string, _ ...any) (driver.Result, error) {
	return nil, nil
}
func (e *rollbackExec) Query(_ context.Context, _ string, _ ...any) (driver.Rows, error) {
	return nil, nil
}

func init() {
	migrate.RegisterExecutor("reg_test_driver", func(_ any) migrate.Executor {
		return &recordingExecutor{}
	})
	migrate.RegisterExecutor("reg_test_a", func(_ any) migrate.Executor {
		return &rollbackExec{group: "group_a"}
	})
	migrate.RegisterExecutor("reg_test_b", func(_ any) migrate.Executor {
		return &rollbackExec{group: "group_b"}
	})
}

// TestMigrationRegistry_MergesGroupsAndOrders proves cross-extension DependsOn
// is resolved when both groups share one orchestrator via the registry.
func TestMigrationRegistry_MergesGroupsAndOrders(t *testing.T) {
	var order []string

	identity := migrate.NewGroup("identity/postgres")
	if err := identity.Register(&migrate.Migration{
		Version: "0001",
		Name:    "users",
		Up: func(ctx context.Context, e migrate.Executor) error {
			order = append(order, "identity")
			return nil
		},
	}); err != nil {
		t.Fatalf("identity.Register: %v", err)
	}

	trove := migrate.NewGroup("trove/postgres", migrate.DependsOn("identity/postgres"))
	if err := trove.Register(&migrate.Migration{
		Version: "0001",
		Name:    "items",
		Up: func(ctx context.Context, e migrate.Executor) error {
			order = append(order, "trove")
			return nil
		},
	}); err != nil {
		t.Fatalf("trove.Register: %v", err)
	}

	r := NewMigrationRegistry()
	r.Contribute("", &fakeRegDriver{}, identity)
	r.Contribute("", &fakeRegDriver{}, trove)

	res, err := r.RunAll(context.Background())
	if err != nil {
		t.Fatalf("RunAll should resolve cross-extension DependsOn, got %v", err)
	}

	if res.Applied != 2 {
		t.Errorf("Applied = %d, want 2", res.Applied)
	}

	if len(order) != 2 || order[0] != "identity" || order[1] != "trove" {
		t.Errorf("execution order = %v, want [identity trove]", order)
	}
}

// TestMigrationRegistry_UnresolvedDepFails proves that a missing DependsOn
// group fails loudly rather than silently succeeding.
func TestMigrationRegistry_UnresolvedDepFails(t *testing.T) {
	// Contribute only trove whose dep "identity/postgres" is absent.
	trove := migrate.NewGroup("trove/postgres", migrate.DependsOn("identity/postgres"))
	if err := trove.Register(&migrate.Migration{
		Version: "0001",
		Name:    "items",
		Up:      func(ctx context.Context, e migrate.Executor) error { return nil },
	}); err != nil {
		t.Fatalf("trove.Register: %v", err)
	}

	r := NewMigrationRegistry()
	r.Contribute("", &fakeRegDriver{}, trove)

	_, err := r.RunAll(context.Background())
	if err == nil {
		t.Fatal("RunAll should fail when a DependsOn group is missing")
	}
	if !strings.Contains(err.Error(), "unknown group") {
		t.Errorf("error should mention 'unknown group', got: %v", err)
	}
}

// TestMigrationRegistry_RollbackReverse verifies RollbackAll iterates databases
// in reverse alphabetical order. db_a < db_b alphabetically, so forward order is
// [db_a, db_b] and rollback order must be [db_b, db_a] — i.e. "b" before "a".
func TestMigrationRegistry_RollbackReverse(t *testing.T) {
	var rollbackOrder []string

	ga := migrate.NewGroup("group_a")
	if err := ga.Register(&migrate.Migration{
		Version: "0001",
		Name:    "m",
		Up:      func(_ context.Context, _ migrate.Executor) error { return nil },
		Down: func(_ context.Context, _ migrate.Executor) error {
			rollbackOrder = append(rollbackOrder, "a")
			return nil
		},
	}); err != nil {
		t.Fatalf("ga.Register: %v", err)
	}

	gb := migrate.NewGroup("group_b")
	if err := gb.Register(&migrate.Migration{
		Version: "0001",
		Name:    "m",
		Up:      func(_ context.Context, _ migrate.Executor) error { return nil },
		Down: func(_ context.Context, _ migrate.Executor) error {
			rollbackOrder = append(rollbackOrder, "b")
			return nil
		},
	}); err != nil {
		t.Fatalf("gb.Register: %v", err)
	}

	r := NewMigrationRegistry()
	r.Contribute("db_a", &fakeRegDriverA{}, ga)
	r.Contribute("db_b", &fakeRegDriverB{}, gb)

	// RollbackAll must reverse alphabetical order: db_b rolls back before db_a.
	_, err := r.RollbackAll(context.Background())
	if err != nil {
		t.Fatalf("RollbackAll: %v", err)
	}

	if len(rollbackOrder) != 2 || rollbackOrder[0] != "b" || rollbackOrder[1] != "a" {
		t.Errorf("rollback order = %v, want [b a]", rollbackOrder)
	}
}
