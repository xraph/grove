package extension

import (
	"context"
	"testing"

	"github.com/xraph/vessel"
)

// TestRegistryFromContainer_GetOrCreateSameInstance verifies that two
// extensions calling registryFromContainer on the same DI container
// get back the exact same *MigrationRegistry pointer.
func TestRegistryFromContainer_GetOrCreateSameInstance(t *testing.T) {
	c := vessel.New()

	e1 := New()
	r1 := e1.registryFromContainer(c)
	if r1 == nil {
		t.Fatal("expected non-nil registry from e1")
	}

	e2 := New()
	r2 := e2.registryFromContainer(c)
	if r2 == nil {
		t.Fatal("expected non-nil registry from e2")
	}

	if r1 != r2 {
		t.Fatal("expected the same registry instance across extensions")
	}
}

// TestMigrate_NoOpWhenContributed verifies that Migrate returns a non-nil
// empty result with no error when e.contributed is true (central registry
// owns forward migration; per-extension pass is legitimately a no-op).
func TestMigrate_NoOpWhenContributed(t *testing.T) {
	e := New()
	e.contributed = true

	res, err := e.Migrate(context.Background())
	if err != nil {
		t.Fatalf("Migrate: unexpected error: %v", err)
	}
	if res == nil {
		t.Fatal("Migrate: expected non-nil result")
	}
	if res.Applied != 0 {
		t.Fatalf("Migrate: Applied = %d, want 0", res.Applied)
	}
}

// TestRollback_ErrorsWhenContributed verifies that Rollback returns a non-nil
// error when e.contributed is true. Central rollback is not available until
// the forge CentralMigrator (Phase 3) lands; silently no-oping would leave
// applied migrations in place with no indication to the caller.
func TestRollback_ErrorsWhenContributed(t *testing.T) {
	e := New()
	e.contributed = true

	res, err := e.Rollback(context.Background())
	if err == nil {
		t.Fatal("Rollback: expected a non-nil error in central migration mode, got nil")
	}
	// result may be nil or zero — either is acceptable; only the error matters.
	if res != nil && res.RolledBack != 0 {
		t.Fatalf("Rollback: RolledBack = %d, want 0", res.RolledBack)
	}
}

// TestWithCentralMigrations_SetsConfig verifies that WithCentralMigrations
// sets config.CentralMigrations to true.
func TestWithCentralMigrations_SetsConfig(t *testing.T) {
	e := New(WithCentralMigrations())
	if !e.config.CentralMigrations {
		t.Fatal("expected config.CentralMigrations to be true")
	}
}

// TestMergeConfigurations_PropagatesCentralMigrations verifies that a
// programmatic CentralMigrations=true is preserved when merging over a
// YAML config that has CentralMigrations=false.
func TestMergeConfigurations_PropagatesCentralMigrations(t *testing.T) {
	e := New()
	yamlConfig := Config{CentralMigrations: false}
	programmaticConfig := Config{CentralMigrations: true}
	merged := e.mergeConfigurations(yamlConfig, programmaticConfig)
	if !merged.CentralMigrations {
		t.Fatal("expected merged CentralMigrations to be true when programmatic config sets it")
	}
}

// TestTryClaimHook_Once verifies that tryClaimHook returns true exactly once
// across multiple calls on the same registry.
func TestTryClaimHook_Once(t *testing.T) {
	r := NewMigrationRegistry()

	if !r.tryClaimHook() {
		t.Fatal("first tryClaimHook should return true")
	}
	if r.tryClaimHook() {
		t.Fatal("second tryClaimHook should return false")
	}
	if r.tryClaimHook() {
		t.Fatal("third tryClaimHook should return false")
	}
}
