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

// TestMigrate_NoOpWhenContributed verifies that Migrate and Rollback return
// immediately with zero counts when e.contributed is true.
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

	res2, err2 := e.Rollback(context.Background())
	if err2 != nil {
		t.Fatalf("Rollback: unexpected error: %v", err2)
	}
	if res2 == nil {
		t.Fatal("Rollback: expected non-nil result")
	}
	if res2.RolledBack != 0 {
		t.Fatalf("Rollback: RolledBack = %d, want 0", res2.RolledBack)
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
