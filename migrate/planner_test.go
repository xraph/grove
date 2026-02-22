package migrate

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// noopFn is a no-op migration function for planner tests.
var noopFn MigrateFunc = func(ctx context.Context, exec Executor) error { return nil }

// newTestMigration creates a migration with the given name and version, using no-op functions.
func newTestMigration(name, version string) *Migration {
	return &Migration{Name: name, Version: version, Up: noopFn, Down: noopFn}
}

func TestPlanMigrations_SingleGroup(t *testing.T) {
	g := NewGroup("core")
	g.MustRegister(
		newTestMigration("create_users", "20240102000000"),
		newTestMigration("create_orders", "20240101000000"),
		newTestMigration("add_indexes", "20240103000000"),
	)

	plan, err := planMigrations([]*Group{g})
	require.NoError(t, err)
	require.Len(t, plan, 3)

	// Migrations within the single group must be in version order.
	assert.Equal(t, "20240101000000", plan[0].Version)
	assert.Equal(t, "20240102000000", plan[1].Version)
	assert.Equal(t, "20240103000000", plan[2].Version)
}

func TestPlanMigrations_TwoGroups_NoDeps(t *testing.T) {
	core := NewGroup("core")
	core.MustRegister(
		newTestMigration("create_users", "20240101000000"),
	)

	billing := NewGroup("billing")
	billing.MustRegister(
		newTestMigration("create_invoices", "20240102000000"),
	)

	plan, err := planMigrations([]*Group{core, billing})
	require.NoError(t, err)
	require.Len(t, plan, 2)

	// Both migrations must appear in the plan.
	versions := make([]string, len(plan))
	for i, m := range plan {
		versions[i] = m.Version
	}
	assert.Contains(t, versions, "20240101000000")
	assert.Contains(t, versions, "20240102000000")
}

func TestPlanMigrations_WithDependencies(t *testing.T) {
	core := NewGroup("core")
	core.MustRegister(
		newTestMigration("create_users", "20240101000000"),
		newTestMigration("create_settings", "20240102000000"),
	)

	billing := NewGroup("billing", DependsOn("core"))
	billing.MustRegister(
		newTestMigration("create_invoices", "20240103000000"),
		newTestMigration("create_payments", "20240104000000"),
	)

	plan, err := planMigrations([]*Group{billing, core})
	require.NoError(t, err)
	require.Len(t, plan, 4)

	// All of core's migrations must appear before any of billing's.
	coreLastIdx := -1
	billingFirstIdx := len(plan)

	for i, m := range plan {
		if m.Group == "core" {
			if i > coreLastIdx {
				coreLastIdx = i
			}
		}
		if m.Group == "billing" {
			if i < billingFirstIdx {
				billingFirstIdx = i
			}
		}
	}

	assert.Less(t, coreLastIdx, billingFirstIdx,
		"all core migrations must come before any billing migration")

	// Verify version order within each group.
	assert.Equal(t, "20240101000000", plan[0].Version)
	assert.Equal(t, "20240102000000", plan[1].Version)
	assert.Equal(t, "20240103000000", plan[2].Version)
	assert.Equal(t, "20240104000000", plan[3].Version)
}

func TestPlanMigrations_TransitiveDeps(t *testing.T) {
	// C depends on B, B depends on A => order: A, B, C
	groupA := NewGroup("A")
	groupA.MustRegister(newTestMigration("a1", "20240101000000"))

	groupB := NewGroup("B", DependsOn("A"))
	groupB.MustRegister(newTestMigration("b1", "20240102000000"))

	groupC := NewGroup("C", DependsOn("B"))
	groupC.MustRegister(newTestMigration("c1", "20240103000000"))

	// Pass in reverse order to verify topo-sort overrides input order.
	plan, err := planMigrations([]*Group{groupC, groupB, groupA})
	require.NoError(t, err)
	require.Len(t, plan, 3)

	assert.Equal(t, "A", plan[0].Group)
	assert.Equal(t, "B", plan[1].Group)
	assert.Equal(t, "C", plan[2].Group)
}

func TestPlanMigrations_CyclicDependency(t *testing.T) {
	groupA := NewGroup("A", DependsOn("B"))
	groupA.MustRegister(newTestMigration("a1", "20240101000000"))

	groupB := NewGroup("B", DependsOn("A"))
	groupB.MustRegister(newTestMigration("b1", "20240102000000"))

	_, err := planMigrations([]*Group{groupA, groupB})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cyclic dependency")
}

func TestPlanMigrations_UnknownDependency(t *testing.T) {
	groupA := NewGroup("A", DependsOn("nonexistent"))
	groupA.MustRegister(newTestMigration("a1", "20240101000000"))

	_, err := planMigrations([]*Group{groupA})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown group")
	assert.Contains(t, err.Error(), "nonexistent")
}

func TestPlanMigrations_DiamondDeps(t *testing.T) {
	// Diamond: A depends on B and C; B depends on D; C depends on D.
	// Expected order: D first, then B and C (any order), then A last.
	groupD := NewGroup("D")
	groupD.MustRegister(newTestMigration("d1", "20240101000000"))

	groupB := NewGroup("B", DependsOn("D"))
	groupB.MustRegister(newTestMigration("b1", "20240102000000"))

	groupC := NewGroup("C", DependsOn("D"))
	groupC.MustRegister(newTestMigration("c1", "20240103000000"))

	groupA := NewGroup("A", DependsOn("B", "C"))
	groupA.MustRegister(newTestMigration("a1", "20240104000000"))

	// Pass in scrambled order to exercise the sort.
	plan, err := planMigrations([]*Group{groupA, groupC, groupB, groupD})
	require.NoError(t, err)
	require.Len(t, plan, 4)

	// Build a position index for each group.
	pos := make(map[string]int, 4)
	for i, m := range plan {
		pos[m.Group] = i
	}

	// D must come before B and C.
	assert.Less(t, pos["D"], pos["B"], "D must come before B")
	assert.Less(t, pos["D"], pos["C"], "D must come before C")

	// B and C must both come before A.
	assert.Less(t, pos["B"], pos["A"], "B must come before A")
	assert.Less(t, pos["C"], pos["A"], "C must come before A")
}
