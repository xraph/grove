package migrate

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// noop is a no-op migration function used across all tests.
var noop MigrateFunc = func(_ context.Context, _ Executor) error { return nil }

func TestNewGroup(t *testing.T) {
	g := NewGroup("core")

	assert.Equal(t, "core", g.Name())
	assert.Empty(t, g.DependsOnGroups())
	assert.Empty(t, g.Migrations())
}

func TestNewGroupWithDependsOn(t *testing.T) {
	g := NewGroup("billing", DependsOn("core", "auth"))

	assert.Equal(t, "billing", g.Name())
	assert.Equal(t, []string{"core", "auth"}, g.DependsOnGroups())
}

func TestGroupRegister(t *testing.T) {
	g := NewGroup("core")

	err := g.Register(
		&Migration{Name: "create_users", Version: "20240115120000", Up: noop, Down: noop},
		&Migration{Name: "create_orders", Version: "20240116120000", Up: noop, Down: noop},
	)
	require.NoError(t, err)

	migrations := g.Migrations()
	require.Len(t, migrations, 2)

	// Verify sorted by version.
	assert.Equal(t, "20240115120000", migrations[0].Version)
	assert.Equal(t, "create_users", migrations[0].Name)
	assert.Equal(t, "20240116120000", migrations[1].Version)
	assert.Equal(t, "create_orders", migrations[1].Name)

	// Verify Group field is set automatically.
	for _, m := range migrations {
		assert.Equal(t, "core", m.Group)
	}
}

func TestGroupRegisterDuplicateVersion(t *testing.T) {
	g := NewGroup("core")

	err := g.Register(
		&Migration{Name: "create_users", Version: "20240115120000", Up: noop, Down: noop},
	)
	require.NoError(t, err)

	// Registering the same version again should return an error.
	err = g.Register(
		&Migration{Name: "create_accounts", Version: "20240115120000", Up: noop, Down: noop},
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate version")
	assert.Contains(t, err.Error(), "20240115120000")
	assert.Contains(t, err.Error(), "core")
}

func TestGroupMustRegisterPanics(t *testing.T) {
	g := NewGroup("core")

	g.MustRegister(
		&Migration{Name: "create_users", Version: "20240115120000", Up: noop, Down: noop},
	)

	// MustRegister should panic when a duplicate version is added.
	assert.Panics(t, func() {
		g.MustRegister(
			&Migration{Name: "create_accounts", Version: "20240115120000", Up: noop, Down: noop},
		)
	})
}

func TestGroupMigrations_Sorted(t *testing.T) {
	g := NewGroup("core")

	// Register in reverse version order to confirm Migrations() sorts.
	err := g.Register(
		&Migration{Name: "add_indexes", Version: "20240301000000", Up: noop, Down: noop},
		&Migration{Name: "create_users", Version: "20240101000000", Up: noop, Down: noop},
		&Migration{Name: "add_email_col", Version: "20240201000000", Up: noop, Down: noop},
	)
	require.NoError(t, err)

	migrations := g.Migrations()
	require.Len(t, migrations, 3)

	expectedVersions := []string{"20240101000000", "20240201000000", "20240301000000"}
	expectedNames := []string{"create_users", "add_email_col", "add_indexes"}

	for i, m := range migrations {
		assert.Equal(t, expectedVersions[i], m.Version, "migration %d version mismatch", i)
		assert.Equal(t, expectedNames[i], m.Name, "migration %d name mismatch", i)
	}
}
