package migrate

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =========================================================================
// MigrationRegistry tests
// =========================================================================

func TestNewMigrationRegistry(t *testing.T) {
	r := NewMigrationRegistry()
	require.NotNil(t, r)
	assert.Empty(t, r.Groups(), "new registry should have no groups")
}

func TestDefaultRegistry_NonNil(t *testing.T) {
	assert.NotNil(t, DefaultRegistry, "DefaultRegistry should be non-nil")
}

func TestMigrationRegistry_Register(t *testing.T) {
	r := NewMigrationRegistry()

	g1 := NewGroup("core")
	g2 := NewGroup("billing", DependsOn("core"))

	r.Register(g1, g2)

	groups := r.Groups()
	require.Len(t, groups, 2)
	assert.Equal(t, "core", groups[0].Name())
	assert.Equal(t, "billing", groups[1].Name())
}

func TestMigrationRegistry_RegisterMultipleCalls(t *testing.T) {
	r := NewMigrationRegistry()

	r.Register(NewGroup("core"))
	r.Register(NewGroup("auth"))
	r.Register(NewGroup("billing"))

	groups := r.Groups()
	require.Len(t, groups, 3)
	assert.Equal(t, "core", groups[0].Name())
	assert.Equal(t, "auth", groups[1].Name())
	assert.Equal(t, "billing", groups[2].Name())
}

func TestMigrationRegistry_Groups_ReturnsCopy(t *testing.T) {
	r := NewMigrationRegistry()
	g := NewGroup("core")
	r.Register(g)

	groups1 := r.Groups()
	groups2 := r.Groups()

	// Mutating the returned slice should not affect the registry.
	groups1[0] = NewGroup("mutated")
	assert.Equal(t, "core", groups2[0].Name(),
		"Groups() should return a copy, not a reference to the internal slice")
}

func TestMigrationRegistry_DuplicateRegistration(t *testing.T) {
	r := NewMigrationRegistry()

	g := NewGroup("core")
	r.Register(g)
	r.Register(g) // registering the same group again

	// The registry does append, so both are present. The planner or migrator
	// would catch true duplicates. The registry just accumulates.
	groups := r.Groups()
	assert.Len(t, groups, 2, "registry stores groups as appended (no dedup at registry level)")
}

// =========================================================================
// Lock tests
// =========================================================================

func TestErrLockHeld(t *testing.T) {
	assert.Error(t, ErrLockHeld)
	assert.Contains(t, ErrLockHeld.Error(), "lock is held")
}

func TestIsLockError_True(t *testing.T) {
	assert.True(t, IsLockError(ErrLockHeld))
}

func TestIsLockError_WrappedError(t *testing.T) {
	wrapped := errors.Join(errors.New("outer"), ErrLockHeld)
	assert.True(t, IsLockError(wrapped), "IsLockError should detect wrapped ErrLockHeld")
}

func TestIsLockError_FalseForOtherError(t *testing.T) {
	assert.False(t, IsLockError(errors.New("something else")))
}

func TestIsLockError_FalseForNil(t *testing.T) {
	assert.False(t, IsLockError(nil))
}

func TestLockInfo_Struct(t *testing.T) {
	info := LockInfo{
		Held:     true,
		LockedBy: "process-123",
		LockedAt: "2025-01-15T12:00:00Z",
	}
	assert.True(t, info.Held)
	assert.Equal(t, "process-123", info.LockedBy)
	assert.Equal(t, "2025-01-15T12:00:00Z", info.LockedAt)
}

func TestLockInfo_Empty(t *testing.T) {
	info := LockInfo{}
	assert.False(t, info.Held)
	assert.Empty(t, info.LockedBy)
	assert.Empty(t, info.LockedAt)
}

// =========================================================================
// Version / Table schema tests
// =========================================================================

func TestMigrationTable_Constant(t *testing.T) {
	assert.Equal(t, "grove_migrations", MigrationTable)
}

func TestMigrationLockTable_Constant(t *testing.T) {
	assert.Equal(t, "grove_migration_locks", MigrationLockTable)
}

func TestMigrationTableSchema_NonEmpty(t *testing.T) {
	schema := MigrationTableSchema()
	assert.NotEmpty(t, schema)
	assert.Contains(t, schema, "CREATE TABLE IF NOT EXISTS")
	assert.Contains(t, schema, MigrationTable)
	assert.Contains(t, schema, "version")
	assert.Contains(t, schema, "name")
	assert.Contains(t, schema, "migrated_at")
}

func TestMigrationLockTableSchema_NonEmpty(t *testing.T) {
	schema := MigrationLockTableSchema()
	assert.NotEmpty(t, schema)
	assert.Contains(t, schema, "CREATE TABLE IF NOT EXISTS")
	assert.Contains(t, schema, MigrationLockTable)
	assert.Contains(t, schema, "locked_at")
	assert.Contains(t, schema, "locked_by")
}

func TestMigrationTableSchema_ValidSQL(t *testing.T) {
	schema := MigrationTableSchema()
	// Basic SQL structure sanity checks.
	upper := strings.ToUpper(schema)
	assert.Contains(t, upper, "PRIMARY KEY")
	assert.Contains(t, upper, "UNIQUE")
}

func TestMigrationLockTableSchema_HasConstraint(t *testing.T) {
	schema := MigrationLockTableSchema()
	assert.Contains(t, schema, "single_lock", "lock table should have a single_lock constraint")
}
