package pgmigrate

import (
	"errors"
	"fmt"
	"hash/fnv"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestIgnoreConcurrentCreate(t *testing.T) {
	dupType := &pgconn.PgError{Code: "23505", Message: `duplicate key value violates unique constraint "pg_type_typname_nsp_index"`}
	dupTable := &pgconn.PgError{Code: "42P07", Message: `relation "grove_migrations" already exists`}
	dupObject := &pgconn.PgError{Code: "42710", Message: `type "grove_migration_locks" already exists`}

	tests := []struct {
		name    string
		err     error
		wantNil bool
	}{
		{"nil", nil, true},
		{"typed 23505", dupType, true},
		{"wrapped typed 23505", fmt.Errorf("pgdriver: exec: %w", dupType), true},
		{"string 23505 fallback", errors.New(`ERROR: duplicate key ... (SQLSTATE 23505)`), true},
		{"typed 42P07", dupTable, true},
		{"wrapped typed 42P07", fmt.Errorf("pgdriver: exec: %w", dupTable), true},
		{"string 42P07 fallback", errors.New(`ERROR: relation "grove_migrations" already exists (SQLSTATE 42P07)`), true},
		{"typed 42710", dupObject, true},
		{"string 42710 fallback", errors.New(`ERROR: type "grove_migration_locks" already exists (SQLSTATE 42710)`), true},
		{"other pg error passes through", &pgconn.PgError{Code: "42501", Message: "permission denied"}, false},
		{"unrelated error passes through", errors.New("connection refused"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ignoreConcurrentCreate(tt.err)
			if tt.wantNil && got != nil {
				t.Fatalf("ignoreConcurrentCreate(%v) = %v; want nil", tt.err, got)
			}
			if !tt.wantNil && got == nil {
				t.Fatalf("ignoreConcurrentCreate(%v) = nil; want the error to pass through", tt.err)
			}
		})
	}
}

// TestBootstrapLockKey pins bootstrapLockKey to its documented derivation and
// keeps it off the migration lock's key 1.
func TestBootstrapLockKey(t *testing.T) {
	h := fnv.New64a()
	_, _ = h.Write([]byte("github.com/xraph/grove/drivers/pgdriver/pgmigrate.bootstrap"))
	if want := int64(h.Sum64()); bootstrapLockKey != want {
		t.Fatalf("bootstrapLockKey = %d, want %d (FNV-1a 64 of the bootstrap symbol)", bootstrapLockKey, want)
	}
	if bootstrapLockKey == 1 {
		t.Fatal("bootstrapLockKey collides with the migration lock's key 1")
	}
}
