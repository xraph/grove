package pgmigrate

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestIgnoreConcurrentCreate(t *testing.T) {
	dupType := &pgconn.PgError{Code: "23505", Message: `duplicate key value violates unique constraint "pg_type_typname_nsp_index"`}

	tests := []struct {
		name    string
		err     error
		wantNil bool
	}{
		{"nil", nil, true},
		{"typed 23505", dupType, true},
		{"wrapped typed 23505", fmt.Errorf("pgdriver: exec: %w", dupType), true},
		{"string 23505 fallback", errors.New(`ERROR: duplicate key ... (SQLSTATE 23505)`), true},
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
