package grovetest

import (
	"context"
	"fmt"
	"testing"

	"github.com/xraph/grove/driver"
)

func TestMockDriver_RecordQueries(t *testing.T) {
	d := NewMockDriver()
	ctx := context.Background()

	// Exec
	_, err := d.Exec(ctx, "INSERT INTO users (name) VALUES ($1)", "Alice")
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}

	// Query
	_, err = d.Query(ctx, "SELECT * FROM users WHERE id = $1", 1)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}

	// QueryRow
	d.QueryRow(ctx, "SELECT name FROM users WHERE id = $1", 2)

	queries := d.Queries()
	if len(queries) != 3 {
		t.Fatalf("expected 3 queries, got %d", len(queries))
	}

	// Check Exec
	if queries[0].Method != "Exec" {
		t.Errorf("query[0] method: got %s, want Exec", queries[0].Method)
	}
	if queries[0].Query != "INSERT INTO users (name) VALUES ($1)" {
		t.Errorf("query[0] query mismatch: %s", queries[0].Query)
	}

	// Check Query
	if queries[1].Method != "Query" {
		t.Errorf("query[1] method: got %s, want Query", queries[1].Method)
	}

	// Check QueryRow
	if queries[2].Method != "QueryRow" {
		t.Errorf("query[2] method: got %s, want QueryRow", queries[2].Method)
	}
}

func TestMockDriver_Reset(t *testing.T) {
	d := NewMockDriver()
	ctx := context.Background()

	_, _ = d.Exec(ctx, "INSERT INTO users (name) VALUES ($1)", "Alice")
	_, _ = d.Query(ctx, "SELECT * FROM users")

	if len(d.Queries()) != 2 {
		t.Fatalf("expected 2 queries before reset, got %d", len(d.Queries()))
	}

	d.Reset()

	if len(d.Queries()) != 0 {
		t.Errorf("expected 0 queries after reset, got %d", len(d.Queries()))
	}
}

func TestMockDriver_LastQuery(t *testing.T) {
	d := NewMockDriver()
	ctx := context.Background()

	// No queries yet
	if d.LastQuery() != nil {
		t.Error("expected nil LastQuery with no queries")
	}

	_, _ = d.Exec(ctx, "INSERT INTO users (name) VALUES ($1)", "Alice")
	_, _ = d.Query(ctx, "SELECT * FROM users WHERE active = $1", true)

	last := d.LastQuery()
	if last == nil {
		t.Fatal("expected non-nil LastQuery")
	}
	if last.Query != "SELECT * FROM users WHERE active = $1" {
		t.Errorf("last query: got %s, want SELECT * FROM users WHERE active = $1", last.Query)
	}
	if last.Method != "Query" {
		t.Errorf("last method: got %s, want Query", last.Method)
	}
}

func TestMockDriver_CustomExecFunc(t *testing.T) {
	d := NewMockDriver()
	ctx := context.Background()

	customResult := &MockResult{rowsAffected: 42, lastInsertID: 7}
	d.ExecFunc = func(_ context.Context, _ string, _ ...any) (driver.Result, error) {
		return customResult, nil
	}

	result, err := d.Exec(ctx, "UPDATE users SET name = $1 WHERE id = $2", "Bob", 1)
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}

	rows, _ := result.RowsAffected()
	if rows != 42 {
		t.Errorf("RowsAffected: got %d, want 42", rows)
	}

	id, _ := result.LastInsertId()
	if id != 7 {
		t.Errorf("LastInsertId: got %d, want 7", id)
	}

	// Verify the query was still recorded
	if len(d.Queries()) != 1 {
		t.Errorf("expected 1 recorded query, got %d", len(d.Queries()))
	}
}

func TestMockDriver_CustomExecFunc_Error(t *testing.T) {
	d := NewMockDriver()
	ctx := context.Background()

	d.ExecFunc = func(_ context.Context, _ string, _ ...any) (driver.Result, error) {
		return nil, fmt.Errorf("exec failed")
	}

	_, err := d.Exec(ctx, "INSERT INTO bad_table VALUES ($1)", "x")
	if err == nil {
		t.Fatal("expected error from custom ExecFunc")
	}
	if err.Error() != "exec failed" {
		t.Errorf("error: got %q, want %q", err.Error(), "exec failed")
	}
}

func TestAssertQueryContains(t *testing.T) {
	d := NewMockDriver()
	ctx := context.Background()

	_, _ = d.Exec(ctx, "INSERT INTO users (name) VALUES ($1)", "Alice")
	_, _ = d.Query(ctx, "SELECT * FROM posts WHERE author_id = $1", 1)

	// This should pass without error on a real *testing.T.
	// We test it by running it against a sub-test.
	t.Run("contains_match", func(t *testing.T) {
		AssertQueryContains(t, d, "INSERT INTO users")
	})

	t.Run("contains_partial", func(t *testing.T) {
		AssertQueryContains(t, d, "posts")
	})
}

func TestAssertQueryCount(t *testing.T) {
	d := NewMockDriver()
	ctx := context.Background()

	_, _ = d.Exec(ctx, "INSERT INTO users (name) VALUES ($1)", "Alice")
	_, _ = d.Query(ctx, "SELECT * FROM users")

	t.Run("correct_count", func(t *testing.T) {
		AssertQueryCount(t, d, 2)
	})
}

func TestMockDriver_BeginTx(t *testing.T) {
	d := NewMockDriver()
	ctx := context.Background()

	tx, err := d.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}

	_, err = tx.Exec(ctx, "INSERT INTO users (name) VALUES ($1)", "Alice")
	if err != nil {
		t.Fatalf("tx.Exec: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("tx.Commit: %v", err)
	}

	// Verify the query was recorded through the driver
	if len(d.Queries()) != 1 {
		t.Errorf("expected 1 query after tx, got %d", len(d.Queries()))
	}
}

func TestMockDriver_OpenPingClose(t *testing.T) {
	d := NewMockDriver()
	ctx := context.Background()

	if err := d.Open(ctx, "mock://test"); err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := d.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestMockDriver_Name(t *testing.T) {
	d := NewMockDriver()
	if d.Name() != "mock" {
		t.Errorf("Name: got %q, want %q", d.Name(), "mock")
	}
}

func TestMockDriver_SupportsReturning(t *testing.T) {
	d := NewMockDriver()
	if !d.SupportsReturning() {
		t.Error("SupportsReturning: got false, want true")
	}
}

func TestMockDialect_Name(t *testing.T) {
	d := NewMockDriver()
	if d.Dialect().Name() != "mock" {
		t.Errorf("Dialect.Name: got %q, want %q", d.Dialect().Name(), "mock")
	}
}

func TestAssertNoQueries(t *testing.T) {
	d := NewMockDriver()

	t.Run("no_queries", func(t *testing.T) {
		AssertNoQueries(t, d)
	})
}

func TestAssertLastQuery(t *testing.T) {
	d := NewMockDriver()
	ctx := context.Background()

	_, _ = d.Exec(ctx, "SELECT 1")

	t.Run("match", func(t *testing.T) {
		AssertLastQuery(t, d, "SELECT 1")
	})
}

func TestAssertLastArgs(t *testing.T) {
	d := NewMockDriver()
	ctx := context.Background()

	_, _ = d.Exec(ctx, "INSERT INTO users (name, age) VALUES ($1, $2)", "Alice", 30)

	t.Run("match", func(t *testing.T) {
		AssertLastArgs(t, d, "Alice", 30)
	})
}
