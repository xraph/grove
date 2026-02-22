package scan

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/xraph/grove"
	"github.com/xraph/grove/schema"
)

// ---------- Mock implementations ----------

// mockRow implements the Row interface for testing.
type mockRow struct {
	values []any
	err    error
}

func (r *mockRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) != len(r.values) {
		return fmt.Errorf("mockRow: expected %d destinations, got %d", len(r.values), len(dest))
	}
	for i, src := range r.values {
		if err := assignValue(dest[i], src); err != nil {
			return err
		}
	}
	return nil
}

// mockRows implements the Rows interface for testing.
type mockRows struct {
	columns []string
	data    [][]any
	index   int
	closed  bool
	err     error
}

func (r *mockRows) Next() bool {
	if r.closed || r.index >= len(r.data) {
		return false
	}
	r.index++
	return true
}

func (r *mockRows) Scan(dest ...any) error {
	if r.closed {
		return fmt.Errorf("mockRows: rows are closed")
	}
	if r.index <= 0 || r.index > len(r.data) {
		return fmt.Errorf("mockRows: no current row")
	}
	row := r.data[r.index-1]
	if len(dest) != len(row) {
		return fmt.Errorf("mockRows: expected %d destinations, got %d", len(row), len(dest))
	}
	for i, src := range row {
		if err := assignValue(dest[i], src); err != nil {
			return err
		}
	}
	return nil
}

func (r *mockRows) Columns() ([]string, error) {
	return r.columns, nil
}

func (r *mockRows) Close() error {
	r.closed = true
	return nil
}

func (r *mockRows) Err() error {
	return r.err
}

// assignValue copies src into the pointer dest, mimicking database/sql Scan behavior.
func assignValue(dest any, src any) error {
	if src == nil {
		return nil
	}

	switch d := dest.(type) {
	case *int64:
		switch s := src.(type) {
		case int64:
			*d = s
		case int:
			*d = int64(s)
		default:
			return fmt.Errorf("assignValue: cannot assign %T to *int64", src)
		}
	case *string:
		s, ok := src.(string)
		if !ok {
			return fmt.Errorf("assignValue: cannot assign %T to *string", src)
		}
		*d = s
	case *float64:
		switch s := src.(type) {
		case float64:
			*d = s
		case int:
			*d = float64(s)
		default:
			return fmt.Errorf("assignValue: cannot assign %T to *float64", src)
		}
	case *bool:
		s, ok := src.(bool)
		if !ok {
			return fmt.Errorf("assignValue: cannot assign %T to *bool", src)
		}
		*d = s
	case **time.Time:
		switch s := src.(type) {
		case time.Time:
			*d = &s
		case *time.Time:
			*d = s
		default:
			return fmt.Errorf("assignValue: cannot assign %T to **time.Time", src)
		}
	case *time.Time:
		switch s := src.(type) {
		case time.Time:
			*d = s
		default:
			return fmt.Errorf("assignValue: cannot assign %T to *time.Time", src)
		}
	case *any:
		*d = src
	default:
		return fmt.Errorf("assignValue: unsupported dest type %T", dest)
	}
	return nil
}

// ---------- Test model structs ----------

type ScanUser struct {
	grove.BaseModel `grove:"table:users,alias:u"`

	ID    int64  `grove:"id,pk,autoincrement"`
	Name  string `grove:"name,notnull"`
	Email string `grove:"email,notnull,unique"`
}

type ScanUserWithNil struct {
	grove.BaseModel `grove:"table:users_with_nil"`

	ID        int64      `grove:"id,pk,autoincrement"`
	Name      string     `grove:"name,notnull"`
	DeletedAt *time.Time `grove:"deleted_at,soft_delete"`
}

// ---------- ScanRow tests ----------

func TestScanRow_SimpleStruct(t *testing.T) {
	table, err := schema.NewTable((*ScanUser)(nil))
	if err != nil {
		t.Fatalf("NewTable failed: %v", err)
	}

	row := &mockRow{
		values: []any{int64(1), "Alice", "alice@example.com"},
	}

	var user ScanUser
	if err := ScanRow(row, &user, table); err != nil {
		t.Fatalf("ScanRow failed: %v", err)
	}

	if user.ID != 1 {
		t.Errorf("ID = %d, want 1", user.ID)
	}
	if user.Name != "Alice" {
		t.Errorf("Name = %q, want %q", user.Name, "Alice")
	}
	if user.Email != "alice@example.com" {
		t.Errorf("Email = %q, want %q", user.Email, "alice@example.com")
	}
}

func TestScanRow_NilFields(t *testing.T) {
	table, err := schema.NewTable((*ScanUserWithNil)(nil))
	if err != nil {
		t.Fatalf("NewTable failed: %v", err)
	}

	row := &mockRow{
		values: []any{int64(42), "Bob", nil},
	}

	var user ScanUserWithNil
	if err := ScanRow(row, &user, table); err != nil {
		t.Fatalf("ScanRow failed: %v", err)
	}

	if user.ID != 42 {
		t.Errorf("ID = %d, want 42", user.ID)
	}
	if user.Name != "Bob" {
		t.Errorf("Name = %q, want %q", user.Name, "Bob")
	}
	if user.DeletedAt != nil {
		t.Errorf("DeletedAt = %v, want nil", user.DeletedAt)
	}
}

func TestScanRow_WithTimeValue(t *testing.T) {
	table, err := schema.NewTable((*ScanUserWithNil)(nil))
	if err != nil {
		t.Fatalf("NewTable failed: %v", err)
	}

	now := time.Now().Truncate(time.Second)
	row := &mockRow{
		values: []any{int64(7), "Charlie", now},
	}

	var user ScanUserWithNil
	if err := ScanRow(row, &user, table); err != nil {
		t.Fatalf("ScanRow failed: %v", err)
	}

	if user.DeletedAt == nil {
		t.Fatal("DeletedAt is nil, want non-nil")
	}
	if !user.DeletedAt.Equal(now) {
		t.Errorf("DeletedAt = %v, want %v", user.DeletedAt, now)
	}
}

func TestScanRow_InvalidDest(t *testing.T) {
	table, err := schema.NewTable((*ScanUser)(nil))
	if err != nil {
		t.Fatalf("NewTable failed: %v", err)
	}

	row := &mockRow{
		values: []any{int64(1), "Alice", "alice@example.com"},
	}

	// Pass non-pointer.
	var user ScanUser
	if err := ScanRow(row, user, table); err == nil {
		t.Fatal("expected error for non-pointer dest, got nil")
	}

	// Pass nil pointer.
	var nilPtr *ScanUser
	if err := ScanRow(row, nilPtr, table); err == nil {
		t.Fatal("expected error for nil pointer dest, got nil")
	}

	// Pass pointer to non-struct.
	var s string
	if err := ScanRow(row, &s, table); err == nil {
		t.Fatal("expected error for pointer to non-struct dest, got nil")
	}
}

func TestScanRow_RowError(t *testing.T) {
	table, err := schema.NewTable((*ScanUser)(nil))
	if err != nil {
		t.Fatalf("NewTable failed: %v", err)
	}

	scanErr := errors.New("database connection lost")
	row := &mockRow{
		err: scanErr,
	}

	var user ScanUser
	if err := ScanRow(row, &user, table); err == nil {
		t.Fatal("expected error from row, got nil")
	} else if !errors.Is(err, scanErr) {
		t.Errorf("expected error %v, got %v", scanErr, err)
	}
}

// ---------- ScanRows tests ----------

func TestScanRows_MultipleRows(t *testing.T) {
	table, err := schema.NewTable((*ScanUser)(nil))
	if err != nil {
		t.Fatalf("NewTable failed: %v", err)
	}

	rows := &mockRows{
		columns: []string{"id", "name", "email"},
		data: [][]any{
			{int64(1), "Alice", "alice@example.com"},
			{int64(2), "Bob", "bob@example.com"},
			{int64(3), "Charlie", "charlie@example.com"},
		},
	}

	var users []ScanUser
	if err := ScanRows(rows, &users, table); err != nil {
		t.Fatalf("ScanRows failed: %v", err)
	}

	if len(users) != 3 {
		t.Fatalf("len(users) = %d, want 3", len(users))
	}

	expected := []struct {
		id    int64
		name  string
		email string
	}{
		{1, "Alice", "alice@example.com"},
		{2, "Bob", "bob@example.com"},
		{3, "Charlie", "charlie@example.com"},
	}

	for i, want := range expected {
		got := users[i]
		if got.ID != want.id {
			t.Errorf("users[%d].ID = %d, want %d", i, got.ID, want.id)
		}
		if got.Name != want.name {
			t.Errorf("users[%d].Name = %q, want %q", i, got.Name, want.name)
		}
		if got.Email != want.email {
			t.Errorf("users[%d].Email = %q, want %q", i, got.Email, want.email)
		}
	}
}

func TestScanRows_EmptyResultSet(t *testing.T) {
	table, err := schema.NewTable((*ScanUser)(nil))
	if err != nil {
		t.Fatalf("NewTable failed: %v", err)
	}

	rows := &mockRows{
		columns: []string{"id", "name", "email"},
		data:    [][]any{},
	}

	var users []ScanUser
	if err := ScanRows(rows, &users, table); err != nil {
		t.Fatalf("ScanRows failed: %v", err)
	}

	if len(users) != 0 {
		t.Errorf("len(users) = %d, want 0", len(users))
	}
}

func TestScanRows_PointerSlice(t *testing.T) {
	table, err := schema.NewTable((*ScanUser)(nil))
	if err != nil {
		t.Fatalf("NewTable failed: %v", err)
	}

	rows := &mockRows{
		columns: []string{"id", "name", "email"},
		data: [][]any{
			{int64(10), "Dave", "dave@example.com"},
		},
	}

	var users []*ScanUser
	if err := ScanRows(rows, &users, table); err != nil {
		t.Fatalf("ScanRows failed: %v", err)
	}

	if len(users) != 1 {
		t.Fatalf("len(users) = %d, want 1", len(users))
	}
	if users[0].ID != 10 {
		t.Errorf("users[0].ID = %d, want 10", users[0].ID)
	}
	if users[0].Name != "Dave" {
		t.Errorf("users[0].Name = %q, want %q", users[0].Name, "Dave")
	}
}

func TestScanRows_SubsetOfColumns(t *testing.T) {
	table, err := schema.NewTable((*ScanUser)(nil))
	if err != nil {
		t.Fatalf("NewTable failed: %v", err)
	}

	// Query only returns id and name, not email.
	rows := &mockRows{
		columns: []string{"id", "name"},
		data: [][]any{
			{int64(5), "Eve"},
		},
	}

	var users []ScanUser
	if err := ScanRows(rows, &users, table); err != nil {
		t.Fatalf("ScanRows failed: %v", err)
	}

	if len(users) != 1 {
		t.Fatalf("len(users) = %d, want 1", len(users))
	}
	if users[0].ID != 5 {
		t.Errorf("users[0].ID = %d, want 5", users[0].ID)
	}
	if users[0].Name != "Eve" {
		t.Errorf("users[0].Name = %q, want %q", users[0].Name, "Eve")
	}
	if users[0].Email != "" {
		t.Errorf("users[0].Email = %q, want empty string", users[0].Email)
	}
}

func TestScanRows_ExtraColumns(t *testing.T) {
	table, err := schema.NewTable((*ScanUser)(nil))
	if err != nil {
		t.Fatalf("NewTable failed: %v", err)
	}

	// Query returns an extra column "extra_col" not in the model.
	rows := &mockRows{
		columns: []string{"id", "name", "email", "extra_col"},
		data: [][]any{
			{int64(1), "Alice", "alice@example.com", "ignored"},
		},
	}

	var users []ScanUser
	if err := ScanRows(rows, &users, table); err != nil {
		t.Fatalf("ScanRows failed: %v", err)
	}

	if len(users) != 1 {
		t.Fatalf("len(users) = %d, want 1", len(users))
	}
	if users[0].ID != 1 {
		t.Errorf("ID = %d, want 1", users[0].ID)
	}
}

func TestScanRows_InvalidDest(t *testing.T) {
	table, err := schema.NewTable((*ScanUser)(nil))
	if err != nil {
		t.Fatalf("NewTable failed: %v", err)
	}

	rows := &mockRows{
		columns: []string{"id", "name", "email"},
		data:    [][]any{},
	}

	// Non-pointer dest.
	var users []ScanUser
	if err := ScanRows(rows, users, table); err == nil {
		t.Fatal("expected error for non-pointer dest, got nil")
	}

	// Pointer to non-slice.
	var s string
	if err := ScanRows(rows, &s, table); err == nil {
		t.Fatal("expected error for pointer to non-slice, got nil")
	}
}

func TestScanRows_RowsError(t *testing.T) {
	table, err := schema.NewTable((*ScanUser)(nil))
	if err != nil {
		t.Fatalf("NewTable failed: %v", err)
	}

	iterErr := errors.New("network timeout during iteration")
	rows := &mockRows{
		columns: []string{"id", "name", "email"},
		data:    [][]any{},
		err:     iterErr,
	}

	var users []ScanUser
	if err := ScanRows(rows, &users, table); err == nil {
		t.Fatal("expected error from rows.Err(), got nil")
	}
}
