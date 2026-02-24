package extension

import (
	"testing"

	"github.com/xraph/grove"
	"github.com/xraph/grove/grovetest"
)

func openMockDB(t *testing.T) *grove.DB {
	t.Helper()
	drv := grovetest.NewMockDriver()
	db, err := grove.Open(drv)
	if err != nil {
		t.Fatalf("open mock db: %v", err)
	}
	return db
}

func TestDBManager_AddGet(t *testing.T) {
	m := NewDBManager()
	db1 := openMockDB(t)
	db2 := openMockDB(t)

	m.Add("primary", db1)
	m.Add("analytics", db2)

	got, err := m.Get("primary")
	if err != nil {
		t.Fatalf("Get(primary): %v", err)
	}
	if got != db1 {
		t.Error("Get(primary) returned wrong DB")
	}

	got, err = m.Get("analytics")
	if err != nil {
		t.Fatalf("Get(analytics): %v", err)
	}
	if got != db2 {
		t.Error("Get(analytics) returned wrong DB")
	}
}

func TestDBManager_GetNotFound(t *testing.T) {
	m := NewDBManager()
	_, err := m.Get("missing")
	if err == nil {
		t.Fatal("expected error for missing database")
	}
}

func TestDBManager_DefaultFirstAdded(t *testing.T) {
	m := NewDBManager()
	db1 := openMockDB(t)
	db2 := openMockDB(t)

	m.Add("first", db1)
	m.Add("second", db2)

	got, err := m.Default()
	if err != nil {
		t.Fatalf("Default(): %v", err)
	}
	if got != db1 {
		t.Error("Default() should return first added DB")
	}
	if m.DefaultName() != "first" {
		t.Errorf("DefaultName() = %q, want %q", m.DefaultName(), "first")
	}
}

func TestDBManager_SetDefault(t *testing.T) {
	m := NewDBManager()
	db1 := openMockDB(t)
	db2 := openMockDB(t)

	m.Add("a", db1)
	m.Add("b", db2)

	if err := m.SetDefault("b"); err != nil {
		t.Fatalf("SetDefault(b): %v", err)
	}

	got, err := m.Default()
	if err != nil {
		t.Fatalf("Default(): %v", err)
	}
	if got != db2 {
		t.Error("Default() should return db2 after SetDefault")
	}
}

func TestDBManager_SetDefaultNotFound(t *testing.T) {
	m := NewDBManager()
	if err := m.SetDefault("missing"); err == nil {
		t.Fatal("expected error for setting default to missing database")
	}
}

func TestDBManager_DefaultEmpty(t *testing.T) {
	m := NewDBManager()
	_, err := m.Default()
	if err == nil {
		t.Fatal("expected error for empty manager default")
	}
}

func TestDBManager_All(t *testing.T) {
	m := NewDBManager()
	db1 := openMockDB(t)
	db2 := openMockDB(t)

	m.Add("a", db1)
	m.Add("b", db2)

	all := m.All()
	if len(all) != 2 {
		t.Fatalf("All() len = %d, want 2", len(all))
	}

	// Verify it's a copy — modifying the returned map shouldn't affect the manager.
	delete(all, "a")
	if m.Len() != 2 {
		t.Error("All() should return a copy, not the internal map")
	}
}

func TestDBManager_Len(t *testing.T) {
	m := NewDBManager()
	if m.Len() != 0 {
		t.Errorf("Len() = %d, want 0", m.Len())
	}

	m.Add("x", openMockDB(t))
	if m.Len() != 1 {
		t.Errorf("Len() = %d, want 1", m.Len())
	}
}

func TestDBManager_Close(t *testing.T) {
	m := NewDBManager()
	m.Add("a", openMockDB(t))
	m.Add("b", openMockDB(t))

	if err := m.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}
}
