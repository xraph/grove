package extension

import (
	"testing"

	"github.com/xraph/grove/kv"
	"github.com/xraph/grove/kv/kvtest"
)

func openMockStore(t *testing.T) *kv.Store {
	t.Helper()
	drv := kvtest.NewMockDriver()
	s, err := kv.Open(drv)
	if err != nil {
		t.Fatalf("open mock store: %v", err)
	}
	return s
}

func TestStoreManager_AddGet(t *testing.T) {
	m := NewStoreManager()
	s1 := openMockStore(t)
	s2 := openMockStore(t)

	m.Add("cache", s1)
	m.Add("sessions", s2)

	got, err := m.Get("cache")
	if err != nil {
		t.Fatalf("Get(cache): %v", err)
	}
	if got != s1 {
		t.Error("Get(cache) returned wrong store")
	}

	got, err = m.Get("sessions")
	if err != nil {
		t.Fatalf("Get(sessions): %v", err)
	}
	if got != s2 {
		t.Error("Get(sessions) returned wrong store")
	}
}

func TestStoreManager_GetNotFound(t *testing.T) {
	m := NewStoreManager()
	_, err := m.Get("missing")
	if err == nil {
		t.Fatal("expected error for missing store")
	}
}

func TestStoreManager_DefaultFirstAdded(t *testing.T) {
	m := NewStoreManager()
	s1 := openMockStore(t)
	s2 := openMockStore(t)

	m.Add("first", s1)
	m.Add("second", s2)

	got, err := m.Default()
	if err != nil {
		t.Fatalf("Default(): %v", err)
	}
	if got != s1 {
		t.Error("Default() should return first added store")
	}
	if m.DefaultName() != "first" {
		t.Errorf("DefaultName() = %q, want %q", m.DefaultName(), "first")
	}
}

func TestStoreManager_SetDefault(t *testing.T) {
	m := NewStoreManager()
	s1 := openMockStore(t)
	s2 := openMockStore(t)

	m.Add("a", s1)
	m.Add("b", s2)

	if err := m.SetDefault("b"); err != nil {
		t.Fatalf("SetDefault(b): %v", err)
	}

	got, err := m.Default()
	if err != nil {
		t.Fatalf("Default(): %v", err)
	}
	if got != s2 {
		t.Error("Default() should return s2 after SetDefault")
	}
}

func TestStoreManager_SetDefaultNotFound(t *testing.T) {
	m := NewStoreManager()
	if err := m.SetDefault("missing"); err == nil {
		t.Fatal("expected error for setting default to missing store")
	}
}

func TestStoreManager_DefaultEmpty(t *testing.T) {
	m := NewStoreManager()
	_, err := m.Default()
	if err == nil {
		t.Fatal("expected error for empty manager default")
	}
}

func TestStoreManager_All(t *testing.T) {
	m := NewStoreManager()
	s1 := openMockStore(t)
	s2 := openMockStore(t)

	m.Add("a", s1)
	m.Add("b", s2)

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

func TestStoreManager_Len(t *testing.T) {
	m := NewStoreManager()
	if m.Len() != 0 {
		t.Errorf("Len() = %d, want 0", m.Len())
	}

	m.Add("x", openMockStore(t))
	if m.Len() != 1 {
		t.Errorf("Len() = %d, want 1", m.Len())
	}
}

func TestStoreManager_Close(t *testing.T) {
	m := NewStoreManager()
	m.Add("a", openMockStore(t))
	m.Add("b", openMockStore(t))

	if err := m.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}
}
