package schema

import (
	"sync"
	"testing"
	"time"

	"github.com/xraph/grove"
)

type RegistryUser struct {
	grove.BaseModel `grove:"table:reg_users,alias:ru"`

	ID    int64  `grove:"id,pk,autoincrement"`
	Name  string `grove:"name,notnull"`
	Email string `grove:"email,unique"`
}

type RegistryPost struct {
	grove.BaseModel `grove:"table:reg_posts"`

	ID    int64  `grove:"id,pk"`
	Title string `grove:"title"`
}

func TestRegistryRegister(t *testing.T) {
	reg := NewRegistry()

	table, err := reg.Register((*RegistryUser)(nil))
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	if table.Name != "reg_users" {
		t.Errorf("Name = %q, want %q", table.Name, "reg_users")
	}
	if table.Alias != "ru" {
		t.Errorf("Alias = %q, want %q", table.Alias, "ru")
	}

	// Register again: should return cached.
	table2, err := reg.Register((*RegistryUser)(nil))
	if err != nil {
		t.Fatalf("Register (cached) failed: %v", err)
	}
	if table2 != table {
		t.Error("expected same pointer from cache")
	}
}

func TestRegistryRegisterNil(t *testing.T) {
	reg := NewRegistry()
	_, err := reg.Register(nil)
	if err == nil {
		t.Fatal("expected error for nil model")
	}
}

func TestRegistryRegisterNonStruct(t *testing.T) {
	reg := NewRegistry()
	_, err := reg.Register((*int)(nil))
	if err == nil {
		t.Fatal("expected error for non-struct model")
	}
}

func TestRegistryGet(t *testing.T) {
	reg := NewRegistry()

	// Get before registration returns nil.
	if got := reg.Get((*RegistryUser)(nil)); got != nil {
		t.Error("expected nil for unregistered model")
	}

	// Register, then Get.
	_, err := reg.Register((*RegistryUser)(nil))
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	table := reg.Get((*RegistryUser)(nil))
	if table == nil {
		t.Fatal("expected non-nil table after registration")
	}
	if table.Name != "reg_users" {
		t.Errorf("Name = %q, want %q", table.Name, "reg_users")
	}
}

func TestRegistryGetNil(t *testing.T) {
	reg := NewRegistry()
	if got := reg.Get(nil); got != nil {
		t.Error("expected nil for nil model")
	}
}

func TestRegistryMustGet(t *testing.T) {
	reg := NewRegistry()

	_, err := reg.Register((*RegistryUser)(nil))
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	// Should not panic.
	table := reg.MustGet((*RegistryUser)(nil))
	if table == nil {
		t.Fatal("expected non-nil table")
	}
}

func TestRegistryMustGetPanics(t *testing.T) {
	reg := NewRegistry()

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for unregistered model")
		}
	}()

	reg.MustGet((*RegistryUser)(nil))
}

func TestRegistryConcurrentRegistration(t *testing.T) {
	reg := NewRegistry()

	type ConcUser struct {
		grove.BaseModel `grove:"table:conc_users"`
		ID              int64  `grove:"id,pk"`
		Name            string `grove:"name"`
	}

	var wg sync.WaitGroup
	const goroutines = 50
	results := make([]*Table, goroutines)
	errs := make([]error, goroutines)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx], errs[idx] = reg.Register((*ConcUser)(nil))
		}(i)
	}

	wg.Wait()

	// All should succeed.
	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d error: %v", i, err)
		}
	}

	// All should return the same pointer (due to sync.Map LoadOrStore).
	first := results[0]
	for i, r := range results {
		if r != first {
			t.Errorf("goroutine %d returned different pointer", i)
		}
	}
}

func TestRegistryMultipleModels(t *testing.T) {
	reg := NewRegistry()

	userTable, err := reg.Register((*RegistryUser)(nil))
	if err != nil {
		t.Fatalf("Register RegistryUser failed: %v", err)
	}

	postTable, err := reg.Register((*RegistryPost)(nil))
	if err != nil {
		t.Fatalf("Register RegistryPost failed: %v", err)
	}

	if userTable.Name == postTable.Name {
		t.Error("expected different table names for different models")
	}

	// Get both.
	if reg.Get((*RegistryUser)(nil)) != userTable {
		t.Error("Get RegistryUser mismatch")
	}
	if reg.Get((*RegistryPost)(nil)) != postTable {
		t.Error("Get RegistryPost mismatch")
	}
}

func TestRegistryDifferentPassStyles(t *testing.T) {
	// Ensure passing value, pointer, and nil pointer all resolve to the same type.
	reg := NewRegistry()

	_, err := reg.Register((*RegistryUser)(nil))
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	// Get with value type.
	table := reg.Get(RegistryUser{})
	if table == nil {
		t.Error("expected non-nil when getting with value type")
	}

	// Get with pointer to value.
	table2 := reg.Get(&RegistryUser{})
	if table2 == nil {
		t.Error("expected non-nil when getting with pointer to value")
	}

	if table != table2 {
		t.Error("expected same table regardless of how model is passed")
	}
}

// Ensure unused import of time doesn't cause issues; it's used by models above.
var _ = time.Now
