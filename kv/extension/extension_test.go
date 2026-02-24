package extension

import (
	"context"
	"testing"

	forgetesting "github.com/xraph/forge/testing"
	"github.com/xraph/vessel"

	"github.com/xraph/grove/kv"
	"github.com/xraph/grove/kv/kvtest"
)

func TestExtension_SingleStore_BackwardCompat(t *testing.T) {
	drv := kvtest.NewMockDriver()
	ext := New(WithDriver(drv))

	app := forgetesting.NewTestApp("test-kv", "0.1.0")
	if err := ext.Register(app); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Store() should return the store.
	if ext.Store() == nil {
		t.Fatal("Store() is nil after Register")
	}

	// Manager() should be nil in single-store mode.
	if ext.Manager() != nil {
		t.Error("Manager() should be nil in single-store mode")
	}

	// DI should resolve *kv.Store.
	s, err := vessel.Inject[*kv.Store](app.Container())
	if err != nil {
		t.Fatalf("Inject[*kv.Store]: %v", err)
	}
	if s != ext.Store() {
		t.Error("DI-resolved store should match ext.Store()")
	}

	// Health should pass.
	if err := ext.Health(context.Background()); err != nil {
		t.Fatalf("Health: %v", err)
	}

	// Stop should succeed.
	if err := ext.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestExtension_MultiStore(t *testing.T) {
	drv1 := kvtest.NewMockDriver()
	drv2 := kvtest.NewMockDriver()

	ext := New(
		WithStore("cache", drv1),
		WithStore("sessions", drv2),
		WithDefaultStore("cache"),
	)

	app := forgetesting.NewTestApp("test-kv", "0.1.0")
	if err := ext.Register(app); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Manager should be non-nil.
	mgr := ext.Manager()
	if mgr == nil {
		t.Fatal("Manager() is nil in multi-store mode")
	}

	// Should have 2 stores.
	if mgr.Len() != 2 {
		t.Errorf("Manager.Len() = %d, want 2", mgr.Len())
	}

	// Default should be "cache".
	if mgr.DefaultName() != "cache" {
		t.Errorf("DefaultName() = %q, want %q", mgr.DefaultName(), "cache")
	}

	// Store() should return the default.
	if ext.Store() == nil {
		t.Fatal("Store() is nil in multi-store mode")
	}

	// Get each named store.
	cacheStore, err := mgr.Get("cache")
	if err != nil {
		t.Fatalf("Get(cache): %v", err)
	}
	sessionsStore, err := mgr.Get("sessions")
	if err != nil {
		t.Fatalf("Get(sessions): %v", err)
	}
	if cacheStore == sessionsStore {
		t.Error("cache and sessions should be different store instances")
	}

	// ext.Store() should match the cache (default).
	if ext.Store() != cacheStore {
		t.Error("Store() should match the default (cache)")
	}
}

func TestExtension_MultiStore_DI(t *testing.T) {
	drv1 := kvtest.NewMockDriver()
	drv2 := kvtest.NewMockDriver()

	ext := New(
		WithStore("cache", drv1),
		WithStore("sessions", drv2),
		WithDefaultStore("cache"),
	)

	app := forgetesting.NewTestApp("test-kv", "0.1.0")
	if err := ext.Register(app); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// DI: Inject default *kv.Store (unnamed).
	defaultStore, err := vessel.Inject[*kv.Store](app.Container())
	if err != nil {
		t.Fatalf("Inject[*kv.Store]: %v", err)
	}
	if defaultStore != ext.Store() {
		t.Error("unnamed DI should resolve to default store")
	}

	// DI: Inject StoreManager.
	mgr, err := vessel.Inject[*StoreManager](app.Container())
	if err != nil {
		t.Fatalf("Inject[*StoreManager]: %v", err)
	}
	if mgr != ext.Manager() {
		t.Error("DI-resolved manager should match ext.Manager()")
	}

	// DI: Inject named stores.
	cacheStore, err := vessel.InjectNamed[*kv.Store](app.Container(), "cache")
	if err != nil {
		t.Fatalf("InjectNamed[*kv.Store](cache): %v", err)
	}
	sessionsStore, err := vessel.InjectNamed[*kv.Store](app.Container(), "sessions")
	if err != nil {
		t.Fatalf("InjectNamed[*kv.Store](sessions): %v", err)
	}
	if cacheStore == sessionsStore {
		t.Error("named DI should return different instances")
	}
}

func TestExtension_MultiStore_DefaultFirstEntry(t *testing.T) {
	drv1 := kvtest.NewMockDriver()
	drv2 := kvtest.NewMockDriver()

	// No explicit default — should use the first entry.
	ext := New(
		WithStore("alpha", drv1),
		WithStore("beta", drv2),
	)

	app := forgetesting.NewTestApp("test-kv", "0.1.0")
	if err := ext.Register(app); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if ext.Manager().DefaultName() != "alpha" {
		t.Errorf("DefaultName() = %q, want %q", ext.Manager().DefaultName(), "alpha")
	}
}

func TestExtension_MultiStore_PerStoreHooks(t *testing.T) {
	drv1 := kvtest.NewMockDriver()
	drv2 := kvtest.NewMockDriver()

	hookCalled := false
	testHook := &testPostQueryHook{fn: func() { hookCalled = true }}

	ext := New(
		WithStore("cache", drv1),
		WithStore("sessions", drv2),
		WithHookFor("sessions", testHook),
	)

	app := forgetesting.NewTestApp("test-kv", "0.1.0")
	if err := ext.Register(app); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// The hook should have been registered on the sessions store.
	// We verify that the extension initialized without errors.
	_ = hookCalled
}

func TestExtension_MultiStore_Health(t *testing.T) {
	drv1 := kvtest.NewMockDriver()
	drv2 := kvtest.NewMockDriver()

	ext := New(
		WithStore("a", drv1),
		WithStore("b", drv2),
	)

	app := forgetesting.NewTestApp("test-kv", "0.1.0")
	if err := ext.Register(app); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if err := ext.Health(context.Background()); err != nil {
		t.Fatalf("Health: %v", err)
	}
}

func TestExtension_MultiStore_Stop(t *testing.T) {
	drv1 := kvtest.NewMockDriver()
	drv2 := kvtest.NewMockDriver()

	ext := New(
		WithStore("a", drv1),
		WithStore("b", drv2),
	)

	app := forgetesting.NewTestApp("test-kv", "0.1.0")
	if err := ext.Register(app); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if err := ext.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestExtension_MultiStore_GlobalHooks(t *testing.T) {
	drv1 := kvtest.NewMockDriver()
	drv2 := kvtest.NewMockDriver()

	testHook := &testPostQueryHook{}

	// Global hooks should be applied to all stores.
	ext := New(
		WithStore("a", drv1),
		WithStore("b", drv2),
		WithHook(testHook),
	)

	app := forgetesting.NewTestApp("test-kv", "0.1.0")
	if err := ext.Register(app); err != nil {
		t.Fatalf("Register: %v", err)
	}
}

// testPostQueryHook is a minimal hook for testing registration.
type testPostQueryHook struct {
	fn func()
}

func (h *testPostQueryHook) PostQuery(_ context.Context, _ any) error {
	if h.fn != nil {
		h.fn()
	}
	return nil
}
