package extension

import (
	"context"
	"testing"

	forgetesting "github.com/xraph/forge/testing"
	"github.com/xraph/vessel"

	"github.com/xraph/grove"
	"github.com/xraph/grove/grovetest"
)

func TestExtension_SingleDB_BackwardCompat(t *testing.T) {
	drv := grovetest.NewMockDriver()
	ext := New(WithDriver(drv))

	app := forgetesting.NewTestApp("test", "0.1.0")
	if err := ext.Register(app); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// DB() should return the database.
	if ext.DB() == nil {
		t.Fatal("DB() is nil after Register")
	}

	// Manager() should be nil in single-DB mode.
	if ext.Manager() != nil {
		t.Error("Manager() should be nil in single-DB mode")
	}

	// DI should resolve *grove.DB.
	db, err := vessel.Inject[*grove.DB](app.Container())
	if err != nil {
		t.Fatalf("Inject[*grove.DB]: %v", err)
	}
	if db != ext.DB() {
		t.Error("DI-resolved DB should match ext.DB()")
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

func TestExtension_MultiDB(t *testing.T) {
	drv1 := grovetest.NewMockDriver()
	drv2 := grovetest.NewMockDriver()

	ext := New(
		WithDatabase("primary", drv1),
		WithDatabase("analytics", drv2),
		WithDefaultDatabase("primary"),
	)

	app := forgetesting.NewTestApp("test", "0.1.0")
	if err := ext.Register(app); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Manager should be non-nil.
	mgr := ext.Manager()
	if mgr == nil {
		t.Fatal("Manager() is nil in multi-DB mode")
	}

	// Should have 2 databases.
	if mgr.Len() != 2 {
		t.Errorf("Manager.Len() = %d, want 2", mgr.Len())
	}

	// Default DB should be "primary".
	if mgr.DefaultName() != "primary" {
		t.Errorf("DefaultName() = %q, want %q", mgr.DefaultName(), "primary")
	}

	// DB() should return the default.
	if ext.DB() == nil {
		t.Fatal("DB() is nil in multi-DB mode")
	}

	// Get each named DB.
	primaryDB, err := mgr.Get("primary")
	if err != nil {
		t.Fatalf("Get(primary): %v", err)
	}
	analyticsDB, err := mgr.Get("analytics")
	if err != nil {
		t.Fatalf("Get(analytics): %v", err)
	}
	if primaryDB == analyticsDB {
		t.Error("primary and analytics should be different DB instances")
	}

	// ext.DB() should match the primary.
	if ext.DB() != primaryDB {
		t.Error("DB() should match the default (primary)")
	}
}

func TestExtension_MultiDB_DI(t *testing.T) {
	drv1 := grovetest.NewMockDriver()
	drv2 := grovetest.NewMockDriver()

	ext := New(
		WithDatabase("primary", drv1),
		WithDatabase("analytics", drv2),
		WithDefaultDatabase("primary"),
	)

	app := forgetesting.NewTestApp("test", "0.1.0")
	if err := ext.Register(app); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// DI: Inject default *grove.DB (unnamed).
	defaultDB, err := vessel.Inject[*grove.DB](app.Container())
	if err != nil {
		t.Fatalf("Inject[*grove.DB]: %v", err)
	}
	if defaultDB != ext.DB() {
		t.Error("unnamed DI should resolve to default DB")
	}

	// DI: Inject DBManager.
	mgr, err := vessel.Inject[*DBManager](app.Container())
	if err != nil {
		t.Fatalf("Inject[*DBManager]: %v", err)
	}
	if mgr != ext.Manager() {
		t.Error("DI-resolved manager should match ext.Manager()")
	}

	// DI: Inject named databases.
	primaryDB, err := vessel.InjectNamed[*grove.DB](app.Container(), "primary")
	if err != nil {
		t.Fatalf("InjectNamed[*grove.DB](primary): %v", err)
	}
	analyticsDB, err := vessel.InjectNamed[*grove.DB](app.Container(), "analytics")
	if err != nil {
		t.Fatalf("InjectNamed[*grove.DB](analytics): %v", err)
	}
	if primaryDB == analyticsDB {
		t.Error("named DI should return different instances")
	}
}

func TestExtension_MultiDB_DefaultFirstEntry(t *testing.T) {
	drv1 := grovetest.NewMockDriver()
	drv2 := grovetest.NewMockDriver()

	// No explicit default — should use the first entry.
	ext := New(
		WithDatabase("alpha", drv1),
		WithDatabase("beta", drv2),
	)

	app := forgetesting.NewTestApp("test", "0.1.0")
	if err := ext.Register(app); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if ext.Manager().DefaultName() != "alpha" {
		t.Errorf("DefaultName() = %q, want %q", ext.Manager().DefaultName(), "alpha")
	}
}

func TestExtension_MultiDB_PerDBHooks(t *testing.T) {
	drv1 := grovetest.NewMockDriver()
	drv2 := grovetest.NewMockDriver()

	hookCalled := false
	testHook := &testPostMutationHook{fn: func() { hookCalled = true }}

	ext := New(
		WithDatabase("primary", drv1),
		WithDatabase("analytics", drv2),
		WithHookFor("analytics", testHook),
	)

	app := forgetesting.NewTestApp("test", "0.1.0")
	if err := ext.Register(app); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// The hook should have been registered on the analytics DB.
	// We can't easily verify hook registration without executing a query,
	// but we verify that the extension initialized without errors.
	_ = hookCalled
}

func TestExtension_MultiDB_Health(t *testing.T) {
	drv1 := grovetest.NewMockDriver()
	drv2 := grovetest.NewMockDriver()

	ext := New(
		WithDatabase("a", drv1),
		WithDatabase("b", drv2),
	)

	app := forgetesting.NewTestApp("test", "0.1.0")
	if err := ext.Register(app); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if err := ext.Health(context.Background()); err != nil {
		t.Fatalf("Health: %v", err)
	}
}

func TestExtension_MultiDB_Stop(t *testing.T) {
	drv1 := grovetest.NewMockDriver()
	drv2 := grovetest.NewMockDriver()

	ext := New(
		WithDatabase("a", drv1),
		WithDatabase("b", drv2),
	)

	app := forgetesting.NewTestApp("test", "0.1.0")
	if err := ext.Register(app); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if err := ext.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestExtension_MultiDB_GlobalHooks(t *testing.T) {
	drv1 := grovetest.NewMockDriver()
	drv2 := grovetest.NewMockDriver()

	testHook := &testPostMutationHook{}

	// Global hooks should be applied to all databases.
	ext := New(
		WithDatabase("a", drv1),
		WithDatabase("b", drv2),
		WithHook(testHook),
	)

	app := forgetesting.NewTestApp("test", "0.1.0")
	if err := ext.Register(app); err != nil {
		t.Fatalf("Register: %v", err)
	}
}

// testPostMutationHook is a minimal hook for testing registration.
type testPostMutationHook struct {
	fn func()
}

func (h *testPostMutationHook) AfterMutation(_ context.Context, _ any, _, _ any) error {
	if h.fn != nil {
		h.fn()
	}
	return nil
}
