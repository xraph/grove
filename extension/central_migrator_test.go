package extension

import (
	"testing"

	forgetesting "github.com/xraph/forge/testing"
	"github.com/xraph/vessel"

	"github.com/xraph/grove/grovetest"
)

// TestCentralMigrations_ResolvableAsForgeCentralMigrator verifies that Forge
// can find the shared registry through forge.CentralMigrator, which is how its
// migrate commands look it up. Vessel matches exact types, so providing only
// *MigrationRegistry is not enough. A second extension reaching for the
// registry on the same container must reuse it without erroring.
func TestCentralMigrations_ResolvableAsForgeCentralMigrator(t *testing.T) {
	app := forgetesting.NewTestApp("test", "0.1.0")

	e := New(WithDriver(grovetest.NewMockDriver()), WithCentralMigrations())
	if err := e.Register(app); err != nil {
		t.Fatalf("Register: %v", err)
	}

	cm, ok := app.CentralMigrator()
	if !ok {
		t.Fatal("app.CentralMigrator(): not found; registry was not provided as forge.CentralMigrator")
	}

	reg, err := vessel.Inject[*MigrationRegistry](app.Container())
	if err != nil {
		t.Fatalf("Inject[*MigrationRegistry]: %v", err)
	}
	got, isReg := cm.(*MigrationRegistry)
	if !isReg {
		t.Fatalf("CentralMigrator is %T, want *MigrationRegistry", cm)
	}
	if got != reg {
		t.Fatal("CentralMigrator and *MigrationRegistry resolve to different instances")
	}

	if again := New(WithCentralMigrations()).registryFromContainer(app.Container()); again != reg {
		t.Fatalf("second registryFromContainer = %p, want the shared registry %p", again, reg)
	}
}

// TestCentralMigrations_OffProvidesNoCentralMigrator verifies that apps which
// do not opt into central migrations keep their old container contents.
func TestCentralMigrations_OffProvidesNoCentralMigrator(t *testing.T) {
	app := forgetesting.NewTestApp("test", "0.1.0")

	e := New(WithDriver(grovetest.NewMockDriver()))
	if err := e.Register(app); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if _, ok := app.CentralMigrator(); ok {
		t.Fatal("app.CentralMigrator() found a migrator without WithCentralMigrations")
	}
	if vessel.HasType[*MigrationRegistry](app.Container()) {
		t.Fatal("*MigrationRegistry provided without WithCentralMigrations")
	}
}
