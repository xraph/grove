package grove_test

import (
	"context"
	"testing"

	"github.com/xraph/grove"
	"github.com/xraph/grove/hook"
)

// hookableDriver is a fake GroveDriver that also implements the optional
// SetHooks surface drivers expose for lifecycle hooks.
type hookableDriver struct {
	engine *hook.Engine
}

func (d *hookableDriver) Name() string                 { return "fake" }
func (d *hookableDriver) Close() error                 { return nil }
func (d *hookableDriver) Ping(context.Context) error   { return nil }
func (d *hookableDriver) SetHooks(engine *hook.Engine) { d.engine = engine }

// plainDriver implements only the mandatory GroveDriver surface.
type plainDriver struct{}

func (plainDriver) Name() string               { return "plain" }
func (plainDriver) Close() error               { return nil }
func (plainDriver) Ping(context.Context) error { return nil }

func TestOpen_PropagatesHookEngineToHookableDrivers(t *testing.T) {
	drv := &hookableDriver{}
	db, err := grove.Open(drv)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if drv.engine == nil {
		t.Fatal("Open must propagate its hook engine to drivers that accept one; " +
			"otherwise db.Hooks() registrations silently never fire")
	}
	if drv.engine != db.Hooks() {
		t.Fatal("driver received a different engine than db.Hooks()")
	}
}

func TestOpen_PlainDriversStillWork(t *testing.T) {
	if _, err := grove.Open(plainDriver{}); err != nil {
		t.Fatalf("Open with a non-hookable driver: %v", err)
	}
}
