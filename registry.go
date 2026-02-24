package grove

import (
	"context"
	"fmt"
	"sync"
)

// DriverFactory is a function that creates and opens a GroveDriver given a DSN.
// Drivers register factories via RegisterDriver so that callers can create
// drivers by name (e.g., from YAML configuration) without importing driver
// packages directly.
//
// Each driver module should register its factory in an init() function or
// provide an explicit Register() function.
type DriverFactory func(ctx context.Context, dsn string) (GroveDriver, error)

var (
	driversMu sync.RWMutex
	driverReg = make(map[string]DriverFactory)
)

// RegisterDriver registers a named driver factory. It is typically called
// from a driver package's init() function. Subsequent calls with the same
// name overwrite the previous registration.
//
// Example (in pgdriver package):
//
//	func init() {
//	    grove.RegisterDriver("postgres", func(ctx context.Context, dsn string) (grove.GroveDriver, error) {
//	        db := New()
//	        if err := db.Open(ctx, dsn); err != nil {
//	            return nil, err
//	        }
//	        return db, nil
//	    })
//	}
func RegisterDriver(name string, factory DriverFactory) {
	driversMu.Lock()
	defer driversMu.Unlock()

	driverReg[name] = factory
}

// OpenDriver creates and opens a driver by its registered name.
// Returns an error if no factory is registered for the given name.
func OpenDriver(ctx context.Context, name, dsn string) (GroveDriver, error) {
	driversMu.RLock()
	factory, ok := driverReg[name]
	driversMu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("grove: unknown driver %q; import the driver package to register it (e.g., _ \"github.com/xraph/grove/drivers/pgdriver\")", name)
	}

	return factory(ctx, dsn)
}

// Drivers returns the names of all registered drivers.
func Drivers() []string {
	driversMu.RLock()
	defer driversMu.RUnlock()

	names := make([]string, 0, len(driverReg))
	for name := range driverReg {
		names = append(names, name)
	}
	return names
}
