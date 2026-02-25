package kv

import (
	"context"
	"fmt"
	"sync"

	"github.com/xraph/grove/kv/driver"
)

// DriverFactory is a function that creates a new, unconnected KV driver instance.
// Drivers register factories via RegisterDriver so that callers can create
// drivers by name (e.g., from YAML configuration) without importing driver
// packages directly.
//
// Each driver module should register its factory in an init() function.
type DriverFactory func() driver.Driver

var (
	driversMu sync.RWMutex
	driverReg = make(map[string]DriverFactory)
)

// RegisterDriver registers a named KV driver factory. It is typically called
// from a driver package's init() function. Subsequent calls with the same
// name overwrite the previous registration.
//
// Example (in redisdriver package):
//
//	func init() {
//	    kv.RegisterDriver("redis", func() driver.Driver { return New() })
//	}
func RegisterDriver(name string, factory DriverFactory) {
	driversMu.Lock()
	defer driversMu.Unlock()

	driverReg[name] = factory
}

// OpenDriver creates a new driver instance by its registered name and opens
// it with the given DSN. Returns an error if no factory is registered for the
// given name.
func OpenDriver(ctx context.Context, name, dsn string) (driver.Driver, error) {
	driversMu.RLock()
	factory, ok := driverReg[name]
	driversMu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("kv: unknown driver %q; import the driver package to register it (e.g., _ \"github.com/xraph/grove/kv/drivers/redisdriver\")", name)
	}

	drv := factory()
	if err := drv.Open(ctx, dsn); err != nil {
		return nil, fmt.Errorf("kv: open driver %q: %w", name, err)
	}

	return drv, nil
}

// Drivers returns the names of all registered KV drivers.
func Drivers() []string {
	driversMu.RLock()
	defer driversMu.RUnlock()

	names := make([]string, 0, len(driverReg))
	for name := range driverReg {
		names = append(names, name)
	}
	return names
}
