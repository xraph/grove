package extension

import (
	"context"
	"fmt"
	"sync"

	"github.com/xraph/grove/kv/driver"
)

var (
	registryMu sync.RWMutex
	registry   = map[string]func() driver.Driver{}
)

// RegisterDriver registers a KV driver factory under the given name.
// Driver sub-modules can call this in their init() function:
//
//	func init() { forgeext.RegisterDriver("redis", func() driver.Driver { return New() }) }
func RegisterDriver(name string, factory func() driver.Driver) {
	registryMu.Lock()
	defer registryMu.Unlock()
	registry[name] = factory
}

// OpenDriver creates a new driver instance by name and opens it with the given DSN.
func OpenDriver(ctx context.Context, name, dsn string) (driver.Driver, error) {
	registryMu.RLock()
	factory, ok := registry[name]
	registryMu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("kv: unknown driver %q; register it with forgeext.RegisterDriver", name)
	}

	drv := factory()
	if err := drv.Open(ctx, dsn); err != nil {
		return nil, fmt.Errorf("kv: open driver %q: %w", name, err)
	}

	return drv, nil
}
