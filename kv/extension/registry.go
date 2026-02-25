package extension

import (
	"context"

	"github.com/xraph/grove/kv"
	"github.com/xraph/grove/kv/driver"
)

// RegisterDriver registers a KV driver factory under the given name.
// This delegates to kv.RegisterDriver for backward compatibility.
// Driver sub-modules should prefer calling kv.RegisterDriver directly
// in their init() function.
func RegisterDriver(name string, factory func() driver.Driver) {
	kv.RegisterDriver(name, factory)
}

// OpenDriver creates a new driver instance by name and opens it with the given DSN.
// This delegates to kv.OpenDriver for backward compatibility.
func OpenDriver(ctx context.Context, name, dsn string) (driver.Driver, error) {
	return kv.OpenDriver(ctx, name, dsn)
}
