package badgerdriver

import "github.com/xraph/grove/kv/driver"

// WithInMemory configures the Badger driver to use in-memory storage.
func WithInMemory() driver.Option {
	return func(o *driver.DriverOptions) {
		o.Extra["in_memory"] = true
	}
}
