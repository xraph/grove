package memcacheddriver

import (
	"time"

	"github.com/xraph/grove/kv/driver"
)

// WithTimeout sets the socket read/write timeout for Memcached operations.
func WithTimeout(d time.Duration) driver.Option {
	return func(o *driver.DriverOptions) {
		o.ReadTimeout = d
		o.WriteTimeout = d
	}
}

// WithMaxIdleConns sets the maximum number of idle connections per server.
func WithMaxIdleConns(n int) driver.Option {
	return func(o *driver.DriverOptions) {
		o.Extra["max_idle_conns"] = n
	}
}
