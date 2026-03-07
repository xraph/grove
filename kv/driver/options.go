package driver

import (
	"crypto/tls"
	"time"

	log "github.com/xraph/go-utils/log"
)

// Option configures driver-level settings.
type Option func(*DriverOptions)

// DriverOptions holds common configuration for KV drivers.
type DriverOptions struct {
	PoolSize     int
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	TLSConfig    *tls.Config
	Logger       log.Logger
	Extra        map[string]any
}

// DefaultDriverOptions returns sensible defaults.
func DefaultDriverOptions() *DriverOptions {
	return &DriverOptions{
		PoolSize:     10,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		Extra:        make(map[string]any),
	}
}

// ApplyOptions creates a DriverOptions by applying all Option funcs to the defaults.
func ApplyOptions(opts []Option) *DriverOptions {
	o := DefaultDriverOptions()
	for _, opt := range opts {
		opt(o)
	}
	return o
}

// WithPoolSize sets the connection pool size.
func WithPoolSize(n int) Option {
	return func(o *DriverOptions) { o.PoolSize = n }
}

// WithDialTimeout sets the connection dial timeout.
func WithDialTimeout(d time.Duration) Option {
	return func(o *DriverOptions) { o.DialTimeout = d }
}

// WithReadTimeout sets the read timeout.
func WithReadTimeout(d time.Duration) Option {
	return func(o *DriverOptions) { o.ReadTimeout = d }
}

// WithWriteTimeout sets the write timeout.
func WithWriteTimeout(d time.Duration) Option {
	return func(o *DriverOptions) { o.WriteTimeout = d }
}

// WithTLSConfig sets the TLS configuration.
func WithTLSConfig(cfg *tls.Config) Option {
	return func(o *DriverOptions) { o.TLSConfig = cfg }
}

// WithLogger sets the logger.
func WithLogger(l log.Logger) Option {
	return func(o *DriverOptions) { o.Logger = l }
}
