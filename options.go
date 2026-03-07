package grove

import (
	"time"

	log "github.com/xraph/go-utils/log"
)

// Option configures a DB instance.
type Option func(*options)

type options struct {
	poolSize     int
	queryTimeout time.Duration
	logger       log.Logger
}

func defaultOptions() *options {
	return &options{
		poolSize:     10,
		queryTimeout: 30 * time.Second,
	}
}

func (o *options) apply(opts []Option) {
	for _, opt := range opts {
		opt(o)
	}
}

// WithPoolSize sets the maximum number of connections in the pool.
// Default: 10.
func WithPoolSize(n int) Option {
	return func(o *options) {
		if n > 0 {
			o.poolSize = n
		}
	}
}

// WithQueryTimeout sets the default timeout for query execution.
// Default: 30s.
func WithQueryTimeout(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.queryTimeout = d
		}
	}
}

// WithLogger sets a structured logger for the DB instance.
func WithLogger(l log.Logger) Option {
	return func(o *options) {
		o.logger = l
	}
}
