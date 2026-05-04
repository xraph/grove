package mongodriver

import "time"

// MongoOption configures the MongoDB driver during Open.
type MongoOption func(*mongoOptions)

// mongoOptions holds MongoDB-specific configuration.
type mongoOptions struct {
	// Database overrides the database name from the connection URI.
	Database string

	// ServerSelectionTimeout caps how long the driver waits for a suitable
	// server before failing. Mongo's default is 30s; we lower it so a
	// transient outage doesn't stall app startup.
	ServerSelectionTimeout time.Duration

	// ConnectTimeout is the per-connection TCP dial timeout.
	ConnectTimeout time.Duration

	// Timeout is the per-operation timeout (mongo v2 unified timeout).
	// Zero leaves it unset.
	Timeout time.Duration

	// PingTimeout caps each individual ping attempt.
	PingTimeout time.Duration

	// PingRetries is the number of additional ping attempts after the first
	// failure. Total attempts = 1 + PingRetries.
	PingRetries int

	// PingRetryBackoff is the base backoff between ping attempts.
	// Actual delay grows exponentially (base * 2^attempt) and is capped.
	PingRetryBackoff time.Duration

	// SkipPing skips the initial connectivity check entirely. Use when the
	// caller wants Open to return immediately and rely on Health() instead.
	SkipPing bool

	// MaxPoolSize caps the number of pooled connections. Zero leaves the
	// driver default (100).
	MaxPoolSize uint64

	// MinPoolSize keeps a baseline of pooled connections warm so chatty
	// workloads (dispatch, polling) don't pay reconnect cost on every burst.
	// Zero leaves the driver default (0).
	MinPoolSize uint64

	// MaxConnIdleTime is the maximum time a connection can sit idle in the
	// pool before being closed. Zero leaves the driver default.
	MaxConnIdleTime time.Duration

	// MaxConnecting caps the number of connections being established
	// concurrently. Zero leaves the driver default.
	MaxConnecting uint64
}

// pingRetryMaxBackoff caps the exponential backoff between ping attempts.
const pingRetryMaxBackoff = 5 * time.Second

func defaultMongoOptions() *mongoOptions {
	return &mongoOptions{
		ServerSelectionTimeout: 10 * time.Second,
		ConnectTimeout:         10 * time.Second,
		PingTimeout:            5 * time.Second,
		PingRetries:            3,
		PingRetryBackoff:       500 * time.Millisecond,
	}
}

func (o *mongoOptions) apply(opts []MongoOption) {
	for _, fn := range opts {
		fn(o)
	}
}

// WithDatabase overrides the database name extracted from the connection URI.
// If not set, the database name from the URI is used.
func WithDatabase(name string) MongoOption {
	return func(o *mongoOptions) {
		o.Database = name
	}
}

// WithServerSelectionTimeout overrides the default server-selection timeout.
func WithServerSelectionTimeout(d time.Duration) MongoOption {
	return func(o *mongoOptions) {
		o.ServerSelectionTimeout = d
	}
}

// WithConnectTimeout overrides the default TCP connect timeout.
func WithConnectTimeout(d time.Duration) MongoOption {
	return func(o *mongoOptions) {
		o.ConnectTimeout = d
	}
}

// WithTimeout sets the mongo v2 unified per-operation timeout.
// A zero value leaves the driver default in place.
func WithTimeout(d time.Duration) MongoOption {
	return func(o *mongoOptions) {
		o.Timeout = d
	}
}

// WithPingTimeout overrides the per-attempt ping timeout used during Open.
func WithPingTimeout(d time.Duration) MongoOption {
	return func(o *mongoOptions) {
		o.PingTimeout = d
	}
}

// WithPingRetries sets the number of additional ping attempts after the first
// failure during Open. Use 0 to disable retries.
func WithPingRetries(n int) MongoOption {
	return func(o *mongoOptions) {
		if n < 0 {
			n = 0
		}
		o.PingRetries = n
	}
}

// WithPingRetryBackoff sets the base backoff between ping attempts.
// The effective delay grows exponentially and is capped at 5s.
func WithPingRetryBackoff(d time.Duration) MongoOption {
	return func(o *mongoOptions) {
		o.PingRetryBackoff = d
	}
}

// WithSkipPing disables the initial connectivity check during Open.
// Useful when the caller prefers to defer connectivity verification to Health().
func WithSkipPing(skip bool) MongoOption {
	return func(o *mongoOptions) {
		o.SkipPing = skip
	}
}

// WithMaxPoolSize caps the pooled connection count.
func WithMaxPoolSize(n uint64) MongoOption {
	return func(o *mongoOptions) {
		o.MaxPoolSize = n
	}
}

// WithMinPoolSize keeps n connections warm in the pool. Use for polling
// workloads (e.g. dispatch dequeue loops) to avoid socket churn.
func WithMinPoolSize(n uint64) MongoOption {
	return func(o *mongoOptions) {
		o.MinPoolSize = n
	}
}

// WithMaxConnIdleTime sets how long an idle pooled connection lives.
func WithMaxConnIdleTime(d time.Duration) MongoOption {
	return func(o *mongoOptions) {
		o.MaxConnIdleTime = d
	}
}

// WithMaxConnecting caps concurrent connection establishment.
func WithMaxConnecting(n uint64) MongoOption {
	return func(o *mongoOptions) {
		o.MaxConnecting = n
	}
}
