package mongodriver

import (
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/xraph/grove/driver"
)

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

	// MaxPoolSize overrides the cap when positive. Zero leaves the URI or
	// Grove default in place, so an unset application config cannot uncap it.
	MaxPoolSize uint64

	// MinPoolSize keeps a baseline of pooled connections warm so chatty
	// workloads (dispatch, polling) don't pay reconnect cost on every burst.
	// The default is zero.
	MinPoolSize *uint64

	// MaxConnIdleTime is the maximum time a connection can sit idle in the
	// pool before being closed. An explicit zero disables idle expiry.
	MaxConnIdleTime *time.Duration

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

// WithMaxPoolSize caps connections per server (default 10).
// Zero leaves the URI setting or Grove default in place.
func WithMaxPoolSize(n uint64) MongoOption {
	return func(o *mongoOptions) {
		o.MaxPoolSize = n
	}
}

// WithMinPoolSize keeps n connections warm in the pool. Use for polling
// workloads (e.g. dispatch dequeue loops) to avoid socket churn.
func WithMinPoolSize(n uint64) MongoOption {
	return func(o *mongoOptions) {
		o.MinPoolSize = &n
	}
}

// WithMaxConnIdleTime sets how long an idle connection lives (default one minute).
// Zero disables idle expiry.
func WithMaxConnIdleTime(d time.Duration) MongoOption {
	return func(o *mongoOptions) {
		o.MaxConnIdleTime = &d
	}
}

// WithMaxConnecting caps concurrent connection establishment.
func WithMaxConnecting(n uint64) MongoOption {
	return func(o *mongoOptions) {
		o.MaxConnecting = n
	}
}

// clientOptions layers pool defaults, URI settings, then explicit options.
func (o *mongoOptions) clientOptions(uri string) *options.ClientOptions {
	defaults := driver.DefaultDriverOptions()
	maxPoolSize := uint64(defaults.PoolSize) //nolint:gosec // DefaultDriverOptions always supplies a positive pool size.
	clientOpts := options.Client().
		SetMaxPoolSize(maxPoolSize).
		SetMaxConnIdleTime(defaults.MaxConnIdleTime).
		ApplyURI(uri)
	if o.ServerSelectionTimeout > 0 {
		clientOpts.SetServerSelectionTimeout(o.ServerSelectionTimeout)
	}
	if o.ConnectTimeout > 0 {
		clientOpts.SetConnectTimeout(o.ConnectTimeout)
	}
	if o.Timeout > 0 {
		clientOpts.SetTimeout(o.Timeout)
	}
	if o.MaxPoolSize > 0 {
		clientOpts.SetMaxPoolSize(o.MaxPoolSize)
	}
	if o.MinPoolSize != nil {
		clientOpts.SetMinPoolSize(*o.MinPoolSize)
	}
	if o.MaxConnIdleTime != nil {
		clientOpts.SetMaxConnIdleTime(*o.MaxConnIdleTime)
	}
	if o.MaxConnecting > 0 {
		clientOpts.SetMaxConnecting(o.MaxConnecting)
	}
	return clientOpts
}
