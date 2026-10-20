package grove

import (
	"time"

	log "github.com/xraph/go-utils/log"
	gometrics "github.com/xraph/go-utils/metrics"
)

// Option configures a DB instance.
type Option func(*options)

type options struct {
	poolSize        int
	queryTimeout    time.Duration
	logger          log.Logger
	metrics         gometrics.Metrics
	collectionLabel func(string) string
	collectionLimit int
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

// WithMetrics records the OpenTelemetry db.client.operation.duration histogram
// on m for every call the driver makes. In a Forge app pass app.Metrics(), and
// the series lands on /_/metrics as db_client_operation_duration_seconds.
// grove's Forge extension does this for you.
//
// Spans need no option: drivers start them on the global tracer provider,
// which forge.New installs. A nil m records nothing.
func WithMetrics(m gometrics.Metrics) Option {
	return func(o *options) {
		o.metrics = m
	}
}

// WithCollectionLabel sets how a collection becomes the db.collection.name
// label on db.client.operation.duration. fn gets the collection as the driver
// reported it, never empty: for SQL the first table as written, schema
// included (tenant_42.orders), for MongoDB and Elasticsearch the collection or
// index. It returns the label value, "" for no collection, or "other" to fold
// it in with the rest. Values still count toward WithCollectionLabelLimit. fn
// runs on every recorded call and must be safe for concurrent use.
//
// Without it, a SQL table loses its schema or database qualifier and other
// names are used as reported. Spans always carry the collection as reported.
// It has no effect without WithMetrics.
func WithCollectionLabel(fn func(collection string) string) Option {
	return func(o *options) {
		o.collectionLabel = fn
	}
}

// WithCollectionLabelLimit caps the distinct db.collection.name values the
// driver puts on db.client.operation.duration. Past n, a collection it hasn't
// seen records as "other", and the ones it has keep their own label. Default:
// 256 (observability.DefaultCollectionLabelLimit). It has no effect without
// WithMetrics.
func WithCollectionLabelLimit(n int) Option {
	return func(o *options) {
		if n > 0 {
			o.collectionLimit = n
		}
	}
}
