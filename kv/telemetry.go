package kv

import (
	"context"
	"errors"

	gometrics "github.com/xraph/go-utils/metrics"

	"github.com/xraph/grove/hook"
	"github.com/xraph/grove/observability"
)

// WithMetrics records the OpenTelemetry db.client.operation.duration
// histogram on m for every command the store sends to its driver. In a Forge
// app pass app.Metrics(); grove's KV Forge extension does this for you.
// Spans need no option: they go to the global tracer provider, which
// forge.New installs.
func WithMetrics(m gometrics.Metrics) Option {
	return func(o *options) { o.metrics = m }
}

// newTelemetry builds the store's db.client telemetry. db.system.name comes
// from the driver, mapped to its OTel name where the conventions have one. A
// missing key is an answer, not a failure.
func newTelemetry(drv interface{ Name() string }) *observability.Telemetry {
	return observability.NewTelemetry(systemName(drv.Name()),
		observability.WithBenign(func(err error) bool { return errors.Is(err, ErrNotFound) }))
}

func systemName(driverName string) string {
	switch driverName {
	case "dynamodb":
		return "aws.dynamodb"
	default:
		return driverName // redis, memcached, badger, bbolt
	}
}

// timed runs one driver call under a span named after the command (GET,
// ZADD, EVAL) and records its duration. Keys and values never reach the
// span or the metric.
func timed[T any](ctx context.Context, tel *observability.Telemetry, op hook.Operation, fn func(context.Context) (T, error)) (T, error) {
	ctx, top := tel.Start(ctx, CommandName(op), "")
	v, err := fn(ctx)
	top.End(err)
	return v, err
}

func timedErr(ctx context.Context, tel *observability.Telemetry, op hook.Operation, fn func(context.Context) error) error {
	ctx, top := tel.Start(ctx, CommandName(op), "")
	err := fn(ctx)
	top.End(err)
	return err
}
