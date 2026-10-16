package kv_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/codes"

	"github.com/xraph/grove/kv"
	"github.com/xraph/grove/kv/kvtest"
	"github.com/xraph/grove/observability"
	"github.com/xraph/grove/observability/telemetrytest"
)

var errReadOnly = errors.New("READONLY You can't write against a read only replica")

// redisLike is the mock driver under a real driver name, with publish support
// and a key that always fails to write.
type redisLike struct {
	*kvtest.MockDriver
}

func (redisLike) Name() string { return "redis" }

func (r redisLike) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if key == "readonly" {
		return errReadOnly
	}
	return r.MockDriver.Set(ctx, key, value, ttl)
}

func (redisLike) Publish(context.Context, string, []byte) error         { return nil }
func (redisLike) Subscribe(context.Context, string, func([]byte)) error { return nil }

func TestStoreTelemetry(t *testing.T) {
	rec := telemetrytest.Spans(t)
	m := telemetrytest.NewMetrics()

	store, err := kv.Open(redisLike{kvtest.NewMockDriver()}, kv.WithMetrics(m))
	if err != nil {
		t.Fatal(err)
	}

	ctx, parent := telemetrytest.Parent(t.Context())
	if err := store.Set(ctx, "session:alice", "token-123"); err != nil {
		t.Fatal(err)
	}
	var v string
	if err := store.Get(ctx, "session:alice", &v); err != nil {
		t.Fatal(err)
	}
	if err := store.Get(ctx, "session:missing", &v); !errors.Is(err, kv.ErrNotFound) {
		t.Fatalf("want not found, got %v", err)
	}
	if err := store.Set(ctx, "readonly", "x"); !errors.Is(err, errReadOnly) {
		t.Fatalf("want the driver error, got %v", err)
	}
	if err := store.Publish(ctx, "jobs:wake", []byte("1")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Exists(ctx, "session:alice"); err != nil {
		t.Fatal(err)
	}
	parent.End()

	spans := telemetrytest.Ended(rec)
	names := make([]string, 0, len(spans))
	for _, s := range spans {
		names = append(names, s.Name())
		if s.Parent().SpanID() != parent.SpanContext().SpanID() {
			t.Errorf("%s is not under the caller's span", s.Name())
		}
		attrs := telemetrytest.AttrMap(s)
		if attrs["db.system.name"] != "redis" || attrs["db.operation.name"] != s.Name() {
			t.Errorf("%s: attributes %v", s.Name(), attrs)
		}
		if telemetrytest.HasAttr(s, "db.collection.name") {
			t.Errorf("%s: a key became a collection", s.Name())
		}
		for k, val := range attrs {
			if strings.Contains(val, "alice") || strings.Contains(val, "token") || strings.Contains(val, "jobs:wake") {
				t.Errorf("%s: %s carries a key or value: %q", s.Name(), k, val)
			}
		}
	}
	if got := strings.Join(names, ","); got != "SET,GET,GET,SET,PUBLISH,EXISTS" {
		t.Fatalf("spans = %s", got)
	}

	if s := spans[2]; s.Status().Code == codes.Error {
		t.Error("a missing key was marked failed")
	}
	if s := spans[3]; s.Status().Code != codes.Error {
		t.Errorf("the failed write was not marked: %v", s.Status())
	} else if v, _ := telemetrytest.Attr(s, "error.type"); v != "*errors.errorString" {
		t.Errorf("error.type = %q", v)
	}

	ops := map[string]uint64{}
	for _, s := range m.Series(observability.OperationDurationMetric) {
		ops[strings.TrimSpace(s.Labels["db.operation.name"]+" "+s.Labels["error.type"])] += s.Count
	}
	want := map[string]uint64{"SET": 1, "SET *errors.errorString": 1, "GET": 2, "PUBLISH": 1, "EXISTS": 1}
	if len(ops) != len(want) {
		t.Fatalf("series = %v, want %v", ops, want)
	}
	for k, n := range want {
		if ops[k] != n {
			t.Errorf("series %q = %d, want %d", k, ops[k], n)
		}
	}
}

func TestStoreTelemetryWithoutMetrics(t *testing.T) {
	rec := telemetrytest.Spans(t)
	store, err := kv.Open(kvtest.NewMockDriver())
	if err != nil {
		t.Fatal(err)
	}
	ctx, parent := telemetrytest.Parent(t.Context())
	if err := store.Set(ctx, "k", "v"); err != nil {
		t.Fatal(err)
	}
	parent.End()

	spans := telemetrytest.Ended(rec)
	if len(spans) != 1 || spans[0].Name() != "SET" {
		t.Fatalf("spans = %d", len(spans))
	}
	if v, _ := telemetrytest.Attr(spans[0], "db.system.name"); v != "mock" {
		t.Errorf("db.system.name = %q", v)
	}
}
