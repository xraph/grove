package mongodriver

import (
	"context"
	"fmt"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver"
	"go.opentelemetry.io/otel/codes"

	"github.com/xraph/grove/observability"
	"github.com/xraph/grove/observability/telemetrytest"
)

func rawCmd(t *testing.T, d bson.D) bson.Raw {
	t.Helper()
	b, err := bson.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func runCommand(ctx context.Context, m *commandMonitor, conn string, req int64, db, name string, cmd bson.Raw, failure error) {
	mon := m.monitor()
	mon.Started(ctx, &event.CommandStartedEvent{
		Command: cmd, DatabaseName: db, CommandName: name, RequestID: req, ConnectionID: conn,
	})
	finished := event.CommandFinishedEvent{CommandName: name, DatabaseName: db, RequestID: req, ConnectionID: conn}
	if failure != nil {
		mon.Failed(ctx, &event.CommandFailedEvent{CommandFinishedEvent: finished, Failure: failure})
		return
	}
	mon.Succeeded(ctx, &event.CommandSucceededEvent{CommandFinishedEvent: finished})
}

func TestCommandMonitorSpansAndMetrics(t *testing.T) {
	rec := telemetrytest.Spans(t)

	db := New()
	address, port := serverOf([]string{"mongo-0.internal:27017", "mongo-1.internal:27017"})
	db.tel.SetConnection("app", address, port)
	m := telemetrytest.NewMetrics()
	db.SetMetrics(m)
	mon := newCommandMonitor(db.tel)

	ctx, parent := telemetrytest.Parent(t.Context())
	runCommand(ctx, mon, "c1", 1, "app", "find",
		rawCmd(t, bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "email", Value: "alice@example.com"}}}}), nil)
	runCommand(ctx, mon, "c1", 2, "app", "getMore",
		rawCmd(t, bson.D{{Key: "getMore", Value: int64(42)}, {Key: "collection", Value: "users"}}), nil)
	runCommand(ctx, mon, "c1", 3, "app", "aggregate",
		rawCmd(t, bson.D{{Key: "aggregate", Value: 1}, {Key: "pipeline", Value: bson.A{}}}), nil)
	runCommand(ctx, mon, "c2", 3, "audit", "insert",
		rawCmd(t, bson.D{{Key: "insert", Value: "events"}, {Key: "documents", Value: bson.A{}}}),
		fmt.Errorf("wrapped: %w", driver.Error{Code: 11000, Message: "E11000 duplicate key error"}))
	parent.End()

	spans := telemetrytest.Ended(rec)
	got := make([]string, 0, len(spans))
	for _, s := range spans {
		got = append(got, s.Name())
		if s.Parent().SpanID() != parent.SpanContext().SpanID() {
			t.Errorf("%s is not under the caller's span", s.Name())
		}
		attrs := telemetrytest.AttrMap(s)
		if attrs["db.system.name"] != "mongodb" || attrs["server.address"] != "mongo-0.internal" || attrs["server.port"] != "27017" {
			t.Errorf("%s: attributes %v", s.Name(), attrs)
		}
		for k, v := range attrs {
			if v == "alice@example.com" || v == "42" {
				t.Errorf("%s: %s carries a value from the command", s.Name(), k)
			}
		}
	}
	if fmt.Sprint(got) != "[find users getMore users aggregate insert events]" {
		t.Fatalf("spans = %v", got)
	}

	insert := spans[3]
	if insert.Status().Code != codes.Error {
		t.Errorf("failed insert status = %v", insert.Status())
	}
	attrs := telemetrytest.AttrMap(insert)
	if attrs["db.namespace"] != "audit" || attrs["error.type"] != "11000" || attrs["db.response.status_code"] != "11000" {
		t.Errorf("failed insert attributes = %v", attrs)
	}

	series := m.Series(observability.OperationDurationMetric)
	if len(series) != 4 {
		t.Fatalf("got %d series, want 4", len(series))
	}
	for _, s := range series {
		if s.Labels["db.system.name"] != "mongodb" || s.Count != 1 {
			t.Errorf("series %v count %d", s.Labels, s.Count)
		}
	}
}

func TestCommandMonitorUnmatchedEventsAreIgnored(t *testing.T) {
	db := New()
	m := telemetrytest.NewMetrics()
	db.SetMetrics(m)
	mon := newCommandMonitor(db.tel).monitor()

	// A finish with no matching start (a monitor attached mid-flight, say)
	// must not panic or record.
	mon.Succeeded(t.Context(), &event.CommandSucceededEvent{
		CommandFinishedEvent: event.CommandFinishedEvent{CommandName: "find", RequestID: 9, ConnectionID: "x"},
	})
	if n := len(m.Series(observability.OperationDurationMetric)); n != 0 {
		t.Fatalf("recorded %d series for an unmatched finish", n)
	}
}

func TestCollectionOf(t *testing.T) {
	long := make([]byte, maxCollectionLen+1)
	for i := range long {
		long[i] = 'a'
	}
	tests := []struct {
		name string
		cmd  bson.D
		want string
	}{
		{"find", bson.D{{Key: "find", Value: "users"}}, "users"},
		{"getMore", bson.D{{Key: "getMore", Value: int64(1)}, {Key: "collection", Value: "jobs"}}, "jobs"},
		{"aggregate", bson.D{{Key: "aggregate", Value: 1}}, ""},
		{"ping", bson.D{{Key: "ping", Value: 1}}, ""},
		{"find", bson.D{{Key: "find", Value: string(long)}}, ""},
	}
	for _, tc := range tests {
		if got := collectionOf(tc.name, rawCmd(t, tc.cmd)); got != tc.want {
			t.Errorf("collectionOf(%s, %v) = %q, want %q", tc.name, tc.cmd, got, tc.want)
		}
	}
	if got := collectionOf("find", nil); got != "" {
		t.Errorf("empty command gave %q", got)
	}
}

func TestServerOf(t *testing.T) {
	if h, p := serverOf(nil); h != "" || p != 0 {
		t.Errorf("no hosts = (%q, %d)", h, p)
	}
	if h, p := serverOf([]string{"cluster0.example.mongodb.net"}); h != "cluster0.example.mongodb.net" || p != 0 {
		t.Errorf("srv host = (%q, %d)", h, p)
	}
}
