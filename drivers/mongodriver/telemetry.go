package mongodriver

import (
	"context"
	"errors"
	"net"
	"strconv"
	"sync"

	gometrics "github.com/xraph/go-utils/metrics"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver"

	"github.com/xraph/grove/observability"
)

// maxCollectionLen bounds the collection name we'll report, the same way the
// SQL summariser bounds table names.
const maxCollectionLen = 128

// newTelemetry builds the db.client telemetry for one MongoDB. A server error
// reports its code (11000 for a duplicate key) as db.response.status_code and
// error.type.
func newTelemetry() *observability.Telemetry {
	return observability.NewTelemetry("mongodb", observability.WithStatusCode(serverCode))
}

func serverCode(err error) string {
	var de driver.Error
	if errors.As(err, &de) && de.Code != 0 {
		return strconv.Itoa(int(de.Code))
	}
	var ce mongo.CommandError
	if errors.As(err, &ce) && ce.Code != 0 {
		return strconv.Itoa(int(ce.Code))
	}
	return ""
}

// SetMetrics records the db.client.operation.duration histogram on m.
// grove.Open calls it when you pass grove.WithMetrics.
func (db *MongoDB) SetMetrics(m gometrics.Metrics) {
	db.tel.SetMetrics(m)
}

// commandMonitor turns the Go driver's command events into grove operations:
// one span and one duration per command sent to the server. That covers
// every find, insert, update, delete, aggregate and getMore, whether it came
// from a grove builder or from the *mongo.Client you got from Client().
type commandMonitor struct {
	tel *observability.Telemetry
	ops sync.Map // opKey -> *observability.Op
}

type opKey struct {
	conn string
	req  int64
}

func newCommandMonitor(tel *observability.Telemetry) *commandMonitor {
	return &commandMonitor{tel: tel}
}

func (m *commandMonitor) monitor() *event.CommandMonitor {
	return &event.CommandMonitor{
		Started:   m.started,
		Succeeded: m.succeeded,
		Failed:    m.failed,
	}
}

func (m *commandMonitor) started(ctx context.Context, e *event.CommandStartedEvent) {
	_, op := m.tel.StartIn(ctx, e.DatabaseName, e.CommandName, collectionOf(e.CommandName, e.Command))
	m.ops.Store(opKey{conn: e.ConnectionID, req: e.RequestID}, op)
}

func (m *commandMonitor) succeeded(_ context.Context, e *event.CommandSucceededEvent) {
	m.end(e.ConnectionID, e.RequestID, nil)
}

func (m *commandMonitor) failed(_ context.Context, e *event.CommandFailedEvent) {
	m.end(e.ConnectionID, e.RequestID, e.Failure)
}

func (m *commandMonitor) end(conn string, req int64, err error) {
	v, ok := m.ops.LoadAndDelete(opKey{conn: conn, req: req})
	if !ok {
		return
	}
	if op, ok := v.(*observability.Op); ok {
		op.End(err)
	}
}

// collectionOf reads the collection a command works on. Most commands name it
// as the value of their first field ({find: "users"}); getMore carries it in
// a collection field. A database-level command ({aggregate: 1}, {ping: 1})
// has none.
func collectionOf(name string, cmd bson.Raw) string {
	if name == "getMore" {
		if v, ok := cmd.Lookup("collection").StringValueOK(); ok && len(v) <= maxCollectionLen {
			return v
		}
		return ""
	}

	elems, err := cmd.Elements()
	if err != nil || len(elems) == 0 {
		return ""
	}
	v, ok := elems[0].Value().StringValueOK()
	if !ok || len(v) > maxCollectionLen {
		return ""
	}
	return v
}

// serverOf reads the first host of a connection string for server.address and
// server.port. A replica set reports its first seed.
func serverOf(hosts []string) (address string, port int) {
	if len(hosts) == 0 {
		return "", 0
	}
	host, portStr, err := net.SplitHostPort(hosts[0])
	if err != nil {
		return hosts[0], 0
	}
	port, _ = strconv.Atoi(portStr) //nolint:errcheck // a bad port is reported as none
	return host, port
}
