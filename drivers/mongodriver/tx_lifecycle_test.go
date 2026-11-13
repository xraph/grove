package mongodriver

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcmongo "github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/xraph/grove/observability/telemetrytest"
)

func TestTransactionTimeoutConfiguration(t *testing.T) {
	require.Equal(t, 30*time.Second, transactionTimeout(nil))
	for _, value := range []time.Duration{0, -1} {
		require.Equal(t, 30*time.Second, transactionTimeout(&value))
	}
	opts := defaultMongoOptions()
	require.Equal(t, 125*time.Millisecond, transactionTimeout(opts.clientOptions("mongodb://localhost/app?timeoutMS=125").Timeout))
	opts.apply([]MongoOption{WithTimeout(75 * time.Millisecond)})
	require.Equal(t, 75*time.Millisecond, transactionTimeout(opts.clientOptions("mongodb://localhost/app?timeoutMS=125").Timeout))
}

func TestTransactionFinalizationReleasesPool(t *testing.T) {
	if testing.Short() {
		t.Skip("requires MongoDB replica set")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	container, err := tcmongo.Run(ctx, "mongo:7", tcmongo.WithReplicaSet("rs"), testcontainers.WithCmdArgs("--setParameter", "enableTestCommands=1"))
	if container != nil {
		t.Cleanup(func() { require.NoError(t, testcontainers.TerminateContainer(container)) })
	}
	if err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}
	uri, err := container.ConnectionString(ctx)
	require.NoError(t, err)
	parsed, err := url.Parse(uri)
	require.NoError(t, err)
	query := parsed.Query()
	query.Set("directConnection", "true")
	parsed.RawQuery = query.Encode()
	db := New()
	require.NoError(t, db.Open(ctx, parsed.String(), WithDatabase("tx_lifecycle"), WithTimeout(250*time.Millisecond), WithMaxPoolSize(1)))
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	_, err = db.Database().Collection("docs").InsertOne(ctx, bson.M{"value": "initial"})
	require.NoError(t, err)
	for _, command := range []string{"commitTransaction", "abortTransaction"} {
		t.Run(command, func(t *testing.T) {
			recorder := telemetrytest.Spans(t)
			traceCtx, parent := telemetrytest.Parent(t.Context())
			defer parent.End()
			txCtx, stop := context.WithCancel(traceCtx)
			defer stop()
			raw, beginErr := db.GroveTx(txCtx, 0, false)
			require.NoError(t, beginErr)
			tx, ok := raw.(*MongoTx)
			require.True(t, ok)
			_, insertErr := db.Database().Collection("docs").InsertOne(tx.SessionContext(txCtx), bson.M{"value": command})
			require.NoError(t, insertErr)
			failpoint := bson.D{{Key: "configureFailPoint", Value: "failCommand"}, {Key: "mode", Value: bson.M{"times": 1}}, {Key: "data", Value: bson.M{"failCommands": bson.A{command}, "blockConnection": true, "blockTimeMS": 1500}}}
			require.NoError(t, db.Client().Database("admin").RunCommand(ctx, failpoint).Err())
			if command == "abortTransaction" {
				stop()
			}
			started := time.Now()
			var finishErr error
			if command == "commitTransaction" {
				finishErr = tx.Commit()
			} else {
				finishErr = tx.Rollback()
			}
			require.ErrorIs(t, finishErr, context.DeadlineExceeded)
			require.Less(t, time.Since(started), time.Second, "finalization ignored its deadline")
			pingCtx, pingCancel := context.WithTimeout(t.Context(), time.Second)
			defer pingCancel()
			require.NoError(t, db.Ping(pingCtx), "finalization retained the only pool slot")
			found := false
			for _, span := range telemetrytest.Ended(recorder) {
				if telemetrytest.AttrMap(span)["db.operation.name"] == command {
					found = true
					require.Equal(t, parent.SpanContext().SpanID(), span.Parent().SpanID())
				}
			}
			require.True(t, found, "transaction command lost telemetry")
		})
	}
	t.Run("canceled commit", func(t *testing.T) {
		txCtx, stop := context.WithCancel(t.Context())
		defer stop()
		raw, beginErr := db.GroveTx(txCtx, 0, false)
		require.NoError(t, beginErr)
		tx, ok := raw.(*MongoTx)
		require.True(t, ok)
		_, insertErr := db.Database().Collection("docs").InsertOne(tx.SessionContext(txCtx), bson.M{"value": "cancel"})
		require.NoError(t, insertErr)
		stop()
		require.ErrorIs(t, tx.Commit(), context.Canceled)
		require.NoError(t, db.Ping(t.Context()))
	})
}
