package pgdriver_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/xraph/grove/drivers/pgdriver"
)

// startPostgres launches a disposable postgres container and returns its DSN.
// Skips when -short is set or no container runtime is available.
func startPostgres(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping container-backed integration test in -short mode")
	}

	ctx := context.Background()
	ctr, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("grove"),
		tcpostgres.WithUsername("grove"),
		tcpostgres.WithPassword("grove"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}
	t.Cleanup(func() {
		require.NoError(t, testcontainers.TerminateContainer(ctr))
	})

	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	return dsn
}

func openPgDB(t *testing.T, dsn string) *pgdriver.PgDB {
	t.Helper()
	db := pgdriver.New()
	require.NoError(t, db.Open(context.Background(), dsn))
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db
}

func waitForNotification(t *testing.T, ch <-chan *pgdriver.Notification) *pgdriver.Notification {
	t.Helper()
	select {
	case n := <-ch:
		return n
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for notification")
		return nil
	}
}

// TestListener_ListenAfterStart is the regression test for the LISTEN /
// WaitForNotification race: Start parks the dedicated connection in
// WaitForNotification, and a subsequent Listen must still be able to
// register the channel and receive notifications.
func TestListener_ListenAfterStart(t *testing.T) {
	dsn := startPostgres(t)
	db := openPgDB(t, dsn)
	ctx := context.Background()

	l := db.NewListener()
	t.Cleanup(func() { require.NoError(t, l.Close()) })

	require.NoError(t, l.Start(ctx))

	// Give the listen goroutine time to park in WaitForNotification so
	// Listen below hits the historical "conn busy" interleaving.
	time.Sleep(200 * time.Millisecond)

	received := make(chan *pgdriver.Notification, 4)
	l.OnNotification("orders", func(n *pgdriver.Notification) {
		received <- n
	})
	require.NoError(t, l.Listen(ctx, "orders"))

	// External NOTIFY: sent over a pool connection, not the dedicated
	// listener connection.
	_, err := db.Exec(ctx, "SELECT pg_notify('orders', 'hello')")
	require.NoError(t, err)

	n := waitForNotification(t, received)
	require.Equal(t, "orders", n.Channel)
	require.Equal(t, "hello", n.Payload)
}

// TestListener_MultipleChannels subscribes to additional channels while the
// listener is already waiting, interleaved with deliveries.
func TestListener_MultipleChannels(t *testing.T) {
	dsn := startPostgres(t)
	db := openPgDB(t, dsn)
	ctx := context.Background()

	l := db.NewListener()
	t.Cleanup(func() { require.NoError(t, l.Close()) })

	require.NoError(t, l.Start(ctx))

	chanA := make(chan *pgdriver.Notification, 4)
	chanB := make(chan *pgdriver.Notification, 4)
	l.OnNotification("alpha", func(n *pgdriver.Notification) { chanA <- n })
	l.OnNotification("beta", func(n *pgdriver.Notification) { chanB <- n })

	require.NoError(t, l.Listen(ctx, "alpha"))
	require.NoError(t, l.Notify(ctx, "alpha", "a1"))
	require.Equal(t, "a1", waitForNotification(t, chanA).Payload)

	// Subscribe to a second channel while the goroutine is parked again.
	require.NoError(t, l.Listen(ctx, "beta"))
	require.NoError(t, l.Notify(ctx, "beta", "b1"))
	require.Equal(t, "b1", waitForNotification(t, chanB).Payload)

	require.NoError(t, l.Notify(ctx, "alpha", "a2"))
	require.Equal(t, "a2", waitForNotification(t, chanA).Payload)
}

// TestListener_Unlisten verifies UNLISTEN executes against the parked
// connection and stops delivery for that channel.
func TestListener_Unlisten(t *testing.T) {
	dsn := startPostgres(t)
	db := openPgDB(t, dsn)
	ctx := context.Background()

	l := db.NewListener()
	t.Cleanup(func() { require.NoError(t, l.Close()) })

	require.NoError(t, l.Start(ctx))

	gone := make(chan *pgdriver.Notification, 4)
	still := make(chan *pgdriver.Notification, 4)
	l.OnNotification("gone", func(n *pgdriver.Notification) { gone <- n })
	l.OnNotification("still", func(n *pgdriver.Notification) { still <- n })
	require.NoError(t, l.Listen(ctx, "gone"))
	require.NoError(t, l.Listen(ctx, "still"))

	require.NoError(t, l.Unlisten(ctx, "gone"))

	// Notifications on the same connection arrive in send order, so
	// receiving "still" proves the earlier "gone" notify was dropped.
	require.NoError(t, l.Notify(ctx, "gone", "dropped"))
	require.NoError(t, l.Notify(ctx, "still", "kept"))

	require.Equal(t, "kept", waitForNotification(t, still).Payload)
	select {
	case n := <-gone:
		t.Fatalf("received notification on unlistened channel: %+v", n)
	default:
	}
}

// TestPgDB_ListenConvenience exercises the db.Listen helper, which performs
// Start, OnNotification, and Listen in sequence.
func TestPgDB_ListenConvenience(t *testing.T) {
	dsn := startPostgres(t)
	db := openPgDB(t, dsn)
	ctx := context.Background()

	received := make(chan *pgdriver.Notification, 4)
	l, err := db.Listen(ctx, "events", func(n *pgdriver.Notification) {
		received <- n
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, l.Close()) })

	require.NoError(t, l.Notify(ctx, "events", "payload"))
	n := waitForNotification(t, received)
	require.Equal(t, "events", n.Channel)
	require.Equal(t, "payload", n.Payload)
}

// TestListener_DoneSignalsExit verifies Done is closed once the listen
// goroutine exits after Close, so consumers (e.g. wake listeners) can detect
// listener death and rebuild it.
func TestListener_DoneSignalsExit(t *testing.T) {
	dsn := startPostgres(t)
	db := openPgDB(t, dsn)
	ctx := context.Background()

	l := db.NewListener()
	require.NoError(t, l.Start(ctx))
	require.NoError(t, l.Listen(ctx, "done_test"))

	select {
	case <-l.Done():
		t.Fatal("Done closed while listener is running")
	default:
	}

	require.NoError(t, l.Close())

	select {
	case <-l.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("Done not closed after Close")
	}
}

// TestListener_ListenBeforeStart preserves the contract that Listen and
// Unlisten require Start to have been called. No container needed.
func TestListener_ListenBeforeStart(t *testing.T) {
	db := pgdriver.New()
	l := db.NewListener()

	require.Error(t, l.Listen(context.Background(), "orders"))
	require.Error(t, l.Unlisten(context.Background(), "orders"))
}
