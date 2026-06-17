package pgmigrate

import (
	"context"
	"testing"

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

// newTestExecutor creates a *pgmigrate.Executor backed by a live postgres
// container. It skips if -short is set or no container runtime is available.
func newTestExecutor(t *testing.T) *Executor {
	t.Helper()
	dsn := startPostgres(t)
	db := pgdriver.New()
	require.NoError(t, db.Open(context.Background(), dsn))
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return New(db)
}

func TestExecutor_LockInfo_ReportsHolder(t *testing.T) {
	exec := newTestExecutor(t)
	ctx := context.Background()
	if err := exec.EnsureLockTable(ctx); err != nil {
		t.Fatal(err)
	}
	if err := exec.AcquireLock(ctx, "host:123"); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = exec.ReleaseLock(ctx) }()

	info, err := exec.LockInfo(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !info.Held || info.LockedBy != "host:123" {
		t.Fatalf("expected held by host:123, got %+v", info)
	}
}
