package pgmigrate

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/xraph/grove/drivers/pgdriver"
	"github.com/xraph/grove/migrate"
)

// liveBootTimeout bounds how long the shared container may take to accept
// connections, and is the timeout of each step of its wait. A cold start on a
// loaded machine has run past the library's 60 second default for a step.
const liveBootTimeout = 4 * time.Minute

// sharedPostgres is the one container every live test borrows. Booting one
// takes 30 to 60 seconds on a busy machine, so each test gets a database of
// its own inside it instead of a container of its own.
var sharedPostgres struct {
	once  sync.Once
	ctr   *tcpostgres.PostgresContainer
	admin *pgdriver.PgDB
	dsn   string // points at the container's default database
	err   error
	dbSeq atomic.Int64
}

// TestMain exists to stop the shared container. It starts lazily, so a run
// that touches no live test never pays for it.
func TestMain(m *testing.M) {
	code := m.Run()
	stopSharedPostgres()
	os.Exit(code)
}

func bootSharedPostgres() {
	ctx, cancel := context.WithTimeout(context.Background(), liveBootTimeout)
	defer cancel()

	ctr, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("grove"),
		tcpostgres.WithUsername("grove"),
		tcpostgres.WithPassword("grove"),
		// The same two steps as tcpostgres.BasicWaitStrategies, with the long
		// timeout on each. The deadline alone only caps the whole wait: every
		// step still gives up after its own 60 second default.
		testcontainers.WithWaitStrategyAndDeadline(liveBootTimeout,
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(liveBootTimeout),
			wait.ForListeningPort("5432/tcp").WithStartupTimeout(liveBootTimeout),
		),
	)
	sharedPostgres.ctr = ctr
	if err != nil {
		sharedPostgres.err = err
		return
	}

	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		sharedPostgres.err = err
		return
	}
	admin := pgdriver.New()
	if err = admin.Open(ctx, dsn); err != nil {
		sharedPostgres.err = err
		return
	}
	sharedPostgres.dsn = dsn
	sharedPostgres.admin = admin
}

func stopSharedPostgres() {
	if sharedPostgres.admin != nil {
		_ = sharedPostgres.admin.Close()
	}
	if sharedPostgres.ctr != nil {
		_ = testcontainers.TerminateContainer(sharedPostgres.ctr)
	}
}

// startPostgres returns the DSN of a new, empty database in the shared
// postgres container. Skips when -short is set or no container runtime is
// available.
func startPostgres(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping container-backed integration test in -short mode")
	}

	sharedPostgres.once.Do(bootSharedPostgres)
	if sharedPostgres.err != nil {
		t.Skipf("container runtime unavailable: %v", sharedPostgres.err)
	}

	ctx := context.Background()
	name := fmt.Sprintf("live_%d", sharedPostgres.dbSeq.Add(1))
	_, err := sharedPostgres.admin.Exec(ctx, "CREATE DATABASE "+name)
	require.NoError(t, err)
	t.Cleanup(func() {
		// FORCE drops the database even if a pooled connection outlived the test.
		_, dropErr := sharedPostgres.admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
		if dropErr != nil {
			t.Logf("drop %s: %v", name, dropErr)
		}
	})

	u, err := url.Parse(sharedPostgres.dsn)
	require.NoError(t, err)
	u.Path = "/" + name
	return u.String()
}

// openLive opens a pool on a fresh database in the shared container. It skips
// like startPostgres does.
func openLive(t *testing.T) *pgdriver.PgDB {
	t.Helper()
	db := pgdriver.New()
	require.NoError(t, db.Open(context.Background(), startPostgres(t)))
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db
}

// newTestExecutor creates a *pgmigrate.Executor backed by a live postgres
// container. It skips if -short is set or no container runtime is available.
func newTestExecutor(t *testing.T) *Executor {
	t.Helper()
	return New(openLive(t))
}

func TestExecutor_LockInfo_NotHeld(t *testing.T) {
	exec := newTestExecutor(t)
	ctx := context.Background()
	if err := exec.EnsureLockTable(ctx); err != nil {
		t.Fatal(err)
	}
	// Do NOT acquire the lock: the lock table has no id=1 row yet.
	info, err := exec.LockInfo(ctx)
	if err != nil {
		t.Fatalf("expected no error for unacquired lock, got: %v", err)
	}
	if info.Held {
		t.Fatalf("expected Held=false for unacquired lock, got %+v", info)
	}
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

// notADriver answers to the name "pg" without being a driver.Driver, the shape
// a mistaken registration or a wrapper that lost its methods would have.
type notADriver struct{}

func (notADriver) Name() string { return "pg" }

func TestRegisteredFactory_rejectsAForeignDriver(t *testing.T) {
	exec, err := migrate.NewExecutorFor(notADriver{})
	require.NoError(t, err)
	require.NotNil(t, exec, "a nil executor would be dereferenced by the orchestrator")

	_, err = migrate.NewOrchestrator(exec).Migrate(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pgmigrate: pgmigrate.notADriver does not implement driver.Driver")
}
