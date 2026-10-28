package pgmigrate

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/grove/drivers/pgdriver"
	"github.com/xraph/grove/migrate"
)

// firstBootReplicas is how many processes boot at once against an empty
// database, the shape of a deployment rolling out several replicas.
const firstBootReplicas = 8

// firstBootRounds is how many fresh schemas each run races on. The window
// between the existence check and the catalog insert is narrow, so one round
// can pass by luck.
const firstBootRounds = 10

// Every replica runs Migrate against a schema grove has never touched, so all
// of them create grove_migrations and grove_migration_locks at the same
// moment. Postgres doesn't make CREATE TABLE IF NOT EXISTS atomic against a
// concurrent create, and before the bootstrap took a lock of its own the
// losers failed with 42P07, 42710 or 23505 before the migration lock (which
// lives in one of those tables) could serialize them.
func TestLive_concurrentFirstBoot(t *testing.T) {
	ctx := context.Background()
	dsn := startPostgres(t)

	const version = "20260101000001"
	group := migrate.NewGroup("first_boot")
	group.MustRegister(&migrate.Migration{
		Name:    "create_widgets",
		Version: version,
		Up: func(ctx context.Context, exec migrate.Executor) error {
			_, err := exec.Exec(ctx, "CREATE TABLE widgets (id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL)")
			return err
		},
	})

	// One pool per replica, as separate processes would have.
	dbs := make([]*pgdriver.PgDB, firstBootReplicas)
	for i := range dbs {
		db := pgdriver.New()
		require.NoError(t, db.Open(ctx, dsn))
		t.Cleanup(func() { _ = db.Close() })
		dbs[i] = db
	}
	admin := dbs[0]

	for round := range firstBootRounds {
		_, err := admin.Exec(ctx, "DROP SCHEMA public CASCADE; CREATE SCHEMA public")
		require.NoError(t, err, "round %d: reset schema", round)

		start := make(chan struct{})
		errs := make([]error, firstBootReplicas)
		applied := make([]int, firstBootReplicas)
		var wg sync.WaitGroup
		for i, db := range dbs {
			wg.Go(func() {
				<-start
				res, err := migrate.NewOrchestrator(New(db), group).Migrate(ctx)
				errs[i] = err
				if res != nil {
					applied[i] = len(res.Applied)
				}
			})
		}
		close(start)
		wg.Wait()

		total := 0
		for i, err := range errs {
			assert.NoError(t, err, "round %d: replica %d", round, i)
			total += applied[i]
		}
		assert.Equal(t, 1, total, "round %d: exactly one replica applies the migration", round)
		assert.Equal(t, 1, recordedVersions(t, admin, version), "round %d", round)
		assert.True(t, tableExists(t, admin, "widgets"), "round %d", round)
		assert.Zero(t, advisoryLocks(t, admin), "round %d: no replica leaves a lock behind", round)
		if t.Failed() {
			t.FailNow()
		}
	}
}
