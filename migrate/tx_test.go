package migrate

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/grove/driver"
)

// txFake is an Executor that also implements TxExecutor. It appends every
// call that matters to log, so a test can assert the exact order of the
// transaction boundaries, the statements and the applied records.
type txFake struct {
	log     []string
	applied []*AppliedMigration

	beginErr    error
	commitErr   error
	rollbackErr error
	recordErr   error

	releaseCalls int
}

func (f *txFake) Exec(_ context.Context, query string, _ ...any) (driver.Result, error) {
	f.log = append(f.log, "exec:"+query)
	return nil, nil
}

func (f *txFake) Query(context.Context, string, ...any) (driver.Rows, error) {
	return nil, nil
}
func (f *txFake) EnsureMigrationTable(context.Context) error { return nil }
func (f *txFake) EnsureLockTable(context.Context) error      { return nil }

func (f *txFake) AcquireLock(context.Context, string) error {
	f.log = append(f.log, "lock")
	return nil
}

func (f *txFake) ReleaseLock(context.Context) error {
	f.log = append(f.log, "unlock")
	f.releaseCalls++
	return nil
}

func (f *txFake) ListApplied(context.Context) ([]*AppliedMigration, error) {
	return f.applied, nil
}

func (f *txFake) RecordApplied(_ context.Context, m *Migration) error {
	if f.recordErr != nil {
		return f.recordErr
	}
	f.log = append(f.log, "record:"+m.Version)
	return nil
}

func (f *txFake) RemoveApplied(_ context.Context, m *Migration) error {
	f.log = append(f.log, "remove:"+m.Version)
	return nil
}

func (f *txFake) BeginTx(context.Context) error {
	if f.beginErr != nil {
		return f.beginErr
	}
	f.log = append(f.log, "BEGIN")
	return nil
}

func (f *txFake) CommitTx(context.Context) error {
	f.log = append(f.log, "COMMIT")
	return f.commitErr
}

func (f *txFake) RollbackTx(context.Context) error {
	f.log = append(f.log, "ROLLBACK")
	return f.rollbackErr
}

// calls returns the log without the lock bookkeeping around the migrations.
func (f *txFake) calls() []string {
	out := make([]string, 0, len(f.log))
	for _, c := range f.log {
		if c == "lock" || c == "unlock" {
			continue
		}
		out = append(out, c)
	}
	return out
}

// plainFake is an Executor with no transaction support at all.
type plainFake struct {
	log []string
}

func (f *plainFake) Exec(_ context.Context, query string, _ ...any) (driver.Result, error) {
	f.log = append(f.log, "exec:"+query)
	return nil, nil
}

func (f *plainFake) Query(context.Context, string, ...any) (driver.Rows, error) {
	return nil, nil
}
func (f *plainFake) EnsureMigrationTable(context.Context) error { return nil }
func (f *plainFake) EnsureLockTable(context.Context) error      { return nil }
func (f *plainFake) AcquireLock(context.Context, string) error  { return nil }
func (f *plainFake) ReleaseLock(context.Context) error          { return nil }
func (f *plainFake) ListApplied(context.Context) ([]*AppliedMigration, error) {
	return nil, nil
}

func (f *plainFake) RecordApplied(_ context.Context, m *Migration) error {
	f.log = append(f.log, "record:"+m.Version)
	return nil
}

func (f *plainFake) RemoveApplied(_ context.Context, m *Migration) error {
	f.log = append(f.log, "remove:"+m.Version)
	return nil
}

func execFunc(query string) Func {
	return func(ctx context.Context, db *DB) error {
		_, err := db.Exec(ctx, query)
		return err
	}
}

func txGroup(t *testing.T, migrations ...*Migration) *Group {
	t.Helper()
	g := NewGroup("app")
	require.NoError(t, g.Register(migrations...))
	return g
}

func TestMigrate_transactionalCommitsWithRecord(t *testing.T) {
	fake := &txFake{}
	g := txGroup(t, Define("v1", "create_x", execFunc("X"), nil))

	res, err := NewOrchestrator(fake, g).Migrate(context.Background())

	require.NoError(t, err)
	assert.Equal(t, []string{"BEGIN", "exec:X", "record:v1", "COMMIT"}, fake.calls())
	require.Len(t, res.Applied, 1)
	assert.Equal(t, "v1", res.Applied[0].Version)
}

func TestMigrate_transactionalRollsBackOnError(t *testing.T) {
	boom := errors.New("boom")
	fake := &txFake{}
	g := txGroup(t, Define("v1", "create_x", func(context.Context, *DB) error { return boom }, nil))

	res, err := NewOrchestrator(fake, g).Migrate(context.Background())

	require.ErrorIs(t, err, boom)
	assert.Equal(t, []string{"BEGIN", "ROLLBACK"}, fake.calls())
	assert.Empty(t, res.Applied)
}

func TestMigrate_transactionalRollsBackOnRecordError(t *testing.T) {
	boom := errors.New("record boom")
	fake := &txFake{recordErr: boom}
	g := txGroup(t, Define("v1", "create_x", execFunc("X"), nil))

	res, err := NewOrchestrator(fake, g).Migrate(context.Background())

	require.ErrorIs(t, err, boom)
	assert.Equal(t, []string{"BEGIN", "exec:X", "ROLLBACK"}, fake.calls())
	assert.Empty(t, res.Applied)
}

func TestMigrate_transactionalRollsBackOnPanic(t *testing.T) {
	fake := &txFake{}
	g := txGroup(t, Define("v1", "create_x", func(context.Context, *DB) error {
		panic("kaboom")
	}, nil))

	assert.PanicsWithValue(t, "kaboom", func() {
		_, _ = NewOrchestrator(fake, g).Migrate(context.Background())
	})

	assert.Contains(t, fake.calls(), "ROLLBACK")
	for _, c := range fake.log {
		assert.NotContains(t, c, "record", "a panicking migration must not be recorded")
	}
	assert.Equal(t, 1, fake.releaseCalls, "the migration lock must still be released")
}

func TestMigrate_transactionalCommitFailure(t *testing.T) {
	boom := errors.New("commit boom")
	fake := &txFake{commitErr: boom}
	g := txGroup(t, Define("v1", "create_x", execFunc("X"), nil))

	res, err := NewOrchestrator(fake, g).Migrate(context.Background())

	require.ErrorIs(t, err, boom)
	assert.Contains(t, err.Error(), "migrate: ")
	assert.Equal(t, []string{"BEGIN", "exec:X", "record:v1", "COMMIT", "ROLLBACK"}, fake.calls())
	assert.Empty(t, res.Applied, "an uncommitted migration is not applied")
}

func TestMigrate_transactionalRollbackFailureKeepsBothErrors(t *testing.T) {
	boom := errors.New("boom")
	rbErr := errors.New("rollback boom")
	fake := &txFake{rollbackErr: rbErr}
	g := txGroup(t, Define("v1", "create_x", func(context.Context, *DB) error { return boom }, nil))

	_, err := NewOrchestrator(fake, g).Migrate(context.Background())

	require.ErrorIs(t, err, boom)
	assert.ErrorIs(t, err, rbErr)
}

func TestMigrate_beginFailure(t *testing.T) {
	boom := errors.New("begin boom")
	fake := &txFake{beginErr: boom}
	g := txGroup(t, Define("v1", "create_x", execFunc("X"), nil))

	res, err := NewOrchestrator(fake, g).Migrate(context.Background())

	require.ErrorIs(t, err, boom)
	assert.Contains(t, err.Error(), "migrate: ")
	assert.Equal(t, []string{}, fake.calls(), "a migration that never began must not run or record")
	assert.Empty(t, res.Applied)
}

func TestMigrate_structMigrationStaysPlain(t *testing.T) {
	fake := &txFake{}
	g := txGroup(t, &Migration{
		Version: "v1",
		Name:    "create_x",
		Up: func(ctx context.Context, exec Executor) error {
			_, err := exec.Exec(ctx, "X")
			return err
		},
	})

	_, err := NewOrchestrator(fake, g).Migrate(context.Background())

	require.NoError(t, err)
	assert.Equal(t, []string{"exec:X", "record:v1"}, fake.calls())
}

func TestMigrate_noTransactionOptionStaysPlain(t *testing.T) {
	fake := &txFake{}
	g := txGroup(t, Define("v1", "create_x", execFunc("X"), nil, NoTransaction()))

	_, err := NewOrchestrator(fake, g).Migrate(context.Background())

	require.NoError(t, err)
	assert.Equal(t, []string{"exec:X", "record:v1"}, fake.calls())
}

func TestMigrate_beginUnsupportedRunsPlain(t *testing.T) {
	fake := &txFake{beginErr: ErrTxUnsupported}
	g := txGroup(t, Define("v1", "create_x", execFunc("X"), nil))

	res, err := NewOrchestrator(fake, g).Migrate(context.Background())

	require.NoError(t, err)
	assert.Equal(t, []string{"exec:X", "record:v1"}, fake.calls())
	assert.NotContains(t, fake.calls(), "COMMIT")
	require.Len(t, res.Applied, 1)
}

func TestMigrate_executorWithoutTx(t *testing.T) {
	fake := &plainFake{}
	g := txGroup(t, Define("v1", "create_x", execFunc("X"), nil))

	res, err := NewOrchestrator(fake, g).Migrate(context.Background())

	require.NoError(t, err)
	assert.Equal(t, []string{"exec:X", "record:v1"}, fake.log)
	require.Len(t, res.Applied, 1)
}

func TestMigrate_transactionalMissingUpIsReportedBeforeBegin(t *testing.T) {
	fake := &txFake{}
	g := txGroup(t, Define("v1", "create_x", nil, nil))

	_, err := NewOrchestrator(fake, g).Migrate(context.Background())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "no Up function")
	assert.Equal(t, []string{}, fake.calls())
}

func TestRollback_transactional(t *testing.T) {
	fake := &txFake{applied: []*AppliedMigration{{Group: "app", Version: "v1", Name: "create_x"}}}
	g := txGroup(t, Define("v1", "create_x", execFunc("U"), execFunc("D")))

	res, err := NewOrchestrator(fake, g).Rollback(context.Background())

	require.NoError(t, err)
	assert.Equal(t, []string{"BEGIN", "exec:D", "remove:v1", "COMMIT"}, fake.calls())
	require.Len(t, res.Rollback, 1)
}

func TestRollback_transactionalWithoutDownStillRemovesInsideTx(t *testing.T) {
	fake := &txFake{applied: []*AppliedMigration{{Group: "app", Version: "v1", Name: "create_x"}}}
	g := txGroup(t, Define("v1", "create_x", execFunc("U"), nil))

	_, err := NewOrchestrator(fake, g).Rollback(context.Background())

	require.NoError(t, err)
	assert.Equal(t, []string{"BEGIN", "remove:v1", "COMMIT"}, fake.calls())
}

func TestRollback_transactionalRollsBackOnError(t *testing.T) {
	boom := errors.New("boom")
	fake := &txFake{applied: []*AppliedMigration{{Group: "app", Version: "v1", Name: "create_x"}}}
	g := txGroup(t, Define("v1", "create_x", execFunc("U"), func(context.Context, *DB) error { return boom }))

	res, err := NewOrchestrator(fake, g).Rollback(context.Background())

	require.ErrorIs(t, err, boom)
	assert.Equal(t, []string{"BEGIN", "ROLLBACK"}, fake.calls())
	assert.Empty(t, res.Rollback)
}

func TestRollback_structMigrationStaysPlain(t *testing.T) {
	fake := &txFake{applied: []*AppliedMigration{{Group: "app", Version: "v1", Name: "create_x"}}}
	g := txGroup(t, &Migration{
		Version: "v1",
		Name:    "create_x",
		Up:      func(context.Context, Executor) error { return nil },
		Down: func(ctx context.Context, exec Executor) error {
			_, err := exec.Exec(ctx, "D")
			return err
		},
	})

	_, err := NewOrchestrator(fake, g).Rollback(context.Background())

	require.NoError(t, err)
	assert.Equal(t, []string{"exec:D", "remove:v1"}, fake.calls())
}
