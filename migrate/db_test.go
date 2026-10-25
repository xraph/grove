package migrate

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/grove/driver"
)

// fakeExecutor is an Executor that names its driver and remembers every Exec
// it was given. Each test passes its own driver name so the process-wide
// renderer registry never sees two tests fight over one entry.
type fakeExecutor struct {
	mockExecutor

	driver  string
	execs   []string
	queries []string
}

func (e *fakeExecutor) DriverName() string {
	if e.driver == "" {
		return "fake"
	}
	return e.driver
}

func (e *fakeExecutor) Exec(_ context.Context, query string, _ ...any) (driver.Result, error) {
	e.execs = append(e.execs, query)
	return nil, nil
}

func (e *fakeExecutor) Query(_ context.Context, query string, _ ...any) (driver.Rows, error) {
	e.queries = append(e.queries, query)
	return nil, nil
}

// unwrapping stands in for a test wrapper that counts or fails statements. It
// embeds Executor, so it has no DriverName of its own.
type unwrapping struct {
	Executor
}

func (u unwrapping) Unwrap() Executor { return u.Executor }

// opaque wraps an Executor without saying how to reach the one inside.
type opaque struct {
	Executor
}

// recordingRenderer keeps what the DB handed it and answers with err.
type recordingRenderer struct {
	ops   []Operation
	execs []Executor
	err   error
}

func (r *recordingRenderer) Apply(_ context.Context, exec Executor, op Operation) error {
	r.ops = append(r.ops, op)
	r.execs = append(r.execs, exec)
	return r.err
}

// useRenderer registers r for driverName and takes it out again when the test
// ends.
func useRenderer(t *testing.T, driverName string, r Renderer) {
	t.Helper()
	RegisterRenderer(driverName, r)
	t.Cleanup(func() {
		renderersMu.Lock()
		delete(renderers, driverName)
		renderersMu.Unlock()
	})
}

func TestDB_ApplyRoutesToRenderer(t *testing.T) {
	rec := &recordingRenderer{}
	useRenderer(t, "db-route", rec)
	exec := &fakeExecutor{driver: "db-route"}

	err := NewDB(exec).NewDropTable("t").IfExists().Exec(context.Background())

	require.NoError(t, err)
	require.Len(t, rec.ops, 1)
	assert.Equal(t, &DropTable{Table: "t", IfExists: true}, rec.ops[0])
	assert.Same(t, exec, rec.execs[0])
}

func TestDB_everyBuilderExecReachesTheRenderer(t *testing.T) {
	rec := &recordingRenderer{}
	useRenderer(t, "db-every", rec)
	db := NewDB(&fakeExecutor{driver: "db-every"})
	ctx := context.Background()

	require.NoError(t, db.NewCreateTable("t").Column("id", Text).Exec(ctx))
	require.NoError(t, db.NewDropTable("t").Exec(ctx))
	require.NoError(t, db.NewAlterTable("t").DropColumn("a").Exec(ctx))
	require.NoError(t, db.NewCreateIndex("i").On("t").Columns("a").Exec(ctx))
	require.NoError(t, db.NewDropIndex("i").Exec(ctx))

	require.Len(t, rec.ops, 5)
	assert.IsType(t, &CreateTable{}, rec.ops[0])
	assert.IsType(t, &DropTable{}, rec.ops[1])
	assert.IsType(t, &AlterTable{}, rec.ops[2])
	assert.IsType(t, &CreateIndex{}, rec.ops[3])
	assert.IsType(t, &DropIndex{}, rec.ops[4])
}

func TestDB_ApplyReturnsTheRendererError(t *testing.T) {
	boom := errors.New("boom")
	useRenderer(t, "db-fails", &recordingRenderer{err: boom})

	err := NewDB(&fakeExecutor{driver: "db-fails"}).NewDropTable("t").Exec(context.Background())

	assert.ErrorIs(t, err, boom)
}

func TestDB_ApplyThroughUnwrap(t *testing.T) {
	rec := &recordingRenderer{}
	useRenderer(t, "db-unwrap", rec)
	inner := &fakeExecutor{driver: "db-unwrap"}
	wrapper := unwrapping{inner}

	err := NewDB(wrapper).NewDropTable("t").Exec(context.Background())

	require.NoError(t, err)
	require.Len(t, rec.execs, 1)
	// The renderer has to run its statements through the wrapper, or a test
	// wrapper that counts or fails statements would never see them.
	assert.IsType(t, unwrapping{}, rec.execs[0])
	assert.Equal(t, wrapper, rec.execs[0])
}

func TestDB_ApplyUnknownDriver(t *testing.T) {
	rec := &recordingRenderer{}
	useRenderer(t, "db-unknown", rec)
	wrapper := opaque{&fakeExecutor{driver: "db-unknown"}}

	err := NewDB(wrapper).NewDropTable("t").Exec(context.Background())

	require.ErrorIs(t, err, ErrUnknownDriver)
	assert.Contains(t, err.Error(), "migrate.opaque")
	assert.Empty(t, rec.ops)
}

func TestDB_ApplyUnknownDriverNamesWhereTheChainEnded(t *testing.T) {
	// The wrapper in front does unwrap, so blaming it would send the reader
	// to the wrong type.
	wrapper := unwrapping{opaque{&mockExecutor{}}}

	err := NewDB(wrapper).NewDropTable("t").Exec(context.Background())

	require.ErrorIs(t, err, ErrUnknownDriver)
	assert.Contains(t, err.Error(), "migrate.opaque")
	assert.NotContains(t, err.Error(), "migrate.unwrapping")
}

func TestDB_ApplyNilExecutor(t *testing.T) {
	err := NewDB(nil).NewDropTable("t").Exec(context.Background())

	assert.ErrorIs(t, err, ErrUnknownDriver)
}

func TestDB_ApplyNoRenderer(t *testing.T) {
	err := NewDB(&fakeExecutor{driver: "nobody"}).NewDropTable("t").Exec(context.Background())

	require.ErrorIs(t, err, ErrSchemaUnsupported)
	assert.Contains(t, err.Error(), "nobody does not support schema builders yet; use db.Exec")
}

func TestDB_ExecPassesThrough(t *testing.T) {
	exec := &fakeExecutor{driver: "db-exec"}

	_, err := NewDB(exec).Exec(context.Background(), "SELECT 1")

	require.NoError(t, err)
	assert.Equal(t, []string{"SELECT 1"}, exec.execs)
}

func TestDB_QueryPassesThrough(t *testing.T) {
	exec := &fakeExecutor{driver: "db-query"}

	_, err := NewDB(exec).Query(context.Background(), "SELECT 2")

	require.NoError(t, err)
	assert.Equal(t, []string{"SELECT 2"}, exec.queries)
}

func TestDB_ExecutorReturnsTheWrappedExecutor(t *testing.T) {
	exec := &fakeExecutor{driver: "db-handle"}

	assert.Same(t, exec, NewDB(exec).Executor())
}

func TestDB_Dialect(t *testing.T) {
	tests := []struct {
		name string
		exec Executor
		want string
	}{
		{name: "named executor", exec: &fakeExecutor{driver: "pg"}, want: "pg"},
		{name: "through a wrapper", exec: unwrapping{&fakeExecutor{driver: "mongo"}}, want: "mongo"},
		{name: "no name anywhere", exec: opaque{&mockExecutor{}}, want: ""},
		{name: "nil executor", exec: nil, want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, NewDB(tt.exec).Dialect())
		})
	}
}

func TestDriverNameOf_followsUnwrap(t *testing.T) {
	name, ok := DriverNameOf(unwrapping{unwrapping{&fakeExecutor{driver: "pg"}}})

	assert.True(t, ok)
	assert.Equal(t, "pg", name)
}

func TestDriverNameOf_stopsOnAnUnwrapCycle(t *testing.T) {
	loop := &selfWrapping{}

	name, ok := DriverNameOf(loop)

	assert.False(t, ok)
	assert.Empty(t, name)
}

// selfWrapping unwraps to itself, which a buggy wrapper could do.
type selfWrapping struct {
	Executor
}

func (s *selfWrapping) Unwrap() Executor { return s }

func TestExecutorAs_followsUnwrap(t *testing.T) {
	fake := &fakeExecutor{driver: "db-as"}

	got, ok := ExecutorAs[*fakeExecutor](unwrapping{fake})

	require.True(t, ok)
	assert.Same(t, fake, got)
}

func TestExecutorAs_missingType(t *testing.T) {
	got, ok := ExecutorAs[*fakeExecutor](unwrapping{&mockExecutor{}})

	assert.False(t, ok)
	assert.Nil(t, got)
}

func TestExecutorAs_nilExecutor(t *testing.T) {
	_, ok := ExecutorAs[*fakeExecutor](nil)

	assert.False(t, ok)
}

func TestDB_BuilderMisuseSkipsRenderer(t *testing.T) {
	rec := &recordingRenderer{}
	useRenderer(t, "db-misuse", rec)
	db := NewDB(&fakeExecutor{driver: "db-misuse"})
	ctx := context.Background()

	errs := []error{
		db.NewDropTable("").Exec(ctx),
		db.NewCreateTable("t").Exec(ctx),
		db.NewAlterTable("t").Exec(ctx),
		db.NewCreateIndex("i").Exec(ctx),
		db.NewDropIndex("").Exec(ctx),
	}

	for _, err := range errs {
		assert.ErrorIs(t, err, ErrInvalidOperation)
	}
	assert.Empty(t, rec.ops)
}

func TestRegisterRenderer_overwrites(t *testing.T) {
	first := &recordingRenderer{}
	second := &recordingRenderer{}
	useRenderer(t, "db-overwrite", first)
	useRenderer(t, "db-overwrite", second)

	err := NewDB(&fakeExecutor{driver: "db-overwrite"}).NewDropTable("t").Exec(context.Background())

	require.NoError(t, err)
	assert.Empty(t, first.ops)
	assert.Len(t, second.ops, 1)
}

func TestRegisterRenderer_nilCountsAsNoRenderer(t *testing.T) {
	useRenderer(t, "db-nil", nil)

	err := NewDB(&fakeExecutor{driver: "db-nil"}).NewDropTable("t").Exec(context.Background())

	assert.ErrorIs(t, err, ErrSchemaUnsupported)
}

func TestUnsupported_wrapsTheSentinel(t *testing.T) {
	err := Unsupported("mysql", "partial indexes")

	require.ErrorIs(t, err, ErrSchemaUnsupported)
	assert.Equal(t, "migrate: schema operation not supported: mysql does not support partial indexes", err.Error())
}
