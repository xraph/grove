package migrate

import (
	"context"
	"fmt"

	"github.com/xraph/grove/driver"
)

// DB is the handle a migration builds its schema through. It wraps an
// [Executor], and the builders it hands out end in Exec, which looks up the
// renderer registered for the executor's driver and has it apply the
// operation. The same handle runs raw statements through Exec and Query for
// anything the builders don't cover.
type DB struct {
	exec Executor
}

// NewDB wraps exec. Tests pass nil when they only inspect what a builder
// produces with Operation. Running a builder against a nil executor returns
// [ErrUnknownDriver].
func NewDB(exec Executor) *DB {
	return &DB{exec: exec}
}

// Exec runs a statement that returns no rows, straight on the executor.
func (db *DB) Exec(ctx context.Context, query string, args ...any) (driver.Result, error) {
	return db.exec.Exec(ctx, query, args...)
}

// Query runs a statement that returns rows, straight on the executor.
func (db *DB) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	return db.exec.Query(ctx, query, args...)
}

// Executor returns the executor the handle was built on.
func (db *DB) Executor() Executor {
	return db.exec
}

// Dialect returns the name of the driver behind the executor, such as "pg" or
// "mongo", or "" when the executor and anything it wraps never name one.
func (db *DB) Dialect() string {
	name, _ := DriverNameOf(db.exec)
	return name
}

// Apply hands op to the renderer registered for the executor's driver. The
// builders call it from Exec after checking the chain, so it validates
// nothing itself. The renderer gets the executor as the handle holds it, not
// the executor inside a wrapper, so a wrapper keeps seeing every statement.
func (db *DB) Apply(ctx context.Context, op Operation) error {
	name, last, ok := resolveDriver(db.exec)
	if !ok {
		return fmt.Errorf("%w: %T has no DriverName and no Unwrap() migrate.Executor", ErrUnknownDriver, last)
	}
	r, ok := rendererFor(name)
	if !ok {
		return Unsupported(name, "schema builders yet; use db.Exec")
	}
	return r.Apply(ctx, db.exec, op)
}

// NewCreateTable starts a CREATE TABLE for table.
func (db *DB) NewCreateTable(table string) *CreateTableQuery {
	q := &CreateTableQuery{db: db, op: &CreateTable{Table: table}}
	q.need("NewCreateTable table name", table)
	return q
}

// NewDropTable starts a DROP TABLE for table.
func (db *DB) NewDropTable(table string) *DropTableQuery {
	q := &DropTableQuery{db: db, op: &DropTable{Table: table}}
	q.need("NewDropTable table name", table)
	return q
}

// NewAlterTable starts an ALTER TABLE for table. Chain several actions and
// they run in order.
func (db *DB) NewAlterTable(table string) *AlterTableQuery {
	q := &AlterTableQuery{db: db, op: &AlterTable{Table: table}}
	q.need("NewAlterTable table name", table)
	return q
}

// NewCreateIndex starts a CREATE INDEX called name. It needs On and at least
// one column before it can run.
func (db *DB) NewCreateIndex(name string) *CreateIndexQuery {
	q := &CreateIndexQuery{db: db, op: &CreateIndex{Name: name}}
	q.need("NewCreateIndex index name", name)
	return q
}

// NewDropIndex starts a DROP INDEX for name.
func (db *DB) NewDropIndex(name string) *DropIndexQuery {
	q := &DropIndexQuery{db: db, op: &DropIndex{Name: name}}
	q.need("NewDropIndex index name", name)
	return q
}
