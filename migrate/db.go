package migrate

// DB is the handle a migration builds its schema through. It wraps an
// [Executor], and the builders it hands out end in Exec, which renders the
// operation for whichever driver the executor runs on.
type DB struct {
	exec Executor
}

// NewDB wraps exec. Tests pass nil when they only inspect what a builder
// produces.
func NewDB(exec Executor) *DB {
	return &DB{exec: exec}
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
