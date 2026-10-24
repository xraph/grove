package migrate

import (
	"context"
	"fmt"
	"path/filepath"
	"regexp"
	"runtime"
)

// Func is the body of a migration written against a [DB]. Up and down
// functions use it so they can reach the schema builders without caring which
// executor runs them.
type Func func(ctx context.Context, db *DB) error

// Option adjusts a migration built by [Define] or registered by [Group.Add].
type Option func(*Migration)

// NoTransaction opts a migration out of the transaction Add and Define give it
// by default. Use it for statements a transaction can't hold, such as CREATE
// INDEX CONCURRENTLY on Postgres.
func NoTransaction() Option {
	return func(m *Migration) {
		m.Transactional = false
	}
}

// Comment sets the migration's description.
func Comment(text string) Option {
	return func(m *Migration) {
		m.Comment = text
	}
}

// migrationFileRE matches a migration file's base name. Fourteen digits is the
// same shape as the timestamp versions the rest of the package already uses.
var migrationFileRE = regexp.MustCompile(`^(\d{14})_(.+)\.go$`)

// Define builds a migration from an up and a down function. It starts with
// Transactional on and then applies opts, so NoTransaction has the last word.
// Up and Down are left nil when the matching function is nil, which keeps the
// migrator's missing-Up check and the rollback-without-Down path working.
// Group.Register fills in Group.
func Define(version, name string, up, down Func, opts ...Option) *Migration {
	m := &Migration{
		Version:       version,
		Name:          name,
		Up:            wrapFunc(up),
		Down:          wrapFunc(down),
		Transactional: true,
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

func wrapFunc(f Func) MigrateFunc {
	if f == nil {
		return nil
	}
	return func(ctx context.Context, exec Executor) error {
		return f(ctx, NewDB(exec))
	}
}

// Add registers a migration whose version and name come from the file that
// calls it, so the file name is the only place the version is written down.
// Call it from an init function in a file named
// <14 digit version>_<name>.go:
//
//	// 20240102030405_create_widgets.go
//	func init() {
//		Migrations.Add(
//			func(ctx context.Context, db *migrate.DB) error {
//				return db.NewCreateTable("widgets").
//					Column("id", migrate.Text, migrate.PrimaryKey()).
//					Exec(ctx)
//			},
//			func(ctx context.Context, db *migrate.DB) error {
//				return db.NewDropTable("widgets").Exec(ctx)
//			},
//		)
//	}
//
// Add panics when the caller's file name doesn't fit that shape, or when the
// group already holds the version. An init that fails to register would
// otherwise leave a migration quietly missing from the plan.
func (g *Group) Add(up, down Func, opts ...Option) {
	_, file, _, ok := runtime.Caller(1)
	if !ok {
		panic("migrate: Add could not find the file that called it")
	}
	addFromFile(g, file, up, down, opts...)
}

// addFromFile is Add with the caller's path passed in, so a test can hand it
// a bad file name without needing a bad file.
func addFromFile(g *Group, file string, up, down Func, opts ...Option) {
	version, name, err := parseMigrationFile(file)
	if err != nil {
		panic(fmt.Sprintf("migrate: Add called from %s; name the file <14 digit version>_<name>.go", filepath.Base(file)))
	}
	g.MustRegister(Define(version, name, up, down, opts...))
}

// parseMigrationFile splits a migration file's path into its version and name.
func parseMigrationFile(path string) (version, name string, err error) {
	base := filepath.Base(path)
	match := migrationFileRE.FindStringSubmatch(base)
	if match == nil {
		return "", "", fmt.Errorf("migrate: file %q does not match <14 digit version>_<name>.go", base)
	}
	return match[1], match[2], nil
}
