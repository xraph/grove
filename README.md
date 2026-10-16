<p align="center">
  <img src="assets/readme-banner.svg" alt="Grove, a polyglot Go ORM with native query builders for PostgreSQL, MySQL, SQLite, MongoDB, ClickHouse and Elasticsearch" width="100%">
</p>

# Grove: polyglot Go ORM with native query syntax

Grove is an open-source Go ORM for PostgreSQL, MySQL, SQLite, Turso, MongoDB, ClickHouse and Elasticsearch. Each driver builds queries in its own database's syntax, so you write `$1` and `DISTINCT ON` against Postgres and BSON filters against MongoDB, through one model layer. You also get Go-code database migrations with dependency ordering, query hooks for multi-tenancy and audit logging, a key-value store over Redis, Badger, BoltDB, DynamoDB and Memcached, and CRDT sync for offline-first apps. It runs standalone or as a [Forge](https://github.com/xraph/forge) extension.

[![Go 1.25+](https://img.shields.io/badge/Go-1.25%2B-00ADD8?logo=go&logoColor=white)](https://go.dev/dl/) [![CI](https://github.com/xraph/grove/actions/workflows/ci.yml/badge.svg)](https://github.com/xraph/grove/actions/workflows/ci.yml) [![Go Reference](https://pkg.go.dev/badge/github.com/xraph/grove.svg)](https://pkg.go.dev/github.com/xraph/grove) [![GitHub Stars](https://img.shields.io/github/stars/xraph/grove?style=flat)](https://github.com/xraph/grove/stargazers) [![MIT License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

[Documentation](https://www.xraph.com/docs/grove) · [Quick start](#quick-start) · [Drivers](#database-drivers) · [Migrations](#migrations) · [Hooks](#query-hooks) · [KV store](#key-value-store) · [CRDT sync](#crdt-sync) · [Forge](#use-it-with-forge) · [FAQ](#faq)

## Quick start

You need [Go 1.25 or later](https://go.dev/dl/). This example uses the SQLite driver, which is pure Go, so you don't need a database server or cgo to try it:

```bash
mkdir hello-grove
cd hello-grove
go mod init example.com/hello-grove
go get github.com/xraph/grove@latest
go get github.com/xraph/grove/drivers/sqlitedriver@latest
```

Save this as `main.go`:

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/xraph/grove"
	"github.com/xraph/grove/drivers/sqlitedriver"
)

type User struct {
	grove.BaseModel `grove:"table:users,alias:u"`

	ID    int64  `grove:"id,pk,autoincrement"`
	Name  string `grove:"name,notnull"`
	Email string `grove:"email,notnull,unique"`
	Role  string `grove:"role,notnull"`
}

func main() {
	ctx := context.Background()

	sqlite := sqlitedriver.New()
	if err := sqlite.Open(ctx, "file:hello.db"); err != nil {
		log.Fatal(err)
	}

	db, err := grove.Open(sqlite)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	q := sqlitedriver.Unwrap(db)

	if _, err := q.NewCreateTable((*User)(nil)).IfNotExists().Exec(ctx); err != nil {
		log.Fatal(err)
	}

	alice := &User{Name: "Alice", Email: "alice@example.com", Role: "admin"}
	if _, err := q.NewInsert(alice).OnConflict("DO NOTHING").Exec(ctx); err != nil {
		log.Fatal(err)
	}

	var admins []User
	err = q.NewSelect(&admins).
		Where("role = ?", "admin").
		OrderExpr("id ASC").
		Limit(10).
		Scan(ctx)
	if err != nil {
		log.Fatal(err)
	}

	for _, u := range admins {
		fmt.Printf("%d %s <%s>\n", u.ID, u.Name, u.Email)
	}
}
```

Run it:

```bash
go run .
# 1 Alice <alice@example.com>
```

Run it twice and you still get one row, because the second insert hits the unique email and `ON CONFLICT DO NOTHING` skips it. The data lives in `hello.db` next to your code.

That's the whole pattern. You open a driver, hand it to `grove.Open`, and call `Unwrap` on the `*grove.DB` to get that driver's typed query builder. Swap `sqlitedriver` for `pgdriver` and you get `pgdriver.Unwrap(db).NewSelect(...)`, with Postgres-only methods like `DistinctOn`, `ForUpdate` and `ForShare` on it.

## Why Grove?

Most Go ORMs give you one query API and translate it for every database. Grove gives each database its own builder, so the SQL (or BSON, or Elasticsearch DSL) you'd write by hand in a console is what you write in Go, and when a database has a feature the others lack, its builder has a method for it and the other builders don't have one.

| You need | Grove gives you |
| --- | --- |
| Queries that look like your database | A query builder per driver: `$1` placeholders and `DISTINCT ON` on Postgres, `PREWHERE`, `SAMPLE` and `FINAL` on ClickHouse, `bson.M` filters and aggregation pipelines on MongoDB |
| An easy move off bun | `grove:"..."` struct tags with a `bun:"..."` fallback, so existing bun models load unchanged |
| Low query overhead | Schema metadata cached per type and pooled byte buffers for query building (see the [benchmarks](#benchmarks)) |
| Schema changes your modules can share | Go-code migrations in named groups, ordered by `migrate.DependsOn`, behind a migration lock |
| Tenant isolation and audit trails | Pre and post query and mutation hooks that can deny a query or inject `WHERE` filters |
| Caching, sessions and rate limits | A key-value `Store` with codecs, middleware and ready-made plugins over five KV backends |
| Offline-first and collaborative apps | CRDT fields (`crdt:lww`, `crdt:counter`, `crdt:set`), a sync server, and TypeScript and Dart clients |
| One way to wire it into a service | A Forge extension that reads `extensions.grove` and provides `*grove.DB` through DI |

The [architecture guide](https://www.xraph.com/docs/grove/architecture) explains how the root package, the driver interfaces and the driver modules fit together.

## Database drivers

Each driver is its own Go module. You only download the client libraries for the databases you use, so a service that talks to SQLite never pulls in the MongoDB or ClickHouse drivers, and your `go.sum` stays short.

| Database | Module | Registered as | Docs |
| --- | --- | --- | --- |
| PostgreSQL | `github.com/xraph/grove/drivers/pgdriver` | `postgres`, `pg` | [PostgreSQL driver](https://www.xraph.com/docs/grove/drivers/postgres) |
| MySQL | `github.com/xraph/grove/drivers/mysqldriver` | `mysql` | [MySQL driver](https://www.xraph.com/docs/grove/drivers/mysql) |
| SQLite | `github.com/xraph/grove/drivers/sqlitedriver` | `sqlite` | [SQLite driver](https://www.xraph.com/docs/grove/drivers/sqlite) |
| Turso and libSQL | `github.com/xraph/grove/drivers/tursodriver` | `turso` | [Turso driver](https://www.xraph.com/docs/grove/drivers/turso) |
| MongoDB | `github.com/xraph/grove/drivers/mongodriver` | `mongodb`, `mongo` | [MongoDB driver](https://www.xraph.com/docs/grove/drivers/mongodb) |
| ClickHouse | `github.com/xraph/grove/drivers/clickhousedriver` | `clickhouse` | [ClickHouse driver](https://www.xraph.com/docs/grove/drivers/clickhouse) |
| Elasticsearch | `github.com/xraph/grove/drivers/esdriver` | `elasticsearch` | [Elasticsearch driver](https://www.xraph.com/docs/grove/drivers/elasticsearch) |

Importing a driver package registers it under the names above. That's what lets you open a database by name, from config, without naming the package in your code:

```go
import _ "github.com/xraph/grove/drivers/pgdriver"

drv, err := grove.OpenDriver(ctx, "postgres", "postgres://localhost:5432/app?sslmode=disable")
db, err := grove.Open(drv)
```

The SQL drivers also accept `?` in `Where`, and `pgdriver` rewrites each one to the next `$n` for you.

## Migrations

Migrations are Go functions. You put them in a named group, and a group can depend on other groups, which is how a library ships its own tables and still runs after your core schema:

```go
var Migrations = migrate.NewGroup("billing", migrate.DependsOn("core"))

func init() {
	Migrations.MustRegister(&migrate.Migration{
		Name:    "create_invoices",
		Version: "20260101000000",
		Up: func(ctx context.Context, exec migrate.Executor) error {
			_, err := exec.Exec(ctx, `CREATE TABLE invoices (id BIGSERIAL PRIMARY KEY, total BIGINT NOT NULL)`)
			return err
		},
		Down: func(ctx context.Context, exec migrate.Executor) error {
			_, err := exec.Exec(ctx, `DROP TABLE invoices`)
			return err
		},
	})
}
```

`migrate.NewOrchestrator(executor, groups...)` plans every group in dependency order and gives you `Migrate`, `Rollback` and `Status`. Each SQL driver and MongoDB has a `<name>migrate` package (`pgmigrate`, `sqlitemigrate`, `mongomigrate` and so on) with the executor, and importing it lets `migrate.NewExecutorFor(drv)` find the right one. Applied versions go in a tracking table, and a lock table stops two instances from migrating at once. The [migrations guide](https://www.xraph.com/docs/grove/migrations/overview) and [multi-module migrations](https://www.xraph.com/docs/grove/migrations/multi-module) cover the rest.

## Query hooks

A hook is any type that implements one or more of `hook.PreQueryHook`, `hook.PostQueryHook`, `hook.PreMutationHook`, `hook.PostMutationHook` or `hook.StreamRowHook`. You add it to the database with a scope, and it runs on every matching query the drivers build.

This one scopes reads on the `orders` table to the tenant in the request context, and refuses the query when there isn't one:

```go
type tenantKey struct{}

type TenantScope struct{}

func (TenantScope) BeforeQuery(ctx context.Context, qc *hook.QueryContext) (*hook.HookResult, error) {
	tenant, _ := ctx.Value(tenantKey{}).(string)
	if tenant == "" {
		return &hook.HookResult{Decision: hook.Deny, Error: grove.ErrHookDenied}, nil
	}

	return &hook.HookResult{
		Decision: hook.Modify,
		Filters:  []hook.ExtraFilter{{Clause: "tenant_id = ?", Args: []any{tenant}}},
	}, nil
}

db.Hooks().AddHook(TenantScope{}, hook.Scope{Tables: []string{"orders"}})
```

Scopes filter by table and by operation (`hook.OpSelect`, `hook.OpInsert`, `hook.OpUpdate`, `hook.OpDelete` and the bulk variants), and a lower `Priority` runs first. The `audit` package is a hook too, for audit logs. See [hooks](https://www.xraph.com/docs/grove/hooks/overview) and [tenant isolation](https://www.xraph.com/docs/grove/hooks/tenant-isolation).

## Key-value store

`github.com/xraph/grove/kv` is a separate module with its own drivers. It gives you `Get`, `Set`, `MGet`, `MSet`, `Delete`, `Exists`, `TTL`, `Expire` and `Scan` over any of them, with values encoded by a codec (JSON by default):

```go
rdb := redisdriver.New()
if err := rdb.Open(ctx, "redis://localhost:6379/0"); err != nil {
	log.Fatal(err)
}

store, err := kv.Open(rdb)
if err != nil {
	log.Fatal(err)
}
defer store.Close()

err = store.Set(ctx, "session:abc", Session{UserID: "u_1"}, kv.WithTTL(30*time.Minute))
```

| Part | What's there |
| --- | --- |
| Drivers | `kv/drivers/redisdriver`, `badgerdriver`, `boltdriver`, `dynamodriver`, `memcacheddriver` |
| Codecs | JSON, MessagePack, gob and Protocol Buffers in `kv/codec` |
| Middleware | cache, circuit breaker, compression, encryption, logging, namespacing, retry and stampede protection in `kv/middleware` |
| Plugins | counters, leaderboards, distributed locks, queues, rate limiters and sessions in `kv/plugins` |
| Forge | `kv/extension`, configured under `extensions.grove_kv` |

Start with the [KV overview](https://www.xraph.com/docs/grove/kv/overview).

## CRDT sync

Tag a field with a CRDT type and Grove records its merge state in a shadow table next to yours. Your table's schema doesn't change. When two devices edit the same record offline and sync later, the field's type decides the result: the latest write wins for `lww`, increments from both sides add up for `counter`, and an element added on one side and removed on the other stays in a `set`.

```go
type Document struct {
	grove.BaseModel `grove:"table:documents"`

	ID        string   `grove:"id,pk"`
	Title     string   `grove:"title,crdt:lww"`
	ViewCount int64    `grove:"view_count,crdt:counter"`
	Tags      []string `grove:"tags,type:jsonb,crdt:set"`
}
```

`crdt.New(crdt.WithNodeID("node-1"))` creates the plugin, which you add as a hook on the CRDT tables. Nodes exchange changes through `crdt.NewSyncer` over HTTP pull and push, with an SSE stream for live updates, and every change carries a hybrid logical clock so nodes agree on order without coordinating. Beyond LWW registers, counters and sets, there are list, document and collaborative text types, plus presence and rooms.

Your frontend talks to the same sync endpoints through [`@grove-js/crdt`](crdt-js/) (TypeScript, with React hooks) or [`grove_crdt`](crdt-dart/) (Dart and Flutter). The [CRDT guide](https://www.xraph.com/docs/grove/crdt) walks through a full setup.

## Use it with Forge

The `github.com/xraph/grove/extension` module plugs Grove into a [Forge](https://github.com/xraph/forge) app. It opens the database during `Register`, provides `*grove.DB` through Forge's DI container, adds your hooks and migration groups, reports health, and closes the connection on shutdown. It needs Go 1.26, the same as Forge.

```bash
go get github.com/xraph/grove/extension@latest
go get github.com/xraph/grove/drivers/pgdriver@latest
```

```go
import (
	"github.com/xraph/forge"
	"github.com/xraph/grove"
	"github.com/xraph/grove/drivers/pgdriver"
	_ "github.com/xraph/grove/drivers/pgdriver/pgmigrate"
	groveext "github.com/xraph/grove/extension"
)

app := forge.NewApp(forge.AppConfig{
	Name:        "accounts",
	HTTPAddress: ":8080",
	Extensions: []forge.Extension{
		groveext.New(
			groveext.WithDSN("postgres", "postgres://localhost:5432/accounts?sslmode=disable"),
			groveext.WithMigrations(Migrations),
			groveext.WithHook(TenantScope{}, hook.Scope{Tables: []string{"orders"}}),
		),
	},
})

err := app.Router().GET("/users/count", func(ctx forge.Context) error {
	db, err := forge.Inject[*grove.DB](app.Container())
	if err != nil {
		return err
	}

	n, err := pgdriver.Unwrap(db).NewSelect((*User)(nil)).Count(ctx.Context())
	if err != nil {
		return err
	}

	return ctx.JSON(http.StatusOK, map[string]int64{"users": n})
})
```

The extension doesn't import any driver itself. You import the driver you want (blank, or by name as above), plus its `<name>migrate` package if you have migrations. Otherwise `WithDSN` fails with an unknown-driver error, or migrations fail with no executor.

You get tracing and metrics for free. Every query runs as a client span (`SELECT users`, `COMMIT`) under the request or job that issued it, and `/_/metrics` gets `db_client_operation_duration_seconds`, labelled by system, operation, table and error type. Outside Forge, pass `grove.WithMetrics(m)` to `grove.Open`. The spans go to whatever OpenTelemetry tracer provider is installed. Grove never records the query text or a bound value. See [tracing and metrics](https://www.xraph.com/docs/grove/integration/observability).

The extension implements `forge.MigratableExtension`, but a plain `app.Run()` doesn't apply migrations. Run your app through Forge's `cli.NewAppRunner` to get `migrate up`, `migrate down` and `migrate status` commands, and call `AutoMigrate()` on the runner if you want pending migrations applied before the server starts.

### Configuration

The extension reads `extensions.grove` from your Forge config, then falls back to a top-level `grove` key. YAML wins for the driver and DSN, and options you pass in code fill whatever YAML leaves empty.

```yaml
extensions:
  grove:
    driver: postgres
    dsn: postgres://localhost:5432/accounts?sslmode=disable
    lock_timeout: 30s
```

| Key | Option | What it does |
| --- | --- | --- |
| `driver`, `dsn` | `WithDSN(driver, dsn)`, or `WithDriver(drv)` for an opened driver | The single database to open |
| `databases` (list of `name`, `driver`, `dsn`) | `WithDatabaseDSN`, `WithDatabase` | Several named databases, each its own `*grove.DB`, managed by a `DBManager` |
| `default` | `WithDefaultDatabase(name)` | Which named database is the default |
| `disable_migrate` | `WithDisableMigrate()` | Makes `Migrate` and `Rollback` return an error |
| `lock_timeout` | `WithLockTimeout(d)` | How long migrations wait for the lock. `0` uses the migrate default, negative waits for the context deadline |
| `central_migrations` | `WithCentralMigrations()` | Hands the migration groups to a shared registry that runs every extension's migrations in one ordered pass. Pair it with `CentralMigrations` in the Forge `AppConfig` |
| `disable_routes` | `WithDisableRoutes()` | Skips the CRDT sync routes |
| `base_path` | `WithBasePath(path)` | Prefix for the CRDT sync routes, `/sync` by default |

`WithCRDT(plugin, scope)`, `WithCRDTEntity`, `WithSyncer` and `WithDriverFactory` are code-only. With `WithCRDT`, the extension mounts `POST /sync/pull`, `POST /sync/push`, `GET /sync/stream` (SSE) and `/sync/ws` (WebSocket) under the base path, plus presence, history and room routes. The [Forge extension guide](https://www.xraph.com/docs/grove/integration/forge-extension) and [configuration reference](https://www.xraph.com/docs/grove/integration/configuration) go deeper.

## Benchmarks

All benchmarks run against in-memory SQLite, comparing Grove with raw `database/sql`, [bun](https://github.com/uptrace/bun) and [GORM](https://gorm.io). You don't need any external services. Run them yourself with `make bench`, and `make bench-update` rewrites the tables below and the [benchmarks page](https://www.xraph.com/docs/grove/concepts/benchmarks).

<details>
<summary>Benchmark tables: insert, select, update, delete, bulk insert, query building and schema cache</summary>

<!-- BENCH:START -->
> Benchmarks generated on 2026-02-22 with go1.25.7 on darwin/arm64. Each benchmark ran 5 times; values are averages.

### Insert

| Library | ns/op | B/op | allocs/op | vs Raw SQL |
|---------|------:|-----:|----------:|-----------:|
| Raw SQL | 4,015 | 880 | 20 | baseline |
| Grove | 4,381 | 1,283 | 28 | +9.1% |
| Bun | 8,459 | 5,470 | 27 | +110.7% |
| GORM | 10,265 | 4,954 | 66 | +155.7% |

### SelectOne

| Library | ns/op | B/op | allocs/op | vs Raw SQL |
|---------|------:|-----:|----------:|-----------:|
| Raw SQL | 4,747 | 1,096 | 39 | baseline |
| Grove | 5,575 | 1,458 | 34 | +17.4% |
| Bun | 6,695 | 5,944 | 43 | +41.0% |
| GORM | 8,070 | 4,383 | 81 | +70.0% |

### SelectMulti

| Library | ns/op | B/op | allocs/op | vs Raw SQL |
|---------|------:|-----:|----------:|-----------:|
| Raw SQL | 58,079 | 20,984 | 388 | baseline |
| Grove | 45,686 | 23,413 | 584 | -21.3% |
| Bun | 67,184 | 23,192 | 395 | +15.7% |
| GORM | 83,217 | 28,570 | 765 | +43.3% |

### Update

| Library | ns/op | B/op | allocs/op | vs Raw SQL |
|---------|------:|-----:|----------:|-----------:|
| Raw SQL | 2,821 | 520 | 13 | baseline |
| Grove | 3,617 | 1,010 | 21 | +28.2% |
| Bun | 4,074 | 5,205 | 19 | +44.4% |
| GORM | 5,468 | 4,020 | 49 | +93.8% |

### Delete

| Library | ns/op | B/op | allocs/op | vs Raw SQL |
|---------|------:|-----:|----------:|-----------:|
| Raw SQL | 3,479 | 232 | 8 | baseline |
| Grove | 5,083 | 553 | 13 | +46.1% |
| Bun | 5,147 | 4,880 | 12 | +47.9% |
| GORM | 6,660 | 2,856 | 36 | +91.4% |

### BulkInsert100

| Library | ns/op | B/op | allocs/op | vs Raw SQL |
|---------|------:|-----:|----------:|-----------:|
| Raw SQL | 151,006 | 60,141 | 1524 | baseline |
| Grove | 105,572 | 42,111 | 1421 | -30.1% |
| Bun | 143,532 | 30,389 | 224 | -4.9% |
| GORM | 187,914 | 93,706 | 1251 | +24.4% |

### BulkInsert1000

| Library | ns/op | B/op | allocs/op | vs Raw SQL |
|---------|------:|-----:|----------:|-----------:|
| Raw SQL | 1,401,957 | 605,384 | 16513 | baseline |
| Grove | 967,190 | 409,753 | 14021 | -31.0% |
| Bun | 1,359,612 | 408,694 | 2033 | -3.0% |
| GORM | 1,782,186 | 894,166 | 12051 | +27.1% |

### BuildSelect

| Variant | ns/op | B/op | allocs/op |
|---------|------:|-----:|----------:|
| Grove | 453 | 864 | 11 |
| Bun | 825 | 1,744 | 17 |

### BuildInsert

| Variant | ns/op | B/op | allocs/op |
|---------|------:|-----:|----------:|
| Grove | 842 | 930 | 19 |
| Bun | 1,012 | 1,425 | 17 |

### BuildUpdate

| Variant | ns/op | B/op | allocs/op |
|---------|------:|-----:|----------:|
| Grove | 380 | 736 | 11 |
| Bun | 597 | 1,184 | 14 |

### SchemaCache

| Variant | ns/op | B/op | allocs/op |
|---------|------:|-----:|----------:|
| CacheHit | 11 | 0 | 0 |
| ColdStart | 3,123 | 3,728 | 75 |

### TagResolution

| Variant | ns/op | B/op | allocs/op |
|---------|------:|-----:|----------:|
| GroveTags | 3,101 | 3,728 | 75 |
| BunFallback | 3,398 | 3,736 | 74 |

<!-- BENCH:END -->

</details>

## Documentation

| Topic | Docs |
| --- | --- |
| Install and run a first query | [Getting started](https://www.xraph.com/docs/grove/getting-started), [basic CRUD](https://www.xraph.com/docs/grove/guides/basic-crud) |
| Define models and tags | [Models](https://www.xraph.com/docs/grove/concepts/models), [dual tag system](https://www.xraph.com/docs/grove/concepts/dual-tags) |
| Move an app off bun | [Migrating from bun](https://www.xraph.com/docs/grove/guides/bun-migration) |
| Change your schema | [Migrations](https://www.xraph.com/docs/grove/migrations/overview) |
| Stream large result sets | [Streaming](https://www.xraph.com/docs/grove/streaming/overview), [change feeds](https://www.xraph.com/docs/grove/streaming/changefeed) |
| Sync data between nodes and clients | [CRDT](https://www.xraph.com/docs/grove/crdt), [TypeScript client](https://www.xraph.com/docs/grove/crdt/typescript-client) |

The exported Go API is on [pkg.go.dev](https://pkg.go.dev/github.com/xraph/grove). The docs source lives in [`docs/`](docs/) in this repository.

## FAQ

### Can I use Grove without Forge?

Yes. The root module depends only on `github.com/xraph/go-utils`, and none of the driver modules import Forge. You need Forge only if you use `extension` or `kv/extension`.

### How do I migrate from bun to Grove?

Keep your models as they are. Grove reads `bun:"..."` tags when a field has no `grove:"..."` tag, so you can switch the query code first and rename tags later, one model at a time. The [bun migration guide](https://www.xraph.com/docs/grove/guides/bun-migration) has the details.

### Does Grove work with MongoDB and Elasticsearch, or only SQL?

Both. `mongodriver` builds BSON filters, find queries and aggregation pipelines, and `esdriver` builds search, aggregation and bulk requests. They share the model layer and hooks with the SQL drivers.

### How do I add multi-tenancy to a Go ORM?

Write a pre-query hook that reads the tenant from the context and returns an extra `WHERE` filter, as in [Query hooks](#query-hooks), and scope it to your tenant tables. A hook that returns `hook.Deny` blocks the query outright.

### Does Grove generate IDs for my models?

No. You declare the key in a tag, such as `grove:"id,pk,autoincrement"`, and pick the Go type yourself.

## Contributing

Issues and focused pull requests are welcome on [GitHub](https://github.com/xraph/grove/issues). Every driver and the KV layer are separate Go modules, so run commands from the module you changed, with `GOWORK=off`:

```bash
git clone https://github.com/xraph/grove.git
cd grove
GOWORK=off go test -race -count=1 ./...
(cd drivers/pgdriver && GOWORK=off go test -race -count=1 ./...)
make test-all
golangci-lint run ./...
goimports -l -local github.com/xraph/grove .
```

The Postgres and Redis container tests need a running Docker daemon and skip without one, so check the `-v` output for SKIP lines. Skip `make build`, `make run` and `make dev`: they point at a `cmd/grove` that doesn't exist. [AGENTS.md](AGENTS.md) has the module list, lint rules and the release workflow. Commits follow [Conventional Commits](https://www.conventionalcommits.org/).

## License

Grove is available under the [MIT License](LICENSE).

Built by [Rex Raphael](https://github.com/juicycleff) and [contributors](https://github.com/xraph/grove/graphs/contributors), as part of the [Forge](https://github.com/xraph/forge) forgery stack.
