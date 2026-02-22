# Grove

A polyglot Go ORM that generates native query syntax per database.

## Features

- **Native Query Syntax** -- Each driver generates queries in its database's native idiom
- **Dual Tag System** -- `grove:"..."` tags with `bun:"..."` fallback for zero-cost migration
- **Near-Raw Performance** -- Zero reflection at query time, pooled buffers, cached metadata
- **Modular Migrations** -- Go-code migrations with multi-module dependency ordering
- **Privacy Hooks** -- Pre/post query hooks for tenant isolation, PII redaction, and audit logging
- **Streaming** -- Server-side cursor streaming with per-row hooks

## Supported Databases

- **PostgreSQL** -- Native `$1` placeholders, `DISTINCT ON`, `FOR UPDATE`, JSONB operators
- **SQLite** -- Lightweight embedded database with `INSERT OR REPLACE`
- **MySQL** -- Backtick quoting, `ON DUPLICATE KEY UPDATE`, `USE INDEX` hints _(planned)_
- **MongoDB** -- Native BSON filter documents, aggregation pipelines _(planned)_

## Quick Start

```go
// Create and open the driver, then pass it to Grove
pgdb := pgdriver.New()
pgdb.Open(ctx, "postgres://user:pass@localhost/mydb", driver.WithPoolSize(20))
db, _ := grove.Open(pgdb)

// Access the typed PG query builder via Unwrap
pg := pgdriver.Unwrap(db)
var users []User
err := pg.NewSelect(&users).
    Where("email ILIKE $1", "%@example.com").
    Where("role = $2", "admin").
    OrderExpr("created_at DESC").
    Limit(50).
    Scan(ctx)
```

## Benchmarks

All benchmarks run on SQLite in-memory databases. No external services required.

Run locally: `make bench`

<!-- BENCH:START -->
_Run `make bench-update` to generate benchmark results._
<!-- BENCH:END -->

## Documentation

Full documentation available in the [docs](docs/) directory.

## License

MIT
