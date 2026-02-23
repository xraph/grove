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

## Documentation

Full documentation available in the [docs](docs/) directory.

## License

MIT
