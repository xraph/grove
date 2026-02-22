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
| Raw SQL | 4,105 | 880 | 20 | baseline |
| Grove | 7,923 | 4,651 | 101 | +93.0% |
| Bun | 8,480 | 5,470 | 27 | +106.6% |
| GORM | 10,612 | 4,954 | 66 | +158.5% |

### SelectOne

| Library | ns/op | B/op | allocs/op | vs Raw SQL |
|---------|------:|-----:|----------:|-----------:|
| Raw SQL | 4,788 | 1,096 | 39 | baseline |
| Bun | 6,721 | 5,944 | 43 | +40.4% |
| GORM | 8,610 | 4,383 | 81 | +79.8% |
| Grove | 9,073 | 4,758 | 105 | +89.5% |

### SelectMulti

| Library | ns/op | B/op | allocs/op | vs Raw SQL |
|---------|------:|-----:|----------:|-----------:|
| Raw SQL | 59,297 | 20,984 | 388 | baseline |
| Grove | 49,411 | 26,618 | 654 | +-16.7% |
| Bun | 67,134 | 23,192 | 395 | +13.2% |
| GORM | 84,350 | 28,572 | 765 | +42.3% |

### Update

| Library | ns/op | B/op | allocs/op | vs Raw SQL |
|---------|------:|-----:|----------:|-----------:|
| Raw SQL | 2,875 | 520 | 13 | baseline |
| Bun | 4,124 | 5,205 | 19 | +43.5% |
| GORM | 5,678 | 4,020 | 49 | +97.5% |
| Grove | 7,272 | 4,233 | 93 | +153.0% |

### Delete

| Library | ns/op | B/op | allocs/op | vs Raw SQL |
|---------|------:|-----:|----------:|-----------:|
| Raw SQL | 3,253 | 232 | 8 | baseline |
| Bun | 5,936 | 4,880 | 12 | +82.5% |
| GORM | 7,641 | 2,856 | 36 | +134.9% |
| Grove | 10,153 | 3,773 | 83 | +212.2% |

### BulkInsert100

| Library | ns/op | B/op | allocs/op | vs Raw SQL |
|---------|------:|-----:|----------:|-----------:|
| Raw SQL | 173,374 | 60,139 | 1524 | baseline |
| Bun | 146,827 | 30,389 | 224 | +-15.3% |
| GORM | 190,825 | 93,710 | 1251 | +10.1% |
| Grove | 295,499 | 70,274 | 702 | +70.4% |

### BulkInsert1000

| Library | ns/op | B/op | allocs/op | vs Raw SQL |
|---------|------:|-----:|----------:|-----------:|
| Raw SQL | 1,418,950 | 605,399 | 16513 | baseline |
| Bun | 1,381,409 | 408,697 | 2033 | +-2.6% |
| GORM | 1,800,682 | 894,169 | 12051 | +26.9% |
| Grove | 12,306,127 | 738,188 | 6115 | +767.3% |

### BuildSelect

| Variant | ns/op | B/op | allocs/op |
|---------|------:|-----:|----------:|
| Bun | 851 | 1,744 | 17 |
| Grove | 3,413 | 4,068 | 81 |

### BuildInsert

| Variant | ns/op | B/op | allocs/op |
|---------|------:|-----:|----------:|
| Bun | 1,021 | 1,425 | 17 |
| Grove | 4,047 | 4,298 | 92 |

### BuildUpdate

| Variant | ns/op | B/op | allocs/op |
|---------|------:|-----:|----------:|
| Bun | 614 | 1,184 | 14 |
| Grove | 3,385 | 3,956 | 83 |

### SchemaCache

| Variant | ns/op | B/op | allocs/op |
|---------|------:|-----:|----------:|
| CacheHit | 11 | 0 | 0 |
| ColdStart | 3,009 | 3,456 | 73 |

### TagResolution

| Variant | ns/op | B/op | allocs/op |
|---------|------:|-----:|----------:|
| GroveTags | 3,023 | 3,456 | 73 |
| BunFallback | 3,410 | 3,464 | 72 |

<!-- BENCH:END -->

## Documentation

Full documentation available in the [docs](docs/) directory.

## License

MIT
