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
| Raw SQL | 4,095 | 880 | 20 | baseline |
| Grove | 7,891 | 4,651 | 101 | +92.7% |
| Bun | 8,397 | 5,470 | 27 | +105.1% |
| GORM | 10,362 | 4,954 | 66 | +153.1% |

### SelectOne

| Library | ns/op | B/op | allocs/op | vs Raw SQL |
|---------|------:|-----:|----------:|-----------:|
| Raw SQL | 4,685 | 1,096 | 39 | baseline |
| Bun | 6,675 | 5,944 | 43 | +42.5% |
| GORM | 8,116 | 4,383 | 81 | +73.2% |
| Grove | 9,270 | 4,758 | 105 | +97.9% |

### SelectMulti

| Library | ns/op | B/op | allocs/op | vs Raw SQL |
|---------|------:|-----:|----------:|-----------:|
| Raw SQL | 58,687 | 20,984 | 388 | baseline |
| Grove | 49,261 | 26,619 | 654 | -16.1% |
| Bun | 66,327 | 23,192 | 395 | +13.0% |
| GORM | 84,393 | 28,573 | 765 | +43.8% |

### Update

| Library | ns/op | B/op | allocs/op | vs Raw SQL |
|---------|------:|-----:|----------:|-----------:|
| Raw SQL | 2,841 | 520 | 13 | baseline |
| Bun | 4,093 | 5,205 | 19 | +44.1% |
| GORM | 5,534 | 4,020 | 49 | +94.8% |
| Grove | 7,240 | 4,234 | 93 | +154.9% |

### Delete

| Library | ns/op | B/op | allocs/op | vs Raw SQL |
|---------|------:|-----:|----------:|-----------:|
| Raw SQL | 3,305 | 232 | 8 | baseline |
| Bun | 5,966 | 4,880 | 12 | +80.5% |
| GORM | 7,273 | 2,856 | 36 | +120.1% |
| Grove | 9,350 | 3,773 | 83 | +182.9% |

### BulkInsert100

| Library | ns/op | B/op | allocs/op | vs Raw SQL |
|---------|------:|-----:|----------:|-----------:|
| Raw SQL | 156,835 | 60,140 | 1524 | baseline |
| Bun | 147,831 | 30,389 | 224 | -5.7% |
| GORM | 193,320 | 93,709 | 1251 | +23.3% |
| Grove | 277,245 | 70,263 | 702 | +76.8% |

### BulkInsert1000

| Library | ns/op | B/op | allocs/op | vs Raw SQL |
|---------|------:|-----:|----------:|-----------:|
| Raw SQL | 1,427,722 | 605,390 | 16513 | baseline |
| Bun | 1,406,115 | 408,697 | 2033 | -1.5% |
| GORM | 1,820,524 | 894,172 | 12051 | +27.5% |
| Grove | 12,454,872 | 738,931 | 6115 | +772.4% |

### BuildSelect

| Variant | ns/op | B/op | allocs/op |
|---------|------:|-----:|----------:|
| Bun | 875 | 1,744 | 17 |
| Grove | 3,787 | 4,068 | 81 |

### BuildInsert

| Variant | ns/op | B/op | allocs/op |
|---------|------:|-----:|----------:|
| Bun | 1,061 | 1,425 | 17 |
| Grove | 4,005 | 4,298 | 92 |

### BuildUpdate

| Variant | ns/op | B/op | allocs/op |
|---------|------:|-----:|----------:|
| Bun | 630 | 1,184 | 14 |
| Grove | 3,443 | 3,956 | 83 |

### SchemaCache

| Variant | ns/op | B/op | allocs/op |
|---------|------:|-----:|----------:|
| CacheHit | 12 | 0 | 0 |
| ColdStart | 3,101 | 3,456 | 73 |

### TagResolution

| Variant | ns/op | B/op | allocs/op |
|---------|------:|-----:|----------:|
| GroveTags | 3,138 | 3,456 | 73 |
| BunFallback | 3,564 | 3,464 | 72 |

<!-- BENCH:END -->

## Documentation

Full documentation available in the [docs](docs/) directory.

## License

MIT
