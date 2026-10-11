# Working in grove

Grove is a polyglot Go ORM. Each driver builds queries in its own database's syntax, and the repo also carries a KV store layer, a CRDT sync layer and Forge extensions for both. It sits at the bottom of the xraph forgery stack, so when you change an exported API here you're changing it for almost every other repo we ship, and none of them see the change until they re-pin.

## Modules

There are 18 go.mod files. Release tags go on 16 of them: the root plus 15 sub-modules.

| Module | Directory | Release tag |
|---|---|---|
| `github.com/xraph/grove` | `.` | `vX.Y.Z` |
| `github.com/xraph/grove/drivers/<name>` | `drivers/<name>/` | `drivers/<name>/vX.Y.Z` |
| `github.com/xraph/grove/extension` | `extension/` | `extension/vX.Y.Z` |
| `github.com/xraph/grove/kv` | `kv/` | `kv/vX.Y.Z` |
| `github.com/xraph/grove/kv/extension` | `kv/extension/` | `kv/extension/vX.Y.Z` |
| `github.com/xraph/grove/kv/drivers/<name>` | `kv/drivers/<name>/` | `kv/drivers/<name>/vX.Y.Z` |
| `github.com/xraph/grove/bench` | `bench/` | not tagged |
| `github.com/xraph/grove/crdt-dart/tool/conformance_server` | `crdt-dart/tool/conformance_server/` | not tagged |

The seven database drivers are `pgdriver`, `mysqldriver`, `sqlitedriver`, `mongodriver`, `tursodriver`, `clickhousedriver` and `esdriver`. The five KV drivers are `badgerdriver`, `boltdriver`, `dynamodriver`, `memcacheddriver` and `redisdriver`.

Outside Go, `crdt-js/` is the TypeScript client (`@grove-js/crdt` on npm), `crdt-dart/` is the Dart client (`grove_crdt` on pub.dev) and `docs/` is the Fumadocs site.

## Build, test, lint

Run Go commands with `GOWORK=off`. CI has no workspace. A local `go.work` can quietly change what resolves, and then you're testing code that CI will never build. Also, `./...` stops at module boundaries, so a command at the root covers the root module only and you have to repeat it from each sub-module's directory.

```sh
GOWORK=off go build ./...
GOWORK=off go test -race -count=1 ./...
(cd drivers/pgdriver && GOWORK=off go test -race -count=1 ./...)
golangci-lint run ./...
goimports -l -local github.com/xraph/grove .   # CI fails if this prints anything
```

| Target | What it does |
|---|---|
| `make test` | `go test -v ./...` on the root module, without the `-race` CI uses |
| `make test-all` | root, every driver, every KV driver, `extension` and `kv` |
| `make vet` / `make vet-all` | `go vet` on the root, or the root plus drivers, KV drivers, `extension`, `bench` and `kv` |
| `make lint` / `make lint-all` | golangci-lint on the root, or the root plus drivers, KV drivers, `extension` and `kv` |
| `make lint-fix` | golangci-lint with `--fix` on the root module |
| `make fmt` / `make fmt-all` | `gofmt -s -w` plus `goimports -local github.com/xraph/grove` |
| `make check` / `make check-all` | fmt, vet and lint in one go |
| `make coverage` | writes `coverage.out` for the root module |
| `make bench` | benchmarks in `bench/` against bun and GORM on in-memory SQLite |
| `make docs` / `make docs-build` | `pnpm install`, then `pnpm dev` or `pnpm build` in `docs/` |

Skip `make build`, `make build-all`, `make run`, `make install`, `make dev` and `make all`. They point at `./cmd/grove`, which doesn't exist. `make tidy-all` misses `kv/extension` and the conformance server, so use `./updatemod.sh` when you tidy.

golangci-lint is v2, configured in `.golangci.yml` at the root, and a run inside a sub-module picks up the same file by walking up the tree. CI runs the latest v2 release (with `--timeout 10m`) from the repo root and nowhere else. So CI lints the root module only. You'll see sub-module lint failures when you run `make lint-all`, not before.

For each module in `.github/workflows/ci.yml`, CI runs `go mod tidy` and fails on any go.mod or go.sum diff, then builds and runs `go test -race -count=1 ./...`. `extension/` is built but its tests never run in CI, and `kv/extension/` isn't in ci.yml at all (only the release workflow builds it), so if you touch either one, run its tests yourself.

### Tests that need Docker

No test reads a database DSN from the environment. The container-backed tests start their own servers through testcontainers-go, and you need a running Docker daemon for them:

- `drivers/pgdriver/listen_test.go` and `drivers/pgdriver/pgmigrate/executor_test.go` (Postgres)
- `kv/drivers/redisdriver/collections_test.go` and `kv/drivers/redisdriver/namespace_test.go` (Redis)

They call `t.Skip` under `-short` and `t.Skipf("container runtime unavailable: ...")` when Docker can't be reached. That means the run still goes green without Docker, so if you changed pgdriver, pgmigrate or redisdriver, read the `-v` output for SKIP lines before you call the change tested.

The other driver tests (MySQL, MongoDB, ClickHouse, Elasticsearch, Turso) cover query builders and dialects with no live server. `sqlitedriver` opens real database files under `t.TempDir()`. The badger, bolt, dynamo and memcached KV drivers have no tests.

## Layout

Root module:

- `grove.go`, `options.go`, `errors.go`, `registry.go`, `model.go`: the `DB` handle and `Open`, functional options, sentinel errors, the driver registry (`RegisterDriver`, `OpenDriver`) and `BaseModel`.
- `driver/`: the interfaces every backend implements (`Driver`, `Dialect`, `Tx`, `Rows`, `Result`). Don't confuse it with `drivers/`, which holds the implementations.
- `schema/`: struct tag parsing (`grove:"..."` with a `bun:"..."` fallback) and cached table, field and relation metadata.
- `scan/`: scanning rows into structs.
- `hook/`: the hook engine and the pre/post query, mutation and stream-row hook interfaces.
- `migrate/`: Go-code migrations, groups with `DependsOn`, the migrator, the planner and the migration lock.
- `stream/`: the generic `Stream[T]` iterator, its transforms and changefeeds.
- `crdt/`: CRDT field types, the sync server and transports, presence and rooms.
- `audit/`, `observability/`, `plugin/`: hooks for audit logs and query metrics, and the plugin registry.
- `grovetest/`: a mock driver and dialect, fixtures and query assertions.
- `internal/`: `pool` (byte buffers), `safe` (identifier quoting) and `tagparser`.

Everything else:

- `drivers/<name>/`: one module per database. All but `esdriver` have a `<name>migrate/` package with the migration executor.
- `kv/`: the KV `Store`, the `kv/driver` interfaces, `codec`, `keyspace`, `middleware`, `plugins` and `crdt`. `kv/kvtest/` has a mock driver and the conformance suite.
- `extension/` and `kv/extension/`: the Forge extensions.
- `bench/`: benchmarks, plus `bench/cmd/benchreport`, which `make bench-update` uses to rewrite the README tables.
- `docs/content/docs/`: the documentation pages.

The root `grove` package imports only `hook`. `driver` imports `schema`, `migrate` imports `driver`, and `schema` must never import the root (it recognises `BaseModel` by package path). Break that order and you get an import cycle.

## Conventions

### Errors and IDs

Sentinel errors are package-level `errors.New` values whose message starts with the package name: `grove: no rows in result set`, `kv: key not found`. When you wrap, use `%w` and your own package's prefix, as in `fmt.Errorf("pgdriver: ...: %w", err)`, and match with `errors.Is`. errname wants `ErrXxx` for sentinels and `XxxError` for error types. errorlint rejects `==` on errors and wrapping without `%w`.

Grove doesn't generate IDs. A model declares its key in a tag (`grove:"id,pk,autoincrement"`) and the caller picks the Go type. Keep ID libraries out of the root module, whose only non-test dependency is `github.com/xraph/go-utils`.

### Drivers

Every driver module has the same shape, and a new one should copy it. `New()` returns an unconnected handle and `Open(ctx, dsn, ...driver.Option)` connects it. `register.go` calls `grove.RegisterDriver` from `init()` under the names `pg` and `postgres`, `mysql`, `sqlite`, `mongo` and `mongodb`, `turso`, `clickhouse` and `elasticsearch`. `unwrap.go` exports `Unwrap(*grove.DB)`, which panics on the wrong driver type. Compile-time checks like `var _ driver.Driver = (*PgDB)(nil)` sit next to the type, and optional features are separate interfaces found by type assertion (`driver.StreamCapable`, `driver.Preparer`, and `BatchDriver`, `TTLDriver`, `ScanDriver` on the KV side).

KV drivers register with `kv.RegisterDriver` the same way, except `dynamodriver`, which has no `register.go`. If you write a KV driver, run it through `kvtest.RunConformanceSuite`.

### Options and logging

Configuration is functional options everywhere: `grove.Option`, `driver.Option`, `extension.ExtOption`, each set by a `WithXxx` function on top of a struct of defaults. The root options drop values they can't use (`WithPoolSize` ignores `n <= 0`).

Logging goes through `github.com/xraph/go-utils/log`. If your code logs, take a `log.Logger` through a `WithLogger` option, default it to `log.NewNoopLogger()` and pass fields as `log.String(...)`. Library code doesn't use `slog` or `fmt.Print`.

### Forge extensions

`extension.Extension` embeds `*forge.BaseExtension`, asserts `var _ forge.Extension = (*Extension)(nil)` and is built with `New(opts ...ExtOption)`. `Register(forge.App)` loads YAML from `extensions.grove` (falling back to `grove`), merges it with the programmatic options and provides the `*grove.DB` through vessel. `Init`, `Start`, `Stop` and `Health` cover the rest of the lifecycle. `kv/extension` has the same shape and reads `extensions.grove_kv`.

Neither extension module imports a driver module, and that's deliberate. Callers blank-import the driver they want or pass `WithDriver` or `WithDriverFactory`, which keeps forge out of the drivers and the drivers out of the extensions.

### Lint rules that bite

- errcheck has `check-blank` and `check-type-assertions` on, so `_ = f()` and a bare `v := x.(T)` both fail. Use comma-ok, or put `//nolint:errcheck // reason` on the line. `Close` on `io.Closer` and `*sql.Rows`, and pgx `Tx.Rollback`, are exempt.
- govet runs every analyzer except `fieldalignment`. That includes `shadow`, so an inner `err :=` that hides an outer `err` gets flagged.
- revive's `exported` rule catches stutter like `grove.GroveDriver`. The existing ones carry `//nolint:revive // ... established public API name`. Don't add new names that need one.
- A nolint names its linter and gives a reason. nolintlint reports directives that suppress nothing, and gosec, errcheck and gocritic are already off in `_test.go` files, so a nolint for any of them in a test file is dead.
- revive also wants lowercase error strings with no trailing punctuation, `_` for unused parameters and `ctx context.Context` as the first parameter. noctx wants context-aware HTTP and SQL calls.
- gosec skips G104, G117, G304, G402 and G704. Files matching `*_gen.go` are excluded from every linter.
- Imports go in three groups: standard library, third party, then `github.com/xraph/grove/...`.

Commit messages follow Conventional Commits, scoped where it helps: `fix(crdt): ...`, `fix(kv): ...`, `build: ...`, `ci: ...`, `docs(crdt-js): ...`.

## Dependencies

Grove is the base layer. Upstream, the root module requires only `github.com/xraph/go-utils` (`kv` and the conformance server require it too), and `extension` and `kv/extension` also require `github.com/xraph/forge` and `github.com/xraph/vessel`. Downstream, nearly every forgery repo pins grove and its drivers: chronicle, relay, keysmith, ledger, sentinel, shield, vault, warden, nexus, bastion, herald, dispatch, trove, weave, authsome and cortex, plus fabriq (`github.com/xraph/fabriq`). Forge's own `github.com/xraph/forge/models` sub-module requires grove too.

To bump an xraph dependency, run `go get` in each module that requires it, then tidy all of them:

```sh
GOWORK=off go get github.com/xraph/go-utils@vX.Y.Z   # repeat in kv/ and crdt-dart/tool/conformance_server/
(cd extension && GOWORK=off go get github.com/xraph/forge@vX.Y.Z)
(cd kv/extension && GOWORK=off go get github.com/xraph/forge@vX.Y.Z)
GOWORK=off ./updatemod.sh                            # go mod tidy in every directory with a go.mod
```

Current versions are in each go.mod. Don't copy them anywhere else.

- Never commit a `go.work` or a `replace` that points at a sibling repo. `go.work` isn't in `.gitignore`, so check `git status` before you commit.
- The `replace` lines that point inside this repo (`github.com/xraph/grove => ../../`, `github.com/xraph/grove/kv => ../`) are intentional. Leave them.
- Sub-modules require their in-repo siblings at a published tag, never `v0.0.0`. Your `replace` is ignored once the module is someone else's dependency, and the `require` line is what they resolve.
- CI fails on any `go mod tidy` diff, so commit go.mod and go.sum for every module you touched.

## Releasing

You cut a release by dispatching `release.yml`. Never push a version tag by hand.

1. Release any upstream xraph module you need first (go-utils, vessel, forge) and bump grove to it.
2. Wait for main CI to go green: `gh run list --workflow ci.yml --branch main --limit 1`.
3. Dispatch the release: `gh workflow run release.yml --ref main -f tag=vX.Y.Z`.
4. Follow it with `gh run list --workflow release.yml --limit 1`, then `gh run watch <run-id>`.

The workflow pushes `vX.Y.Z` and re-verifies all 16 tagged modules (tidy diff, build and `-race` tests, with the two extension modules build-only). It then pushes `drivers/<name>/vX.Y.Z` for the seven drivers, `extension/vX.Y.Z`, `kv/vX.Y.Z`, `kv/extension/vX.Y.Z` and `kv/drivers/<name>/vX.Y.Z` for the five KV drivers, and creates the GitHub release. `bench` and the conformance server don't get tags.

The root tag goes out before verification. If a verify step fails you're left with `vX.Y.Z` on the remote, no sub-module tags beside it, and a Go proxy that may already have cached the root, so fix the problem and release the next patch version. Don't move the tag.

Once grove is out, each downstream repo re-pins (`go get github.com/xraph/grove@vX.Y.Z`, plus every `grove/drivers/...`, `grove/extension` or `grove/kv/...` module it imports) and releases in dependency order.

The clients ship on their own. `gh workflow run release-grove-js.yml --ref main -f version=X.Y.Z` publishes `@grove-js/crdt` from `crdt-js/`; add `-f dry_run=true` to rehearse. `grove_crdt` publishes from a `grove_crdt-v<version>` tag matching `crdt-dart/pubspec.yaml`. pub.dev's automated publishing only trusts a tag push, so that's the one tag you push yourself, and the steps are at the top of `.github/workflows/release-grove-dart.yml`.

## Branches

You can push straight to main. Its ruleset blocks only branch deletion and force-pushes, and there are no required status checks, so nothing stops a red push. CI runs on every push to main and on every pull request against it.
