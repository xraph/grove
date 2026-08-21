# @grove-js/crdt — Copy-on-Write Hardening & Feature Parity (Spec)

**Date:** 2026-08-20
**Package:** `crdt-js` (published as `@grove-js/crdt`)
**Status:** Approved for implementation

## Problem

`@grove-js/crdt` has a well-tested CRDT core (358 passing tests) whose defects
cluster at the boundary between that core and its consumers: React, the storage
adapter, and the batch-write API. Every finding below was reproduced against
commit `4c822a0`.

## Confirmed defects

### D1 — `useSyncExternalStore` snapshot instability (severity: critical)

`CRDTStore.resolveDocument()` builds a fresh object on every call, so
`getDocument()` never returns a referentially stable value:

```
expected { _table: 't', _pk: 'p', f: 1 } to be { ... }   // same value, new reference
```

React compares snapshots with `Object.is`, re-renders on mismatch, and calls
`getSnapshot` again — an unbounded render loop. Affected hooks:
`useDocument`, `useCollection`, `useSet`, `useNestedDocument`, `usePresence`.

`useList` already carries a hand-rolled `useState` + subscription workaround
(`react.tsx:590`) with a comment naming this exact failure mode, so the bug is
known but was only patched at one of six call sites.

`PresenceManager.getPresence` documents the inverse of the truth: *"Returns a
new array on each call for useSyncExternalStore compatibility."*

### D2 — `:` in a primary key silently breaks persistence and notification

`CRDTStore.applyChanges` (`store.ts:791`) rebuilds the affected document key
with `key.split(":", 2)`, so table `docs` + pk `doc:1` resolves to
`("docs", "doc")`. Reproduced: the merge lands in memory, but zero subscribers
fire and the document is never persisted.

```
A2: colon in pk breaks subscriber notification → expected 0 to be 1
```

Composite primary keys (`tenant:42`, URLs) are common. Root cause is the use of
a delimited string as a compound map key.

### D3 — Text fields are invisible to `getDocument`

`resolveDocument`'s switch has no `text` case and falls through to
`state.value`, which is `undefined` for text fields. `getText()` returns
`"hello"` while `getDocument().body` returns `undefined`. `documentResolve`
(nested documents, `merge.ts`) has the same omission.

### D4 — `BatchWriter.incrementCounter` silently discards writes

`CRDTStore.incrementCounter` emits **cumulative per-node totals**, which merge
max-per-node and are therefore idempotent under redelivery. `BatchWriter`
emits a raw delta instead. Reproduced: `increment(5)` then
`batch().incrementCounter(3).commit()` yields **5**, not 8 — the batch write
loses the max-merge. `BatchWriter` also bypasses plugin hooks and undo
recording entirely.

### D5 — `SyncHook` and `PresenceHook` are dead code

`dispatchBeforePull`, `dispatchAfterPull`, `dispatchBeforePush`,
`dispatchAfterPush`, `dispatchBeforePresenceUpdate`, `dispatchOnPresenceEvent`
have **zero** production call sites. They are exported, documented with
worked examples, and unit-tested against the dispatcher directly — so the
tests pass while the feature does nothing. Root cause: sync orchestration
lives inside the React `useCRDT` hook rather than in the store or client.

### D6 — Presence never seeds from the server snapshot

`CRDTClient.getPresence(topic)` exists but nothing calls it. Peers already
present when you join stay invisible until they emit an event, and the same
gap reopens after every stream reconnect.

### D7 — `CRDTStream.connect()` can start two connect loops

The re-entrancy guard tests `_connected`, which stays `false` until response
headers arrive. Two calls in quick succession start two `connectLoop()`s; the
second overwrites `abortController`, leaking the first connection with no way
to abort it.

### D8 — Local writes are quadratic

Every local write deep-clones the whole field state via
`JSON.parse(JSON.stringify(...))` to record undo state
(`captureFieldState`, `store.ts:1060`):

| list appends | wall time |
|---|---|
| 500 | 137 ms |
| 1000 | 520 ms |
| 2000 | 2116 ms |

Clean 4x per doubling. 3000 appends exceeds a 5 s timeout (~5,600 ms). Text
writes are unaffected (2000 inserts in 14 ms) because span coalescing keeps
the cloned state small.

Post-fix measurement (Task 3): 3000 appends run in ~750 ms. The residual cost
is the O(N) record spread in the pure list merge, which the "Accepted limit"
decision below deliberately keeps. Removing it needs a persistent map or a
Map-backed runtime representation (measured floor ~185 ms) — out of scope.

`persistPending()` additionally copies the entire pending array on every
write, and no persist path is debounced.

### D9 — List reads have a hard ceiling around 6,000 elements

`listElements` / `listNodeIds` recurse once per element. A sequentially built
list throws `RangeError: Maximum call stack size exceeded` between 5,000 and
8,000 items — the read becomes impossible, not merely slow.

```
2000 ok / 5000 ok / 8000 FAIL: RangeError
```

### D10 — No client-side tombstone GC

Go has horizon-based compaction (`crdt/compact.go`, with `Compact(before HLC)`
on `RGAListState`, `ORSetState`, `TextState`, and `State`). The client has no
equivalent, so OR-Set `removed` keys and RGA tombstones accumulate without
bound in IndexedDB.

### D11 — No sync outside React

The pull -> apply -> push -> clear cycle exists only inside `useCRDT`
(`react.tsx:133`). Non-React consumers must reimplement it, and any such
reimplementation inherits the hook's race: `clearPendingChanges()` drops
*everything*, including writes made while the push was in flight. Those
writes are lost, not deferred.

### D12 — Test and packaging gaps

`vitest.config.ts` matches only `*.test.ts` under `environment: "node"`, so no
React test can run — which is why D1 survived 358 passing tests.
`tsconfig.json` has no `exclude`, so `dist/__tests__/` is published to npm.
`package.json` declares no `sideEffects: false`.

## Feature gaps versus the Go implementation

- **WebSocket transport** — `crdt/transport_ws.go` exists server-side; the
  client is SSE-only.
- **Tombstone compaction** — `crdt/compact.go` (see D10).
- **Reconnect backoff** — the client retries on a fixed 5 s delay with no
  jitter, so every client retries in lockstep after an outage.
- **Request timeouts and retry** — `pull`/`push` have no timeout and never
  retry, despite `TransportError.retryable === true`.
- **Offline queue** — `CRDTErrorCode.OfflineQueueFull` is defined but unused.
- **Stream liveness detection** — a silently dead socket never reconnects.
- Not in scope for this spec: time travel (`timetravel.go`), validation
  limits (`validation.go`), inspect (`inspect.go`), metrics (`metrics.go`).

## Design decisions

**Copy-on-write state.** `DocumentState` and all nested CRDT state become
immutable. Merge functions never mutate their inputs; they return new objects
that structurally share untouched substructure. This was chosen over a
version-counter cache and over per-field caching because it fixes three
problems at once:

1. Resolved-document caching keys on **object identity** (a `WeakMap` keyed by
   the `DocumentState`), so no version bookkeeping is needed and D1 is fixed by
   construction rather than per-hook.
2. Undo's `previousState` becomes a pointer copy instead of a JSON deep clone,
   which removes the dominant cost in D8.
3. `MergeEvent.local` stops being a live reference that the merge mutates
   underneath plugin authors.

**Accepted limit.** A shallow spread of an N-key record per merge keeps write
cost O(N) asymptotically; only the constant improves (a shallow spread is far
cheaper than a nested JSON clone). Persistent HAMT structures would restore
O(log N) but add a dependency and significant complexity. The O(N) spread is
accepted and pinned with a benchmark; revisit only if profiling demands it.

**Compound map keys.** Delimited string keys (`"table:pk"`) are replaced with
nested maps, which fixes D2 by construction rather than by escaping.

## Non-goals

- Server-side (Go) changes. This spec covers `crdt-js` only.
- Time travel, validation limits, inspect, and metrics parity.
- Changing the wire format. All types in `types.ts` mirror Go JSON tags and
  must stay byte-compatible.

## Success criteria

- All 358 existing tests still pass, unmodified except where they assert
  behavior this spec deliberately changes.
- React hooks render under `@testing-library/react` without loop warnings.
- 3000 list appends complete in roughly 750 ms, down from ~5,600 ms
  (measured 7.5x improvement), pinned by a regression test with a 2000 ms
  ceiling. NOT under 250 ms: that figure was an unvalidated guess that
  contradicted this spec's own "Accepted limit" decision, which keeps write
  cost O(N) per merge and improves only the constant. Corrected 2026-08-20
  after Task 3 measured the real floor.
- A 50,000-element list resolves without throwing.
- `SyncHook` and `PresenceHook` have live production call sites proven by test.
- A WebSocket transport passes the same conformance suite as `HttpTransport`.
