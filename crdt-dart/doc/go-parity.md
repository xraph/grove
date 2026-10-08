# Where grove_crdt follows Go instead of crdt-js

`grove_crdt` started as a port of `crdt-js`. Wherever the two disagree with the Go `crdt` package, Dart does what Go does, because the Go server is the one your data lives on. If you read the Go code and wonder why the Dart client behaves the way it does, start here.

1. Field HLC after applying a counter, set, list or document change: Go keeps the newer of local and change HLC (`MergeField`, `pickNewer`). crdt-js stamps the change HLC, so a redelivered older change regresses the field clock.
2. Nested document state merge: Go merges same-type paths with `MergeField`, so counters, sets and text merge. crdt-js merges only list and document sub-states and resolves every other same-type pair by higher HLC.
3. List insert with a zero `node_id`: Go uses the change HLC as the node id. crdt-js requires a truthy `node_id`, and since Go always emits the zero struct, crdt-js inserts under the zero key.
4. Set element keys: Go keys by the raw element bytes as sent, and by `json.Marshal` (sorted object keys, with `<`, `>`, `&`, U+2028 and U+2029 escaped) for its own adds. crdt-js uses `JSON.stringify`, so an element such as `"a<b"` gets a different key and a crdt-js remove cannot match a Go add. Go sorts elements by bytes; crdt-js sorts by UTF-16 units.
5. Record delete: crdt-js pushes `field: ""` with `crdt_type: "lww"`. Go stores a `_tombstone` row and emits `crdt_type: ""`, which Go's own `ValidateChangeRecord` would reject if relayed.
6. Tombstones: Go `MergeState` lets a tombstone win only when it is newer than every field write. Go server storage (`ReadState`) and crdt-js treat tombstones as sticky. Dart applies changes sticky, matching what the server serves, and ports `MergeState` exactly for state-level merges.
7. Undo: crdt-js restores the previous field state locally and pushes nothing, so the next pull brings the undone value back. Dart undo emits compensating changes.
8. WebSocket keep-alive: the Go client pings every 30 seconds and crdt-js never pings. crdt-js routes `error` replies to the pending request, while the Go client leaves them hanging. Dart pings and routes errors.
9. HTTP retry: crdt-js retries every 5xx twice, including a deterministic hook rejection. Dart classifies the rejection first.
10. Multi-table pull: crdt-js pulls every table in one request and keeps one watermark, which skips rows behind the 10,000-row page limit (see below). Dart pulls per table.
11. Document resolution: Go `DocumentCRDTState.Resolve` splits dotted paths into nested maps, and crdt-js `documentResolve` returns a flat map keyed by full path. Dart follows Go.

## Choices the Dart client makes that a Go reader should know

Go has no opinion on these, or an opinion that does not survive the trip into Dart. You will meet them in the tests.

- A lone surrogate in a string becomes U+FFFD. Go has already decoded it that way by the time it hits a struct, so Dart does the same on the way in and when it compares node ids.
- Text offsets are runes (code points), as in Go, never UTF-16 units. An emoji counts as one.
- Set keys are byte-exact, as in Go. A received element keeps the bytes it arrived with, and Dart's own adds use the `json.Marshal` form.
- When a document holds both a leaf value at a path and nested paths under it, Go walks a map, so which one wins depends on iteration order. Dart applies paths in ascending length with a stable tie-break, so the nested path always wins and the answer never changes between runs.
- A push is marked rejected only when the server's text says so (a validation, drift, batch-size or hook message under a 400, 422, 500 or WebSocket error frame). A 401, 403, 408, 429, 413 or any other 5xx is never read as a verdict on a change. Those changes stay pushable and the engine retries later.
- Plugin hooks fail closed. A `beforeWrite`, `beforeMerge`, `beforePull`, `beforePush` or transform hook that throws cancels the operation, and `beforePersist` and `afterHydrate` rethrow, so plaintext is never stored and ciphertext is never served. crdt-js lets the exception escape instead.

## Server behaviour clients must live with

Where grove v1.7.0 differs from the pending branch `fix/crdt-sync-defects`, the line says "grove v1.7.0 behaviour" and names the change. The Dart tests pass against both.

- A pull reads each table with its own page of 10,000 rows and reports one `latest_hlc`, the highest across tables. The cursor is node-blind: rows are compared by `(timestamp, counter)` and the node id is ignored. grove v1.7.0 behaviour: `fix/crdt-sync-defects` cuts the window at the earliest full page.
- A change pushed with an older stamp than a device has already synced past is never delivered to that device. Pull returns rows whose stored HLC is past the cursor, and the server merges an older stamp into a set, list, text or document row without moving its HLC. `fix/crdt-sync-defects` does not change this. The skipped case in `test/conformance/convergence_test.dart` reproduces it with `--run-skipped`.
- WebSocket `subscribe` streams from HLC zero, one `change` message per record, whatever you already hold.
- The WebSocket server never answers `presence_get` or `unsubscribe`. Both come back as an `error` with "unknown message type".
- A hook rejection fails the whole push with a 500 after the earlier changes of that batch were already merged. grove v1.7.0 behaviour: `fix/crdt-sync-defects` runs every hook before merging anything. The engine bisects to the one refused change, so it ends in the same place on either server.
- A hook that returns nil drops its change without a word. The only trace is `merged` being lower than the number of changes you sent.
- Drift validation is absolute, past and future. grove v1.7.0 behaviour: a change stamped more than `MaxHLCDrift` (one hour by default) in the past is refused, so a device that edited offline for days gets its changes re-stamped. `fix/crdt-sync-defects` refuses only future-dated changes and accepts old ones as they are.
- foundry's stream ignores `since` and always starts from HLC zero.
- The presence channel is shared by every SSE consumer, so each presence event reaches only one stream.
- The SSE stream of the grove Forge extension sends no keep-alive at v1.7.0. The Dart stream expects a comment line every 15 seconds and reconnects after 45 seconds of silence, so against a v1.7.0 server a quiet stream reconnects quietly every 45 seconds (an idle disconnect, then an idle connect, and no error event). Fixed in `extension/` on `feat/crdt-dart` (Task 21, commit 11453d9): the extension's stream now sends a comment every 15 seconds while idle. `streamKeepAlive` in `crdt/server.go` is still set and never read, so a server built on `crdt.Server` directly stays silent. The conformance server sends the comment, as the extension now does.
