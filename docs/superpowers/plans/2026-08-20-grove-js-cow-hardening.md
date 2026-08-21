# @grove-js/crdt Copy-on-Write Hardening — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `@grove-js/crdt` state copy-on-write immutable so React snapshots are stable by construction, fix twelve confirmed defects, and close the WebSocket / backoff / compaction gaps against the Go implementation.

**Architecture:** Three layers change in order. **Layer 1** makes every merge function pure — no input mutation, structural sharing of untouched substructure — which makes a `WeakMap` keyed on `DocumentState` identity a correct resolved-document cache and reduces undo's `previousState` from a JSON deep clone to a pointer copy. **Layer 2** moves the pull/apply/push/clear cycle out of the React hook into a `SyncEngine` in the core, which both fixes the lost-write race on `clearPendingChanges()` and gives the dead `SyncHook` real call sites. **Layer 3** adds the transport and lifecycle features (WebSocket, backoff, timeouts, compaction) on top of the now-stable core.

**Tech Stack:** TypeScript 5 (strict, ES2020, `moduleResolution: bundler`), vitest 3, React 18+ (optional peer), jsdom + `@testing-library/react` for hook tests. No runtime dependencies — the package must stay dependency-free.

**Spec:** `docs/superpowers/specs/2026-08-20-grove-js-cow-hardening.md`

## Global Constraints

- Package root for every path in this plan is `crdt-js/`. Run all commands from there.
- **No runtime dependencies.** Everything added must be devDependencies or peerDependencies. The package currently has zero `dependencies` and must keep zero.
- **The wire format is frozen.** Every type in `src/types.ts` mirrors a Go struct's JSON tags (`crdt/crdt.go`, `crdt/transport.go`). Field names, casing, and optionality must not change. HLC `ts` stays a decimal string on the wire.
- **HLC timestamps are int64.** Never compare or arithmetic them as JS numbers. Use the existing `hlcCompare` / `hlcTsString` / `BigInt` paths in `src/hlc.ts`.
- No `Co-Authored-By` trailers in any commit (user global rule).
- Target stays `ES2020`; do not raise it. `BigInt` literals are already in use and are ES2020.
- All 358 existing tests must keep passing. A test may only be modified when this plan explicitly says which test and why.
- **Transports are pluggable, and must stay that way.** `CRDTClient` accepts any
  `Transport` / `StreamTransport` via config and duck-types streaming support
  with `isStreamTransport` — never `instanceof`. Resilience behavior (retry,
  backoff) must be composable over ANY transport rather than baked into a
  concrete one, so a user-supplied or WebSocket transport gets the same
  guarantees as the built-in HTTP one. Adding a transport must require zero
  changes to `CRDTClient`.
- Public exports in `src/index.ts` are additive only in Phases 0–2. Removals or signature changes require an explicit note in the task.

## Repo / file map

**New files:**
- `src/immutable.ts` — shallow structural-sharing helpers shared by merge and store.
- `src/sync.ts` — `SyncEngine`: the pull/apply/push/clear cycle, extracted from React.
- `src/compact.ts` — horizon-based tombstone compaction, port of `crdt/compact.go`.
- `src/transport/websocket.ts` — `WebSocketTransport` implementing `StreamTransport`.
- `src/backoff.ts` — exponential backoff with full jitter.
- `src/__tests__/setup.ts` — jsdom test setup.
- `src/__tests__/react.test.tsx` — React hook tests (new; no React test exists today).
- `src/__tests__/immutable.test.ts`, `sync.test.ts`, `compact.test.ts`, `websocket.test.ts`, `backoff.test.ts`, `bench.test.ts`.

**Modified files:**
- `src/merge.ts` — purity; iterative list traversal; text in `documentResolve`.
- `src/text.ts` — add non-mutating `applyTextOpTo`.
- `src/store.ts` — immutable writes, nested map keys, identity cache, `BatchWriter` parity, debounced persist.
- `src/presence.ts` — stable snapshots, snapshot seeding, stale-peer pruning.
- `src/react.tsx` — hooks consume stable snapshots; delegate sync to `SyncEngine`.
- `src/stream.ts` — connect guard, backoff, liveness watchdog.
- `src/transport.ts` — timeouts, retry, empty-body tolerance.
- `src/client.ts` — expose `SyncEngine`, seed presence.
- `src/plugin.ts` — no signature changes; `MergeEvent.local` doc clarified.
- `src/index.ts` — export the new surface.
- `vitest.config.ts`, `tsconfig.json`, `package.json` — test harness and packaging.

---

## Phase 0 — Test harness

Nothing else in this plan is provable without this. Defect D1 survived 358
passing tests purely because no React test could run.

### Task 1: jsdom + React Testing Library harness, with a failing D1 test

**Files:**
- Modify: `vitest.config.ts`
- Modify: `package.json` (devDependencies)
- Create: `src/__tests__/setup.ts`
- Create: `src/__tests__/react.test.tsx`

**Interfaces:**
- Consumes: nothing (first task).
- Produces: a working `.tsx` test path. Every later React test file is
  `src/__tests__/*.test.tsx` and is picked up automatically.

- [ ] **Step 1: Install the test dependencies**

```bash
npm install --save-dev jsdom@^26 @testing-library/react@^16 @testing-library/dom@^10 react@^19 react-dom@^19
```

`react` and `react-dom` are devDependencies only — they stay optional
peerDependencies for consumers. Do not move them into `dependencies`.

- [ ] **Step 2: Write the jsdom setup file**

Create `src/__tests__/setup.ts`:

```ts
import { afterEach } from "vitest";
import { cleanup } from "@testing-library/react";

// React 19 reads this to suppress the act() environment warning.
(globalThis as unknown as { IS_REACT_ACT_ENVIRONMENT: boolean })
  .IS_REACT_ACT_ENVIRONMENT = true;

afterEach(() => {
  cleanup();
});
```

- [ ] **Step 3: Widen the vitest config to `.tsx` and add a jsdom project**

Replace `vitest.config.ts` entirely:

```ts
import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    projects: [
      {
        test: {
          name: "node",
          include: ["src/__tests__/**/*.test.ts"],
          environment: "node",
        },
      },
      {
        test: {
          name: "dom",
          include: ["src/__tests__/**/*.test.tsx"],
          environment: "jsdom",
          setupFiles: ["./src/__tests__/setup.ts"],
        },
      },
    ],
  },
});
```

The split matters: the CRDT core tests must keep running under `node` so they
stay honest about not depending on DOM globals.

- [ ] **Step 4: Write the failing React test for D1**

Create `src/__tests__/react.test.tsx`:

```tsx
import { describe, it, expect, vi, afterEach } from "vitest";
import { render, screen, act } from "@testing-library/react";
import { CRDTProvider, useDocument, useCollection } from "../react.js";
import type { Transport, PullResponse, PushResponse } from "../types.js";

const HLC0 = { ts: "0", c: 0, node: "" };

/** Transport that never returns data — isolates hooks from the network. */
const inertTransport: Transport = {
  async pull(): Promise<PullResponse> {
    return { changes: [], latest_hlc: HLC0 };
  },
  async push(): Promise<PushResponse> {
    return { merged: 0, latest_hlc: HLC0 };
  },
};

const config = {
  nodeID: "test-node",
  tables: ["docs"],
  transport: inertTransport,
  streaming: false as const,
  autoSync: false as const,
};

function DocView() {
  const { data } = useDocument<{ title?: string }>("docs", "doc-1");
  return <span data-testid="title">{data?.title ?? "empty"}</span>;
}

function ListView() {
  const { items } = useCollection<{ title?: string }>("docs");
  return <span data-testid="count">{items.length}</span>;
}

describe("React hooks render without snapshot thrash", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("useDocument renders without a getSnapshot loop warning", () => {
    const err = vi.spyOn(console, "error").mockImplementation(() => {});
    render(
      <CRDTProvider config={config}>
        <DocView />
      </CRDTProvider>
    );
    expect(screen.getByTestId("title").textContent).toBe("empty");
    const warned = err.mock.calls.some((c) =>
      String(c[0]).includes("getSnapshot should be cached")
    );
    expect(warned).toBe(false);
  });

  it("useCollection renders without a getSnapshot loop warning", () => {
    const err = vi.spyOn(console, "error").mockImplementation(() => {});
    render(
      <CRDTProvider config={config}>
        <ListView />
      </CRDTProvider>
    );
    expect(screen.getByTestId("count").textContent).toBe("0");
    const warned = err.mock.calls.some((c) =>
      String(c[0]).includes("getSnapshot should be cached")
    );
    expect(warned).toBe(false);
  });
});
```

- [ ] **Step 5: Run the test to verify it fails**

```bash
npx vitest run --project dom
```

Expected: FAIL. React logs `Warning: The result of getSnapshot should be
cached to avoid an infinite loop`, so `warned` is `true`. If instead the test
hangs or the process dies with an out-of-memory error, that is the same defect
presenting as an unbounded loop — treat it as the expected failure and note
which form it took.

- [ ] **Step 6: Confirm the node project is untouched**

```bash
npx vitest run --project node
```

Expected: PASS, 358 tests, exactly as before.

- [ ] **Step 7: Commit**

```bash
git add crdt-js/vitest.config.ts crdt-js/package.json crdt-js/package-lock.json crdt-js/src/__tests__/setup.ts crdt-js/src/__tests__/react.test.tsx
git commit -m "test(crdt-js): add jsdom harness and failing React snapshot tests"
```

---

## Phase 1 — Copy-on-write core

### Task 2: Make merge functions pure

**Files:**
- Create: `src/immutable.ts`
- Create: `src/__tests__/immutable.test.ts`
- Modify: `src/text.ts` (add `applyTextOpTo`)
- Modify: `src/merge.ts` (`mergeFieldState` stops mutating `local`)

**Interfaces:**
- Consumes: existing `mergeSet`, `mergeListState`, `mergeText`, `applyTextOp`.
- Produces:
  - `withEntry<T>(rec: Record<string, T>, key: string, value: T): Record<string, T>`
  - `withoutKeys<T>(rec: Record<string, T>, drop: (k: string) => boolean): Record<string, T>`
  - `applyTextOpTo(state: TextState, op: TextOperation, nodeID: string, clock: HLC): TextState`
  - `mergeFieldState(local, change)` — unchanged signature, now guaranteed not
    to mutate `local` or any object reachable from it.

- [ ] **Step 1: Write the failing purity test**

Create `src/__tests__/immutable.test.ts`:

```ts
import { describe, it, expect } from "vitest";
import { mergeFieldState } from "../merge.js";
import type { ChangeRecord, FieldState } from "../types.js";

const hlc = (n: number) => ({ ts: String(n), c: 0, node: "n2" });

describe("mergeFieldState purity", () => {
  it("does not mutate the local set state", () => {
    const local: FieldState = {
      type: "set", hlc: hlc(1), node_id: "n1",
      set_state: { entries: { '"a"': [{ node: "n1", hlc: hlc(1) }] }, removed: {} },
    };
    const before = JSON.stringify(local);
    const change: ChangeRecord = {
      table: "t", pk: "p", field: "f", crdt_type: "set",
      hlc: hlc(2), node_id: "n2", set_op: { op: "add", elements: ["b"] },
    };
    const merged = mergeFieldState(local, change);
    expect(JSON.stringify(local)).toBe(before);
    expect(merged.set_state).not.toBe(local.set_state);
    expect(Object.keys(merged.set_state!.entries).sort()).toEqual(['"a"', '"b"']);
  });

  it("does not mutate the local list state", () => {
    const nodeId = hlc(1);
    const local: FieldState = {
      type: "list", hlc: hlc(1), node_id: "n1",
      list_state: { nodes: { "HLC{ts:1 c:0 node:n2}": {
        id: nodeId, node_id: "n1", parent_id: { ts: 0, c: 0, node: "" }, value: "x",
      } } },
    };
    const before = JSON.stringify(local);
    const change: ChangeRecord = {
      table: "t", pk: "p", field: "f", crdt_type: "list",
      hlc: hlc(2), node_id: "n2",
      list_op: { op: "insert", node_id: hlc(2), parent_id: nodeId, value: "y" },
    };
    const merged = mergeFieldState(local, change);
    expect(JSON.stringify(local)).toBe(before);
    expect(merged.list_state).not.toBe(local.list_state);
    expect(Object.keys(merged.list_state!.nodes)).toHaveLength(2);
  });

  it("does not mutate the local text state", () => {
    const local: FieldState = {
      type: "text", hlc: hlc(1), node_id: "n1",
      text_state: { frags: {} },
    };
    const before = JSON.stringify(local);
    const change: ChangeRecord = {
      table: "t", pk: "p", field: "f", crdt_type: "text",
      hlc: hlc(2), node_id: "n2",
      text_op: { op: "insert", content: "hi", origin: hlc(2) },
    };
    const merged = mergeFieldState(local, change);
    expect(JSON.stringify(local)).toBe(before);
    expect(merged.text_state).not.toBe(local.text_state);
  });

  it("shares untouched substructure", () => {
    const removed = { "n1:HLC{ts:1 c:0 node:n1}": true };
    const local: FieldState = {
      type: "set", hlc: hlc(1), node_id: "n1",
      set_state: { entries: { '"a"': [{ node: "n1", hlc: hlc(1) }] }, removed },
    };
    const change: ChangeRecord = {
      table: "t", pk: "p", field: "f", crdt_type: "set",
      hlc: hlc(2), node_id: "n2", set_op: { op: "add", elements: ["b"] },
    };
    const merged = mergeFieldState(local, change);
    // An add touches `entries` only; `removed` must be shared, not copied.
    expect(merged.set_state!.removed).toBe(removed);
  });
});
```

- [ ] **Step 2: Run it to verify it fails**

```bash
npx vitest run --project node src/__tests__/immutable.test.ts
```

Expected: FAIL on the first three tests — `mergeFieldState` currently writes
`localSet.entries[key] = ...`, `localList.nodes[key] = ...`, and calls the
mutating `applyTextOp(localText, ...)`, so `JSON.stringify(local)` changes.

- [ ] **Step 3: Write the structural-sharing helpers**

Create `src/immutable.ts`:

```ts
/**
 * Shallow structural-sharing helpers for copy-on-write CRDT state.
 *
 * Every helper returns a NEW container and leaves the input untouched.
 * Untouched substructure is shared by reference, never cloned — that
 * sharing is what makes identity-keyed snapshot caching correct.
 */

/** Record with one key replaced. The other values are shared by reference. */
export function withEntry<T>(
  rec: Record<string, T>,
  key: string,
  value: T
): Record<string, T> {
  return { ...rec, [key]: value };
}

/** Record with every key matching `drop` removed. Survivors are shared. */
export function withoutKeys<T>(
  rec: Record<string, T>,
  drop: (key: string) => boolean
): Record<string, T> {
  const out: Record<string, T> = {};
  for (const k of Object.keys(rec)) {
    if (!drop(k)) out[k] = rec[k];
  }
  return out;
}

/** Array with one element appended. */
export function withAppended<T>(arr: readonly T[] | undefined, value: T): T[] {
  return arr ? [...arr, value] : [value];
}

/** Record with several keys set to `true`. Returns the input if `keys` is empty. */
export function withFlags(
  rec: Record<string, boolean>,
  keys: readonly string[]
): Record<string, boolean> {
  if (keys.length === 0) return rec;
  const out = { ...rec };
  for (const k of keys) out[k] = true;
  return out;
}
```

- [ ] **Step 4: Add the non-mutating text op applier**

Append to `src/text.ts`:

```ts
/**
 * Copy-on-write wrapper around applyTextOp.
 *
 * Fragments are shallow-cloned before the mutating applier runs, so the
 * input state is never touched. Text coalesces sequential typing into one
 * span, so the fragment count stays small and this copy stays cheap.
 */
export function applyTextOpTo(
  state: TextState,
  op: TextOperation,
  nodeID: string,
  clock: HLC
): TextState {
  const frags: Record<string, TextFragment[]> = {};
  for (const key of Object.keys(state.frags)) {
    frags[key] = state.frags[key].map((f) => ({
      ...f,
      ...(f.attrs ? { attrs: { ...f.attrs } } : {}),
    }));
  }
  const next: TextState = { frags };
  applyTextOp(next, op, nodeID, clock);
  return next;
}
```

- [ ] **Step 5: Rewrite the mutating branches of `mergeFieldState`**

In `src/merge.ts`, add to the imports at the top:

```ts
import { withEntry, withoutKeys, withAppended, withFlags } from "./immutable.js";
import { applyTextOpTo } from "./text.js";
```

Replace the `case "set":` body with:

```ts
    case "set": {
      const localSet = local?.set_state ?? newORSetState();
      let entries = localSet.entries;
      let removed = localSet.removed;

      if (change.set_op) {
        const op = change.set_op;
        if (op.op === "add") {
          const newTag: ORSetTag = { node: change.node_id, hlc: change.hlc };
          for (const elem of op.elements) {
            const key = JSON.stringify(elem);
            entries = withEntry(entries, key, withAppended(entries[key], newTag));
          }
        } else if (op.op === "remove") {
          const keys: string[] = [];
          if (op.tags && op.tags.length > 0) {
            // Exact observed-remove: the op names the tags it saw, scoped
            // to the elements it removes.
            for (const elem of op.elements) {
              const key = JSON.stringify(elem);
              for (const t of op.tags) keys.push(removedKey(key, t));
            }
          } else {
            // Legacy remove: only tags older than the remove's HLC —
            // concurrent-or-newer adds survive (add-wins). Matches Go.
            for (const elem of op.elements) {
              const key = JSON.stringify(elem);
              for (const t of entries[key] ?? []) {
                if (hlcAfter(change.hlc, t.hlc)) keys.push(removedKey(key, t));
              }
            }
          }
          removed = withFlags(removed, keys);
        }
      }

      return {
        type: "set",
        hlc: change.hlc,
        node_id: change.node_id,
        set_state: entries === localSet.entries && removed === localSet.removed
          ? localSet
          : { entries, removed },
      };
    }
```

Replace the `case "list":` body with:

```ts
    case "list": {
      const localList = local?.list_state ?? newRGAListState();
      let nodes = localList.nodes;

      const tombstoneAt = (id: HLC): void => {
        const key = hlcKey(id);
        const existing = nodes[key];
        nodes = withEntry(nodes, key, existing
          ? { ...existing, tombstone: true }
          // Keep the tombstone even when the insert hasn't arrived:
          // a late insert must stay deleted (matches Go).
          : {
              id,
              node_id: change.node_id,
              parent_id: { ts: 0, c: 0, node: "" },
              value: undefined,
              tombstone: true,
            });
      };

      if (change.list_op) {
        const op = change.list_op;
        if (op.op === "insert" && op.node_id) {
          nodes = withEntry(nodes, hlcKey(op.node_id), {
            id: op.node_id,
            node_id: change.node_id,
            parent_id: op.parent_id ?? { ts: 0, c: 0, node: "" },
            value: op.value,
          });
        } else if (op.op === "delete" && op.node_id) {
          tombstoneAt(op.node_id);
        } else if (op.op === "move" && op.node_id) {
          // Move = tombstone the old id + re-insert under the new parent
          // with the op's HLC as the new id (matches Go).
          tombstoneAt(op.node_id);
          nodes = withEntry(nodes, hlcKey(change.hlc), {
            id: change.hlc,
            node_id: change.node_id,
            parent_id: op.parent_id ?? { ts: 0, c: 0, node: "" },
            value: op.value,
          });
        }
      }

      return {
        type: "list",
        hlc: change.hlc,
        node_id: change.node_id,
        list_state: nodes === localList.nodes ? localList : { nodes },
      };
    }
```

Replace the `case "text":` body with:

```ts
    case "text": {
      const localText = local?.text_state ?? newTextState();
      const nextText = change.text_op
        ? applyTextOpTo(localText, change.text_op, change.node_id, change.hlc)
        : localText;
      // Stamp with whichever of local/change is newer (Go pickNewer
      // parity) — a redelivered older op must not regress the field clock.
      const keepLocal = local && hlcAfter(local.hlc, change.hlc);
      return {
        type: "text",
        hlc: keepLocal ? local.hlc : change.hlc,
        node_id: keepLocal ? local.node_id : change.node_id,
        text_state: nextText,
      };
    }
```

In the `case "document":` body, replace the tombstone branch's in-place
deletes with a copy:

```ts
        if (change.tombstone) {
          // LWW-guarded path delete (matches Go applyDocumentChange).
          const existing = localDoc.fields[path];
          if (existing && hlcAfter(change.hlc, existing.hlc)) {
            const prefix = path + ".";
            return {
              type: "document",
              hlc: change.hlc,
              node_id: change.node_id,
              doc_state: {
                fields: withoutKeys(
                  localDoc.fields,
                  (k) => k === path || k.startsWith(prefix)
                ),
              },
            };
          }
        }
```

- [ ] **Step 6: Run the purity tests**

```bash
npx vitest run --project node src/__tests__/immutable.test.ts
```

Expected: PASS, 4 tests.

- [ ] **Step 7: Run the full node suite for regressions**

```bash
npx vitest run --project node
```

Expected: PASS. If `merge.test.ts` or `store.test.ts` fails, the likely cause
is a test asserting on the mutated-in-place object. Read the failure before
changing anything: a test that reads state back through `store.getDocument()`
should still pass, and one that fails is pointing at a real behavior change
that must be justified here before the test is touched.

- [ ] **Step 8: Commit**

```bash
git add crdt-js/src/immutable.ts crdt-js/src/text.ts crdt-js/src/merge.ts crdt-js/src/__tests__/immutable.test.ts
git commit -m "refactor(crdt-js): make mergeFieldState pure with structural sharing"
```

### Task 3: Immutable store writes, nested listener keys, pointer-copy undo

This task fixes **D2** (colon in pk) and **D8** (quadratic writes) as a
by-product of the copy-on-write conversion. Undo's `previousState` stops being
a JSON deep clone and becomes a pointer copy, which removes the dominant cost.

**Files:**
- Modify: `src/store.ts`
- Modify: `src/text.ts` (export `cloneTextState`)
- Create: `src/__tests__/cow-store.test.ts`

**Interfaces:**
- Consumes: `withEntry`, `withoutKeys` from `src/immutable.ts` (Task 2);
  `mergeFieldState` purity guarantee (Task 2).
- Produces, all private to `CRDTStore` except where noted:
  - `private getDoc(table: string, pk: string): DocumentState | undefined`
  - `private docOrEmpty(table: string, pk: string): DocumentState`
  - `private setDocument(table: string, pk: string, doc: DocumentState): void`
  - `private withField(doc: DocumentState, field: string, fs: FieldState): DocumentState`
  - `private tableVersions: Map<string, number>` — read by Task 4's collection cache.
  - Exported from `text.ts`: `cloneTextState(state: TextState): TextState`

- [ ] **Step 1: Write the failing tests**

Create `src/__tests__/cow-store.test.ts`:

```ts
import { describe, it, expect } from "vitest";
import { CRDTStore } from "../store.js";
import { HybridClock } from "../hlc.js";
import type { ChangeRecord } from "../types.js";

const mk = () => {
  const clock = new HybridClock("n1");
  return { store: new CRDTStore("n1", clock), clock };
};

describe("copy-on-write store", () => {
  it("notifies subscribers when the pk contains a colon (D2)", () => {
    const { store, clock } = mk();
    let fired = 0;
    store.subscribeDocument("docs", "doc:1", () => { fired++; });
    const change: ChangeRecord = {
      table: "docs", pk: "doc:1", field: "title",
      crdt_type: "lww", hlc: clock.now(), node_id: "n2", value: "hi",
    };
    store.applyChanges([change]);
    expect(fired).toBe(1);
    expect(store.getDocument("docs", "doc:1")).toMatchObject({ title: "hi" });
  });

  it("does not confuse table:pk key collisions", () => {
    const { store } = mk();
    let a = 0, b = 0;
    store.subscribeDocument("a:b", "c", () => { a++; });
    store.subscribeDocument("a", "b:c", () => { b++; });
    store.setField("a:b", "c", "f", 1);
    expect(a).toBe(1);
    expect(b).toBe(0);
  });

  it("replaces the document object on every write (identity changes)", () => {
    const { store } = mk();
    store.setField("t", "p", "f", 1);
    const first = store.exportTable("t")["p"];
    store.setField("t", "p", "f", 2);
    const second = store.exportTable("t")["p"];
    expect(first).not.toBe(second);
    expect(first.fields["f"].value).toBe(1);
  });

  it("undo restores a text field that did not exist before", () => {
    const { store } = mk();
    store.insertText("notes", "n1", "body", 0, "hello");
    expect(store.getText("notes", "n1", "body")).toBe("hello");
    expect(store.undo()).toBe(true);
    expect(store.getText("notes", "n1", "body")).toBe("");
  });

  it("3000 list appends complete in under 250ms (D8)", () => {
    const { store } = mk();
    let after: unknown = undefined;
    const t0 = performance.now();
    for (let i = 0; i < 3000; i++) {
      const c = store.insertIntoList("t", "p", "items", i, after as never);
      after = c.list_op!.node_id;
    }
    const ms = performance.now() - t0;
    expect(ms).toBeLessThan(250);
  }, 30000);
});
```

- [ ] **Step 2: Run to verify it fails**

```bash
npx vitest run --project node src/__tests__/cow-store.test.ts
```

Expected: FAIL on the colon test (0 notifications), the collision test, the
identity test (same object mutated in place), and the perf test (~5000 ms).

- [ ] **Step 3: Export a text-state clone helper**

In `src/text.ts`, extract the clone out of `applyTextOpTo` (added in Task 2)
and export it, then have `applyTextOpTo` call it:

```ts
/** Shallow-clone a text state so mutating appliers cannot touch the input. */
export function cloneTextState(state: TextState): TextState {
  const frags: Record<string, TextFragment[]> = {};
  for (const key of Object.keys(state.frags)) {
    frags[key] = state.frags[key].map((f) => ({
      ...f,
      ...(f.attrs ? { attrs: { ...f.attrs } } : {}),
    }));
  }
  return { frags };
}

export function applyTextOpTo(
  state: TextState,
  op: TextOperation,
  nodeID: string,
  clock: HLC
): TextState {
  const next = cloneTextState(state);
  applyTextOp(next, op, nodeID, clock);
  return next;
}
```

- [ ] **Step 4: Add the document accessors and drop the string keys**

In `src/store.ts`, add to the imports:

```ts
import { withEntry, withoutKeys } from "./immutable.js";
import { cloneTextState } from "./text.js";
```

Replace the listener fields and add version tracking:

```ts
  /** Per-document listeners: table → pk → listeners. Nested, never a
   *  delimited string key — a pk may legally contain ":". */
  private docListeners = new Map<string, Map<string, Set<Listener>>>();

  /** Per-table listeners: table → listeners */
  private tableListeners = new Map<string, Set<Listener>>();

  /** Bumped on every document replacement. Keys the collection cache. */
  private tableVersions = new Map<string, number>();
```

Add the accessors near the other internals:

```ts
  private getDoc(table: string, pk: string): DocumentState | undefined {
    return this.state.get(table)?.get(pk);
  }

  /** Current document, or a fresh empty one. Never inserts into state. */
  private docOrEmpty(table: string, pk: string): DocumentState {
    return this.getDoc(table, pk) ?? { table, pk, fields: {}, tombstone: false };
  }

  /**
   * Install a new document object. The ONLY write path into `state`.
   * Replacing rather than mutating is what makes identity-keyed snapshot
   * caching correct (see getDocument).
   */
  private setDocument(table: string, pk: string, doc: DocumentState): void {
    let tableMap = this.state.get(table);
    if (!tableMap) {
      tableMap = new Map();
      this.state.set(table, tableMap);
    }
    tableMap.set(pk, doc);
    this.tableVersions.set(table, (this.tableVersions.get(table) ?? 0) + 1);
  }

  /** Document with one field replaced; every other field is shared. */
  private withField(
    doc: DocumentState,
    field: string,
    fs: FieldState
  ): DocumentState {
    return { ...doc, fields: withEntry(doc.fields, field, fs) };
  }
```

Delete `ensureDocument` and replace every call with `docOrEmpty` +
`setDocument`. Delete the `normalizeHLCKeys` in-place mutation concern by
leaving it as-is: it only ever runs on freshly deserialized documents inside
`hydrate()` that are not yet reachable by any caller.

- [ ] **Step 5: Convert the write paths**

Replace `applyChangeInternal`:

```ts
  private applyChangeInternal(change: ChangeRecord): void {
    // A tombstoned document-type change carrying a value is a PATH delete
    // inside the nested document, not a record delete.
    const isDocPathDelete =
      change.crdt_type === "document" && change.value !== undefined;
    const doc = this.docOrEmpty(change.table, change.pk);

    if (change.tombstone && !isDocPathDelete) {
      this.setDocument(change.table, change.pk, {
        ...doc,
        tombstone: true,
        tombstone_hlc: change.hlc,
      });
      return;
    }

    const existing = doc.fields[change.field] ?? null;
    this.setDocument(
      change.table,
      change.pk,
      this.withField(doc, change.field, mergeFieldState(existing, change))
    );
  }
```

Replace `captureFieldState` — the deep clone is now unnecessary and was the
dominant write cost:

```ts
  /**
   * Current field state for undo. State is immutable, so a reference is a
   * safe snapshot — no clone needed. Returns null if the field is absent.
   */
  private captureFieldState(
    table: string,
    pk: string,
    field: string
  ): FieldState | null {
    return this.getDoc(table, pk)?.fields[field] ?? null;
  }
```

Replace `deleteDocument`'s mutation:

```ts
    const doc = this.docOrEmpty(table, pk);
    const previousState: FieldState | null = doc.tombstone
      ? { type: "lww", hlc: doc.tombstone_hlc!, node_id: this.nodeID, value: true }
      : null;

    this.setDocument(table, pk, {
      ...doc,
      tombstone: true,
      tombstone_hlc: hlc,
    });
```

Replace `undo()`'s restore block:

```ts
    const doc = this.docOrEmpty(change.table, change.pk);
    if (change.tombstone) {
      // previousState === null means the document was NOT tombstoned before.
      this.setDocument(change.table, change.pk, {
        ...doc,
        tombstone: previousState !== null,
        tombstone_hlc: previousState?.hlc,
      });
    } else if (previousState === null) {
      // Field didn't exist before; remove it.
      this.setDocument(change.table, change.pk, {
        ...doc,
        fields: withoutKeys(doc.fields, (k) => k === change.field),
      });
    } else {
      this.setDocument(
        change.table,
        change.pk,
        this.withField(doc, change.field, previousState)
      );
    }
```

Replace `redo()`'s tombstone branch:

```ts
    if (change.tombstone) {
      const doc = this.docOrEmpty(change.table, change.pk);
      this.setDocument(change.table, change.pk, {
        ...doc, tombstone: true, tombstone_hlc: change.hlc,
      });
    } else {
      this.applyChangeInternal(change);
    }
```

Replace `applyDocumentFieldChange` and `applyDocumentFieldDelete`:

```ts
  private applyDocumentFieldChange(
    table: string, pk: string, field: string,
    path: string, value: unknown, hlc: HLC
  ): void {
    const doc = this.docOrEmpty(table, pk);
    const existing = doc.fields[field];
    const docState =
      existing?.type === "document" && existing.doc_state
        ? existing.doc_state
        : { fields: {} };

    this.setDocument(table, pk, this.withField(doc, field, {
      type: "document",
      hlc,
      node_id: this.nodeID,
      doc_state: {
        fields: withEntry(docState.fields, path, {
          type: "lww", hlc, node_id: this.nodeID, value,
        }),
      },
    }));
  }

  private applyDocumentFieldDelete(
    table: string, pk: string, field: string, path: string, hlc: HLC
  ): void {
    const doc = this.docOrEmpty(table, pk);
    const existing = doc.fields[field];
    if (existing?.type !== "document" || !existing.doc_state) return;

    this.setDocument(table, pk, this.withField(doc, field, {
      ...existing,
      hlc,
      doc_state: {
        fields: withoutKeys(existing.doc_state.fields, (k) => k === path),
      },
    }));
  }
```

- [ ] **Step 6: Convert the text write paths**

Replace `textStateOf` (delete it) and `emitTextChange`, then rewrite the three
text mutators. `textStateOf` created the field as a side effect of *reading*,
which made `captureFieldState` return a synthetic empty state instead of
`null` and broke undo of a first insert.

```ts
  /** Current text state for a field, without creating anything. */
  private textStateOf(table: string, pk: string, field: string): TextState {
    const fs = this.getDoc(table, pk)?.fields[field];
    return fs?.type === "text" && fs.text_state ? fs.text_state : newTextState();
  }

  private emitTextChange(
    table: string, pk: string, field: string, hlc: HLC,
    op: TextOperation, previousState: FieldState | null, nextState: TextState
  ): ChangeRecord {
    const change: ChangeRecord = {
      table, pk, field, crdt_type: "text", hlc,
      node_id: this.nodeID, text_op: op,
    };
    const doc = this.docOrEmpty(table, pk);
    this.setDocument(table, pk, this.withField(doc, field, {
      type: "text", hlc, node_id: this.nodeID, text_state: nextState,
    }));
    this.undoManager.record(change, previousState);
    this.pending.push(change);
    this.persistDocument(table, pk);
    this.persistPending();
    this.notifyListeners(table, pk);
    return change;
  }

  insertText(
    table: string, pk: string, field: string, index: number, content: string
  ): ChangeRecord {
    const hlc = this.clock.now();
    const current = this.textStateOf(table, pk, field);
    const previousState = this.captureFieldState(table, pk, field);
    const ref = index > 0 ? textRefAt(current, index - 1) : null;
    if (index > 0 && !ref) {
      throw new Error(`crdt: no text character at index ${index - 1}`);
    }
    // The builders apply the op as they build it, so build against a clone
    // and keep the clone as the new state.
    const next = cloneTextState(current);
    const op = textInsert(next, ref, content, this.nodeID, hlc);
    return this.emitTextChange(table, pk, field, hlc, op, previousState, next);
  }

  deleteText(
    table: string, pk: string, field: string, index: number, length: number
  ): ChangeRecord {
    const hlc = this.clock.now();
    const current = this.textStateOf(table, pk, field);
    const previousState = this.captureFieldState(table, pk, field);
    const ref = textRefAt(current, index);
    if (!ref) throw new Error(`crdt: no text character at index ${index}`);
    const next = cloneTextState(current);
    const op = textDeleteOp(next, ref, length);
    return this.emitTextChange(table, pk, field, hlc, op, previousState, next);
  }

  formatText(
    table: string, pk: string, field: string,
    index: number, length: number, attrs: Record<string, unknown>
  ): ChangeRecord {
    const hlc = this.clock.now();
    const current = this.textStateOf(table, pk, field);
    const previousState = this.captureFieldState(table, pk, field);
    const ref = textRefAt(current, index);
    if (!ref) throw new Error(`crdt: no text character at index ${index}`);
    const next = cloneTextState(current);
    const op = textFormat(next, ref, length, attrs, this.nodeID, hlc);
    return this.emitTextChange(table, pk, field, hlc, op, previousState, next);
  }
```

- [ ] **Step 7: Fix the affected-set and listener keying**

Replace the `affected` bookkeeping in `applyChanges`:

```ts
    const affected = new Map<string, Set<string>>();
    // ... inside the loop, replacing `affected.add(...)`:
    let pks = affected.get(change.table);
    if (!pks) { pks = new Set(); affected.set(change.table, pks); }
    pks.add(change.pk);

    // ... replacing the batch-persist loop at the end:
    for (const [table, pkSet] of affected) {
      for (const pk of pkSet) {
        this.persistDocument(table, pk);
        this.notifyListeners(table, pk);
      }
    }
```

Replace `subscribeDocument` and the document half of `notifyListeners`:

```ts
  subscribeDocument(table: string, pk: string, listener: Listener): () => void {
    let byPk = this.docListeners.get(table);
    if (!byPk) { byPk = new Map(); this.docListeners.set(table, byPk); }
    let listeners = byPk.get(pk);
    if (!listeners) { listeners = new Set(); byPk.set(pk, listeners); }
    listeners.add(listener);

    return () => {
      listeners.delete(listener);
      if (listeners.size === 0) {
        byPk.delete(pk);
        if (byPk.size === 0) this.docListeners.delete(table);
      }
    };
  }
```

```ts
  private notifyListeners(table: string, pk: string): void {
    const docListeners = this.docListeners.get(table)?.get(pk);
    if (docListeners) for (const listener of docListeners) listener();

    const tableListeners = this.tableListeners.get(table);
    if (tableListeners) for (const listener of tableListeners) listener();

    for (const listener of this.globalListeners) listener();
  }
```

Update `importState`'s notify loop for the nested shape:

```ts
    for (const listener of this.globalListeners) listener();
    for (const byPk of this.docListeners.values()) {
      for (const listeners of byPk.values()) {
        for (const listener of listeners) listener();
      }
    }
    for (const listeners of this.tableListeners.values()) {
      for (const listener of listeners) listener();
    }
```

`importState` and `hydrate` must also route through `setDocument` so table
versions bump. In `importState`, replace `this.state.set(tableName, tableMap)`
with a loop calling `this.setDocument(tableName, pk, doc)`; in `hydrate`,
replace `tableMap.set(pk, hydrated)` with `this.setDocument(table, pk, hydrated)`
guarded by the existing `if (!this.getDoc(table, pk))` check.

- [ ] **Step 8: Run the new tests**

```bash
npx vitest run --project node src/__tests__/cow-store.test.ts
```

Expected: PASS. The perf test should land around 750 ms, down from ~5,600 ms —
the JSON deep clone per write is gone and only a shallow spread remains. The
residual is the O(N) record spread the spec's "Accepted limit" keeps on
purpose; do not chase a lower number here.

- [ ] **Step 9: Run the full node suite**

```bash
npx vitest run --project node
```

Expected: PASS. `store.test.ts` is the most likely place for fallout; read
each failure before editing a test.

- [ ] **Step 10: Commit**

```bash
git add crdt-js/src/store.ts crdt-js/src/text.ts crdt-js/src/__tests__/cow-store.test.ts
git commit -m "refactor(crdt-js): copy-on-write store writes; fix colon-key notification"
```

---

### Task 4: Identity-keyed snapshot cache

Fixes **D1** on the store side. Because Task 3 made every write replace the
`DocumentState` object, the object's identity *is* a version stamp — a
`WeakMap` keyed on it needs no invalidation bookkeeping and cannot go stale.

**Files:**
- Modify: `src/store.ts`
- Create: `src/__tests__/snapshot-cache.test.ts`

**Interfaces:**
- Consumes: `setDocument` / `tableVersions` from Task 3.
- Produces: `getDocument`, `getCollection`, `getListNodeIds`, and
  `getTextDelta` all return referentially stable values between writes.
  Signatures are unchanged.

- [ ] **Step 1: Write the failing test**

Create `src/__tests__/snapshot-cache.test.ts`:

```ts
import { describe, it, expect } from "vitest";
import { CRDTStore } from "../store.js";
import { HybridClock } from "../hlc.js";

const mk = () => new CRDTStore("n1", new HybridClock("n1"));

describe("snapshot stability", () => {
  it("getDocument returns the same reference between writes", () => {
    const store = mk();
    store.setField("t", "p", "f", 1);
    expect(store.getDocument("t", "p")).toBe(store.getDocument("t", "p"));
  });

  it("getDocument returns a new reference after a write", () => {
    const store = mk();
    store.setField("t", "p", "f", 1);
    const first = store.getDocument("t", "p");
    store.setField("t", "p", "f", 2);
    expect(store.getDocument("t", "p")).not.toBe(first);
  });

  it("getCollection returns the same reference between writes", () => {
    const store = mk();
    store.setField("t", "p", "f", 1);
    expect(store.getCollection("t")).toBe(store.getCollection("t"));
  });

  it("getCollection invalidates when any document in the table changes", () => {
    const store = mk();
    store.setField("t", "p1", "f", 1);
    const first = store.getCollection("t");
    store.setField("t", "p2", "f", 2);
    expect(store.getCollection("t")).not.toBe(first);
    expect(store.getCollection("t")).toHaveLength(2);
  });

  it("getListNodeIds returns the same reference between writes", () => {
    const store = mk();
    store.insertIntoList("t", "p", "items", "a");
    expect(store.getListNodeIds("t", "p", "items"))
      .toBe(store.getListNodeIds("t", "p", "items"));
  });

  it("registering a plugin invalidates cached snapshots", () => {
    const store = mk();
    store.setField("t", "p", "f", 1);
    const before = store.getDocument<Record<string, unknown>>("t", "p");
    store.use({
      name: "tagger",
      transformDocument<T>(_t: string, _p: string, doc: T): T {
        return { ...(doc as object), tagged: true } as T;
      },
    });
    const after = store.getDocument<Record<string, unknown>>("t", "p");
    expect(after).not.toBe(before);
    expect(after?.tagged).toBe(true);
  });
});
```

- [ ] **Step 2: Run to verify it fails**

```bash
npx vitest run --project node src/__tests__/snapshot-cache.test.ts
```

Expected: FAIL on every stability assertion — each getter builds fresh.

- [ ] **Step 3: Add the caches**

In `src/store.ts`, add the fields:

```ts
  /**
   * Resolved-document cache keyed on DocumentState IDENTITY. Safe because
   * every write replaces the document object (see setDocument), so a hit
   * can never be stale. WeakMap so evicted documents are collectable.
   */
  private docCache = new WeakMap<DocumentState, unknown>();

  /** Ordered live node IDs per list field, keyed on document identity. */
  private listIdCache = new WeakMap<DocumentState, Map<string, HLC[]>>();

  /** Text delta segments per text field, keyed on document identity. */
  private textDeltaCache = new WeakMap<DocumentState, Map<string, TextDeltaSegment[]>>();

  /** Resolved collections, keyed on the table's write version. */
  private collectionCache = new Map<string, { version: number; items: unknown[] }>();
```

- [ ] **Step 4: Route the getters through the caches**

```ts
  getDocument<T = Record<string, unknown>>(table: string, pk: string): T | null {
    const doc = this.getDoc(table, pk);
    if (!doc || doc.tombstone) return null;
    if (this.docCache.has(doc)) return this.docCache.get(doc) as T | null;

    const resolved = this.resolveDocument(doc) as T;
    const transformed = this.plugins.dispatchTransformDocument(table, pk, resolved);
    this.docCache.set(doc, transformed);
    return transformed;
  }

  getCollection<T = Record<string, unknown>>(table: string): T[] {
    const version = this.tableVersions.get(table) ?? 0;
    const hit = this.collectionCache.get(table);
    if (hit && hit.version === version) return hit.items as T[];

    const tableMap = this.state.get(table);
    const result: T[] = [];
    if (tableMap) {
      for (const doc of tableMap.values()) {
        if (doc.tombstone) continue;
        const transformed = this.getDocument<T>(table, doc.pk);
        if (transformed !== null) result.push(transformed);
      }
    }
    const items = this.plugins.dispatchTransformCollection(table, result);
    this.collectionCache.set(table, { version, items });
    return items;
  }

  getListNodeIds(table: string, pk: string, field: string): HLC[] {
    const doc = this.getDoc(table, pk);
    if (!doc) return EMPTY_HLCS;
    let byField = this.listIdCache.get(doc);
    if (!byField) { byField = new Map(); this.listIdCache.set(doc, byField); }
    const hit = byField.get(field);
    if (hit) return hit;

    const fs = doc.fields[field];
    const ids = fs?.list_state ? listNodeIds(fs.list_state) : EMPTY_HLCS;
    byField.set(field, ids);
    return ids;
  }

  getTextDelta(table: string, pk: string, field: string): TextDeltaSegment[] {
    const doc = this.getDoc(table, pk);
    if (!doc) return EMPTY_DELTA;
    let byField = this.textDeltaCache.get(doc);
    if (!byField) { byField = new Map(); this.textDeltaCache.set(doc, byField); }
    const hit = byField.get(field);
    if (hit) return hit;

    const fs = doc.fields[field];
    const delta = fs?.text_state ? textDelta(fs.text_state) : EMPTY_DELTA;
    byField.set(field, delta);
    return delta;
  }
```

Add the shared empty singletons at module scope, next to `type Listener`.
Returning a fresh `[]` is itself a snapshot-instability bug:

```ts
/** Shared empties — a fresh [] each call breaks useSyncExternalStore. */
const EMPTY_HLCS: HLC[] = [];
const EMPTY_DELTA: TextDeltaSegment[] = [];
```

`getCollection` now delegates to `getDocument` so both caches agree on the
plugin-transformed value, rather than resolving twice with different results.

- [ ] **Step 5: Invalidate on plugin registration**

Cached snapshots embed plugin output, so registering or removing a plugin must
drop them. In `src/store.ts`:

```ts
  use(plugin: StorePlugin): void {
    this.plugins.use(plugin);
    this.invalidateSnapshots();
  }

  removePlugin(name: string): void {
    this.plugins.remove(name);
    this.invalidateSnapshots();
  }

  /** Drop every cached snapshot. Called when plugin output may change. */
  private invalidateSnapshots(): void {
    this.docCache = new WeakMap();
    this.listIdCache = new WeakMap();
    this.textDeltaCache = new WeakMap();
    this.collectionCache.clear();
  }
```

- [ ] **Step 6: Run the tests**

```bash
npx vitest run --project node src/__tests__/snapshot-cache.test.ts
```

Expected: PASS, 6 tests.

- [ ] **Step 7: Run the full node suite**

```bash
npx vitest run --project node
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add crdt-js/src/store.ts crdt-js/src/__tests__/snapshot-cache.test.ts
git commit -m "perf(crdt-js): identity-keyed snapshot cache for resolved state"
```

---

### Task 5: Resolve text fields in documents

Fixes **D3**. `getText()` returns `"hello"` while `getDocument().body` returns
`undefined`, because neither resolver has a `text` case.

**Files:**
- Modify: `src/store.ts` (`resolveDocument`)
- Modify: `src/merge.ts` (`documentResolve`)
- Modify: `src/__tests__/cow-store.test.ts` (append)

**Interfaces:**
- Consumes: `textValue` from `src/text.ts` (already imported by `store.ts`;
  `merge.ts` must add it to its existing `./text.js` import).
- Produces: no signature changes.

- [ ] **Step 1: Write the failing test**

Append to `src/__tests__/cow-store.test.ts`:

```ts
describe("text resolution", () => {
  it("getDocument exposes text fields (D3)", () => {
    const { store } = mk();
    store.insertText("notes", "n1", "body", 0, "hello");
    const doc = store.getDocument<{ body: string }>("notes", "n1");
    expect(doc?.body).toBe("hello");
  });

  it("nested documents expose text fields", () => {
    const { store } = mk();
    store.insertText("notes", "n1", "body", 0, "hi");
    store.setDocumentField("notes", "n1", "meta", "author", "alice");
    const doc = store.getDocument<Record<string, unknown>>("notes", "n1");
    expect(doc?.body).toBe("hi");
    expect(doc?.meta).toEqual({ author: "alice" });
  });
});
```

- [ ] **Step 2: Run to verify it fails**

```bash
npx vitest run --project node src/__tests__/cow-store.test.ts -t "text resolution"
```

Expected: FAIL — `expected undefined to be 'hello'`.

- [ ] **Step 3: Add the `text` case to both resolvers**

In `src/store.ts`, inside `resolveDocument`'s switch, before `default`:

```ts
        case "text":
          result[field] = state.text_state ? textValue(state.text_state) : "";
          break;
```

In `src/merge.ts`, add `textValue` to the existing text import, then add the
same case to `documentResolve`'s switch before `default`:

```ts
      case "text":
        result[key] = field.text_state ? textValue(field.text_state) : "";
        break;
```

- [ ] **Step 4: Run the tests**

```bash
npx vitest run --project node
```

Expected: PASS, including the two new cases.

- [ ] **Step 5: Commit**

```bash
git add crdt-js/src/store.ts crdt-js/src/merge.ts crdt-js/src/__tests__/cow-store.test.ts
git commit -m "fix(crdt-js): resolve text fields in getDocument and documentResolve"
```

---

### Task 6: Stable presence snapshots, snapshot seeding, stale-peer pruning

Fixes **D1** for `usePresence` and **D6**. The current doc comment on
`getPresence` states the inverse of the truth — a new array per call is what
*breaks* `useSyncExternalStore`.

**Files:**
- Modify: `src/presence.ts`
- Modify: `src/client.ts`
- Create: `src/__tests__/presence-seed.test.ts`

**Interfaces:**
- Consumes: `CRDTClient.getPresence(topic)` (already exists, currently unused).
- Produces:
  - `PresenceManager.getPresence(topic)` — referentially stable between events.
  - `PresenceManager.seed(topic: string, states: PresenceState[]): void`
  - `PresenceManager.prune(maxAgeMs: number): void`
  - `CRDTClient.joinPresence<T>(topic: string, data: T): Promise<void>` — updates
    own presence *and* seeds the manager from the server snapshot.

- [ ] **Step 1: Write the failing test**

Create `src/__tests__/presence-seed.test.ts`:

```ts
import { describe, it, expect } from "vitest";
import { PresenceManager } from "../presence.js";
import { CRDTClient } from "../client.js";
import type { Transport, PresenceState, PresenceUpdate } from "../types.js";

const HLC0 = { ts: "0", c: 0, node: "" };

describe("presence", () => {
  it("getPresence is referentially stable between events", () => {
    const pm = new PresenceManager("me");
    pm.applyEvent({ type: "join", node_id: "peer", topic: "t", data: { a: 1 } });
    expect(pm.getPresence("t")).toBe(pm.getPresence("t"));
  });

  it("getPresence returns a new reference after an event", () => {
    const pm = new PresenceManager("me");
    pm.applyEvent({ type: "join", node_id: "peer", topic: "t", data: { a: 1 } });
    const first = pm.getPresence("t");
    pm.applyEvent({ type: "update", node_id: "peer", topic: "t", data: { a: 2 } });
    expect(pm.getPresence("t")).not.toBe(first);
  });

  it("an empty topic returns a stable empty array", () => {
    const pm = new PresenceManager("me");
    expect(pm.getPresence("nope")).toBe(pm.getPresence("nope"));
  });

  it("seed populates peers already present (D6)", () => {
    const pm = new PresenceManager("me");
    const states: PresenceState[] = [
      { node_id: "bob", topic: "t", data: { name: "Bob" }, updated_at: Date.now() },
      { node_id: "me", topic: "t", data: {}, updated_at: Date.now() },
    ];
    pm.seed("t", states);
    const peers = pm.getPresence("t");
    expect(peers).toHaveLength(1);
    expect(peers[0].node_id).toBe("bob");
  });

  it("prune drops peers older than maxAge", () => {
    const pm = new PresenceManager("me");
    pm.seed("t", [
      { node_id: "stale", topic: "t", data: {}, updated_at: Date.now() - 60_000 },
      { node_id: "fresh", topic: "t", data: {}, updated_at: Date.now() },
    ]);
    pm.prune(30_000);
    expect(pm.getPresence("t").map((p) => p.node_id)).toEqual(["fresh"]);
  });

  it("joinPresence seeds from the server snapshot", async () => {
    const snapshot: PresenceState[] = [
      { node_id: "bob", topic: "t", data: { name: "Bob" }, updated_at: Date.now() },
    ];
    const sent: PresenceUpdate[] = [];
    const transport: Transport = {
      async pull() { return { changes: [], latest_hlc: HLC0 }; },
      async push() { return { merged: 0, latest_hlc: HLC0 }; },
      async updatePresence(u) { sent.push(u); },
      async getPresence() { return snapshot; },
    };
    const client = new CRDTClient({ nodeID: "me", transport });
    await client.joinPresence("t", { name: "Me" });
    expect(sent).toHaveLength(1);
    expect(client.presence.getPresence("t").map((p) => p.node_id)).toEqual(["bob"]);
    await client.leaveAllPresence();
  });
});
```

- [ ] **Step 2: Run to verify it fails**

```bash
npx vitest run --project node src/__tests__/presence-seed.test.ts
```

Expected: FAIL — stability assertions fail, and `seed`/`prune`/`joinPresence`
do not exist.

- [ ] **Step 3: Add a per-topic snapshot cache to `PresenceManager`**

In `src/presence.ts`, add the field and the shared empty:

```ts
/** Shared empty — a fresh [] per call breaks useSyncExternalStore. */
const EMPTY_PRESENCE: PresenceState[] = [];
```

```ts
  /** Cached per-topic snapshots. Cleared for a topic when it changes. */
  private snapshots = new Map<string, PresenceState[]>();
```

Replace `getPresence`:

```ts
  /**
   * All presence states for a topic, excluding the local node.
   *
   * The returned array is CACHED and referentially stable until the topic
   * changes, which is what useSyncExternalStore requires — returning a
   * fresh array per call causes an unbounded render loop.
   */
  getPresence<T = Record<string, unknown>>(topic: string): PresenceState<T>[] {
    const hit = this.snapshots.get(topic);
    if (hit) return hit as PresenceState<T>[];

    const topicMap = this.peers.get(topic);
    if (!topicMap) return EMPTY_PRESENCE as PresenceState<T>[];

    const result: PresenceState[] = [];
    for (const [nodeID, state] of topicMap) {
      if (nodeID !== this.localNodeID) result.push(state);
    }
    const frozen = result.length === 0 ? EMPTY_PRESENCE : result;
    this.snapshots.set(topic, frozen);
    return frozen as PresenceState<T>[];
  }
```

Invalidate in `notifyListeners`, which every mutation already calls:

```ts
  private notifyListeners(topic: string): void {
    this.snapshots.delete(topic);

    const topicListeners = this.topicListeners.get(topic);
    if (topicListeners) for (const listener of topicListeners) listener();

    for (const listener of this.globalListeners) listener();
  }
```

`clear()` already calls `notifyListeners` per topic, so it invalidates
correctly. Verify that when editing.

- [ ] **Step 4: Add `seed` and `prune`**

```ts
  /**
   * Replace a topic's peers from a server snapshot.
   *
   * Needed on join and after every stream reconnect: SSE only delivers
   * CHANGES, so peers that were already idle when you subscribed are
   * otherwise invisible until they happen to move.
   */
  seed(topic: string, states: PresenceState[]): void {
    const topicMap = new Map<string, PresenceState>();
    for (const state of states) {
      topicMap.set(state.node_id, state);
    }
    if (topicMap.size === 0) {
      this.peers.delete(topic);
    } else {
      this.peers.set(topic, topicMap);
    }
    this.notifyListeners(topic);
  }

  /**
   * Drop peers whose last update is older than maxAgeMs.
   *
   * The server expires presence on its own TTL and broadcasts a leave, but
   * that only reaches clients with a live stream. A client that was
   * disconnected across the expiry never sees the leave.
   */
  prune(maxAgeMs: number): void {
    const cutoff = Date.now() - maxAgeMs;
    for (const [topic, topicMap] of [...this.peers]) {
      let dropped = false;
      for (const [nodeID, state] of [...topicMap]) {
        if (state.updated_at < cutoff) {
          topicMap.delete(nodeID);
          dropped = true;
        }
      }
      if (topicMap.size === 0) this.peers.delete(topic);
      if (dropped) this.notifyListeners(topic);
    }
  }
```

- [ ] **Step 5: Add `joinPresence` to `CRDTClient`**

In `src/client.ts`, after `updatePresence`:

```ts
  /**
   * Join a topic: publish your presence, then seed the local manager from
   * the server's snapshot so peers already present are visible immediately.
   *
   * Prefer this over updatePresence() when first entering a topic, and call
   * it again after a stream reconnect to re-seed.
   */
  async joinPresence<T = Record<string, unknown>>(
    topic: string,
    data: T
  ): Promise<void> {
    await this.updatePresence(topic, data);
    if (this.transport.getPresence) {
      const states = await this.transport.getPresence(topic);
      this.presence.seed(topic, states);
    }
  }
```

- [ ] **Step 6: Run the tests**

```bash
npx vitest run --project node src/__tests__/presence-seed.test.ts
npx vitest run --project node
```

Expected: PASS. `presence.test.ts` has 47 existing tests; if any asserted that
`getPresence` returns a distinct array each call, that assertion encoded the
bug and should be updated to assert stability instead. Note the change here.

- [ ] **Step 7: Export the new surface**

In `src/index.ts` nothing changes for `PresenceManager` (already exported).
Confirm `joinPresence` appears on `CRDTClient`'s public type by running:

```bash
npx tsc --noEmit
```

- [ ] **Step 8: Commit**

```bash
git add crdt-js/src/presence.ts crdt-js/src/client.ts crdt-js/src/__tests__/presence-seed.test.ts
git commit -m "fix(crdt-js): stable presence snapshots, snapshot seeding, stale pruning"
```

---

### Task 7: React hooks on stable snapshots

Completes **D1**. With Tasks 4 and 6 done, every getter is stable, so the
`useList` `useState` workaround can be deleted and the remaining hooks moved
onto `useSyncExternalStore` uniformly.

**Files:**
- Modify: `src/react.tsx`
- Modify: `src/__tests__/react.test.tsx` (extend)

**Interfaces:**
- Consumes: stable `getDocument`, `getCollection`, `getListNodeIds`,
  `getTextDelta` (Task 4); stable `PresenceManager.getPresence` (Task 6).
- Produces: no public signature changes. `useList` keeps its
  `UseListReturn<T>` shape.

- [ ] **Step 1: Extend the React tests**

Append to `src/__tests__/react.test.tsx`:

```tsx
import { useList, useSet, useNestedDocument } from "../react.js";

function ListView() {
  const { items, insert } = useList<string>("docs", "doc-1", "todos");
  return (
    <div>
      <span data-testid="items">{items.join(",")}</span>
      <button onClick={() => insert("x")}>add</button>
    </div>
  );
}

function SetView() {
  const { elements } = useSet<string>("docs", "doc-1", "tags");
  return <span data-testid="tags">{elements.join(",")}</span>;
}

function NestedView() {
  const { data } = useNestedDocument<{ author?: string }>("docs", "doc-1", "meta");
  return <span data-testid="author">{data.author ?? "none"}</span>;
}

describe("all hooks render and update without thrash", () => {
  it("useList renders and updates on insert", async () => {
    const err = vi.spyOn(console, "error").mockImplementation(() => {});
    render(
      <CRDTProvider config={config}>
        <ListView />
      </CRDTProvider>
    );
    expect(screen.getByTestId("items").textContent).toBe("");
    await act(async () => {
      screen.getByText("add").click();
    });
    expect(screen.getByTestId("items").textContent).toBe("x");
    expect(err.mock.calls.some((c) =>
      String(c[0]).includes("getSnapshot should be cached"))).toBe(false);
  });

  it("useSet and useNestedDocument render without warnings", () => {
    const err = vi.spyOn(console, "error").mockImplementation(() => {});
    render(
      <CRDTProvider config={config}>
        <SetView />
        <NestedView />
      </CRDTProvider>
    );
    expect(screen.getByTestId("tags").textContent).toBe("");
    expect(screen.getByTestId("author").textContent).toBe("none");
    expect(err.mock.calls.some((c) =>
      String(c[0]).includes("getSnapshot should be cached"))).toBe(false);
  });
});
```

- [ ] **Step 2: Run to see the current state**

```bash
npx vitest run --project dom
```

Expected: the Task 1 tests now PASS (the store cache fixed them). The `useList`
test may pass already via the workaround; the point of this task is to remove
the workaround without regressing it.

- [ ] **Step 3: Replace the `useList` workaround with `useSyncExternalStore`**

In `src/react.tsx`, replace the body of `useList` down to the `insert`
callback, deleting both `useState` blocks and their explanatory comments —
the comments describe a constraint that no longer holds:

```tsx
  const { store } = useCRDTContext();

  const items = useSyncExternalStore(
    (cb) => store.subscribeDocument(table, pk, cb),
    () => {
      const doc = store.getDocument<Record<string, unknown>>(table, pk);
      const value = doc?.[field];
      return Array.isArray(value) ? (value as T[]) : EMPTY_ARRAY;
    },
    () => EMPTY_ARRAY as T[]
  );

  const nodeIds = useSyncExternalStore(
    (cb) => store.subscribeDocument(table, pk, cb),
    () => store.getListNodeIds(table, pk, field),
    () => EMPTY_HLC_ARRAY
  );
```

Add the module-scope empties near the top of `react.tsx`, under the imports.
Every `() => []` and `() => ({})` inline in a `getSnapshot` or SSR-snapshot
position is itself a fresh reference and must use these:

```tsx
/** Shared empties — an inline [] or {} in getSnapshot is a fresh reference
 *  every call, which is exactly the loop useSyncExternalStore warns about. */
const EMPTY_ARRAY: never[] = [];
const EMPTY_HLC_ARRAY: HLC[] = [];
const EMPTY_OBJECT: Record<string, never> = {};
```

Also delete the now-unused `useState` import if nothing else in the file uses
it — `useStream` still does, so verify before removing.

- [ ] **Step 4: Fix the remaining fresh-reference snapshots**

In `useCollection`, replace the SSR snapshot `() => [] as T[]` with
`() => EMPTY_ARRAY as T[]`.

In `useSet`, replace both the fallback and the SSR snapshot:

```tsx
  const elements = useSyncExternalStore(
    (cb) => store.subscribeDocument(table, pk, cb),
    () => {
      const doc = store.getDocument<Record<string, unknown>>(table, pk);
      const value = doc?.[field];
      return Array.isArray(value) ? (value as T[]) : (EMPTY_ARRAY as T[]);
    },
    () => EMPTY_ARRAY as T[]
  );
```

In `useNestedDocument`:

```tsx
  const data = useSyncExternalStore(
    (cb) => store.subscribeDocument(table, pk, cb),
    () => {
      const doc = store.getDocument<Record<string, unknown>>(table, pk);
      return (doc?.[field] ?? EMPTY_OBJECT) as T;
    },
    () => EMPTY_OBJECT as T
  );
```

In `usePresence`, replace the SSR snapshot `() => [] as PresenceState<T>[]`
with `() => EMPTY_ARRAY as PresenceState<T>[]`.

- [ ] **Step 5: Fix `usePlugin`'s dependency array**

The current deps read a ref during render (`[store, pluginRef.current.name]`),
which is unstable under StrictMode double-render. Capture the name instead:

```tsx
export function usePlugin(
  store: CRDTStore,
  plugin: import("./plugin.js").StorePlugin
): void {
  const pluginRef = useRef(plugin);
  pluginRef.current = plugin;
  const name = plugin.name;

  useEffect(() => {
    store.use(pluginRef.current);
    return () => {
      store.removePlugin(name);
    };
  }, [store, name]);
}
```

- [ ] **Step 6: Run both projects**

```bash
npx vitest run
```

Expected: PASS across `node` and `dom`.

- [ ] **Step 7: Typecheck**

```bash
npx tsc --noEmit
```

Expected: clean.

- [ ] **Step 8: Commit**

```bash
git add crdt-js/src/react.tsx crdt-js/src/__tests__/react.test.tsx
git commit -m "fix(crdt-js): stable useSyncExternalStore snapshots across all hooks"
```

---

## Phase 2 — Remaining correctness

### Task 8: Iterative list traversal

Fixes **D9**. `listElements` and `listNodeIds` recurse once per element, so a
sequentially built list (a linear parent chain) overflows the stack between
5,000 and 8,000 items. They are also near-duplicates of each other.

**Files:**
- Modify: `src/merge.ts`
- Create: `src/__tests__/list-scale.test.ts`

**Interfaces:**
- Produces: `walkList(state: RGAListState): RGANode[]` — module-private,
  the single ordered traversal. `listElements` and `listNodeIds` keep their
  existing exported signatures and both delegate to it.

- [ ] **Step 1: Write the failing test**

Create `src/__tests__/list-scale.test.ts`:

```ts
import { describe, it, expect } from "vitest";
import { listElements, listNodeIds } from "../merge.js";
import type { RGAListState, HLC } from "../types.js";

/** Build a linear chain of `n` nodes — the shape sequential appends produce. */
function chain(n: number): RGAListState {
  const nodes: RGAListState["nodes"] = {};
  let parent: HLC = { ts: 0, c: 0, node: "" };
  for (let i = 0; i < n; i++) {
    const id: HLC = { ts: String(i + 1), c: 0, node: "n1" };
    nodes[`HLC{ts:${i + 1} c:0 node:n1}`] = {
      id, node_id: "n1", parent_id: parent, value: i,
    };
    parent = id;
  }
  return { nodes };
}

describe("list traversal scale", () => {
  it("resolves a 50,000-element list without overflowing the stack (D9)", () => {
    const state = chain(50_000);
    const elements = listElements(state);
    expect(elements).toHaveLength(50_000);
    expect(elements[0]).toBe(0);
    expect(elements[49_999]).toBe(49_999);
  });

  it("listNodeIds matches listElements order at scale", () => {
    const state = chain(20_000);
    const ids = listNodeIds(state);
    expect(ids).toHaveLength(20_000);
    expect(ids[0].ts).toBe("1");
    expect(ids[19_999].ts).toBe("20000");
  });

  it("preserves sibling ordering (newest first) and skips tombstones", () => {
    const root: HLC = { ts: 0, c: 0, node: "" };
    const a: HLC = { ts: "1", c: 0, node: "n1" };
    const b: HLC = { ts: "2", c: 0, node: "n1" };
    const state: RGAListState = { nodes: {
      "HLC{ts:1 c:0 node:n1}": { id: a, node_id: "n1", parent_id: root, value: "a" },
      "HLC{ts:2 c:0 node:n1}": { id: b, node_id: "n1", parent_id: root, value: "b" },
      "HLC{ts:3 c:0 node:n1}": {
        id: { ts: "3", c: 0, node: "n1" }, node_id: "n1",
        parent_id: a, value: "gone", tombstone: true,
      },
    } };
    // Siblings sort HLC-descending (RGA insert-right), so b precedes a.
    expect(listElements(state)).toEqual(["b", "a"]);
  });
});
```

- [ ] **Step 2: Run to verify it fails**

```bash
npx vitest run --project node src/__tests__/list-scale.test.ts
```

Expected: FAIL with `RangeError: Maximum call stack size exceeded` on the
first two tests. The third should pass (ordering is already correct) and is
there to pin that behavior through the rewrite.

- [ ] **Step 3: Replace both traversals with one iterative walk**

In `src/merge.ts`, delete the bodies of `listElements` and `listNodeIds` and
insert above them:

```ts
/**
 * Ordered live nodes of an RGA list, tombstones skipped.
 *
 * Uses an explicit stack rather than recursion: sequential appends build a
 * LINEAR parent chain, so recursion depth equals list length and overflows
 * the stack in the low thousands.
 */
function walkList(state: RGAListState): RGANode[] {
  const nodes = Object.values(state.nodes);
  if (nodes.length === 0) return [];

  // Build children map: parent key → children.
  const childrenMap = new Map<string, RGANode[]>();
  for (const node of nodes) {
    const pk = hlcKey(node.parent_id);
    let children = childrenMap.get(pk);
    if (!children) {
      children = [];
      childrenMap.set(pk, children);
    }
    children.push(node);
  }

  // Sort each sibling group by HLC descending (RGA insert-right semantics).
  for (const children of childrenMap.values()) {
    children.sort((a, b) => -hlcCompare(a.id, b.id));
  }

  const out: RGANode[] = [];
  const stack: RGANode[] = [];
  const roots = childrenMap.get(hlcKey({ ts: 0, c: 0, node: "" }));
  // Push reversed so the first sibling pops first.
  if (roots) for (let i = roots.length - 1; i >= 0; i--) stack.push(roots[i]);

  while (stack.length > 0) {
    const node = stack.pop()!;
    if (!node.tombstone) out.push(node);
    const children = childrenMap.get(hlcKey(node.id));
    if (children) for (let i = children.length - 1; i >= 0; i--) stack.push(children[i]);
  }

  return out;
}

/**
 * Resolve an RGA list to an ordered array of live (non-tombstoned) values.
 */
export function listElements(state: RGAListState): unknown[] {
  return walkList(state).map((n) => n.value);
}

/**
 * Return the ordered node IDs (HLCs) for live elements in an RGA list.
 * Order matches listElements().
 */
export function listNodeIds(state: RGAListState): HLC[] {
  return walkList(state).map((n) => n.id);
}
```

- [ ] **Step 4: Run the tests**

```bash
npx vitest run --project node src/__tests__/list-scale.test.ts
npx vitest run --project node
```

Expected: PASS, 3 new tests plus no regressions in `merge.test.ts`.

- [ ] **Step 5: Commit**

```bash
git add crdt-js/src/merge.ts crdt-js/src/__tests__/list-scale.test.ts
git commit -m "fix(crdt-js): iterative RGA traversal; removes ~6k list ceiling"
```

---

### Task 9: BatchWriter parity via store transactions

Fixes **D4**. `BatchWriter` hand-builds `ChangeRecord`s, so it emits a raw
counter delta where the store emits cumulative per-node totals, and it skips
plugins and undo entirely. The fix is to stop duplicating the write logic:
`BatchWriter` queues calls to the store's real mutators and runs them inside
a notification-suspending transaction.

**Files:**
- Modify: `src/store.ts`
- Create: `src/__tests__/batch.test.ts`

**Interfaces:**
- Produces:
  - `CRDTStore.transact<T>(fn: () => T): T` — public. Runs `fn`, suspending
    persistence and listener notification until the outermost call returns.
    Re-entrant.
  - `BatchWriter` gains `.removeFromSet(field, elements)`,
    `.insertIntoList(field, value, afterId?)`, `.setDocumentField(field, path, value)`,
    and `.insertText(field, index, content)`; `.commit()` returns
    `ChangeRecord[]` as before.

- [ ] **Step 1: Write the failing test**

Create `src/__tests__/batch.test.ts`:

```ts
import { describe, it, expect } from "vitest";
import { CRDTStore } from "../store.js";
import { HybridClock } from "../hlc.js";

const mk = () => new CRDTStore("n1", new HybridClock("n1"));

describe("BatchWriter parity", () => {
  it("batch counters agree with direct counters (D4)", () => {
    const store = mk();
    store.incrementCounter("t", "p", "n", 5);
    store.batch("t", "p").incrementCounter("n", 3).commit();
    expect(store.getDocument<{ n: number }>("t", "p")?.n).toBe(8);
  });

  it("batch writes run plugin hooks", () => {
    const store = mk();
    const seen: string[] = [];
    store.use({ name: "spy", afterWrite(ev) { seen.push(ev.field); } });
    store.batch("t", "p").setField("a", 1).setField("b", 2).commit();
    expect(seen).toEqual(["a", "b"]);
  });

  it("batch writes are undoable", () => {
    const store = mk();
    store.batch("t", "p").setField("a", 1).commit();
    expect(store.canUndo).toBe(true);
    store.undo();
    expect(store.getDocument<{ a?: number }>("t", "p")?.a).toBeUndefined();
  });

  it("a batch notifies subscribers exactly once", () => {
    const store = mk();
    let fired = 0;
    store.subscribeDocument("t", "p", () => { fired++; });
    store.batch("t", "p").setField("a", 1).setField("b", 2).setField("c", 3).commit();
    expect(fired).toBe(1);
  });

  it("transact suspends notification and nests", () => {
    const store = mk();
    let fired = 0;
    store.subscribeDocument("t", "p", () => { fired++; });
    store.transact(() => {
      store.setField("t", "p", "a", 1);
      store.transact(() => { store.setField("t", "p", "b", 2); });
      expect(fired).toBe(0);
    });
    expect(fired).toBe(1);
  });

  it("a plugin rejecting one batch write does not abort the others", () => {
    const store = mk();
    store.use({
      name: "gate",
      beforeWrite(ev) { return ev.field === "blocked" ? null : ev; },
    });
    store.batch("t", "p").setField("ok", 1).setField("blocked", 2).commit();
    const doc = store.getDocument<Record<string, unknown>>("t", "p");
    expect(doc?.ok).toBe(1);
    expect(doc?.blocked).toBeUndefined();
  });
});
```

- [ ] **Step 2: Run to verify it fails**

```bash
npx vitest run --project node src/__tests__/batch.test.ts
```

Expected: FAIL — counter is 5 not 8; no plugin calls; nothing undoable;
`transact` does not exist.

- [ ] **Step 3: Add transactions to `CRDTStore`**

Add the fields and the public method:

```ts
  /** Depth of nested transact() calls. Zero means notify immediately. */
  private txDepth = 0;

  /** Documents touched while suspended: table → pks. */
  private txTouched = new Map<string, Set<string>>();

  /** True when pending changes were written while suspended. */
  private txPendingDirty = false;

  /**
   * Run `fn` as one transaction: persistence and listener notification are
   * suspended until the outermost call returns, so a multi-write batch
   * produces exactly one render instead of one per field.
   *
   * Re-entrant — nested calls join the outer transaction.
   */
  transact<T>(fn: () => T): T {
    this.txDepth++;
    try {
      return fn();
    } finally {
      this.txDepth--;
      if (this.txDepth === 0) this.flushTransaction();
    }
  }

  private flushTransaction(): void {
    const touched = this.txTouched;
    const pendingDirty = this.txPendingDirty;
    this.txTouched = new Map();
    this.txPendingDirty = false;

    for (const [table, pks] of touched) {
      for (const pk of pks) {
        this.persistDocument(table, pk);
        this.notifyListeners(table, pk);
      }
    }
    if (pendingDirty) this.persistPending();
  }
```

Route the two fire-and-forget paths and the notifier through the suspension.
Rename the existing bodies to `*Now` and add the guards:

```ts
  private persistDocument(table: string, pk: string): void {
    if (this.txDepth > 0) {
      let pks = this.txTouched.get(table);
      if (!pks) { pks = new Set(); this.txTouched.set(table, pks); }
      pks.add(pk);
      return;
    }
    this.persistDocumentNow(table, pk);
  }

  private persistPending(): void {
    if (this.txDepth > 0) { this.txPendingDirty = true; return; }
    this.persistPendingNow();
  }

  private notifyListeners(table: string, pk: string): void {
    if (this.txDepth > 0) {
      let pks = this.txTouched.get(table);
      if (!pks) { pks = new Set(); this.txTouched.set(table, pks); }
      pks.add(pk);
      return;
    }
    this.notifyListenersNow(table, pk);
  }
```

`flushTransaction` must call `persistDocumentNow` / `notifyListenersNow` /
`persistPendingNow` directly, not the guarded wrappers — `txDepth` is already
zero by then, so either works, but calling the `*Now` forms makes the
intent explicit and survives future guard changes.

- [ ] **Step 4: Rewrite `BatchWriter` to defer to the store**

Replace the whole `BatchWriter` class:

```ts
/**
 * Batch writer for atomic multi-field updates on a single document.
 *
 * Queues calls to the store's own mutators and replays them inside a
 * single transact(), so batched writes get identical semantics to direct
 * ones — plugin hooks, undo recording, and cumulative counter totals —
 * with one persist and one notification.
 *
 * @example
 * ```ts
 * const changes = store.batch("users", "user-1")
 *   .setField("name", "Alice")
 *   .incrementCounter("login_count")
 *   .addToSet("tags", ["admin"])
 *   .commit();
 * ```
 */
export class BatchWriter {
  private ops: Array<() => ChangeRecord | null> = [];

  constructor(
    private store: CRDTStore,
    private table: string,
    private pk: string
  ) {}

  setField(field: string, value: unknown): this {
    this.ops.push(() => this.store.setField(this.table, this.pk, field, value));
    return this;
  }

  incrementCounter(field: string, delta = 1): this {
    this.ops.push(() =>
      this.store.incrementCounter(this.table, this.pk, field, delta));
    return this;
  }

  decrementCounter(field: string, delta = 1): this {
    this.ops.push(() =>
      this.store.decrementCounter(this.table, this.pk, field, delta));
    return this;
  }

  addToSet(field: string, elements: unknown[]): this {
    this.ops.push(() => this.store.addToSet(this.table, this.pk, field, elements));
    return this;
  }

  removeFromSet(field: string, elements: unknown[]): this {
    this.ops.push(() =>
      this.store.removeFromSet(this.table, this.pk, field, elements));
    return this;
  }

  insertIntoList(field: string, value: unknown, afterId?: HLC): this {
    this.ops.push(() =>
      this.store.insertIntoList(this.table, this.pk, field, value, afterId));
    return this;
  }

  setDocumentField(field: string, path: string, value: unknown): this {
    this.ops.push(() =>
      this.store.setDocumentField(this.table, this.pk, field, path, value));
    return this;
  }

  insertText(field: string, index: number, content: string): this {
    this.ops.push(() =>
      this.store.insertText(this.table, this.pk, field, index, content));
    return this;
  }

  /**
   * Commit all queued writes as one transaction.
   * Returns the changes that were actually applied — a write a plugin
   * rejected is omitted, and does not abort the rest of the batch.
   */
  commit(): ChangeRecord[] {
    const ops = this.ops;
    this.ops = [];
    if (ops.length === 0) return [];

    return this.store.transact(() => {
      const applied: ChangeRecord[] = [];
      for (const op of ops) {
        const change = op();
        if (change) applied.push(change);
      }
      return applied;
    });
  }
}
```

Delete `CRDTStore.commitBatch` — nothing calls it once `BatchWriter` routes
through the public mutators. Check for external callers first:

```bash
grep -rn "commitBatch" crdt-js/src crdt-js/../docs
```

If any documentation references it, update that reference in the same commit.

- [ ] **Step 5: Run the tests**

```bash
npx vitest run --project node src/__tests__/batch.test.ts
npx vitest run --project node
```

Expected: PASS. Existing `store.test.ts` batch tests may assert the old
`commitBatch` shape or the old per-field notification count; those assertions
encoded D4 and should be updated, with a note in the commit body.

- [ ] **Step 6: Commit**

```bash
git add crdt-js/src/store.ts crdt-js/src/__tests__/batch.test.ts
git commit -m "fix(crdt-js): route BatchWriter through store mutators via transact()"
```

---

### Task 10: Stream connect re-entrancy and liveness watchdog

Fixes **D7** and adds detection for a silently dead socket.

**Files:**
- Modify: `src/stream.ts`
- Modify: `src/__tests__/stream.test.ts` (append)

**Interfaces:**
- Produces: `StreamConfig` gains `idleTimeout?: number` (ms, default 45000,
  `0` disables). Add it to `src/types.ts` alongside `reconnectDelay`.

- [ ] **Step 1: Write the failing tests**

Append to `src/__tests__/stream.test.ts` (reuse the file's existing fetch
mock helpers rather than inventing new ones — read the top of the file first):

```ts
describe("connect re-entrancy", () => {
  it("a second connect() does not start a second loop", async () => {
    let opens = 0;
    const fetchImpl = (async () => {
      opens++;
      return new Response(new ReadableStream({ start() {} }), {
        status: 200, headers: { "Content-Type": "text/event-stream" },
      });
    }) as unknown as typeof fetch;

    const stream = new CRDTStream("http://x", {}, {}, fetchImpl);
    stream.connect();
    stream.connect();
    await new Promise((r) => setTimeout(r, 20));
    expect(opens).toBe(1);
    stream.disconnect();
  });

  it("disconnect then connect starts a fresh loop", async () => {
    let opens = 0;
    const fetchImpl = (async () => {
      opens++;
      return new Response(new ReadableStream({ start() {} }), {
        status: 200, headers: { "Content-Type": "text/event-stream" },
      });
    }) as unknown as typeof fetch;

    const stream = new CRDTStream("http://x", {}, {}, fetchImpl);
    stream.connect();
    await new Promise((r) => setTimeout(r, 20));
    stream.disconnect();
    stream.connect();
    await new Promise((r) => setTimeout(r, 20));
    expect(opens).toBe(2);
    stream.disconnect();
  });
});
```

- [ ] **Step 2: Run to verify it fails**

```bash
npx vitest run --project node src/__tests__/stream.test.ts -t "connect re-entrancy"
```

Expected: FAIL — `expected 2 to be 1`. The `_connected` guard is still false
when the second `connect()` lands, so two loops start.

- [ ] **Step 3: Fix the guard**

In `src/stream.ts`, replace `connect()`:

```ts
  /** Start the SSE connection. Idempotent while a loop is running. */
  connect(): void {
    // Guard on shouldReconnect, not _connected: _connected stays false
    // until response headers arrive, so two quick calls would otherwise
    // start two loops and leak the first AbortController.
    if (this.shouldReconnect) return;
    this.shouldReconnect = true;
    void this.connectLoop();
  }
```

- [ ] **Step 4: Add the idle watchdog**

Add the field and config plumbing:

```ts
  private idleTimeout: number;
  private idleTimer: ReturnType<typeof setTimeout> | null = null;
```

```ts
    this.idleTimeout = config?.idleTimeout ?? 45_000;
```

Add the helpers:

```ts
  /**
   * Abort the connection if nothing arrives for idleTimeout ms.
   *
   * A TCP connection can die without the read ever completing, which leaves
   * the reader parked forever and no reconnect is ever attempted. The
   * server's SSE keep-alive comments are enough to keep this armed.
   */
  private armIdleTimer(): void {
    if (this.idleTimeout <= 0) return;
    this.clearIdleTimer();
    this.idleTimer = setTimeout(() => {
      this.abortController?.abort();
    }, this.idleTimeout);
  }

  private clearIdleTimer(): void {
    if (this.idleTimer) {
      clearTimeout(this.idleTimer);
      this.idleTimer = null;
    }
  }
```

In `connectOnce`, call `this.armIdleTimer()` right after `this._connected = true`,
call it again after each successful `reader.read()` that returns data, and call
`this.clearIdleTimer()` in the `finally` block next to `reader.releaseLock()`.
Also call `this.clearIdleTimer()` at the top of `disconnect()`.

- [ ] **Step 5: Add `idleTimeout` to the config type**

In `src/types.ts`, in `StreamConfig`:

```ts
  /**
   * Abort and reconnect if no bytes arrive for this many ms (default:
   * 45000). Set 0 to disable. Server keep-alive comments reset it.
   */
  idleTimeout?: number;
```

- [ ] **Step 6: Run the tests**

```bash
npx vitest run --project node src/__tests__/stream.test.ts
npx vitest run --project node
```

Expected: PASS, including all 27 existing stream tests.

- [ ] **Step 7: Commit**

```bash
git add crdt-js/src/stream.ts crdt-js/src/types.ts crdt-js/src/__tests__/stream.test.ts
git commit -m "fix(crdt-js): guard stream connect re-entrancy; add idle watchdog"
```

---

### Task 11: Debounce persistence

`persistPending()` copies the entire pending array on every write and hands it
to the adapter. With IndexedDB that is a real write per keystroke.

**Files:**
- Modify: `src/store.ts`
- Create: `src/__tests__/persist-debounce.test.ts`

**Interfaces:**
- Produces: `CRDTStore` constructor gains an optional fourth argument
  `options?: { persistDebounceMs?: number }` (default 50; `0` disables,
  which existing tests rely on for synchronous assertions).
  `CRDTStore.flushPersistence(): Promise<void>` forces a pending flush.

- [ ] **Step 1: Write the failing test**

Create `src/__tests__/persist-debounce.test.ts`:

```ts
import { describe, it, expect, vi } from "vitest";
import { CRDTStore } from "../store.js";
import { HybridClock } from "../hlc.js";
import { MemoryStorage } from "../storage.js";

describe("persistence debounce", () => {
  it("coalesces rapid writes into one savePendingChanges call", async () => {
    const storage = new MemoryStorage();
    const spy = vi.spyOn(storage, "savePendingChanges");
    const store = new CRDTStore("n1", new HybridClock("n1"), storage, {
      persistDebounceMs: 20,
    });
    for (let i = 0; i < 50; i++) store.setField("t", "p", `f${i}`, i);
    expect(spy).not.toHaveBeenCalled();
    await store.flushPersistence();
    expect(spy).toHaveBeenCalledTimes(1);
    expect(spy.mock.calls[0][0]).toHaveLength(50);
  });

  it("persistDebounceMs: 0 writes synchronously", () => {
    const storage = new MemoryStorage();
    const spy = vi.spyOn(storage, "savePendingChanges");
    const store = new CRDTStore("n1", new HybridClock("n1"), storage, {
      persistDebounceMs: 0,
    });
    store.setField("t", "p", "f", 1);
    expect(spy).toHaveBeenCalledTimes(1);
  });
});
```

- [ ] **Step 2: Run to verify it fails**

```bash
npx vitest run --project node src/__tests__/persist-debounce.test.ts
```

Expected: FAIL — the constructor takes no options and `flushPersistence` does
not exist.

- [ ] **Step 3: Implement the debounce**

In `src/store.ts`:

```ts
  private persistDebounceMs: number;
  private pendingPersistTimer: ReturnType<typeof setTimeout> | null = null;
  private docPersistQueue = new Map<string, Set<string>>();
```

Extend the constructor signature (the fourth parameter is optional, so every
existing two- and three-argument call site keeps compiling):

```ts
  constructor(
    nodeID: string,
    clock: HybridClock,
    storage?: StorageAdapter,
    options?: { persistDebounceMs?: number }
  ) {
    // ... existing assignments ...
    this.persistDebounceMs = options?.persistDebounceMs ?? 50;
    this.ready = this.hydrate();
  }
```

```ts
  private persistPendingNow(): void {
    if (this.persistDebounceMs <= 0) {
      this.storage.savePendingChanges([...this.pending]).catch(() => {});
      return;
    }
    if (this.pendingPersistTimer) return;
    this.pendingPersistTimer = setTimeout(() => {
      this.pendingPersistTimer = null;
      this.storage.savePendingChanges([...this.pending]).catch(() => {});
    }, this.persistDebounceMs);
  }

  /**
   * Force any debounced writes to storage and wait for them.
   * Call before unload, or in tests that assert on the adapter.
   */
  async flushPersistence(): Promise<void> {
    if (this.pendingPersistTimer) {
      clearTimeout(this.pendingPersistTimer);
      this.pendingPersistTimer = null;
    }
    const queued = this.docPersistQueue;
    this.docPersistQueue = new Map();
    const writes: Promise<void>[] = [];
    for (const [table, pks] of queued) {
      for (const pk of pks) {
        const doc = this.getDoc(table, pk);
        if (doc) {
          const transformed = this.plugins.dispatchBeforePersist(table, pk, doc);
          writes.push(this.storage.saveDocument(table, pk, transformed));
        }
      }
    }
    writes.push(this.storage.savePendingChanges([...this.pending]));
    await Promise.allSettled(writes);
  }
```

Apply the same debounce shape to `persistDocumentNow`, queueing into
`docPersistQueue` and draining on the same timer. Document-level writes are
already deduplicated per `(table, pk)` by the Set, so a burst of edits to one
document collapses to a single `saveDocument`.

- [ ] **Step 4: Run the tests**

```bash
npx vitest run --project node src/__tests__/persist-debounce.test.ts
npx vitest run --project node
```

Expected: PASS. `storage.test.ts` has 20 tests that assert on adapter calls;
any that now race the debounce should pass `{ persistDebounceMs: 0 }` rather
than sleeping. Note each one changed.

- [ ] **Step 5: Commit**

```bash
git add crdt-js/src/store.ts crdt-js/src/__tests__/persist-debounce.test.ts
git commit -m "perf(crdt-js): debounce document and pending-change persistence"
```

---

## Phase 3 — Sync orchestration

### Task 12: Extract `SyncEngine` with a safe pending clear

Fixes **D11**. The pull/apply/push/clear cycle exists only inside `useCRDT`,
and its `clearPendingChanges()` drops writes made while the push was in
flight — those writes are lost, not deferred.

**Files:**
- Create: `src/sync.ts`
- Create: `src/__tests__/sync.test.ts`
- Modify: `src/store.ts` (`clearPendingChanges` overload, plugin accessor)
- Modify: `src/client.ts`, `src/index.ts`

**Interfaces:**
- Produces:
  - `CRDTStore.clearPendingChanges(pushed?: readonly ChangeRecord[]): void`
    — no argument clears everything (unchanged behavior); with an argument
    clears exactly those records by identity.
  - `CRDTStore.pluginManager: PluginManager` — `@internal` getter.
  - `class SyncEngine { constructor(client: CRDTClient, store: CRDTStore); sync(): Promise<SyncReport>; get lastSyncTime(): number | null; get lastPulledHLC(): HLC | null }`
  - `CRDTClient.sync` is NOT added — the engine needs both halves, so
    construction stays explicit.

- [ ] **Step 1: Write the failing test**

Create `src/__tests__/sync.test.ts`:

```ts
import { describe, it, expect } from "vitest";
import { SyncEngine } from "../sync.js";
import { CRDTClient } from "../client.js";
import { CRDTStore } from "../store.js";
import type { Transport, PushRequest, ChangeRecord } from "../types.js";

const HLC0 = { ts: "0", c: 0, node: "" };

function mk(onPush?: (req: PushRequest) => void) {
  const transport: Transport = {
    async pull() { return { changes: [], latest_hlc: HLC0 }; },
    async push(req) {
      onPush?.(req);
      return { merged: req.changes.length, latest_hlc: HLC0 };
    },
  };
  const client = new CRDTClient({ nodeID: "n1", transport });
  const store = new CRDTStore("n1", client.clock, undefined, { persistDebounceMs: 0 });
  return { client, store, engine: new SyncEngine(client, store) };
}

describe("SyncEngine", () => {
  it("pushes pending changes and clears them", async () => {
    const { store, engine } = mk();
    store.setField("t", "p", "f", 1);
    const report = await engine.sync();
    expect(report.pushed).toBe(1);
    expect(store.pendingCount).toBe(0);
  });

  it("does not lose writes made while a push is in flight (D11)", async () => {
    let release!: () => void;
    const gate = new Promise<void>((r) => { release = r; });
    const transport: Transport = {
      async pull() { return { changes: [], latest_hlc: HLC0 }; },
      async push(req) {
        await gate;
        return { merged: req.changes.length, latest_hlc: HLC0 };
      },
    };
    const client = new CRDTClient({ nodeID: "n1", transport });
    const store = new CRDTStore("n1", client.clock, undefined, { persistDebounceMs: 0 });
    const engine = new SyncEngine(client, store);

    store.setField("t", "p", "a", 1);
    const inFlight = engine.sync();
    // This write lands while the push is still awaiting the gate.
    store.setField("t", "p", "b", 2);
    release();
    await inFlight;

    expect(store.pendingCount).toBe(1);
    expect(store.getPendingChanges()[0].field).toBe("b");
  });

  it("runs beforePush and afterPush hooks (D5)", async () => {
    const { store, engine } = mk();
    const calls: string[] = [];
    store.use({
      name: "sync-spy",
      beforePush(changes: ChangeRecord[]) { calls.push(`before:${changes.length}`); return changes; },
      afterPush(ev: { pushed: number }) { calls.push(`after:${ev.pushed}`); },
    });
    store.setField("t", "p", "f", 1);
    await engine.sync();
    expect(calls).toEqual(["before:1", "after:1"]);
  });

  it("runs beforePull and afterPull hooks (D5)", async () => {
    const { store, engine } = mk();
    const calls: string[] = [];
    store.use({
      name: "pull-spy",
      beforePull(ev) { calls.push("before"); return ev; },
      afterPull(ev) { calls.push(`after:${ev.changes.length}`); },
    });
    await engine.sync();
    expect(calls).toEqual(["before", "after:0"]);
  });

  it("beforePush returning null cancels the push and keeps pending", async () => {
    let pushes = 0;
    const { store, engine } = mk(() => { pushes++; });
    store.use({ name: "veto", beforePush() { return null; } });
    store.setField("t", "p", "f", 1);
    await engine.sync();
    expect(pushes).toBe(0);
    expect(store.pendingCount).toBe(1);
  });
});
```

- [ ] **Step 2: Run to verify it fails**

```bash
npx vitest run --project node src/__tests__/sync.test.ts
```

Expected: FAIL — `src/sync.ts` does not exist.

- [ ] **Step 3: Make `clearPendingChanges` selective**

In `src/store.ts`:

```ts
  /**
   * Clear pending changes after a successful push.
   *
   * Pass the exact records that were pushed to clear only those. Clearing
   * everything would also discard writes made WHILE the push was in
   * flight, silently losing them.
   */
  clearPendingChanges(pushed?: readonly ChangeRecord[]): void {
    if (!pushed) {
      this.pending = [];
    } else {
      const sent = new Set(pushed);
      this.pending = this.pending.filter((c) => !sent.has(c));
    }
    this.persistPending();
  }

  /** @internal Sync orchestration needs the hook chain. */
  get pluginManager(): PluginManager {
    return this.plugins;
  }
```

Identity comparison is sound because `getPendingChanges()` returns the same
object references that `pending` holds.

- [ ] **Step 4: Write the sync engine**

Create `src/sync.ts`:

```ts
/**
 * Sync orchestration: the pull → apply → push → clear cycle.
 *
 * Lives in the core rather than in the React hook so non-React consumers
 * get the same behavior, and so SyncHook has real call sites.
 */

import type { ChangeRecord, HLC, SyncReport } from "./types.js";
import type { CRDTClient } from "./client.js";
import type { CRDTStore } from "./store.js";

export class SyncEngine {
  private _lastSyncTime: number | null = null;
  private _lastPulledHLC: HLC | null = null;
  private inFlight: Promise<SyncReport> | null = null;

  constructor(
    private client: CRDTClient,
    private store: CRDTStore
  ) {}

  /** Timestamp of the last successful sync, ms since epoch. */
  get lastSyncTime(): number | null {
    return this._lastSyncTime;
  }

  /** Server HLC watermark from the last successful pull. */
  get lastPulledHLC(): HLC | null {
    return this._lastPulledHLC;
  }

  /**
   * Run one full sync. Concurrent calls share the in-flight run rather
   * than racing each other into a double push.
   */
  sync(): Promise<SyncReport> {
    if (this.inFlight) return this.inFlight;
    this.inFlight = this.run().finally(() => {
      this.inFlight = null;
    });
    return this.inFlight;
  }

  private async run(): Promise<SyncReport> {
    const plugins = this.store.pluginManager;
    const report: SyncReport = { pulled: 0, pushed: 0, merged: 0, conflicts: 0 };

    // Snapshot BEFORE any await in this run — including the pull leg — so
    // writes landing anywhere during the round trip stay pending instead of
    // being cleared out from under the user. This runs synchronously in the
    // same tick that called sync(), before control ever yields.
    const snapshot: ChangeRecord[] = this.store.getPendingChanges();

    // --- Pull ---
    const pullEvent = plugins.dispatchBeforePull({
      tables: [],
      since: this._lastPulledHLC ?? undefined,
    });
    if (pullEvent) {
      const resp = await this.client.pull(
        pullEvent.tables.length > 0 ? pullEvent.tables : undefined,
        pullEvent.since
      );
      report.pulled = resp.changes.length;
      if (resp.changes.length > 0) {
        this.store.applyChanges(resp.changes);
      }
      if (resp.latest_hlc) this._lastPulledHLC = resp.latest_hlc;
      plugins.dispatchAfterPull({ ...pullEvent, changes: resp.changes });
    }

    // --- Push ---
    // NOTE: `snapshot` is taken at the TOP of run(), before the pull await —
    // see the comment there. Taking it here instead is wrong: a write made
    // synchronously right after sync() returns lands while run() is
    // suspended on the pull, so it would be inside this snapshot and get
    // cleared. Corrected 2026-08-20 after Task 12 caught it.
    if (snapshot.length > 0) {
      const toPush = plugins.dispatchBeforePush(snapshot);
      if (toPush && toPush.length > 0) {
        const resp = await this.client.push(toPush);
        report.pushed = toPush.length;
        report.merged = resp.merged;
        this.store.clearPendingChanges(snapshot);
        plugins.dispatchAfterPush({ pushed: toPush.length, changes: toPush });
      }
    }

    this._lastSyncTime = Date.now();
    return report;
  }
}
```

Note the `clearPendingChanges(snapshot)` call clears the pre-push snapshot,
not `toPush` — a `beforePush` plugin may filter or rewrite the array, and the
records the user queued are still the ones to retire.

- [ ] **Step 5: Export it**

In `src/index.ts`, next to the store exports:

```ts
// Sync orchestration
export { SyncEngine } from "./sync.js";
```

- [ ] **Step 6: Run the tests**

```bash
npx vitest run --project node src/__tests__/sync.test.ts
npx vitest run --project node
npx tsc --noEmit
```

Expected: PASS, 5 new tests, no regressions, clean typecheck.

- [ ] **Step 7: Commit**

```bash
git add crdt-js/src/sync.ts crdt-js/src/store.ts crdt-js/src/index.ts crdt-js/src/__tests__/sync.test.ts
git commit -m "feat(crdt-js): SyncEngine with in-flight-safe pending clear"
```

---

### Task 13: Wire the presence hooks

Completes **D5**. `dispatchBeforePresenceUpdate` and `dispatchOnPresenceEvent`
still have no call sites after Task 12.

**Files:**
- Modify: `src/client.ts`
- Modify: `src/__tests__/sync.test.ts` (append)

**Interfaces:**
- Consumes: `CRDTStore.pluginManager` (Task 12).
- Produces: `CRDTClient` gains an optional `store` link so presence hooks can
  reach the plugin chain: `CRDTClient.attachStore(store: CRDTStore): void`.

- [ ] **Step 1: Write the failing test**

Append to `src/__tests__/sync.test.ts`:

```ts
describe("presence hooks", () => {
  it("beforePresenceUpdate can rewrite outgoing data (D5)", async () => {
    const sent: unknown[] = [];
    const transport: Transport = {
      async pull() { return { changes: [], latest_hlc: HLC0 }; },
      async push() { return { merged: 0, latest_hlc: HLC0 }; },
      async updatePresence(u) { sent.push(u.data); },
      async getPresence() { return []; },
    };
    const client = new CRDTClient({ nodeID: "n1", transport });
    const store = new CRDTStore("n1", client.clock, undefined, { persistDebounceMs: 0 });
    client.attachStore(store);
    store.use({
      name: "redact",
      beforePresenceUpdate(_topic, data) {
        return { ...(data as object), redacted: true };
      },
    });
    await client.updatePresence("t", { name: "Alice" });
    expect(sent[0]).toEqual({ name: "Alice", redacted: true });
    await client.leaveAllPresence();
  });

  it("beforePresenceUpdate returning null cancels the update", async () => {
    let calls = 0;
    const transport: Transport = {
      async pull() { return { changes: [], latest_hlc: HLC0 }; },
      async push() { return { merged: 0, latest_hlc: HLC0 }; },
      async updatePresence() { calls++; },
      async getPresence() { return []; },
    };
    const client = new CRDTClient({ nodeID: "n1", transport });
    const store = new CRDTStore("n1", client.clock, undefined, { persistDebounceMs: 0 });
    client.attachStore(store);
    store.use({ name: "veto", beforePresenceUpdate() { return null; } });
    await client.updatePresence("t", { name: "Alice" });
    expect(calls).toBe(0);
  });

  it("onPresenceEvent fires for inbound events", () => {
    const client = new CRDTClient({
      nodeID: "n1",
      transport: {
        async pull() { return { changes: [], latest_hlc: HLC0 }; },
        async push() { return { merged: 0, latest_hlc: HLC0 }; },
      },
    });
    const store = new CRDTStore("n1", client.clock, undefined, { persistDebounceMs: 0 });
    client.attachStore(store);
    const seen: string[] = [];
    store.use({ name: "spy", onPresenceEvent(ev) { seen.push(ev.type); } });
    client.applyPresenceEvent({ type: "join", node_id: "peer", topic: "t", data: {} });
    expect(seen).toEqual(["join"]);
  });
});
```

- [ ] **Step 2: Run to verify it fails**

```bash
npx vitest run --project node src/__tests__/sync.test.ts -t "presence hooks"
```

Expected: FAIL — `attachStore` and `applyPresenceEvent` do not exist.

- [ ] **Step 3: Add the store link and the hook call sites**

In `src/client.ts`, add the field and method:

```ts
  private store: CRDTStore | null = null;

  /**
   * Link a store so presence operations run through its plugin chain.
   * Optional — presence works without it, just without hooks.
   */
  attachStore(store: CRDTStore): void {
    this.store = store;
  }

  /**
   * Apply an inbound presence event, running PresenceHook first.
   * Prefer this over calling client.presence.applyEvent() directly.
   */
  applyPresenceEvent(event: PresenceEvent): void {
    this.store?.pluginManager.dispatchOnPresenceEvent({
      type: event.type,
      nodeId: event.node_id,
      topic: event.topic,
      data: event.data,
    });
    this.presence.applyEvent(event);
  }
```

Import `CRDTStore` as a type only, to avoid a runtime import cycle
(`store.ts` does not import `client.ts`, but keep it type-only regardless):

```ts
import type { CRDTStore } from "./store.js";
import type { PresenceEvent } from "./types.js";
```

In `updatePresence`, run the hook before sending:

```ts
    const hooked = this.store
      ? this.store.pluginManager.dispatchBeforePresenceUpdate(topic, data)
      : data;
    if (hooked === null) return;

    this.lastPresenceData.set(topic, hooked);

    await this.transport.updatePresence({
      node_id: this.nodeID,
      topic,
      data: hooked as Record<string, unknown>,
    });

    this.startHeartbeat(topic);
```

- [ ] **Step 4: Verify no dead dispatches remain**

```bash
for h in dispatchBeforePull dispatchAfterPull dispatchBeforePush \
         dispatchAfterPush dispatchBeforePresenceUpdate dispatchOnPresenceEvent; do
  printf "%-32s " "$h"
  grep -rn "$h" crdt-js/src | grep -v "src/plugin.ts" | grep -v "__tests__" | wc -l
done
```

Expected: every count is at least 1. This exact command returned all zeros
before this plan, which is what D5 was.

- [ ] **Step 5: Run the tests**

```bash
npx vitest run --project node
npx tsc --noEmit
```

Expected: PASS, clean.

- [ ] **Step 6: Commit**

```bash
git add crdt-js/src/client.ts crdt-js/src/__tests__/sync.test.ts
git commit -m "feat(crdt-js): wire PresenceHook call sites through CRDTClient"
```

---

### Task 14: React delegates to `SyncEngine`

**Files:**
- Modify: `src/react.tsx`
- Modify: `src/__tests__/react.test.tsx` (append)

**Interfaces:**
- Consumes: `SyncEngine` (Task 12), `CRDTClient.attachStore` /
  `applyPresenceEvent` (Task 13).
- Produces: `UseCRDTReturn` gains `engine: SyncEngine`. `sync`, `status`,
  `lastSyncTime`, and `pendingCount` keep their current types.

- [ ] **Step 1: Write the failing test**

Append to `src/__tests__/react.test.tsx`:

```tsx
import { useSyncStatus } from "../react.js";

function SyncView() {
  const { pendingCount, sync } = useSyncStatus();
  return (
    <div>
      <span data-testid="pending">{pendingCount}</span>
      <button onClick={() => void sync()}>sync</button>
    </div>
  );
}

describe("sync integration", () => {
  it("pushes through SyncEngine and clears pending", async () => {
    const pushed: number[] = [];
    const transport: Transport = {
      async pull() { return { changes: [], latest_hlc: HLC0 }; },
      async push(req) {
        pushed.push(req.changes.length);
        return { merged: req.changes.length, latest_hlc: HLC0 };
      },
    };
    render(
      <CRDTProvider config={{ ...config, transport }}>
        <DocView />
        <SyncView />
      </CRDTProvider>
    );
    expect(screen.getByTestId("pending").textContent).toBe("0");
    await act(async () => {
      screen.getByText("sync").click();
    });
    expect(pushed).toEqual([]);
  });
});
```

- [ ] **Step 2: Run it**

```bash
npx vitest run --project dom
```

Expected: PASS or FAIL depending on the current hook wiring — this test pins
the behavior through the refactor rather than driving it. Record which.

- [ ] **Step 3: Replace `useCRDT`'s inline sync**

In `src/react.tsx`, add the import and replace the `sync` callback and the
stream handler:

```tsx
import { SyncEngine } from "./sync.js";
```

```tsx
  const engineRef = useRef<SyncEngine | null>(null);
  if (!engineRef.current) {
    clientRef.current!.attachStore(storeRef.current!);
    engineRef.current = new SyncEngine(clientRef.current!, storeRef.current!);
  }
  const engine = engineRef.current;

  const sync = useCallback(async () => {
    setStatus("syncing");
    try {
      await engine.sync();
      setPendingCount(store.pendingCount);
      setLastSyncTime(engine.lastSyncTime);
      setStatus("connected");
    } catch {
      setStatus("error");
    }
  }, [engine, store]);
```

In the stream effect, route presence through the client so the hook fires:

```tsx
        case "presence":
          client.applyPresenceEvent(event.data);
          break;
```

Add `engine` to the returned object and to `CRDTContextValue`.

- [ ] **Step 4: Run everything**

```bash
npx vitest run
npx tsc --noEmit
```

Expected: PASS, clean.

- [ ] **Step 5: Commit**

```bash
git add crdt-js/src/react.tsx crdt-js/src/__tests__/react.test.tsx
git commit -m "refactor(crdt-js): useCRDT delegates sync to SyncEngine"
```

---

## Phase 4 — Features

### Task 15: Exponential backoff with full jitter

The stream retries on a fixed 5 s delay, so after a server outage every client
reconnects in lockstep.

**Files:**
- Create: `src/backoff.ts`
- Create: `src/__tests__/backoff.test.ts`
- Modify: `src/stream.ts`, `src/types.ts`, `src/index.ts`

**Interfaces:**
- Produces:
  - `interface BackoffOptions { initialDelay?: number; maxDelay?: number; factor?: number; jitter?: boolean; random?: () => number }`
  - `class Backoff { constructor(opts?: BackoffOptions); next(): number; reset(): void; get attempt(): number }`
  - `StreamConfig` gains `maxReconnectDelay?: number` (default 30000).

- [ ] **Step 1: Write the failing test**

Create `src/__tests__/backoff.test.ts`:

```ts
import { describe, it, expect } from "vitest";
import { Backoff } from "../backoff.js";

describe("Backoff", () => {
  it("grows geometrically without jitter", () => {
    const b = new Backoff({ initialDelay: 100, factor: 2, maxDelay: 10_000, jitter: false });
    expect(b.next()).toBe(100);
    expect(b.next()).toBe(200);
    expect(b.next()).toBe(400);
    expect(b.next()).toBe(800);
  });

  it("clamps at maxDelay", () => {
    const b = new Backoff({ initialDelay: 1000, factor: 10, maxDelay: 5000, jitter: false });
    b.next(); b.next();
    expect(b.next()).toBe(5000);
    expect(b.next()).toBe(5000);
  });

  it("full jitter keeps the delay within [0, ceiling]", () => {
    // random() === 1 yields the ceiling; random() === 0 yields zero.
    const hi = new Backoff({ initialDelay: 100, factor: 2, jitter: true, random: () => 1 });
    const lo = new Backoff({ initialDelay: 100, factor: 2, jitter: true, random: () => 0 });
    expect(hi.next()).toBe(100);
    expect(lo.next()).toBe(0);
  });

  it("reset returns to the initial delay", () => {
    const b = new Backoff({ initialDelay: 100, factor: 2, jitter: false });
    b.next(); b.next(); b.next();
    expect(b.attempt).toBe(3);
    b.reset();
    expect(b.attempt).toBe(0);
    expect(b.next()).toBe(100);
  });
});
```

- [ ] **Step 2: Run to verify it fails**

```bash
npx vitest run --project node src/__tests__/backoff.test.ts
```

Expected: FAIL — module not found.

- [ ] **Step 3: Implement it**

Create `src/backoff.ts`:

```ts
/**
 * Exponential backoff with full jitter.
 *
 * Full jitter (delay = random(0, ceiling)) rather than the raw geometric
 * delay: a fixed schedule makes every client that dropped during an outage
 * retry at the same instant, which is what knocks a recovering server back
 * over.
 */

export interface BackoffOptions {
  /** First delay in ms (default: 1000). */
  initialDelay?: number;
  /** Ceiling in ms (default: 30000). */
  maxDelay?: number;
  /** Growth multiplier (default: 2). */
  factor?: number;
  /** Apply full jitter (default: true). */
  jitter?: boolean;
  /** Randomness source, for tests (default: Math.random). */
  random?: () => number;
}

export class Backoff {
  private initialDelay: number;
  private maxDelay: number;
  private factor: number;
  private jitter: boolean;
  private random: () => number;
  private attempts = 0;

  constructor(opts?: BackoffOptions) {
    this.initialDelay = opts?.initialDelay ?? 1000;
    this.maxDelay = opts?.maxDelay ?? 30_000;
    this.factor = opts?.factor ?? 2;
    this.jitter = opts?.jitter ?? true;
    this.random = opts?.random ?? Math.random;
  }

  /** Number of delays issued since the last reset. */
  get attempt(): number {
    return this.attempts;
  }

  /** Next delay in ms, advancing the schedule. */
  next(): number {
    const ceiling = Math.min(
      this.maxDelay,
      this.initialDelay * Math.pow(this.factor, this.attempts)
    );
    this.attempts++;
    return this.jitter ? Math.floor(this.random() * ceiling) : ceiling;
  }

  /** Return to the initial delay. Call after a successful connection. */
  reset(): void {
    this.attempts = 0;
  }
}
```

- [ ] **Step 4: Use it in the stream**

In `src/stream.ts`, import `Backoff`, add the field, and construct it from
config:

```ts
  private backoff: Backoff;
```

```ts
    this.backoff = new Backoff({
      initialDelay: config?.reconnectDelay ?? 5000,
      maxDelay: config?.maxReconnectDelay ?? 30_000,
    });
```

In `connectLoop`, reset on a successful connection and use the schedule for
the wait. Inside `connectOnce`, add `this.backoff.reset();` immediately after
`this._connected = true;`. Then replace the wait at the bottom of the loop:

```ts
      // Wait before reconnecting, with jittered exponential backoff.
      const delay = this.backoff.next();
      await new Promise((resolve) => setTimeout(resolve, delay));
```

Add to `StreamConfig` in `src/types.ts`:

```ts
  /** Ceiling for reconnect backoff in ms (default: 30000). */
  maxReconnectDelay?: number;
```

Note the behavior change in the commit body: `reconnectDelay` becomes the
*initial* delay of a jittered schedule rather than a fixed interval.

- [ ] **Step 5: Export and test**

In `src/index.ts`:

```ts
// Reconnect backoff
export { Backoff } from "./backoff.js";
export type { BackoffOptions } from "./backoff.js";
```

```bash
npx vitest run --project node
```

Expected: PASS. `stream.test.ts` tests that assert an exact 5 s reconnect
interval must be updated to construct the stream with `{ jitter: false }`
semantics via `reconnectDelay` plus a tolerance, or to assert "reconnected"
rather than "reconnected after exactly N ms". Note each one changed.

- [ ] **Step 6: Commit**

```bash
git add crdt-js/src/backoff.ts crdt-js/src/stream.ts crdt-js/src/types.ts crdt-js/src/index.ts crdt-js/src/__tests__/backoff.test.ts
git commit -m "feat(crdt-js): jittered exponential backoff for stream reconnect"
```

---

### Task 16: Request timeouts, retry, and empty-body tolerance

`pull`/`push` have no timeout and never retry, despite `TransportError`
setting `retryable: true`. `updatePresence` also calls `response.json()`
unconditionally, which throws on a `204 No Content`.

**Files:**
- Modify: `src/transport.ts`
- Create: `src/transport/retry.ts`
- Modify: `src/__tests__/transport.test.ts` (append)
- Create: `src/__tests__/retry.test.ts`
- Modify: `src/index.ts`

**Interfaces:**
- Produces: `HttpTransportConfig` gains `timeout?: number` (ms, default 30000;
  `0` disables), `retries?: number` (default 2), and
  `backoff?: BackoffOptions`. `HttpTransport.request` keeps its protected
  signature.
- Produces: `withRetry<T extends Transport>(inner: T, opts?: RetryOptions): T`
  in `src/transport/retry.ts`, plus `interface RetryOptions { retries?: number;
  backoff?: BackoffOptions; isRetryable?: (err: unknown) => boolean }`.

**Why the decorator exists.** Timeout stays inside `HttpTransport` because real
cancellation needs `AbortSignal`, which is fetch-specific. Retry does NOT:
baking it into `HttpTransport` would mean a WebSocket transport or any
user-supplied transport silently gets none, which breaks the pluggability
constraint. `withRetry` wraps any `Transport`, and preserves streaming by
delegating `subscribe` when the inner transport has it — so
`isStreamTransport(withRetry(ws))` stays true.

- [ ] **Step 1: Write the failing tests**

Append to `src/__tests__/transport.test.ts`:

```ts
describe("timeout, retry, empty body", () => {
  it("retries a 503 and then succeeds", async () => {
    let calls = 0;
    const fetchImpl = (async () => {
      calls++;
      if (calls < 3) return new Response("busy", { status: 503 });
      return new Response(JSON.stringify({ changes: [], latest_hlc: { ts: "0", c: 0, node: "" } }),
        { status: 200 });
    }) as unknown as typeof fetch;

    const t = new HttpTransport({
      baseURL: "http://x", fetch: fetchImpl, retries: 3,
      backoff: { initialDelay: 1, jitter: false },
    });
    const resp = await t.pull({ tables: ["a"], node_id: "n1" });
    expect(calls).toBe(3);
    expect(resp.changes).toEqual([]);
  });

  it("does not retry a 400", async () => {
    let calls = 0;
    const fetchImpl = (async () => {
      calls++;
      return new Response("bad", { status: 400 });
    }) as unknown as typeof fetch;

    const t = new HttpTransport({
      baseURL: "http://x", fetch: fetchImpl, retries: 3,
      backoff: { initialDelay: 1, jitter: false },
    });
    await expect(t.pull({ tables: ["a"], node_id: "n1" })).rejects.toThrow();
    expect(calls).toBe(1);
  });

  it("tolerates a 204 with no body on presence update", async () => {
    const fetchImpl = (async () =>
      new Response(null, { status: 204 })) as unknown as typeof fetch;
    const t = new HttpTransport({ baseURL: "http://x", fetch: fetchImpl });
    await expect(
      t.updatePresence({ node_id: "n1", topic: "t", data: {} })
    ).resolves.toBeUndefined();
  });

  it("aborts after the timeout", async () => {
    const fetchImpl = ((_u: string, init?: RequestInit) =>
      new Promise((_resolve, reject) => {
        init?.signal?.addEventListener("abort", () =>
          reject(new DOMException("aborted", "AbortError")));
      })) as unknown as typeof fetch;

    const t = new HttpTransport({
      baseURL: "http://x", fetch: fetchImpl, timeout: 20, retries: 0,
    });
    await expect(t.pull({ tables: ["a"], node_id: "n1" })).rejects.toThrow();
  });
});
```

- [ ] **Step 2: Run to verify it fails**

```bash
npx vitest run --project node src/__tests__/transport.test.ts -t "timeout, retry, empty body"
```

Expected: FAIL — no retry (calls is 1), 204 throws a JSON parse error, no
timeout support.

- [ ] **Step 3: Implement**

In `src/transport.ts`, add the import:

```ts
import { Backoff } from "./backoff.js";
import type { BackoffOptions } from "./backoff.js";
```

Then add to `HttpTransportConfig`:

```ts
  /** Per-request timeout in ms (default: 30000). 0 disables. */
  timeout?: number;
  /** Retry attempts for retryable failures (default: 2). */
  retries?: number;
  /** Backoff schedule between retries. */
  backoff?: BackoffOptions;
```

Add the fields and replace `request`:

```ts
  protected timeout: number;
  protected retries: number;
  protected backoffOpts: BackoffOptions;
```

```ts
/** 5xx, 429, and 408 are worth retrying; other 4xx are the caller's fault. */
function isRetryableStatus(status: number): boolean {
  return status >= 500 || status === 429 || status === 408;
}

/** Fetch with an AbortSignal-backed timeout. */
async function fetchWithTimeout(
  fetchImpl: typeof fetch,
  url: string,
  init: RequestInit,
  timeout: number
): Promise<Response> {
  if (timeout <= 0) return fetchImpl(url, init);
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeout);
  try {
    return await fetchImpl(url, { ...init, signal: controller.signal });
  } finally {
    clearTimeout(timer);
  }
}
```

```ts
  protected async request<T>(path: string, body: unknown): Promise<T> {
    const backoff = new Backoff(this.backoffOpts);
    let lastError: unknown;

    for (let attempt = 0; attempt <= this.retries; attempt++) {
      if (attempt > 0) {
        await new Promise((r) => setTimeout(r, backoff.next()));
      }

      const authHeaders = this.auth
        ? await Promise.resolve(this.auth.getHeaders())
        : {};

      try {
        const response = await fetchWithTimeout(
          this.fetchImpl,
          `${this.baseURL}${path}`,
          {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              Accept: "application/json",
              ...this.headers,
              ...authHeaders,
            },
            body: JSON.stringify(body),
          },
          this.timeout
        );

        if (!response.ok) {
          const text = await response.text().catch(() => "");
          const err = new TransportError(
            `CRDT ${path} returned ${response.status}: ${text}`,
            response.status
          );
          if (!isRetryableStatus(response.status)) throw err;
          lastError = err;
          continue;
        }

        return (await parseJsonOrEmpty<T>(response));
      } catch (err) {
        // A non-retryable TransportError was thrown deliberately above.
        if (err instanceof TransportError && !isRetryableStatus(err.statusCode ?? 0)) {
          throw err;
        }
        lastError = err;
      }
    }

    throw lastError instanceof Error
      ? lastError
      : new TransportError(`CRDT ${path} failed after ${this.retries + 1} attempts`);
  }
```

Add the body parser at module scope:

```ts
/**
 * Parse a JSON response, tolerating an empty body.
 *
 * The presence endpoint answers 204 No Content, and response.json() throws
 * on an empty body.
 */
async function parseJsonOrEmpty<T>(response: Response): Promise<T> {
  if (response.status === 204) return undefined as T;
  const text = await response.text();
  if (text.trim() === "") return undefined as T;
  return JSON.parse(text) as T;
}
```

Apply `fetchWithTimeout` and `parseJsonOrEmpty` to `getPresence` too.

- [ ] **Step 4: Write the failing decorator test**

Create `src/__tests__/retry.test.ts`:

```ts
import { describe, it, expect } from "vitest";
import { withRetry } from "../transport/retry.js";
import { isStreamTransport } from "../transport.js";
import { TransportError } from "../errors.js";
import type { Transport, StreamTransport, PullResponse } from "../types.js";

const HLC0 = { ts: "0", c: 0, node: "" };
const opts = { retries: 3, backoff: { initialDelay: 1, jitter: false } };

describe("withRetry", () => {
  it("retries a retryable failure from ANY transport, not just HTTP", async () => {
    let calls = 0;
    const flaky: Transport = {
      async pull(): Promise<PullResponse> {
        calls++;
        if (calls < 3) throw new TransportError("boom", 503);
        return { changes: [], latest_hlc: HLC0 };
      },
      async push() { return { merged: 0, latest_hlc: HLC0 }; },
    };
    const resp = await withRetry(flaky, opts).pull({ tables: [], node_id: "n1" });
    expect(calls).toBe(3);
    expect(resp.changes).toEqual([]);
  });

  it("does not retry a non-retryable failure", async () => {
    let calls = 0;
    const bad: Transport = {
      async pull(): Promise<PullResponse> {
        calls++;
        throw new TransportError("bad request", 400);
      },
      async push() { return { merged: 0, latest_hlc: HLC0 }; },
    };
    await expect(
      withRetry(bad, opts).pull({ tables: [], node_id: "n1" })
    ).rejects.toThrow();
    expect(calls).toBe(1);
  });

  it("preserves streaming capability", () => {
    const ws: StreamTransport = {
      async pull() { return { changes: [], latest_hlc: HLC0 }; },
      async push() { return { merged: 0, latest_hlc: HLC0 }; },
      subscribe() {
        return {
          on() { return () => {}; }, connect() {}, disconnect() {},
          connected: false, lastHLC: null,
        };
      },
    };
    const wrapped = withRetry(ws, opts);
    expect(isStreamTransport(wrapped)).toBe(true);
    expect(typeof wrapped.subscribe).toBe("function");
  });

  it("passes optional presence methods through only when present", async () => {
    const bare: Transport = {
      async pull() { return { changes: [], latest_hlc: HLC0 }; },
      async push() { return { merged: 0, latest_hlc: HLC0 }; },
    };
    expect(withRetry(bare, opts).updatePresence).toBeUndefined();

    let seen = 0;
    const withPresence: Transport = {
      ...bare,
      async updatePresence() { seen++; },
      async getPresence() { return []; },
    };
    await withRetry(withPresence, opts).updatePresence!({
      node_id: "n1", topic: "t", data: {},
    });
    expect(seen).toBe(1);
  });
});
```

- [ ] **Step 5: Run to verify it fails**

```bash
npx vitest run --project node src/__tests__/retry.test.ts
```

Expected: FAIL — module not found.

- [ ] **Step 6: Implement the decorator**

Create `src/transport/retry.ts`:

```ts
/**
 * Retry decorator for any Transport.
 *
 * Retry lives here rather than inside HttpTransport so that a WebSocket
 * transport, or any transport a consumer plugs in, gets the same
 * resilience. Timeout stays in HttpTransport because real cancellation
 * needs AbortSignal, which is fetch-specific.
 */

import type {
  Transport, StreamTransport, PullRequest, PullResponse,
  PushRequest, PushResponse, PresenceUpdate, PresenceState,
} from "../types.js";
import { CRDTError } from "../errors.js";
import { Backoff } from "../backoff.js";
import type { BackoffOptions } from "../backoff.js";

export interface RetryOptions {
  /** Retry attempts after the first try (default: 2). */
  retries?: number;
  /** Backoff schedule between attempts. */
  backoff?: BackoffOptions;
  /** Override which errors are worth retrying. */
  isRetryable?: (err: unknown) => boolean;
}

/** Default policy: honor CRDTError.retryable; retry unknown errors once. */
function defaultIsRetryable(err: unknown): boolean {
  if (err instanceof CRDTError) return err.retryable;
  return err instanceof Error;
}

async function runWithRetry<T>(
  op: () => Promise<T>,
  retries: number,
  backoffOpts: BackoffOptions | undefined,
  isRetryable: (err: unknown) => boolean
): Promise<T> {
  const backoff = new Backoff(backoffOpts);
  let lastError: unknown;

  for (let attempt = 0; attempt <= retries; attempt++) {
    if (attempt > 0) {
      await new Promise((r) => setTimeout(r, backoff.next()));
    }
    try {
      return await op();
    } catch (err) {
      if (!isRetryable(err)) throw err;
      lastError = err;
    }
  }
  throw lastError;
}

/**
 * Wrap a transport so its pull/push (and presence, when supported) retry
 * on retryable failures. Streaming is delegated untouched — a live
 * subscription has its own reconnect loop, and retrying subscribe() would
 * fight it.
 */
export function withRetry<T extends Transport>(
  inner: T,
  opts?: RetryOptions
): T {
  const retries = opts?.retries ?? 2;
  const isRetryable = opts?.isRetryable ?? defaultIsRetryable;
  const run = <R>(op: () => Promise<R>) =>
    runWithRetry(op, retries, opts?.backoff, isRetryable);

  const wrapped: Transport = {
    pull: (req: PullRequest): Promise<PullResponse> => run(() => inner.pull(req)),
    push: (req: PushRequest): Promise<PushResponse> => run(() => inner.push(req)),
  };

  // Optional members must stay ABSENT when the inner transport lacks them:
  // CRDTClient tests `if (!this.transport.updatePresence)` to decide whether
  // presence is supported, so an always-present stub would claim support the
  // inner transport does not have.
  if (inner.updatePresence) {
    wrapped.updatePresence = (u: PresenceUpdate): Promise<void> =>
      run(() => inner.updatePresence!(u));
  }
  if (inner.getPresence) {
    wrapped.getPresence = (topic: string): Promise<PresenceState[]> =>
      run(() => inner.getPresence!(topic));
  }

  const streaming = inner as StreamTransport;
  if (typeof streaming.subscribe === "function") {
    (wrapped as StreamTransport).subscribe = (config) =>
      streaming.subscribe(config);
  }

  return wrapped as T;
}
```

- [ ] **Step 7: Export it**

In `src/index.ts`:

```ts
// Retry decorator (composes over any transport)
export { withRetry } from "./transport/retry.js";
export type { RetryOptions } from "./transport/retry.js";
```

- [ ] **Step 8: Run the tests**

```bash
npx vitest run --project node
npx tsc --noEmit
```

Expected: PASS, including all 16 existing transport tests and the 4 new
decorator tests. Clean typecheck.

- [ ] **Step 9: Commit**

```bash
git add crdt-js/src/transport.ts crdt-js/src/transport/retry.ts crdt-js/src/index.ts crdt-js/src/__tests__/transport.test.ts crdt-js/src/__tests__/retry.test.ts
git commit -m "feat(crdt-js): request timeouts plus a retry decorator for any transport"
```

---

### Task 17: WebSocket transport

The server ships `crdt/transport_ws.go`; the client is SSE-only. One socket
carries pull, push, presence, and the change stream, multiplexed by
`request_id`.

**Files:**
- Create: `src/transport/websocket.ts`
- Create: `src/__tests__/websocket.test.ts`
- Modify: `src/index.ts`

**Interfaces:**
- Produces:
  - `type WSMessageType` — the exact string union from `crdt/transport_ws.go`.
  - `interface WebSocketMessage { type: WSMessageType; payload?: unknown; request_id?: string }`
  - `interface WebSocketTransportConfig { url: string; protocols?: string | string[]; WebSocketImpl?: typeof WebSocket; requestTimeout?: number; backoff?: BackoffOptions; auth?: AuthProvider }`
  - `class WebSocketTransport implements StreamTransport` — `pull`, `push`,
    `updatePresence`, `subscribe`, plus `close()`.

**Deliberately NOT implemented: `getPresence`.** The Go server's
`handleMessage` (`crdt/transport_ws.go:512+`) handles only `pull_request`,
`push_request`, `subscribe`, `presence_update` and `ping`. `presence_get` and
`unsubscribe` constants exist but hit the `default:` branch and come back as an
error frame. Defining `getPresence` here would therefore ship a method that
always rejects. Leaving it absent is the correct use of the optional-member
contract: `CRDTClient.getPresence` then throws its clear "Transport does not
support presence" message, and `joinPresence`'s `if (this.transport.getPresence)`
feature-detect skips snapshot seeding instead of failing the join. For the same
reason `disconnect()` must NOT send an `unsubscribe` frame — the server would
answer with an uncorrelated error frame and the client would emit a spurious
stream error.

- [ ] **Step 1: Write the failing test**

Create `src/__tests__/websocket.test.ts`:

```ts
import { describe, it, expect, vi } from "vitest";
import { WebSocketTransport } from "../transport/websocket.js";
import type { WebSocketMessage } from "../transport/websocket.js";

const HLC0 = { ts: "0", c: 0, node: "" };

/** Minimal in-memory WebSocket double that echoes canned responses. */
class FakeSocket {
  static OPEN = 1;
  readyState = 0;
  onopen: (() => void) | null = null;
  onmessage: ((ev: { data: string }) => void) | null = null;
  onclose: (() => void) | null = null;
  onerror: (() => void) | null = null;
  sent: WebSocketMessage[] = [];
  respond: (msg: WebSocketMessage) => WebSocketMessage | null = () => null;

  constructor(public url: string) {
    queueMicrotask(() => {
      this.readyState = FakeSocket.OPEN;
      this.onopen?.();
    });
  }
  send(data: string): void {
    const msg = JSON.parse(data) as WebSocketMessage;
    this.sent.push(msg);
    const reply = this.respond(msg);
    if (reply) queueMicrotask(() => this.onmessage?.({ data: JSON.stringify(reply) }));
  }
  close(): void {
    this.readyState = 3;
    this.onclose?.();
  }
}

function mkTransport() {
  let socket!: FakeSocket;
  const Impl = function (url: string) {
    socket = new FakeSocket(url);
    return socket;
  } as unknown as typeof WebSocket;
  const transport = new WebSocketTransport({ url: "ws://x", WebSocketImpl: Impl });
  return { transport, socket: () => socket };
}

describe("WebSocketTransport", () => {
  it("correlates a pull request with its response by request_id", async () => {
    const { transport, socket } = mkTransport();
    await new Promise((r) => queueMicrotask(r as () => void));
    socket().respond = (msg) =>
      msg.type === "pull_request"
        ? { type: "pull_response", request_id: msg.request_id,
            payload: { changes: [], latest_hlc: HLC0 } }
        : null;

    const resp = await transport.pull({ tables: ["a"], node_id: "n1" });
    expect(resp.changes).toEqual([]);
    expect(socket().sent[0].type).toBe("pull_request");
    transport.close();
  });

  it("rejects a request when the server answers with an error frame", async () => {
    const { transport, socket } = mkTransport();
    await new Promise((r) => queueMicrotask(r as () => void));
    socket().respond = (msg) => ({
      type: "error", request_id: msg.request_id, payload: { error: "nope" },
    });
    await expect(transport.push({ changes: [], node_id: "n1" })).rejects.toThrow("nope");
    transport.close();
  });

  it("subscribe sends a subscribe frame and emits inbound changes", async () => {
    const { transport, socket } = mkTransport();
    await new Promise((r) => queueMicrotask(r as () => void));
    const sub = transport.subscribe({ tables: ["docs"] });
    const events: string[] = [];
    sub.on((e) => { events.push(e.type); });
    sub.connect();
    await new Promise((r) => setTimeout(r, 5));

    expect(socket().sent.some((m) => m.type === "subscribe")).toBe(true);
    socket().onmessage?.({ data: JSON.stringify({
      type: "change",
      payload: { table: "docs", pk: "1", field: "f", crdt_type: "lww",
                 hlc: HLC0, node_id: "n2", value: 1 },
    }) });
    expect(events).toContain("change");
    sub.disconnect();
    transport.close();
  });
});
```

- [ ] **Step 2: Run to verify it fails**

```bash
npx vitest run --project node src/__tests__/websocket.test.ts
```

Expected: FAIL — module not found.

- [ ] **Step 3: Implement the transport**

Create `src/transport/websocket.ts`:

```ts
/**
 * WebSocket transport — one socket carrying pull, push, presence, and the
 * change stream, multiplexed by request_id.
 *
 * Wire format mirrors crdt/transport_ws.go exactly. Uses the global
 * WebSocket by default so the package stays dependency-free; pass
 * WebSocketImpl to supply one (e.g. `ws` under Node).
 */

import type {
  StreamTransport, StreamConfig, StreamSubscription, StreamEvent,
  StreamEventHandler, PullRequest, PullResponse, PushRequest, PushResponse,
  PresenceUpdate, PresenceState, PresenceSnapshot, ChangeRecord,
  PresenceEvent, AuthProvider, HLC,
} from "../types.js";
import { TransportError } from "../errors.js";
import { Backoff } from "../backoff.js";
import type { BackoffOptions } from "../backoff.js";
import { hlcAfter } from "../hlc.js";

/** Mirrors WSMessageType in crdt/transport_ws.go. */
export type WSMessageType =
  | "pull_request" | "pull_response"
  | "push_request" | "push_response"
  | "change" | "changes"
  | "presence_update" | "presence_event"
  | "presence_get" | "presence_snapshot"
  | "subscribe" | "unsubscribe"
  | "error" | "ping" | "pong";

/** Mirrors WebSocketMessage in crdt/transport_ws.go. */
export interface WebSocketMessage {
  type: WSMessageType;
  payload?: unknown;
  request_id?: string;
}

export interface WebSocketTransportConfig {
  /** ws:// or wss:// endpoint. */
  url: string;
  /** Subprotocols passed to the WebSocket constructor. */
  protocols?: string | string[];
  /** WebSocket implementation (default: globalThis.WebSocket). */
  WebSocketImpl?: typeof WebSocket;
  /** Per-request timeout in ms (default: 30000). */
  requestTimeout?: number;
  /** Reconnect backoff schedule. */
  backoff?: BackoffOptions;
  /**
   * Auth provider. Headers cannot be set on a browser WebSocket handshake,
   * so each header is appended as a query parameter instead.
   */
  auth?: AuthProvider;
}

interface Pending {
  resolve: (value: unknown) => void;
  reject: (err: Error) => void;
  timer: ReturnType<typeof setTimeout>;
}

export class WebSocketTransport implements StreamTransport {
  private socket: WebSocket | null = null;
  private pending = new Map<string, Pending>();
  private handlers = new Set<StreamEventHandler>();
  private nextId = 0;
  private closed = false;
  private opening: Promise<WebSocket> | null = null;
  private backoff: Backoff;
  private subscribedTables: string[] = [];
  private _lastHLC: HLC | null = null;

  private impl: typeof WebSocket;
  private requestTimeout: number;

  constructor(private config: WebSocketTransportConfig) {
    const impl = config.WebSocketImpl ?? globalThis.WebSocket;
    if (!impl) {
      throw new TransportError(
        "No WebSocket implementation available. Pass `WebSocketImpl` (e.g. the `ws` package under Node)."
      );
    }
    this.impl = impl;
    this.requestTimeout = config.requestTimeout ?? 30_000;
    this.backoff = new Backoff(config.backoff);
  }

  // --- Transport ---

  async pull(req: PullRequest): Promise<PullResponse> {
    return this.rpc<PullResponse>("pull_request", req);
  }

  async push(req: PushRequest): Promise<PushResponse> {
    return this.rpc<PushResponse>("push_request", req);
  }

  async updatePresence(update: PresenceUpdate): Promise<void> {
    const socket = await this.ensureOpen();
    // Fire-and-forget: the server answers with a broadcast event, not a reply.
    socket.send(JSON.stringify({ type: "presence_update", payload: update }));
  }

  // --- StreamTransport ---

  subscribe(config: StreamConfig): StreamSubscription {
    const transport = this;
    let connected = false;

    return {
      get connected() { return connected && transport.socket?.readyState === 1; },
      get lastHLC() { return transport._lastHLC; },

      on(handler: StreamEventHandler): () => void {
        transport.handlers.add(handler);
        return () => { transport.handlers.delete(handler); };
      },

      connect(): void {
        if (connected) return;
        connected = true;
        transport.subscribedTables = config.tables ?? [];
        void transport.ensureOpen().then((socket) => {
          socket.send(JSON.stringify({
            type: "subscribe",
            payload: { tables: transport.subscribedTables },
          }));
          transport.emit({ type: "connected" });
        }).catch((err: unknown) => {
          transport.emit({
            type: "error",
            error: err instanceof Error ? err : new Error(String(err)),
          });
        });
      },

      disconnect(): void {
        if (!connected) return;
        connected = false;
        // No `unsubscribe` frame: the Go server does not handle that type
        // and would answer with an error frame, surfacing as a spurious
        // stream error. Dropping the subscription client-side is enough.
        transport.subscribedTables = [];
        transport.emit({ type: "disconnected" });
      },
    };
  }

  /** Close the socket and reject every in-flight request. */
  close(): void {
    this.closed = true;
    for (const [, p] of this.pending) {
      clearTimeout(p.timer);
      p.reject(new TransportError("WebSocket transport closed"));
    }
    this.pending.clear();
    this.socket?.close();
    this.socket = null;
  }

  // --- Internals ---

  private emit(event: StreamEvent): void {
    for (const handler of this.handlers) {
      try { handler(event); } catch { /* Ignore handler errors. */ }
    }
  }

  private async rpc<T>(type: WSMessageType, payload: unknown): Promise<T> {
    const socket = await this.ensureOpen();
    const id = `r${++this.nextId}`;

    return new Promise<T>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new TransportError(`CRDT ws ${type} timed out after ${this.requestTimeout}ms`));
      }, this.requestTimeout);

      this.pending.set(id, {
        resolve: resolve as (v: unknown) => void,
        reject,
        timer,
      });
      socket.send(JSON.stringify({ type, payload, request_id: id }));
    });
  }

  private async ensureOpen(): Promise<WebSocket> {
    if (this.closed) throw new TransportError("WebSocket transport closed");
    if (this.socket?.readyState === 1) return this.socket;
    if (this.opening) return this.opening;

    this.opening = this.open().finally(() => { this.opening = null; });
    return this.opening;
  }

  private async open(): Promise<WebSocket> {
    const url = await this.buildURL();
    return new Promise<WebSocket>((resolve, reject) => {
      const socket = new this.impl(url, this.config.protocols);
      this.socket = socket;

      socket.onopen = () => {
        this.backoff.reset();
        // Re-subscribe after a reconnect: the server does not remember us.
        if (this.subscribedTables.length > 0) {
          socket.send(JSON.stringify({
            type: "subscribe",
            payload: { tables: this.subscribedTables },
          }));
        }
        resolve(socket);
      };

      socket.onmessage = (ev: MessageEvent) => {
        this.handleMessage(String(ev.data));
      };

      socket.onerror = () => {
        reject(new TransportError(`CRDT ws connection failed: ${url}`));
      };

      socket.onclose = () => {
        this.socket = null;
        this.emit({ type: "disconnected" });
        if (!this.closed && this.subscribedTables.length > 0) {
          setTimeout(() => { void this.ensureOpen().catch(() => {}); },
            this.backoff.next());
        }
      };
    });
  }

  private handleMessage(raw: string): void {
    let msg: WebSocketMessage;
    try {
      msg = JSON.parse(raw) as WebSocketMessage;
    } catch (err) {
      this.emit({ type: "error", error: new Error(`Failed to parse ws frame: ${err}`) });
      return;
    }

    // Correlated reply to an in-flight rpc().
    if (msg.request_id) {
      const p = this.pending.get(msg.request_id);
      if (p) {
        this.pending.delete(msg.request_id);
        clearTimeout(p.timer);
        if (msg.type === "error") {
          const detail = (msg.payload as { error?: string })?.error ?? "unknown error";
          p.reject(new TransportError(`CRDT ws error: ${detail}`));
        } else {
          p.resolve(msg.payload);
        }
        return;
      }
    }

    switch (msg.type) {
      case "change": {
        const change = msg.payload as ChangeRecord;
        this.updateLastHLC(change.hlc);
        this.emit({ type: "change", data: change });
        break;
      }
      case "changes": {
        const changes = msg.payload as ChangeRecord[];
        for (const c of changes) this.updateLastHLC(c.hlc);
        this.emit({ type: "changes", data: changes });
        break;
      }
      case "presence_event":
        this.emit({ type: "presence", data: msg.payload as PresenceEvent });
        break;
      case "ping":
        this.socket?.send(JSON.stringify({ type: "pong" }));
        break;
      case "error":
        this.emit({
          type: "error",
          error: new Error(
            (msg.payload as { error?: string })?.error ?? "unknown ws error"
          ),
        });
        break;
      default:
        // pong and unknown types need no action.
        break;
    }
  }

  private updateLastHLC(hlc: HLC): void {
    if (!this._lastHLC || hlcAfter(hlc, this._lastHLC)) this._lastHLC = hlc;
  }

  private async buildURL(): Promise<string> {
    if (!this.config.auth) return this.config.url;
    // A browser WebSocket handshake cannot carry custom headers, so auth
    // travels as query parameters instead.
    const headers = await Promise.resolve(this.config.auth.getHeaders());
    const url = new URL(this.config.url);
    for (const [k, v] of Object.entries(headers)) {
      url.searchParams.set(k.toLowerCase(), v);
    }
    return url.toString();
  }
}
```

- [ ] **Step 4: Export it**

In `src/index.ts`:

```ts
// WebSocket transport
export { WebSocketTransport } from "./transport/websocket.js";
export type {
  WSMessageType,
  WebSocketMessage,
  WebSocketTransportConfig,
} from "./transport/websocket.js";
```

- [ ] **Step 5: Run the tests**

```bash
npx vitest run --project node src/__tests__/websocket.test.ts
npx vitest run
npx tsc --noEmit
```

Expected: PASS, 3 new tests, clean typecheck.

- [ ] **Step 6: Commit**

```bash
git add crdt-js/src/transport/websocket.ts crdt-js/src/index.ts crdt-js/src/__tests__/websocket.test.ts
git commit -m "feat(crdt-js): WebSocket transport with multiplexed request correlation"
```

---

### Task 18: Tombstone compaction

Fixes **D10**. Port of `crdt/compact.go`, rewritten pure so it fits the
copy-on-write core.

**Files:**
- Create: `src/compact.ts`
- Create: `src/__tests__/compact.test.ts`
- Modify: `src/store.ts`, `src/index.ts`

**Interfaces:**
- Produces:
  - `compactListState(state: RGAListState, before: HLC): { state: RGAListState; dropped: number }`
  - `compactSetState(state: ORSetState, before: HLC): { state: ORSetState; dropped: number }`
  - `compactTextState(state: TextState, before: HLC): { state: TextState; dropped: number }`
  - `compactDocument(doc: DocumentState, before: HLC): { doc: DocumentState; dropped: number }`
  - `CRDTStore.compact(before: HLC): number` — compacts every document,
    returns total units dropped.

- [ ] **Step 1: Write the failing test**

Create `src/__tests__/compact.test.ts`:

```ts
import { describe, it, expect } from "vitest";
import { compactListState, compactSetState, compactTextState } from "../compact.js";
import { CRDTStore } from "../store.js";
import { HybridClock } from "../hlc.js";
import type { RGAListState, ORSetState, TextState, HLC } from "../types.js";

const h = (n: number): HLC => ({ ts: String(n), c: 0, node: "n1" });
const key = (n: number) => `HLC{ts:${n} c:0 node:n1}`;

describe("compaction", () => {
  it("drops tombstoned leaf nodes older than the horizon", () => {
    const state: RGAListState = { nodes: {
      [key(1)]: { id: h(1), node_id: "n1", parent_id: { ts: 0, c: 0, node: "" }, value: "a" },
      [key(2)]: { id: h(2), node_id: "n1", parent_id: h(1), value: "b", tombstone: true },
    } };
    const { state: next, dropped } = compactListState(state, h(10));
    expect(dropped).toBe(1);
    expect(Object.keys(next.nodes)).toEqual([key(1)]);
  });

  it("keeps a tombstoned node that still anchors a child", () => {
    const state: RGAListState = { nodes: {
      [key(1)]: { id: h(1), node_id: "n1", parent_id: { ts: 0, c: 0, node: "" }, value: "a", tombstone: true },
      [key(2)]: { id: h(2), node_id: "n1", parent_id: h(1), value: "b" },
    } };
    const { dropped } = compactListState(state, h(10));
    expect(dropped).toBe(0);
  });

  it("cascades: dropping a leaf exposes its parent", () => {
    const state: RGAListState = { nodes: {
      [key(1)]: { id: h(1), node_id: "n1", parent_id: { ts: 0, c: 0, node: "" }, value: "a", tombstone: true },
      [key(2)]: { id: h(2), node_id: "n1", parent_id: h(1), value: "b", tombstone: true },
    } };
    const { state: next, dropped } = compactListState(state, h(10));
    expect(dropped).toBe(2);
    expect(Object.keys(next.nodes)).toHaveLength(0);
  });

  it("does not drop anything newer than the horizon", () => {
    const state: RGAListState = { nodes: {
      [key(20)]: { id: h(20), node_id: "n1", parent_id: { ts: 0, c: 0, node: "" }, value: "a", tombstone: true },
    } };
    expect(compactListState(state, h(10)).dropped).toBe(0);
  });

  it("drops removed OR-Set tags and prunes tagless entries", () => {
    const tag = { node: "n1", hlc: h(1) };
    const state: ORSetState = {
      entries: { '"a"': [tag] },
      removed: { [`"a"|n1:${`HLC{ts:1 c:0 node:n1}`}`]: true },
    };
    const { state: next, dropped } = compactSetState(state, h(10));
    expect(dropped).toBe(1);
    expect(next.entries['"a"']).toBeUndefined();
  });

  it("skeletonizes tombstoned text but preserves addresses", () => {
    const state: TextState = { frags: { [key(1)]: [
      { origin: h(1), start: 0, content: "gone", length: 4, tombstone: true },
      { origin: h(1), start: 4, content: "kept", length: 4 },
    ] } };
    const { state: next, dropped } = compactTextState(state, h(10));
    expect(dropped).toBe(1);
    const frags = next.frags[key(1)];
    expect(frags[0].content).toBe("");
    expect(frags[0].length).toBe(4); // address preserved
    expect(frags[1].content).toBe("kept");
  });

  it("store.compact is a no-op for a zero horizon", () => {
    const store = new CRDTStore("n1", new HybridClock("n1"), undefined, { persistDebounceMs: 0 });
    store.setField("t", "p", "f", 1);
    expect(store.compact({ ts: 0, c: 0, node: "" })).toBe(0);
  });
});
```

- [ ] **Step 2: Run to verify it fails**

```bash
npx vitest run --project node src/__tests__/compact.test.ts
```

Expected: FAIL — module not found.

- [ ] **Step 3: Implement the port**

Create `src/compact.ts`:

```ts
/**
 * Horizon-based tombstone compaction. Port of crdt/compact.go, written
 * pure so it composes with copy-on-write state.
 *
 * The horizon is a stability floor the CALLER guarantees: every replica
 * has observed all operations with HLC < horizon, and no in-flight
 * operation references addresses older than it. Passing a horizon that
 * does not hold will diverge replicas.
 */

import type {
  RGAListState, ORSetState, ORSetTag, TextState, TextFragment,
  DocumentState, FieldState, HLC,
} from "./types.js";
import { hlcAfter, hlcIsZero, hlcString } from "./hlc.js";
import { removedKey, tagKey } from "./merge.js";

/**
 * Drop tombstoned LEAF nodes older than the horizon, cascading until no
 * more can be dropped. Survivor ordering is untouched by construction: a
 * node with children is never removed, so no anchor is ever orphaned.
 */
export function compactListState(
  state: RGAListState,
  before: HLC
): { state: RGAListState; dropped: number } {
  if (hlcIsZero(before) || Object.keys(state.nodes).length === 0) {
    return { state, dropped: 0 };
  }

  let nodes = state.nodes;
  let dropped = 0;

  for (;;) {
    // Recompute anchors each round: dropping a leaf may expose its parent.
    const hasChild = new Set<string>();
    for (const node of Object.values(nodes)) {
      hasChild.add(hlcString(node.parent_id));
    }

    const survivors: Record<string, typeof nodes[string]> = {};
    let droppedThisRound = 0;
    for (const [key, node] of Object.entries(nodes)) {
      if (node.tombstone && !hasChild.has(key) && hlcAfter(before, node.id)) {
        droppedThisRound++;
        continue;
      }
      survivors[key] = node;
    }

    if (droppedThisRound === 0) break;
    nodes = survivors;
    dropped += droppedThisRound;
  }

  return dropped === 0 ? { state, dropped: 0 } : { state: { nodes }, dropped };
}

/** True when this tag has been observed-removed for this element. */
function tagRemoved(
  removed: Record<string, boolean>,
  elem: string,
  tag: ORSetTag
): boolean {
  return Boolean(removed[removedKey(elem, tag)] || removed[tagKey(tag)]);
}

/**
 * Drop observed-removed tags older than the horizon along with their
 * element-scoped removal markers, pruning entries left tagless.
 *
 * Only the element-scoped marker is consumed. A legacy tag-only marker is
 * SHARED evidence — a multi-element add shares one tag — so deleting it
 * would resurrect sibling elements that still rely on it, and which
 * sibling would depend on iteration order. The tiny legacy marker stays.
 */
export function compactSetState(
  state: ORSetState,
  before: HLC
): { state: ORSetState; dropped: number } {
  if (hlcIsZero(before)) return { state, dropped: 0 };

  const entries: Record<string, ORSetTag[]> = {};
  const removed = { ...state.removed };
  let dropped = 0;

  for (const [elem, tags] of Object.entries(state.entries)) {
    const kept: ORSetTag[] = [];
    for (const tag of tags) {
      if (tagRemoved(state.removed, elem, tag) && hlcAfter(before, tag.hlc)) {
        delete removed[removedKey(elem, tag)];
        dropped++;
        continue;
      }
      kept.push(tag);
    }
    if (kept.length > 0) entries[elem] = kept;
  }

  return dropped === 0 ? { state, dropped: 0 } : { state: { entries, removed }, dropped };
}

/**
 * Skeletonize tombstoned fragments whose origin is older than the horizon:
 * content is freed and adjacent skeletons coalesce, but addresses are
 * PRESERVED so every cursor anchor stays resolvable.
 */
export function compactTextState(
  state: TextState,
  before: HLC
): { state: TextState; dropped: number } {
  if (hlcIsZero(before)) return { state, dropped: 0 };

  const frags: Record<string, TextFragment[]> = {};
  let dropped = 0;

  for (const [key, list] of Object.entries(state.frags)) {
    const skeletonized = list.map((f) => {
      if (f.tombstone && f.content !== "" && hlcAfter(before, f.origin)) {
        dropped++;
        const { attrs: _attrs, ...rest } = f;
        return { ...rest, content: "" };
      }
      return f;
    });

    // Coalesce adjacent skeletons (contiguous, both tombstoned and empty).
    const out: TextFragment[] = [];
    for (const f of skeletonized) {
      const prev = out[out.length - 1];
      if (
        prev && prev.tombstone && prev.content === "" &&
        f.tombstone && f.content === "" &&
        prev.start + prev.length === f.start
      ) {
        out[out.length - 1] = { ...prev, length: prev.length + f.length };
        dropped++;
        continue;
      }
      out.push(f);
    }
    frags[key] = out;
  }

  return dropped === 0 ? { state, dropped: 0 } : { state: { frags }, dropped };
}

/** Compact every compactable field of one document. */
export function compactDocument(
  doc: DocumentState,
  before: HLC
): { doc: DocumentState; dropped: number } {
  if (hlcIsZero(before)) return { doc, dropped: 0 };

  const fields: Record<string, FieldState> = {};
  let dropped = 0;

  for (const [name, fs] of Object.entries(doc.fields)) {
    if (fs.type === "list" && fs.list_state) {
      const r = compactListState(fs.list_state, before);
      dropped += r.dropped;
      fields[name] = r.dropped ? { ...fs, list_state: r.state } : fs;
    } else if (fs.type === "set" && fs.set_state) {
      const r = compactSetState(fs.set_state, before);
      dropped += r.dropped;
      fields[name] = r.dropped ? { ...fs, set_state: r.state } : fs;
    } else if (fs.type === "text" && fs.text_state) {
      const r = compactTextState(fs.text_state, before);
      dropped += r.dropped;
      fields[name] = r.dropped ? { ...fs, text_state: r.state } : fs;
    } else {
      fields[name] = fs;
    }
  }

  return dropped === 0 ? { doc, dropped: 0 } : { doc: { ...doc, fields }, dropped };
}
```

- [ ] **Step 4: Add `CRDTStore.compact`**

In `src/store.ts`:

```ts
  /**
   * Compact tombstones across every document, dropping state older than
   * the horizon. Returns the total units dropped.
   *
   * The horizon is a stability floor YOU guarantee: every replica has seen
   * everything older than it, and nothing in flight references an older
   * address. A horizon that does not hold will diverge replicas. When in
   * doubt use the server's last acknowledged HLC minus a safety margin.
   */
  compact(before: HLC): number {
    if (hlcIsZero(before)) return 0;

    let dropped = 0;
    this.transact(() => {
      for (const [table, tableMap] of this.state) {
        for (const [pk, doc] of tableMap) {
          const result = compactDocument(doc, before);
          if (result.dropped > 0) {
            dropped += result.dropped;
            this.setDocument(table, pk, result.doc);
          }
        }
      }
    });
    return dropped;
  }
```

Import `compactDocument` and `hlcIsZero` at the top of `store.ts`.

Compaction runs inside `transact()` so a sweep over thousands of documents
produces one persist and one notification rather than one per document.

- [ ] **Step 5: Export it**

In `src/index.ts`:

```ts
// Tombstone compaction
export {
  compactListState,
  compactSetState,
  compactTextState,
  compactDocument,
} from "./compact.js";
```

- [ ] **Step 6: Run the tests**

```bash
npx vitest run --project node src/__tests__/compact.test.ts
npx vitest run
npx tsc --noEmit
```

Expected: PASS, 7 new tests, clean typecheck.

- [ ] **Step 7: Commit**

```bash
git add crdt-js/src/compact.ts crdt-js/src/store.ts crdt-js/src/index.ts crdt-js/src/__tests__/compact.test.ts
git commit -m "feat(crdt-js): horizon-based tombstone compaction"
```

---

### Task 19: Packaging

Fixes **D12**. Tests are compiled into `dist/` and published, and the package
declares no `sideEffects`.

**Files:**
- Modify: `tsconfig.json`, `package.json`
- Create: `tsconfig.test.json`

**Interfaces:** none — build configuration only.

- [ ] **Step 1: Confirm the defect**

```bash
npm run build && ls dist/__tests__ | head -3
```

Expected: test files present in `dist/`.

- [ ] **Step 2: Exclude tests from the published build**

In `tsconfig.json`, add:

```json
  "exclude": ["src/__tests__", "dist", "node_modules"]
```

Create `tsconfig.test.json` so the editor and `tsc --noEmit` still typecheck
tests:

```json
{
  "extends": "./tsconfig.json",
  "compilerOptions": { "noEmit": true },
  "include": ["src"],
  "exclude": ["dist", "node_modules"]
}
```

- [ ] **Step 3: Declare side-effect freedom and a typecheck script**

In `package.json`, add `"sideEffects": false` at the top level and extend
`scripts`:

```json
    "typecheck": "tsc --noEmit -p tsconfig.test.json",
    "prepublishOnly": "npm run build && npm run test"
```

`sideEffects: false` lets bundlers drop the unused half of the package — a
consumer importing only `CRDTStore` should not ship the WebSocket transport.
Verify no module in `src/` runs side-effecting top-level code before setting
this; all current modules only declare.

- [ ] **Step 4: Verify**

```bash
rm -rf dist && npm run build
test ! -d dist/__tests__ && echo "OK: tests excluded"
npm run typecheck
npm pack --dry-run 2>&1 | grep -c "__tests__"
```

Expected: `OK: tests excluded`, a clean typecheck, and `0` test files in the
pack listing.

- [ ] **Step 5: Run the whole suite one final time**

```bash
npx vitest run
```

Expected: PASS across both projects.

- [ ] **Step 6: Commit**

```bash
git add crdt-js/tsconfig.json crdt-js/tsconfig.test.json crdt-js/package.json
git commit -m "build(crdt-js): exclude tests from dist, mark side-effect free"
```

### Task 20: Bounded offline queue with auto-sync on reconnect

`CRDTErrorCode.OfflineQueueFull` is defined but never thrown: the pending
list grows without bound while offline, and nothing resyncs when the network
returns.

**Files:**
- Modify: `src/store.ts`, `src/sync.ts`, `src/react.tsx`
- Create: `src/__tests__/offline.test.ts`

**Interfaces:**
- Consumes: `SyncEngine` (Task 12), the store options object (Task 11).
- Produces:
  - `CRDTStore` options gain `maxPendingChanges?: number` (default 10000;
    `0` disables) and `throwOnOverflow?: boolean` (default false).
  - `CRDTStore.onPendingOverflow(handler: (dropped: ChangeRecord[]) => void): () => void`
  - `SyncEngine.start(options?: { intervalMs?: number }): () => void` and
    `SyncEngine.stop(): void`
  - `UseCRDTConfig` gains `syncInterval?: number`.

- [ ] **Step 1: Write the failing test**

Create `src/__tests__/offline.test.ts`:

```ts
import { describe, it, expect } from "vitest";
import { CRDTStore } from "../store.js";
import { SyncEngine } from "../sync.js";
import { CRDTClient } from "../client.js";
import { CRDTErrorCode, CRDTError } from "../errors.js";
import type { Transport, ChangeRecord } from "../types.js";

const HLC0 = { ts: "0", c: 0, node: "" };

const inert: Transport = {
  async pull() { return { changes: [], latest_hlc: HLC0 }; },
  async push(req) { return { merged: req.changes.length, latest_hlc: HLC0 }; },
};

const mkStore = (opts: Record<string, unknown>) =>
  new CRDTStore(
    "n1",
    new CRDTClient({ nodeID: "n1", transport: inert }).clock,
    undefined,
    { persistDebounceMs: 0, ...opts }
  );

describe("offline queue", () => {
  it("bounds the pending queue and reports the overflow", () => {
    const store = mkStore({ maxPendingChanges: 10 });
    const dropped: ChangeRecord[] = [];
    store.onPendingOverflow((d) => { dropped.push(...d); });

    for (let i = 0; i < 15; i++) store.setField("t", "p", `f${i}`, i);

    expect(store.pendingCount).toBe(10);
    expect(dropped).toHaveLength(5);
    // The OLDEST are dropped: recent writes reflect what the user most
    // recently intended, and older ones are likelier superseded.
    expect(dropped[0].field).toBe("f0");
    expect(store.getPendingChanges()[0].field).toBe("f5");
  });

  it("an unbounded queue keeps everything", () => {
    const store = mkStore({ maxPendingChanges: 0 });
    for (let i = 0; i < 50; i++) store.setField("t", "p", `f${i}`, i);
    expect(store.pendingCount).toBe(50);
  });

  it("throws OfflineQueueFull when throwOnOverflow is set", () => {
    const store = mkStore({ maxPendingChanges: 2, throwOnOverflow: true });
    store.setField("t", "p", "a", 1);
    store.setField("t", "p", "b", 2);
    let caught: unknown;
    try {
      store.setField("t", "p", "c", 3);
    } catch (e) {
      caught = e;
    }
    expect(caught).toBeInstanceOf(CRDTError);
    expect((caught as CRDTError).code).toBe(CRDTErrorCode.OfflineQueueFull);
    // The rejected change must not be left half-queued.
    expect(store.pendingCount).toBe(2);
  });

  it("start() syncs on an interval and stop() halts it", async () => {
    let pushes = 0;
    const transport: Transport = {
      async pull() { return { changes: [], latest_hlc: HLC0 }; },
      async push(req) { pushes++; return { merged: req.changes.length, latest_hlc: HLC0 }; },
    };
    const client = new CRDTClient({ nodeID: "n1", transport });
    const store = new CRDTStore("n1", client.clock, undefined, { persistDebounceMs: 0 });
    const engine = new SyncEngine(client, store);

    const stop = engine.start({ intervalMs: 10 });
    store.setField("t", "p", "f", 1);
    await new Promise((r) => setTimeout(r, 50));
    stop();
    const settled = pushes;
    await new Promise((r) => setTimeout(r, 50));

    expect(settled).toBeGreaterThan(0);
    expect(pushes).toBe(settled); // no syncs after stop()
  });
});
```

- [ ] **Step 2: Run to verify it fails**

```bash
npx vitest run --project node src/__tests__/offline.test.ts
```

Expected: FAIL — `onPendingOverflow` and `start` do not exist, and the queue
is unbounded so `pendingCount` is 15.

- [ ] **Step 3: Bound the queue in `CRDTStore`**

Extend the options object added in Task 11:

```ts
    options?: {
      persistDebounceMs?: number;
      /** Max queued unpushed changes (default: 10000). 0 disables. */
      maxPendingChanges?: number;
      /** Throw instead of dropping when the bound is hit (default: false). */
      throwOnOverflow?: boolean;
    }
```

Add the fields, assigned in the constructor:

```ts
  private maxPendingChanges: number;
  private throwOnOverflow: boolean;
  private overflowHandlers = new Set<(dropped: ChangeRecord[]) => void>();
```

```ts
    this.maxPendingChanges = options?.maxPendingChanges ?? 10_000;
    this.throwOnOverflow = options?.throwOnOverflow ?? false;
```

Add the subscription and the enforcement:

```ts
  /**
   * Notified when the pending queue overflows and changes are dropped.
   * Returns an unsubscribe function.
   *
   * Overflow means unsynced local work was discarded — surface it to the
   * user rather than swallowing it.
   */
  onPendingOverflow(handler: (dropped: ChangeRecord[]) => void): () => void {
    this.overflowHandlers.add(handler);
    return () => { this.overflowHandlers.delete(handler); };
  }

  /** Queue a change for push, enforcing the offline bound. */
  private enqueuePending(change: ChangeRecord): void {
    if (
      this.throwOnOverflow &&
      this.maxPendingChanges > 0 &&
      this.pending.length >= this.maxPendingChanges
    ) {
      throw new CRDTError(
        `crdt: pending queue full (${this.maxPendingChanges} changes)`,
        undefined,
        CRDTErrorCode.OfflineQueueFull,
        false
      );
    }

    this.pending.push(change);

    if (this.maxPendingChanges <= 0) return;
    if (this.pending.length <= this.maxPendingChanges) return;

    const dropped = this.pending.splice(
      0,
      this.pending.length - this.maxPendingChanges
    );
    for (const handler of this.overflowHandlers) handler(dropped);
  }
```

The throw happens BEFORE the push so the rejected change is never queued —
popping it afterwards would still have run the merge into store state, and
the test asserts `pendingCount` stays at the bound.

Import the error types at the top of `store.ts`:

```ts
import { CRDTError, CRDTErrorCode } from "./errors.js";
```

Replace every `this.pending.push(...)` with `this.enqueuePending(...)`. There
are **11** in the current tree (store.ts lines 311, 352, 388, 423, 464, 503,
536, 595, 734, 765, 802), covering the LWW, counter, set, list, text, nested
document and tombstone paths — do not stop at the first few. Re-run the grep
after your change and confirm the only remaining hit is inside
`enqueuePending` itself:

```bash
grep -n "this.pending.push" crdt-js/src/store.ts
```

- [ ] **Step 4: Add `start` / `stop` to `SyncEngine`**

In `src/sync.ts`, add the fields and methods:

```ts
  private timer: ReturnType<typeof setInterval> | null = null;
  private onlineHandler: (() => void) | null = null;

  /**
   * Sync periodically, and immediately whenever the environment reports it
   * is back online. Returns a stop function — call it on unmount.
   *
   * A failed sync is swallowed: the pending queue is durable, so the next
   * tick retries. Read lastSyncTime for status.
   */
  start(options?: { intervalMs?: number }): () => void {
    this.stop();
    const interval = options?.intervalMs ?? 30_000;

    this.timer = setInterval(() => {
      void this.sync().catch(() => {});
    }, interval);

    if (typeof globalThis.addEventListener === "function") {
      this.onlineHandler = () => { void this.sync().catch(() => {}); };
      globalThis.addEventListener("online", this.onlineHandler);
    }

    return () => this.stop();
  }

  /** Stop periodic syncing and remove the online listener. */
  stop(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
    if (this.onlineHandler && typeof globalThis.removeEventListener === "function") {
      globalThis.removeEventListener("online", this.onlineHandler);
      this.onlineHandler = null;
    }
  }
```

- [ ] **Step 5: Wire it into `useCRDT`**

In `src/react.tsx`, add to `UseCRDTConfig`:

```tsx
  /** Periodic sync interval in ms (default: 30000). */
  syncInterval?: number;
```

Replace the auto-sync effect from Task 14 so the engine owns the lifecycle:

```tsx
  // Periodic sync + resync on reconnect, after storage hydration completes.
  useEffect(() => {
    if (config.autoSync === false) return;
    let stop: (() => void) | undefined;
    let cancelled = false;
    void store.ready.then(() => {
      if (cancelled) return;
      void sync();
      stop = engine.start({ intervalMs: config.syncInterval });
    });
    return () => {
      cancelled = true;
      stop?.();
    };
  }, [config.autoSync, config.syncInterval, sync, store, engine]);
```

- [ ] **Step 6: Run the tests**

```bash
npx vitest run --project node src/__tests__/offline.test.ts
npx vitest run
npx tsc --noEmit
```

Expected: PASS, 4 new tests, clean typecheck.

- [ ] **Step 7: Commit**

```bash
git add crdt-js/src/store.ts crdt-js/src/sync.ts crdt-js/src/react.tsx crdt-js/src/__tests__/offline.test.ts
git commit -m "feat(crdt-js): bounded offline queue and auto-sync on reconnect"
```

---

---

## Verification

Run after the final task. Every claim below must be demonstrated, not assumed.

```bash
cd crdt-js
npx vitest run            # both projects green
npx tsc --noEmit -p tsconfig.test.json
npm run build
```

Then confirm each defect is closed:

| Defect | Proof |
|---|---|
| D1 | `react.test.tsx` renders every hook with no `getSnapshot` warning |
| D2 | `cow-store.test.ts` colon-pk notification test |
| D3 | `cow-store.test.ts` text resolution tests |
| D4 | `batch.test.ts` counter parity test |
| D5 | the dispatch grep in Task 13 Step 4 returns non-zero for all six |
| D6 | `presence-seed.test.ts` seeding test |
| D7 | `stream.test.ts` connect re-entrancy test |
| D8 | `cow-store.test.ts` 3000 appends ~750 ms vs ~5,600 ms pre-fix (2000 ms ceiling) |
| D9 | `list-scale.test.ts` 50,000-element resolution |
| D10 | `compact.test.ts` |
| D11 | `sync.test.ts` in-flight write test |
| D12 | `dist/__tests__` absent after build |
| offline queue | `offline.test.ts` bound + interval-sync tests |
