import { describe, it, expect } from "vitest";
import { compactListState, compactSetState, compactTextState, compactDocument } from "../compact.js";
import { CRDTStore } from "../store.js";
import type { StateSnapshot } from "../store.js";
import { HybridClock } from "../hlc.js";
import type { RGAListState, ORSetState, TextState, DocumentState, HLC } from "../types.js";

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

  it("compactDocument compacts list and set fields, leaves other field types untouched", () => {
    const doc: DocumentState = {
      table: "t",
      pk: "p",
      tombstone: false,
      fields: {
        items: {
          type: "list",
          hlc: h(1),
          node_id: "n1",
          list_state: { nodes: {
            [key(1)]: { id: h(1), node_id: "n1", parent_id: { ts: 0, c: 0, node: "" }, value: "a" },
            [key(2)]: { id: h(2), node_id: "n1", parent_id: h(1), value: "b", tombstone: true },
          } },
        },
        tags: {
          type: "set",
          hlc: h(1),
          node_id: "n1",
          set_state: {
            entries: { '"a"': [{ node: "n1", hlc: h(1) }] },
            removed: { [`"a"|n1:${key(1)}`]: true },
          },
        },
        // A field type compaction must leave untouched entirely.
        title: { type: "lww", hlc: h(1), node_id: "n1", value: "hello" },
      },
    };

    const { doc: next, dropped } = compactDocument(doc, h(10));
    expect(dropped).toBe(2); // 1 dropped list leaf + 1 dropped set tag
    expect(Object.keys(next.fields.items.list_state!.nodes)).toEqual([key(1)]);
    expect(next.fields.tags.set_state!.entries['"a"']).toBeUndefined();
    // Untouched field keeps its exact reference, not just an equal copy.
    expect(next.fields.title).toBe(doc.fields.title);
  });

  it("does not recurse into nested doc_state (Go's State.Compact has no TypeDocument case)", () => {
    const doc: DocumentState = {
      table: "t",
      pk: "p",
      tombstone: false,
      fields: {
        nested: {
          type: "document",
          hlc: h(1),
          node_id: "n1",
          doc_state: {
            fields: {
              inner: {
                type: "list",
                hlc: h(1),
                node_id: "n1",
                list_state: { nodes: {
                  [key(1)]: { id: h(1), node_id: "n1", parent_id: { ts: 0, c: 0, node: "" }, value: "x" },
                  [key(2)]: { id: h(2), node_id: "n1", parent_id: h(1), value: "y", tombstone: true },
                } },
              },
            },
          },
        },
      },
    };

    const { doc: next, dropped } = compactDocument(doc, h(10));
    expect(dropped).toBe(0);
    // Not recursed: the droppable tombstone inside the nested doc_state
    // survives untouched, and the field keeps its exact reference.
    expect(next.fields.nested).toBe(doc.fields.nested);
    expect(Object.keys(next.fields.nested.doc_state!.fields.inner.list_state!.nodes))
      .toEqual([key(1), key(2)]);
  });

  it("returns the identical object at every level when nothing is dropped", () => {
    const listState: RGAListState = { nodes: {
      [key(1)]: { id: h(1), node_id: "n1", parent_id: { ts: 0, c: 0, node: "" }, value: "a" },
    } };
    expect(compactListState(listState, h(10)).state).toBe(listState);

    const setState: ORSetState = {
      entries: { '"a"': [{ node: "n1", hlc: h(1) }] },
      removed: {},
    };
    expect(compactSetState(setState, h(10)).state).toBe(setState);

    const textState: TextState = { frags: { [key(1)]: [
      { origin: h(1), start: 0, content: "kept", length: 4 },
    ] } };
    expect(compactTextState(textState, h(10)).state).toBe(textState);

    const doc: DocumentState = {
      table: "t",
      pk: "p",
      tombstone: false,
      fields: {
        items: { type: "list", hlc: h(1), node_id: "n1", list_state: listState },
        tags: { type: "set", hlc: h(1), node_id: "n1", set_state: setState },
      },
    };
    expect(compactDocument(doc, h(10)).doc).toBe(doc);
  });

  it("CRDTStore.compact sweeps every document across every table", () => {
    const store = new CRDTStore("n1", new HybridClock("n1"), undefined, { persistDebounceMs: 0 });

    const snapshot: StateSnapshot = {
      version: 1,
      nodeId: "n1",
      timestamp: Date.now(),
      pending: [],
      tables: {
        t1: {
          p1: {
            table: "t1",
            pk: "p1",
            tombstone: false,
            fields: {
              items: {
                type: "list",
                hlc: h(1),
                node_id: "n1",
                list_state: { nodes: {
                  [key(1)]: { id: h(1), node_id: "n1", parent_id: { ts: 0, c: 0, node: "" }, value: "a" },
                  [key(2)]: { id: h(2), node_id: "n1", parent_id: h(1), value: "b", tombstone: true },
                } },
              },
            },
          },
        },
        t2: {
          p2: {
            table: "t2",
            pk: "p2",
            tombstone: false,
            fields: {
              tags: {
                type: "set",
                hlc: h(1),
                node_id: "n1",
                set_state: {
                  entries: {
                    '"a"': [{ node: "n1", hlc: h(1) }],
                    '"b"': [{ node: "n1", hlc: h(3) }],
                  },
                  removed: { [`"a"|n1:${key(1)}`]: true },
                },
              },
            },
          },
        },
      },
    };
    store.importState(snapshot);

    // Visible reads are unaffected by compaction: the dropped state was
    // already dead weight, never part of a resolved value.
    const before1 = store.getDocument<{ items: unknown[] }>("t1", "p1");
    const before2 = store.getDocument<{ tags: unknown[] }>("t2", "p2");
    expect(before1?.items).toEqual(["a"]);
    expect(before2?.tags).toEqual(["b"]);

    const dropped = store.compact(h(10));
    expect(dropped).toBe(2); // 1 list node in t1 + 1 set tag in t2

    const after1 = store.getDocument<{ items: unknown[] }>("t1", "p1");
    const after2 = store.getDocument<{ tags: unknown[] }>("t2", "p2");
    expect(after1?.items).toEqual(["a"]);
    expect(after2?.tags).toEqual(["b"]);

    // The mid-iteration setDocument path actually rewrote the raw state.
    const exported = store.exportState();
    expect(Object.keys(exported.tables.t1.p1.fields.items.list_state!.nodes)).toEqual([key(1)]);
    expect(exported.tables.t2.p2.fields.tags.set_state!.entries['"a"']).toBeUndefined();
    expect(exported.tables.t2.p2.fields.tags.set_state!.entries['"b"']).toBeDefined();
  });

  it("CRDTStore.compact is a no-op that preserves document identity when nothing drops", () => {
    const store = new CRDTStore("n1", new HybridClock("n1"), undefined, { persistDebounceMs: 0 });

    const snapshot: StateSnapshot = {
      version: 1,
      nodeId: "n1",
      timestamp: Date.now(),
      pending: [],
      tables: {
        t: {
          p: {
            table: "t",
            pk: "p",
            tombstone: false,
            fields: {
              items: {
                type: "list",
                hlc: h(1),
                node_id: "n1",
                list_state: { nodes: {
                  [key(1)]: { id: h(1), node_id: "n1", parent_id: { ts: 0, c: 0, node: "" }, value: "a" },
                } },
              },
            },
          },
        },
      },
    };
    store.importState(snapshot);

    const before = store.getDocument("t", "p");
    const beforeCollection = store.getCollection("t");
    expect(store.compact(h(10))).toBe(0);
    const after = store.getDocument("t", "p");
    // Not just equal — the exact same cached reference, proving setDocument
    // was never called for this document (which is what keeps the
    // identity-keyed snapshot cache from invalidating on a no-op sweep).
    expect(after).toBe(before);
    // getCollection's cache is keyed on tableVersions, which setDocument
    // bumps on every call — this pins that compact() actually SKIPS
    // setDocument for untouched documents, not merely that compactDocument
    // itself returns an identical reference.
    expect(store.getCollection("t")).toBe(beforeCollection);
  });
});
