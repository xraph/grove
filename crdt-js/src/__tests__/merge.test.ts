import { describe, it, expect } from "vitest";
import type { HLC, ChangeRecord, ORSetTag, ORSetState, RGAListState, RGANode, DocumentCRDTState } from "../types.js";
import {
  mergeLWW,
  newPNCounterState,
  mergeCounter,
  counterValue,
  tagKey,
  newORSetState,
  mergeSet,
  setElements,
  mergeFieldState,
  newRGAListState,
  mergeListState,
  listElements,
  listNodeIds,
  newDocumentCRDTState,
  mergeDocumentState,
  documentResolve,
} from "../merge.js";
import type { LWWValue } from "../merge.js";

// ---------------------------------------------------------------------------
// mergeLWW
// ---------------------------------------------------------------------------

describe("mergeLWW", () => {
  it("returns remote when remote HLC is higher", () => {
    const local: LWWValue = {
      value: "old",
      hlc: { ts: 100, c: 0, node: "a" },
      nodeID: "a",
    };
    const remote: LWWValue = {
      value: "new",
      hlc: { ts: 200, c: 0, node: "b" },
      nodeID: "b",
    };
    expect(mergeLWW(local, remote).value).toBe("new");
  });

  it("returns local when local HLC is higher", () => {
    const local: LWWValue = {
      value: "keep",
      hlc: { ts: 300, c: 0, node: "a" },
      nodeID: "a",
    };
    const remote: LWWValue = {
      value: "discard",
      hlc: { ts: 200, c: 0, node: "b" },
      nodeID: "b",
    };
    expect(mergeLWW(local, remote).value).toBe("keep");
  });

  it("breaks tie by node ID (higher node wins)", () => {
    const local: LWWValue = {
      value: "from-a",
      hlc: { ts: 100, c: 0, node: "a" },
      nodeID: "a",
    };
    const remote: LWWValue = {
      value: "from-b",
      hlc: { ts: 100, c: 0, node: "b" },
      nodeID: "b",
    };
    // "b" > "a" => hlcAfter(remote.hlc, local.hlc) is true => remote wins
    expect(mergeLWW(local, remote).value).toBe("from-b");
  });

  it("returns remote when local is null", () => {
    const remote: LWWValue = {
      value: "val",
      hlc: { ts: 1, c: 0, node: "a" },
      nodeID: "a",
    };
    expect(mergeLWW(null, remote)).toBe(remote);
  });

  it("returns local when remote is null", () => {
    const local: LWWValue = {
      value: "val",
      hlc: { ts: 1, c: 0, node: "a" },
      nodeID: "a",
    };
    expect(mergeLWW(local, null)).toBe(local);
  });

  it("preserves the winning value", () => {
    const obj = { nested: true };
    const local: LWWValue = {
      value: obj,
      hlc: { ts: 200, c: 0, node: "a" },
      nodeID: "a",
    };
    const remote: LWWValue = {
      value: "other",
      hlc: { ts: 100, c: 0, node: "b" },
      nodeID: "b",
    };
    expect(mergeLWW(local, remote).value).toBe(obj);
  });
});

// ---------------------------------------------------------------------------
// newPNCounterState
// ---------------------------------------------------------------------------

describe("newPNCounterState", () => {
  it("creates empty state with empty inc and dec maps", () => {
    const s = newPNCounterState();
    expect(s).toEqual({ inc: {}, dec: {} });
  });
});

// ---------------------------------------------------------------------------
// mergeCounter
// ---------------------------------------------------------------------------

describe("mergeCounter", () => {
  it("takes max per node for increments", () => {
    const local = { inc: { "node-1": 10 }, dec: { "node-1": 2 } };
    const remote = { inc: { "node-1": 8, "node-2": 5 }, dec: {} };

    const merged = mergeCounter(local, remote);
    // node-1 inc: max(10, 8) = 10, node-2 inc: 5, node-1 dec: 2
    expect(counterValue(merged)).toBe(13);
  });

  it("takes max per node for decrements", () => {
    const local = { inc: {}, dec: { "node-1": 3 } };
    const remote = { inc: {}, dec: { "node-1": 5 } };

    const merged = mergeCounter(local, remote);
    expect(merged.dec["node-1"]).toBe(5);
  });

  it("is idempotent", () => {
    const a = { inc: { "node-1": 10 }, dec: {} };
    const merged1 = mergeCounter(a, a);
    const merged2 = mergeCounter(merged1, a);
    expect(counterValue(merged1)).toBe(counterValue(merged2));
  });

  it("is commutative", () => {
    const a = { inc: { "node-1": 10 }, dec: {} };
    const b = { inc: { "node-2": 5 }, dec: {} };
    expect(counterValue(mergeCounter(a, b))).toBe(
      counterValue(mergeCounter(b, a))
    );
  });

  it("returns remote when local is null", () => {
    const c = { inc: { a: 5 }, dec: {} };
    expect(counterValue(mergeCounter(null, c))).toBe(5);
  });

  it("returns local when remote is null", () => {
    const c = { inc: { a: 5 }, dec: {} };
    expect(counterValue(mergeCounter(c, null))).toBe(5);
  });

  it("handles disjoint node sets", () => {
    const local = { inc: { "node-1": 10 }, dec: {} };
    const remote = { inc: { "node-2": 7 }, dec: {} };
    const merged = mergeCounter(local, remote);
    expect(merged.inc["node-1"]).toBe(10);
    expect(merged.inc["node-2"]).toBe(7);
  });
});

// ---------------------------------------------------------------------------
// counterValue
// ---------------------------------------------------------------------------

describe("counterValue", () => {
  it("returns sum(inc) - sum(dec)", () => {
    const state = { inc: { "node-1": 10, "node-2": 5 }, dec: { "node-1": 3 } };
    expect(counterValue(state)).toBe(12);
  });

  it("returns 0 for empty state", () => {
    expect(counterValue(newPNCounterState())).toBe(0);
  });

  it("returns negative value when dec > inc", () => {
    const state = { inc: { "node-1": 2 }, dec: { "node-1": 5 } };
    expect(counterValue(state)).toBe(-3);
  });
});

// ---------------------------------------------------------------------------
// tagKey
// ---------------------------------------------------------------------------

describe("tagKey", () => {
  it("produces deterministic Go-compatible key", () => {
    const tag: ORSetTag = {
      node: "n1",
      hlc: { ts: 100, c: 0, node: "n1" },
    };
    expect(tagKey(tag)).toBe("n1:HLC{ts:100 c:0 node:n1}");
  });
});

// ---------------------------------------------------------------------------
// newORSetState
// ---------------------------------------------------------------------------

describe("newORSetState", () => {
  it("creates empty state with empty entries and removed maps", () => {
    const s = newORSetState();
    expect(s).toEqual({ entries: {}, removed: {} });
  });
});

// ---------------------------------------------------------------------------
// mergeSet
// ---------------------------------------------------------------------------

describe("mergeSet", () => {
  const hlcA: HLC = { ts: 1, c: 0, node: "node-1" };
  const hlcB: HLC = { ts: 2, c: 0, node: "node-2" };

  it("unions entries from both sides", () => {
    const local: ORSetState = {
      entries: { '"a"': [{ node: "node-1", hlc: hlcA }] },
      removed: {},
    };
    const remote: ORSetState = {
      entries: { '"b"': [{ node: "node-2", hlc: hlcB }] },
      removed: {},
    };
    const merged = mergeSet(local, remote);
    expect(Object.keys(merged.entries)).toContain('"a"');
    expect(Object.keys(merged.entries)).toContain('"b"');
  });

  it("unions removed flags from both sides", () => {
    const local: ORSetState = {
      entries: {},
      removed: { key1: true },
    };
    const remote: ORSetState = {
      entries: {},
      removed: { key2: true },
    };
    const merged = mergeSet(local, remote);
    expect(merged.removed["key1"]).toBe(true);
    expect(merged.removed["key2"]).toBe(true);
  });

  it("deduplicates tags per element", () => {
    const tag: ORSetTag = { node: "node-1", hlc: hlcA };
    const state: ORSetState = {
      entries: { '"a"': [tag] },
      removed: {},
    };
    const merged = mergeSet(state, state);
    // Same tag from both sides should be deduplicated to 1
    expect(merged.entries['"a"'].length).toBe(1);
  });

  it("returns remote when local is null", () => {
    const remote: ORSetState = {
      entries: { '"a"': [{ node: "node-1", hlc: hlcA }] },
      removed: {},
    };
    expect(mergeSet(null, remote)).toBe(remote);
  });

  it("returns local when remote is null", () => {
    const local: ORSetState = {
      entries: { '"a"': [{ node: "node-1", hlc: hlcA }] },
      removed: {},
    };
    expect(mergeSet(local, null)).toBe(local);
  });

  it("is commutative", () => {
    const a: ORSetState = {
      entries: { '"x"': [{ node: "node-1", hlc: hlcA }] },
      removed: {},
    };
    const b: ORSetState = {
      entries: { '"y"': [{ node: "node-2", hlc: hlcB }] },
      removed: {},
    };
    const ab = setElements(mergeSet(a, b));
    const ba = setElements(mergeSet(b, a));
    expect(ab).toEqual(ba);
  });

  it("is idempotent", () => {
    const a: ORSetState = {
      entries: { '"x"': [{ node: "node-1", hlc: hlcA }] },
      removed: {},
    };
    const merged1 = mergeSet(a, a);
    const merged2 = mergeSet(merged1, a);
    expect(setElements(merged1)).toEqual(setElements(merged2));
  });
});

// ---------------------------------------------------------------------------
// setElements
// ---------------------------------------------------------------------------

describe("setElements", () => {
  it("returns elements with at least one non-removed tag", () => {
    const hlc1: HLC = { ts: 1, c: 0, node: "node-1" };
    const hlc2: HLC = { ts: 2, c: 0, node: "node-1" };
    const state: ORSetState = {
      entries: {
        '"hello"': [{ node: "node-1", hlc: hlc1 }],
        '"world"': [{ node: "node-1", hlc: hlc2 }],
      },
      removed: {},
    };
    expect(setElements(state)).toEqual(["hello", "world"]);
  });

  it("excludes elements whose tags are all removed", () => {
    const hlc1: HLC = { ts: 1, c: 0, node: "node-1" };
    const tag: ORSetTag = { node: "node-1", hlc: hlc1 };
    const state: ORSetState = {
      entries: { '"hello"': [tag] },
      removed: { [tagKey(tag)]: true },
    };
    expect(setElements(state)).toEqual([]);
  });

  it("handles concurrent add-remove (add wins with new tag)", () => {
    const hlc1: HLC = { ts: 1, c: 0, node: "node-1" };
    const hlc2: HLC = { ts: 2, c: 0, node: "node-1" };
    const tag1: ORSetTag = { node: "node-1", hlc: hlc1 };
    const tag2: ORSetTag = { node: "node-1", hlc: hlc2 };

    // tag1 is removed but tag2 is not => element is present
    const state: ORSetState = {
      entries: { '"x"': [tag1, tag2] },
      removed: { [tagKey(tag1)]: true },
    };
    expect(setElements(state)).toEqual(["x"]);
  });

  it("returns elements sorted by key", () => {
    const hlc: HLC = { ts: 1, c: 0, node: "n" };
    const state: ORSetState = {
      entries: {
        '"b"': [{ node: "n", hlc }],
        '"a"': [{ node: "n", hlc: { ts: 2, c: 0, node: "n" } }],
      },
      removed: {},
    };
    expect(setElements(state)).toEqual(["a", "b"]);
  });

  it("JSON.parse values that are valid JSON strings", () => {
    const hlc: HLC = { ts: 1, c: 0, node: "n" };
    const state: ORSetState = {
      entries: { "42": [{ node: "n", hlc }] },
      removed: {},
    };
    // "42" is valid JSON => parsed to number 42
    expect(setElements(state)).toEqual([42]);
  });

  it("returns raw key when JSON.parse fails", () => {
    const hlc: HLC = { ts: 1, c: 0, node: "n" };
    const state: ORSetState = {
      entries: { "not-json{": [{ node: "n", hlc }] },
      removed: {},
    };
    expect(setElements(state)).toEqual(["not-json{"]);
  });

  it("returns empty array for empty state", () => {
    expect(setElements(newORSetState())).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// mergeFieldState
// ---------------------------------------------------------------------------

describe("mergeFieldState", () => {
  describe("lww type", () => {
    it("merges LWW when remote HLC is higher", () => {
      const local = {
        type: "lww" as const,
        hlc: { ts: 100, c: 0, node: "a" },
        node_id: "a",
        value: "local",
      };
      const change: ChangeRecord = {
        table: "t",
        pk: "1",
        field: "f",
        crdt_type: "lww",
        hlc: { ts: 200, c: 0, node: "b" },
        node_id: "b",
        value: "remote",
      };
      const result = mergeFieldState(local, change);
      expect(result.value).toBe("remote");
    });

    it("keeps local when local HLC is higher", () => {
      const local = {
        type: "lww" as const,
        hlc: { ts: 300, c: 0, node: "a" },
        node_id: "a",
        value: "local",
      };
      const change: ChangeRecord = {
        table: "t",
        pk: "1",
        field: "f",
        crdt_type: "lww",
        hlc: { ts: 200, c: 0, node: "b" },
        node_id: "b",
        value: "remote",
      };
      const result = mergeFieldState(local, change);
      expect(result.value).toBe("local");
    });

    it("creates new field state when local is null", () => {
      const change: ChangeRecord = {
        table: "t",
        pk: "1",
        field: "f",
        crdt_type: "lww",
        hlc: { ts: 100, c: 0, node: "a" },
        node_id: "a",
        value: "new",
      };
      const result = mergeFieldState(null, change);
      expect(result.type).toBe("lww");
      expect(result.value).toBe("new");
    });
  });

  describe("counter type", () => {
    it("applies counter delta to existing state", () => {
      const local = {
        type: "counter" as const,
        hlc: { ts: 100, c: 0, node: "a" },
        node_id: "a",
        counter_state: { inc: { a: 5 }, dec: {} },
      };
      const change: ChangeRecord = {
        table: "t",
        pk: "1",
        field: "f",
        crdt_type: "counter",
        hlc: { ts: 200, c: 0, node: "b" },
        node_id: "b",
        counter_delta: { inc: 3, dec: 0 },
      };
      const result = mergeFieldState(local, change);
      expect(result.counter_state).toBeDefined();
      expect(counterValue(result.counter_state!)).toBe(8); // 5 + 3
    });

    it("creates new counter state when local is null", () => {
      const change: ChangeRecord = {
        table: "t",
        pk: "1",
        field: "f",
        crdt_type: "counter",
        hlc: { ts: 100, c: 0, node: "a" },
        node_id: "a",
        counter_delta: { inc: 7, dec: 0 },
      };
      const result = mergeFieldState(null, change);
      expect(result.type).toBe("counter");
      expect(counterValue(result.counter_state!)).toBe(7);
    });

    it("handles change without counter_delta", () => {
      const change: ChangeRecord = {
        table: "t",
        pk: "1",
        field: "f",
        crdt_type: "counter",
        hlc: { ts: 100, c: 0, node: "a" },
        node_id: "a",
      };
      const result = mergeFieldState(null, change);
      expect(result.type).toBe("counter");
      expect(counterValue(result.counter_state!)).toBe(0);
    });

    it("treats deltas as cumulative per-node snapshots (max-merge, idempotent)", () => {
      // The wire delta is the sending node's cumulative totals — matching
      // Go's ApplyChange — so redelivery cannot double-count.
      const change1: ChangeRecord = {
        table: "t",
        pk: "1",
        field: "f",
        crdt_type: "counter",
        hlc: { ts: 100, c: 0, node: "a" },
        node_id: "a",
        counter_delta: { inc: 3, dec: 0 },
      };
      const state1 = mergeFieldState(null, change1);

      const change2: ChangeRecord = {
        table: "t",
        pk: "1",
        field: "f",
        crdt_type: "counter",
        hlc: { ts: 200, c: 0, node: "a" },
        node_id: "a",
        counter_delta: { inc: 7, dec: 0 }, // cumulative: 3 then +4 more
      };
      const state2 = mergeFieldState(state1, change2);
      expect(counterValue(state2.counter_state!)).toBe(7);

      // Redelivering an older snapshot changes nothing.
      const replayed = mergeFieldState(state2, change1);
      expect(counterValue(replayed.counter_state!)).toBe(7);
    });
  });

  describe("set type", () => {
    it("applies add operation", () => {
      const change: ChangeRecord = {
        table: "t",
        pk: "1",
        field: "f",
        crdt_type: "set",
        hlc: { ts: 100, c: 0, node: "a" },
        node_id: "a",
        set_op: { op: "add", elements: ["x", "y"] },
      };
      const result = mergeFieldState(null, change);
      expect(result.type).toBe("set");
      const elems = setElements(result.set_state!);
      expect(elems).toContain("x");
      expect(elems).toContain("y");
    });

    it("applies remove operation", () => {
      // First add
      const addChange: ChangeRecord = {
        table: "t",
        pk: "1",
        field: "f",
        crdt_type: "set",
        hlc: { ts: 100, c: 0, node: "a" },
        node_id: "a",
        set_op: { op: "add", elements: ["x"] },
      };
      const afterAdd = mergeFieldState(null, addChange);
      expect(setElements(afterAdd.set_state!)).toContain("x");

      // Then remove
      const removeChange: ChangeRecord = {
        table: "t",
        pk: "1",
        field: "f",
        crdt_type: "set",
        hlc: { ts: 200, c: 0, node: "a" },
        node_id: "a",
        set_op: { op: "remove", elements: ["x"] },
      };
      const afterRemove = mergeFieldState(afterAdd, removeChange);
      expect(setElements(afterRemove.set_state!)).not.toContain("x");
    });

    it("creates new set state when local is null", () => {
      const change: ChangeRecord = {
        table: "t",
        pk: "1",
        field: "f",
        crdt_type: "set",
        hlc: { ts: 100, c: 0, node: "a" },
        node_id: "a",
        set_op: { op: "add", elements: ["a"] },
      };
      const result = mergeFieldState(null, change);
      expect(result.set_state).toBeDefined();
    });

    it("handles change without set_op", () => {
      const change: ChangeRecord = {
        table: "t",
        pk: "1",
        field: "f",
        crdt_type: "set",
        hlc: { ts: 100, c: 0, node: "a" },
        node_id: "a",
      };
      const result = mergeFieldState(null, change);
      expect(result.type).toBe("set");
      expect(setElements(result.set_state!)).toEqual([]);
    });
  });

  describe("default (unknown type) fallback", () => {
    it("treats unknown crdt_type as LWW", () => {
      const change: ChangeRecord = {
        table: "t",
        pk: "1",
        field: "f",
        crdt_type: "unknown_type" as any,
        hlc: { ts: 100, c: 0, node: "a" },
        node_id: "a",
        value: "fallback",
      };
      const result = mergeFieldState(null, change);
      expect(result.value).toBe("fallback");
      expect(result.type).toBe("unknown_type");
    });
  });
});

// ---------------------------------------------------------------------------
// List CRDT merge
// ---------------------------------------------------------------------------

describe("List CRDT merge", () => {
  /** Helper to build an RGA node. */
  function makeNode(ts: number, node: string, parentTs = 0, value?: unknown): RGANode {
    return {
      id: { ts, c: 0, node },
      node_id: node,
      parent_id: { ts: parentTs, c: 0, node: parentTs === 0 ? "" : node },
      value: value ?? `val-${ts}`,
    };
  }

  /** Helper to build key for an HLC. */
  function nk(ts: number, node: string): string {
    return `${ts}:0:${node}`;
  }

  it("merges two empty lists", () => {
    const merged = mergeListState(newRGAListState(), newRGAListState());
    expect(Object.keys(merged.nodes)).toHaveLength(0);
    expect(listElements(merged)).toEqual([]);
  });

  it("merges disjoint lists", () => {
    const local: RGAListState = {
      nodes: { [nk(1, "a")]: makeNode(1, "a", 0, "x") },
    };
    const remote: RGAListState = {
      nodes: { [nk(2, "b")]: makeNode(2, "b", 0, "y") },
    };
    const merged = mergeListState(local, remote);
    expect(Object.keys(merged.nodes)).toHaveLength(2);

    const elems = listElements(merged);
    expect(elems).toContain("x");
    expect(elems).toContain("y");
  });

  it("preserves tombstones from both sides", () => {
    const localNode = makeNode(1, "a", 0, "x");
    const remoteNode = { ...makeNode(2, "b", 0, "y"), tombstone: true };

    const local: RGAListState = { nodes: { [nk(1, "a")]: localNode } };
    const remote: RGAListState = { nodes: { [nk(2, "b")]: remoteNode } };
    const merged = mergeListState(local, remote);

    expect(Object.keys(merged.nodes)).toHaveLength(2);
    // Only non-tombstoned nodes appear in elements.
    const elems = listElements(merged);
    expect(elems).toEqual(["x"]);
  });

  it("listElements returns visible elements in order", () => {
    // Build a 3-element list: A -> B -> C (chained via parent_id).
    const nodeA = makeNode(1, "a", 0, "A");
    const nodeB: RGANode = {
      id: { ts: 2, c: 0, node: "a" },
      node_id: "a",
      parent_id: { ts: 1, c: 0, node: "a" },
      value: "B",
    };
    const nodeC: RGANode = {
      id: { ts: 3, c: 0, node: "a" },
      node_id: "a",
      parent_id: { ts: 2, c: 0, node: "a" },
      value: "C",
    };

    const state: RGAListState = {
      nodes: {
        [nk(1, "a")]: nodeA,
        [nk(2, "a")]: nodeB,
        [nk(3, "a")]: nodeC,
      },
    };

    expect(listElements(state)).toEqual(["A", "B", "C"]);
  });

  it("listNodeIds returns HLC IDs", () => {
    const nodeA = makeNode(1, "a", 0, "A");
    const nodeB: RGANode = {
      id: { ts: 2, c: 0, node: "a" },
      node_id: "a",
      parent_id: { ts: 1, c: 0, node: "a" },
      value: "B",
    };

    const state: RGAListState = {
      nodes: {
        [nk(1, "a")]: nodeA,
        [nk(2, "a")]: nodeB,
      },
    };

    const ids = listNodeIds(state);
    expect(ids).toHaveLength(2);
    expect(ids[0].ts).toBe(1);
    expect(ids[1].ts).toBe(2);
  });
});

// ---------------------------------------------------------------------------
// Document CRDT merge
// ---------------------------------------------------------------------------

describe("Document CRDT merge", () => {
  it("merges two empty documents", () => {
    const merged = mergeDocumentState(newDocumentCRDTState(), newDocumentCRDTState());
    expect(Object.keys(merged.fields)).toHaveLength(0);
    expect(documentResolve(merged)).toEqual({});
  });

  it("merges disjoint paths", () => {
    const local: DocumentCRDTState = {
      fields: {
        name: { type: "lww", hlc: { ts: 1, c: 0, node: "a" }, node_id: "a", value: "Alice" },
      },
    };
    const remote: DocumentCRDTState = {
      fields: {
        email: { type: "lww", hlc: { ts: 2, c: 0, node: "b" }, node_id: "b", value: "alice@example.com" },
      },
    };

    const merged = mergeDocumentState(local, remote);
    const resolved = documentResolve(merged);
    expect(resolved.name).toBe("Alice");
    expect(resolved.email).toBe("alice@example.com");
  });

  it("LWW resolution for shared paths", () => {
    const local: DocumentCRDTState = {
      fields: {
        name: { type: "lww", hlc: { ts: 100, c: 0, node: "a" }, node_id: "a", value: "Alice" },
      },
    };
    const remote: DocumentCRDTState = {
      fields: {
        name: { type: "lww", hlc: { ts: 200, c: 0, node: "b" }, node_id: "b", value: "Bob" },
      },
    };

    const merged = mergeDocumentState(local, remote);
    const resolved = documentResolve(merged);
    // Remote has higher HLC, so Bob wins.
    expect(resolved.name).toBe("Bob");
  });

  it("documentResolve builds nested structure", () => {
    const state: DocumentCRDTState = {
      fields: {
        title: { type: "lww", hlc: { ts: 1, c: 0, node: "a" }, node_id: "a", value: "My Doc" },
        count: {
          type: "counter",
          hlc: { ts: 2, c: 0, node: "a" },
          node_id: "a",
          counter_state: { inc: { a: 5 }, dec: {} },
        },
      },
    };

    const resolved = documentResolve(state);
    expect(resolved.title).toBe("My Doc");
    expect(resolved.count).toBe(5);
  });

  it("documentResolve handles deep paths", () => {
    // Simulate a nested document within a document field.
    const innerDoc: DocumentCRDTState = {
      fields: {
        street: { type: "lww", hlc: { ts: 1, c: 0, node: "a" }, node_id: "a", value: "123 Main St" },
        city: { type: "lww", hlc: { ts: 2, c: 0, node: "a" }, node_id: "a", value: "Springfield" },
      },
    };
    const state: DocumentCRDTState = {
      fields: {
        address: {
          type: "document",
          hlc: { ts: 3, c: 0, node: "a" },
          node_id: "a",
          doc_state: innerDoc,
        },
      },
    };

    const resolved = documentResolve(state);
    expect(resolved.address).toEqual({ street: "123 Main St", city: "Springfield" });
  });
});

// ---------------------------------------------------------------------------
// Field clock under redelivery (Go ApplyChange keeps the newer HLC)
// ---------------------------------------------------------------------------

describe("mergeFieldState field clock", () => {
  const older: HLC = { ts: 100, c: 0, node: "a" };
  const newer: HLC = { ts: 200, c: 0, node: "b" };

  const changeAt = (
    crdt_type: ChangeRecord["crdt_type"],
    hlc: HLC,
    extra: Partial<ChangeRecord>,
  ): ChangeRecord => ({
    table: "t", pk: "1", field: "f", crdt_type, hlc, node_id: hlc.node, ...extra,
  });

  const cases: Array<[string, (hlc: HLC) => ChangeRecord]> = [
    ["counter", (hlc) => changeAt("counter", hlc, { counter_delta: { inc: 1, dec: 0 } })],
    ["set", (hlc) => changeAt("set", hlc, { set_op: { op: "add", elements: [hlc.node] } })],
    ["list", (hlc) => changeAt("list", hlc, {
      list_op: { op: "insert", node_id: hlc, parent_id: { ts: 0, c: 0, node: "" }, value: hlc.node },
    })],
    ["document", (hlc) => changeAt("document", hlc, { value: { path: `p.${hlc.node}`, value: 1 } })],
  ];

  it.each(cases)("%s: a redelivered older change does not regress the field HLC", (_type, make) => {
    const afterNewer = mergeFieldState(null, make(newer));
    const afterOlder = mergeFieldState(afterNewer, make(older));
    expect(afterOlder.hlc).toEqual(newer);
    expect(afterOlder.node_id).toBe("b");
  });
});

// ---------------------------------------------------------------------------
// Set element keys follow Go json.Marshal bytes
// ---------------------------------------------------------------------------

describe("set element keys (Go parity)", () => {
  const goHLC: HLC = { ts: 100, c: 0, node: "go" };
  const jsHLC: HLC = { ts: 200, c: 0, node: "js" };

  it("removes an element a Go writer added under its json.Marshal key", () => {
    // Go ORSetState.Add keys "a<b" as json.Marshal does: "a\u003cb".
    const goState: ORSetState = { entries: { '"a\\u003cb"': [{ node: "go", hlc: goHLC }] }, removed: {} };
    const local = { type: "set" as const, hlc: goHLC, node_id: "go", set_state: goState };
    const removed = mergeFieldState(local, {
      table: "t", pk: "1", field: "tags", crdt_type: "set", hlc: jsHLC, node_id: "js",
      set_op: { op: "remove", elements: ["a<b"] },
    });
    expect(setElements(removed.set_state!)).toEqual([]);
  });

  it("keys objects by sorted field names, so key order does not split an element", () => {
    let state = mergeFieldState(null, {
      table: "t", pk: "1", field: "tags", crdt_type: "set", hlc: goHLC, node_id: "go",
      set_op: { op: "add", elements: [{ b: 1, a: 2 }] },
    });
    state = mergeFieldState(state, {
      table: "t", pk: "1", field: "tags", crdt_type: "set", hlc: jsHLC, node_id: "js",
      set_op: { op: "add", elements: [{ a: 2, b: 1 }] },
    });
    expect(Object.keys(state.set_state!.entries)).toEqual(['{"a":2,"b":1}']);
  });

  it("orders elements by UTF-8 bytes like Go sort.Strings", () => {
    const tag = [{ node: "go", hlc: goHLC }];
    // U+FF01 encodes as EF BC 81, U+1F600 as F0 9F 98 80: Go puts U+FF01 first.
    // In UTF-16 the emoji's high surrogate (D83D) sorts before FF01.
    const state: ORSetState = { entries: { '"\u{1F600}"': tag, '"！"': tag }, removed: {} };
    expect(setElements(state)).toEqual(["！", "\u{1F600}"]);
  });
});

// ---------------------------------------------------------------------------
// Set states written before element keys were canonical
// ---------------------------------------------------------------------------

describe("set states with legacy element keys", () => {
  const t1: ORSetTag = { node: "js", hlc: { ts: 100, c: 0, node: "js" } };
  // "a<b" keyed by JSON.stringify, as crdt-js stored it before.
  const legacy = (removed: boolean): ORSetState => ({
    entries: { '"a<b"': [t1] },
    removed: removed ? { ['"a<b"|' + tagKey(t1)]: true } : {},
  });

  it("does not show an element twice next to its canonical twin", () => {
    const fresh: ORSetState = { entries: { '"a\\u003cb"': [{ node: "go", hlc: { ts: 150, c: 0, node: "go" } }] }, removed: {} };
    expect(setElements(mergeSet(legacy(false), fresh))).toEqual(["a<b"]);
  });

  it("keeps a removed element removed when it reappears under the canonical key", () => {
    const readd: ORSetState = { entries: { '"a\\u003cb"': [t1] }, removed: {} };
    expect(setElements(mergeSet(legacy(true), readd))).toEqual([]);
  });

  it("lets a remove match an element stored under its legacy key", () => {
    const local = { type: "set" as const, hlc: t1.hlc, node_id: "js", set_state: legacy(false) };
    const out = mergeFieldState(local, {
      table: "t", pk: "1", field: "tags", crdt_type: "set", hlc: { ts: 200, c: 0, node: "js" }, node_id: "js",
      set_op: { op: "remove", elements: ["a<b"], tags: [t1] },
    });
    expect(setElements(out.set_state!)).toEqual([]);
  });
});
