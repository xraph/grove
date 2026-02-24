import { describe, it, expect } from "vitest";
import type { HLC, ChangeRecord, ORSetTag, ORSetState } from "../types.js";
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

    it("accumulates increments from same node", () => {
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
        counter_delta: { inc: 4, dec: 0 },
      };
      const state2 = mergeFieldState(state1, change2);
      expect(counterValue(state2.counter_state!)).toBe(7); // 3 + 4
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
