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
