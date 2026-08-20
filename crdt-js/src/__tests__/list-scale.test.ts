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
