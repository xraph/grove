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
