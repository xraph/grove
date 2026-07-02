import { describe, it, expect } from "vitest";
import type { HLC, TextState, TextOperation } from "../types.js";
import {
  newTextState,
  textInsert,
  textDeleteOp,
  textFormat,
  textValue,
  textLength,
  textDelta,
  textRefAt,
  textIndexOf,
  applyTextOp,
  mergeText,
} from "../text.js";
import { CRDTStore } from "../store.js";
import { HybridClock } from "../hlc.js";

function newStore(nodeID: string): CRDTStore {
  return new CRDTStore(nodeID, new HybridClock(nodeID));
}

function hlc(ts: number, node: string): HLC {
  return { ts, c: 0, node };
}

/** Type chunks sequentially from one node. */
function typeChunks(
  st: TextState,
  node: string,
  startTS: number,
  chunks: string[]
): { op: TextOperation; hlc: HLC; node: string }[] {
  const ops: { op: TextOperation; hlc: HLC; node: string }[] = [];
  let ts = startTS;
  for (const chunk of chunks) {
    const len = textLength(st);
    const ref = len > 0 ? textRefAt(st, len - 1) : null;
    const clock = hlc(ts, node);
    ops.push({ op: textInsert(st, ref, chunk, node, clock), hlc: clock, node });
    ts++;
  }
  return ops;
}

describe("text CRDT", () => {
  it("coalesces sequential typing into one origin", () => {
    const st = newTextState();
    typeChunks(st, "a", 1, ["h", "e", "l", "l", "o"]);
    expect(textValue(st)).toBe("hello");
    expect(Object.keys(st.frags)).toHaveLength(1);
  });

  it("splits on middle insert", () => {
    const st = newTextState();
    typeChunks(st, "a", 1, ["hello"]);
    const ref = textRefAt(st, 1)!;
    textInsert(st, ref, "XY", "b", hlc(100, "b"));
    expect(textValue(st)).toBe("heXYllo");
  });

  it("deletes ranges across origins", () => {
    const st = newTextState();
    typeChunks(st, "a", 1, ["hello"]);
    textInsert(st, textRefAt(st, 4)!, " world", "b", hlc(100, "b"));
    textDeleteOp(st, textRefAt(st, 4)!, 4);
    expect(textValue(st)).toBe("hellrld");
  });

  it("formats with LWW attributes and merges delta runs", () => {
    const st = newTextState();
    typeChunks(st, "a", 1, ["hello world"]);
    textFormat(st, textRefAt(st, 0)!, 5, { bold: true }, "a", hlc(50, "a"));
    const delta = textDelta(st);
    expect(delta).toHaveLength(2);
    expect(delta[0]).toEqual({ insert: "hello", attributes: { bold: true } });
    expect(delta[1]).toEqual({ insert: " world" });
    // Older conflicting format loses; newer null clears.
    textFormat(st, textRefAt(st, 0)!, 5, { bold: null }, "b", hlc(40, "b"));
    expect(textDelta(st)[0].attributes).toEqual({ bold: true });
    textFormat(st, textRefAt(st, 0)!, 5, { bold: null }, "b", hlc(60, "b"));
    expect(textDelta(st)[0].attributes).toBeUndefined();
  });

  it("keeps relative positions across concurrent edits", () => {
    const st = newTextState();
    typeChunks(st, "a", 1, ["hello world"]);
    const anchor = textRefAt(st, 4)!;
    textInsert(st, null, ">> ", "b", hlc(100, "b"));
    expect(textIndexOf(st, anchor)).toBe(7);
  });

  it("collapses tombstoned anchors", () => {
    const st = newTextState();
    typeChunks(st, "a", 1, ["abcdef"]);
    const anchor = textRefAt(st, 2)!;
    textDeleteOp(st, textRefAt(st, 1)!, 3);
    expect(textValue(st)).toBe("aef");
    expect(textIndexOf(st, anchor)).toBe(1);
  });

  it("converges concurrent inserts at the same point", () => {
    const base = newTextState();
    const baseOps = typeChunks(base, "a", 1, ["ab"]);

    const r1 = newTextState();
    const r2 = newTextState();
    for (const { op, hlc: clock, node } of baseOps) {
      applyTextOp(r1, op, node, clock);
      applyTextOp(r2, op, node, clock);
    }
    const ref = textRefAt(base, 0)!;
    const op1 = textInsert(r1, ref, "X", "n1", hlc(100, "n1"));
    const op2 = textInsert(r2, ref, "Y", "n2", hlc(101, "n2"));
    applyTextOp(r1, op2, "n2", hlc(101, "n2"));
    applyTextOp(r2, op1, "n1", hlc(100, "n1"));
    expect(textValue(r1)).toBe(textValue(r2));
    expect(textValue(r1)).toBe("aYXb");
  });

  it("mergeText is commutative and idempotent", () => {
    const a = newTextState();
    typeChunks(a, "a", 1, ["shared"]);
    const b = newTextState();
    typeChunks(b, "b", 100, ["other"]);
    const ab = mergeText(a, b);
    const ba = mergeText(b, a);
    expect(textValue(ab)).toBe(textValue(ba));
    expect(textValue(mergeText(ab, ab))).toBe(textValue(ab));
  });

  it("handles unicode by rune offsets", () => {
    const st = newTextState();
    typeChunks(st, "a", 1, ["héllo 🌍"]);
    expect(textLength(st)).toBe(7);
    textInsert(st, textRefAt(st, 5)!, "brave ", "b", hlc(100, "b"));
    expect(textValue(st)).toBe("héllo brave 🌍");
  });
});

describe("store text integration", () => {
  it("edits text fields and supports undo", () => {
    const store = newStore("n1");
    store.insertText("notes", "1", "body", 0, "hello");
    store.insertText("notes", "1", "body", 5, " world");
    expect(store.getText("notes", "1", "body")).toBe("hello world");

    store.deleteText("notes", "1", "body", 5, 6);
    expect(store.getText("notes", "1", "body")).toBe("hello");

    store.formatText("notes", "1", "body", 0, 5, { bold: true });
    expect(store.getTextDelta("notes", "1", "body")).toEqual([
      { insert: "hello", attributes: { bold: true } },
    ]);

    // Undo the format, then the delete.
    expect(store.undo()).toBe(true);
    expect(store.getTextDelta("notes", "1", "body")[0]?.attributes).toBeUndefined();
    expect(store.undo()).toBe(true);
    expect(store.getText("notes", "1", "body")).toBe("hello world");
  });

  it("anchors cursors through getTextRefAt/getTextIndexOf", () => {
    const store = newStore("n1");
    store.insertText("notes", "1", "body", 0, "hello world");
    const anchor = store.getTextRefAt("notes", "1", "body", 4)!;
    store.insertText("notes", "1", "body", 0, ">> ");
    expect(store.getTextIndexOf("notes", "1", "body", anchor)).toBe(7);
  });

  it("exposes list node ids in document order", () => {
    const store = newStore("n1");
    store.insertIntoList("t", "1", "items", "first");
    const ids = store.getListNodeIds("t", "1", "items");
    expect(ids).toHaveLength(1);
    store.insertIntoList("t", "1", "items", "second", ids[0]);
    expect(store.getListNodeIds("t", "1", "items")).toHaveLength(2);
  });
});
