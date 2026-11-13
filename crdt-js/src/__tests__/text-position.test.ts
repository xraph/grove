import { expect, it } from "vitest";
import { newTextState, textInsert, textRefAt, textPositionAt, textResolvePosition, textDeleteOp, textValue } from "../text.js";

it("anchors both sides of Unicode gaps through insertions and deletions", () => {
  const state = newTextState();
  textInsert(state, null, "A😀BC", "a", { ts: 1, c: 0, node: "a" });
  const before = textPositionAt(state, 2, "before")!;
  const after = textPositionAt(state, 2, "after")!;
  textInsert(state, after.ref!, "XY", "b", { ts: 2, c: 0, node: "b" });
  expect(textValue(state)).toBe("A😀XYBC");
  expect(textResolvePosition(state, JSON.parse(JSON.stringify(before)))).toBe(4);
  expect(textResolvePosition(state, after)).toBe(2);
  textDeleteOp(state, textRefAt(state, 1)!, 3);
  expect(textValue(state)).toBe("ABC");
  expect(textResolvePosition(state, before)).toBe(1);
  expect(textResolvePosition(state, after)).toBe(1);
});

it("handles empty documents, boundaries, invalid offsets and missing edits", () => {
  const state = newTextState();
  const start = textPositionAt(state, 0, "after")!;
  const end = textPositionAt(state, 0, "before")!;
  textInsert(state, null, "hello", "a", { ts: 1, c: 0, node: "a" });
  expect(textResolvePosition(state, start)).toBe(0);
  expect(textResolvePosition(state, end)).toBe(5);
  for (const index of [-1, 6, NaN, Infinity, 1.5]) expect(textPositionAt(state, index)).toBeNull();
  expect(textResolvePosition(state, { ref: { origin: { ts: 99, c: 0, node: "missing" }, offset: 0 }, side: "after" })).toBeNull();
});
