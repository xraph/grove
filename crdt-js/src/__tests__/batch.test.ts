import { describe, it, expect } from "vitest";
import { CRDTStore } from "../store.js";
import { HybridClock } from "../hlc.js";
import type { StorePlugin, WriteHook } from "../plugin.js";

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
    store.use({ name: "spy", afterWrite(ev) { seen.push(ev.field); } } as StorePlugin & WriteHook);
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
    } as StorePlugin & WriteHook);
    store.batch("t", "p").setField("ok", 1).setField("blocked", 2).commit();
    const doc = store.getDocument<Record<string, unknown>>("t", "p");
    expect(doc?.ok).toBe(1);
    expect(doc?.blocked).toBeUndefined();
  });
});
