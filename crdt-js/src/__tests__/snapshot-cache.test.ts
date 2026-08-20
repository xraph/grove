import { describe, it, expect } from "vitest";
import { CRDTStore } from "../store.js";
import { HybridClock } from "../hlc.js";
import type { StorePlugin, ReadHook } from "../plugin.js";

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
    } as StorePlugin & ReadHook);
    const after = store.getDocument<Record<string, unknown>>("t", "p");
    expect(after).not.toBe(before);
    expect(after?.tagged).toBe(true);
  });
});
