import { describe, it, expect, vi } from "vitest";
import { CRDTStore } from "../store.js";
import { HybridClock } from "../hlc.js";
import { MemoryStorage } from "../storage.js";

describe("persistence debounce", () => {
  it("coalesces rapid writes into one savePendingChanges call", async () => {
    const storage = new MemoryStorage();
    const spy = vi.spyOn(storage, "savePendingChanges");
    const store = new CRDTStore("n1", new HybridClock("n1"), storage, {
      persistDebounceMs: 20,
    });
    for (let i = 0; i < 50; i++) store.setField("t", "p", `f${i}`, i);
    expect(spy).not.toHaveBeenCalled();
    await store.flushPersistence();
    expect(spy).toHaveBeenCalledTimes(1);
    expect(spy.mock.calls[0][0]).toHaveLength(50);
  });

  it("persistDebounceMs: 0 writes synchronously", () => {
    const storage = new MemoryStorage();
    const spy = vi.spyOn(storage, "savePendingChanges");
    const store = new CRDTStore("n1", new HybridClock("n1"), storage, {
      persistDebounceMs: 0,
    });
    store.setField("t", "p", "f", 1);
    expect(spy).toHaveBeenCalledTimes(1);
  });

  it("does not re-persist pending changes on a document-only drain (applyChanges)", async () => {
    // applyChanges only touches document state, never this.pending — the
    // debounce drain must not re-write an unchanged pending array just
    // because a document write happened to share its timer.
    const storage = new MemoryStorage();
    const spy = vi.spyOn(storage, "savePendingChanges");
    const store = new CRDTStore("n1", new HybridClock("n1"), storage, {
      persistDebounceMs: 10,
    });
    await store.ready;

    store.applyChanges([
      {
        table: "t",
        pk: "p",
        field: "f",
        crdt_type: "lww",
        hlc: { ts: 1, c: 0, node: "remote" },
        node_id: "remote",
        value: "hi",
      },
    ]);

    // Let the real debounce timer fire and drain.
    await new Promise((r) => setTimeout(r, 30));
    expect(spy).not.toHaveBeenCalled();
  });

  it("does not re-persist pending changes on a document-only flush (undo)", async () => {
    // undo() also only touches document state. Mirrors the drain test but
    // through the explicit flushPersistence() escape hatch instead of the
    // timer, since flushPersistence gates on the same dirty flag.
    const storage = new MemoryStorage();
    const store = new CRDTStore("n1", new HybridClock("n1"), storage, {
      persistDebounceMs: 10,
    });
    await store.ready;
    store.setField("t", "p", "f", 1);
    await store.flushPersistence(); // clears the dirty flag from setField

    const spy = vi.spyOn(storage, "savePendingChanges");
    store.undo();
    await store.flushPersistence();
    expect(spy).not.toHaveBeenCalled();
  });
});
