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
});
