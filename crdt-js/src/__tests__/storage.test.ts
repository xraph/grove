import { describe, it, expect, vi, beforeEach } from "vitest";
import { MemoryStorage } from "../storage.js";
import { CRDTStore } from "../store.js";
import { HybridClock } from "../hlc.js";
import type { StorageAdapter, DocumentState, ChangeRecord } from "../types.js";

// ---------------------------------------------------------------------------
// MemoryStorage
// ---------------------------------------------------------------------------

describe("MemoryStorage", () => {
  it("loadState returns empty Map", async () => {
    const storage = new MemoryStorage();
    const state = await storage.loadState();
    expect(state).toBeInstanceOf(Map);
    expect(state.size).toBe(0);
  });

  it("saveDocument is a no-op (does not throw)", async () => {
    const storage = new MemoryStorage();
    const doc: DocumentState = {
      table: "t",
      pk: "1",
      fields: {},
      tombstone: false,
    };
    await expect(storage.saveDocument("t", "1", doc)).resolves.toBeUndefined();
  });

  it("deleteDocument is a no-op (does not throw)", async () => {
    const storage = new MemoryStorage();
    await expect(storage.deleteDocument("t", "1")).resolves.toBeUndefined();
  });

  it("loadPendingChanges returns empty array", async () => {
    const storage = new MemoryStorage();
    const changes = await storage.loadPendingChanges();
    expect(changes).toEqual([]);
  });

  it("savePendingChanges is a no-op (does not throw)", async () => {
    const storage = new MemoryStorage();
    await expect(storage.savePendingChanges([])).resolves.toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// Mock StorageAdapter for CRDTStore integration
// ---------------------------------------------------------------------------

function createMockStorage(): StorageAdapter & {
  saved: { table: string; pk: string; doc: DocumentState }[];
  savedPending: ChangeRecord[][];
  deletedDocs: { table: string; pk: string }[];
} {
  return {
    saved: [],
    savedPending: [],
    deletedDocs: [],

    loadState: vi.fn(async () => new Map()),
    loadPendingChanges: vi.fn(async () => []),

    saveDocument: vi.fn(async function (
      this: { saved: { table: string; pk: string; doc: DocumentState }[] },
      table: string,
      pk: string,
      doc: DocumentState
    ) {
      this.saved.push({ table, pk, doc });
    }),

    deleteDocument: vi.fn(async function (
      this: { deletedDocs: { table: string; pk: string }[] },
      table: string,
      pk: string
    ) {
      this.deletedDocs.push({ table, pk });
    }),

    savePendingChanges: vi.fn(async function (
      this: { savedPending: ChangeRecord[][] },
      changes: ChangeRecord[]
    ) {
      this.savedPending.push(changes);
    }),
  };
}

// ---------------------------------------------------------------------------
// CRDTStore with StorageAdapter
// ---------------------------------------------------------------------------

describe("CRDTStore with StorageAdapter", () => {
  let storage: ReturnType<typeof createMockStorage>;
  let clock: HybridClock;

  beforeEach(() => {
    storage = createMockStorage();
    clock = new HybridClock("test-node", { nowFn: () => 1000 });
  });

  it("calls loadState on init", async () => {
    const store = new CRDTStore("test-node", clock, storage);
    await store.ready;
    expect(storage.loadState).toHaveBeenCalledTimes(1);
  });

  it("calls loadPendingChanges on init", async () => {
    const store = new CRDTStore("test-node", clock, storage);
    await store.ready;
    expect(storage.loadPendingChanges).toHaveBeenCalledTimes(1);
  });

  it("ready resolves even with default MemoryStorage", async () => {
    const store = new CRDTStore("test-node", clock);
    await expect(store.ready).resolves.toBeUndefined();
  });

  it("saveDocument called after setField", async () => {
    const store = new CRDTStore("test-node", clock, storage);
    await store.ready;
    store.setField("users", "1", "name", "Alice");

    // Allow microtask to complete for fire-and-forget persistence.
    await new Promise((r) => setTimeout(r, 10));
    expect(storage.saveDocument).toHaveBeenCalled();
    expect(storage.saved.length).toBeGreaterThanOrEqual(1);
    expect(storage.saved[0].table).toBe("users");
    expect(storage.saved[0].pk).toBe("1");
  });

  it("savePendingChanges called after setField", async () => {
    const store = new CRDTStore("test-node", clock, storage);
    await store.ready;
    store.setField("users", "1", "name", "Alice");

    await new Promise((r) => setTimeout(r, 10));
    expect(storage.savePendingChanges).toHaveBeenCalled();
    expect(storage.savedPending.length).toBeGreaterThanOrEqual(1);
    expect(storage.savedPending[0]).toHaveLength(1);
  });

  it("savePendingChanges called after incrementCounter", async () => {
    const store = new CRDTStore("test-node", clock, storage);
    await store.ready;
    store.incrementCounter("items", "1", "views", 1);

    await new Promise((r) => setTimeout(r, 10));
    expect(storage.savePendingChanges).toHaveBeenCalled();
  });

  it("savePendingChanges called after decrementCounter", async () => {
    const store = new CRDTStore("test-node", clock, storage);
    await store.ready;
    store.decrementCounter("items", "1", "stock", 1);

    await new Promise((r) => setTimeout(r, 10));
    expect(storage.savePendingChanges).toHaveBeenCalled();
  });

  it("saveDocument called after addToSet", async () => {
    const store = new CRDTStore("test-node", clock, storage);
    await store.ready;
    store.addToSet("items", "1", "tags", ["cool"]);

    await new Promise((r) => setTimeout(r, 10));
    expect(storage.saveDocument).toHaveBeenCalled();
  });

  it("saveDocument called after removeFromSet", async () => {
    const store = new CRDTStore("test-node", clock, storage);
    await store.ready;
    store.addToSet("items", "1", "tags", ["cool"]);
    store.removeFromSet("items", "1", "tags", ["cool"]);

    await new Promise((r) => setTimeout(r, 10));
    expect(storage.saveDocument).toHaveBeenCalledTimes(2);
  });

  it("saveDocument called after deleteDocument", async () => {
    const store = new CRDTStore("test-node", clock, storage);
    await store.ready;
    store.setField("users", "1", "name", "Alice");
    store.deleteDocument("users", "1");

    await new Promise((r) => setTimeout(r, 10));
    // saveDocument called for setField + deleteDocument.
    expect(storage.saveDocument).toHaveBeenCalledTimes(2);
  });

  it("saveDocument called after applyChanges", async () => {
    const store = new CRDTStore("test-node", clock, storage);
    await store.ready;

    store.applyChanges([
      {
        table: "users",
        pk: "1",
        field: "name",
        crdt_type: "lww",
        hlc: { ts: 500, c: 0, node: "remote" },
        node_id: "remote",
        value: "Bob",
      },
    ]);

    await new Promise((r) => setTimeout(r, 10));
    expect(storage.saveDocument).toHaveBeenCalled();
  });

  it("clearPendingChanges persists empty array", async () => {
    const store = new CRDTStore("test-node", clock, storage);
    await store.ready;
    store.setField("users", "1", "name", "Alice");
    store.clearPendingChanges();

    await new Promise((r) => setTimeout(r, 10));
    // The last savePendingChanges call should be for the empty array.
    const lastSaved = storage.savedPending[storage.savedPending.length - 1];
    expect(lastSaved).toEqual([]);
  });

  it("hydrates persisted state on init", async () => {
    const preloadedState = new Map<string, Map<string, DocumentState>>();
    const docs = new Map<string, DocumentState>();
    docs.set("1", {
      table: "users",
      pk: "1",
      fields: {
        name: {
          type: "lww",
          hlc: { ts: 100, c: 0, node: "other" },
          node_id: "other",
          value: "Persisted",
        },
      },
      tombstone: false,
    });
    preloadedState.set("users", docs);

    storage.loadState = vi.fn(async () => preloadedState);

    const store = new CRDTStore("test-node", clock, storage);
    await store.ready;

    const doc = store.getDocument<{ name: string }>("users", "1");
    expect(doc).not.toBeNull();
    expect(doc!.name).toBe("Persisted");
  });

  it("hydrates persisted pending changes on init", async () => {
    const pendingChanges: ChangeRecord[] = [
      {
        table: "users",
        pk: "2",
        field: "name",
        crdt_type: "lww",
        hlc: { ts: 200, c: 0, node: "test-node" },
        node_id: "test-node",
        value: "Pending",
      },
    ];

    storage.loadPendingChanges = vi.fn(async () => pendingChanges);

    const store = new CRDTStore("test-node", clock, storage);
    await store.ready;

    const pending = store.getPendingChanges();
    expect(pending).toHaveLength(1);
    expect(pending[0].value).toBe("Pending");
  });

  it("storage errors do not break synchronous operations", async () => {
    storage.saveDocument = vi.fn(async () => {
      throw new Error("Storage write failed");
    });
    storage.savePendingChanges = vi.fn(async () => {
      throw new Error("Storage write failed");
    });

    const store = new CRDTStore("test-node", clock, storage);
    await store.ready;

    // Should not throw — persistence is fire-and-forget.
    expect(() => store.setField("users", "1", "name", "Alice")).not.toThrow();
    expect(store.getDocument("users", "1")).not.toBeNull();
  });
});
