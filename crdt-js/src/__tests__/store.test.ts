import { describe, it, expect, vi } from "vitest";
import { CRDTStore } from "../store.js";
import { HybridClock } from "../hlc.js";
import { counterValue, setElements } from "../merge.js";
import type { ChangeRecord } from "../types.js";
import type { StorePlugin, WriteHook, MergeHook, ReadHook } from "../plugin.js";

function createTestStore(nodeID = "test-node") {
  let time = 1000;
  const clock = new HybridClock(nodeID, { nowFn: () => time++ });
  const store = new CRDTStore(nodeID, clock);
  return { store, clock };
}

// ---------------------------------------------------------------------------
// getDocument
// ---------------------------------------------------------------------------

describe("CRDTStore", () => {
  describe("getDocument", () => {
    it("returns null for non-existent document", () => {
      const { store } = createTestStore();
      expect(store.getDocument("users", "1")).toBeNull();
    });

    it("returns resolved document after setField", () => {
      const { store } = createTestStore();
      store.setField("users", "1", "name", "Alice");
      const doc = store.getDocument<{ name: string }>("users", "1");
      expect(doc).not.toBeNull();
      expect(doc!.name).toBe("Alice");
    });

    it("returns null for tombstoned document", () => {
      const { store } = createTestStore();
      store.setField("users", "1", "name", "Alice");
      store.deleteDocument("users", "1");
      expect(store.getDocument("users", "1")).toBeNull();
    });

    it("includes _table and _pk metadata fields", () => {
      const { store } = createTestStore();
      store.setField("users", "1", "name", "Alice");
      const doc = store.getDocument<Record<string, unknown>>("users", "1");
      expect(doc!._table).toBe("users");
      expect(doc!._pk).toBe("1");
    });
  });

  // ---------------------------------------------------------------------------
  // getCollection
  // ---------------------------------------------------------------------------

  describe("getCollection", () => {
    it("returns empty array for non-existent table", () => {
      const { store } = createTestStore();
      expect(store.getCollection("missing")).toEqual([]);
    });

    it("returns all non-tombstoned documents in table", () => {
      const { store } = createTestStore();
      store.setField("users", "1", "name", "Alice");
      store.setField("users", "2", "name", "Bob");
      store.setField("users", "3", "name", "Charlie");
      store.deleteDocument("users", "3");
      expect(store.getCollection("users")).toHaveLength(2);
    });

    it("excludes tombstoned documents", () => {
      const { store } = createTestStore();
      store.setField("users", "1", "name", "Alice");
      store.deleteDocument("users", "1");
      const col = store.getCollection<Record<string, unknown>>("users");
      expect(col.every((d) => d._pk !== "1")).toBe(true);
    });
  });

  // ---------------------------------------------------------------------------
  // setField
  // ---------------------------------------------------------------------------

  describe("setField", () => {
    it("creates a new document if it does not exist", () => {
      const { store } = createTestStore();
      store.setField("users", "1", "name", "Alice");
      expect(store.getDocument("users", "1")).not.toBeNull();
    });

    it("updates an existing field", () => {
      const { store } = createTestStore();
      store.setField("users", "1", "name", "Alice");
      store.setField("users", "1", "name", "Bob");
      const doc = store.getDocument<{ name: string }>("users", "1");
      expect(doc!.name).toBe("Bob");
    });

    it("returns a ChangeRecord with correct properties", () => {
      const { store } = createTestStore();
      const change = store.setField("users", "1", "name", "Alice")!;
      expect(change.table).toBe("users");
      expect(change.pk).toBe("1");
      expect(change.field).toBe("name");
      expect(change.crdt_type).toBe("lww");
      expect(change.node_id).toBe("test-node");
      expect(change.value).toBe("Alice");
      expect(change.hlc.ts).toBeGreaterThan(0);
    });

    it("adds the change to pending", () => {
      const { store } = createTestStore();
      expect(store.pendingCount).toBe(0);
      store.setField("users", "1", "name", "Alice");
      expect(store.pendingCount).toBe(1);
    });

    it("uses the clock to generate monotonically increasing HLCs", () => {
      const { store } = createTestStore();
      const c1 = store.setField("users", "1", "name", "a")!;
      const c2 = store.setField("users", "1", "name", "b")!;
      expect(c2.hlc.ts >= c1.hlc.ts).toBe(true);
      if (c2.hlc.ts === c1.hlc.ts) {
        expect(c2.hlc.c).toBeGreaterThan(c1.hlc.c);
      }
    });
  });

  // ---------------------------------------------------------------------------
  // incrementCounter
  // ---------------------------------------------------------------------------

  describe("incrementCounter", () => {
    it("creates counter field with default delta of 1", () => {
      const { store } = createTestStore();
      store.incrementCounter("users", "1", "views");
      const doc = store.getDocument<{ views: number }>("users", "1");
      expect(doc!.views).toBe(1);
    });

    it("accepts custom delta", () => {
      const { store } = createTestStore();
      store.incrementCounter("users", "1", "views", 5);
      const doc = store.getDocument<{ views: number }>("users", "1");
      expect(doc!.views).toBe(5);
    });

    it("accumulates multiple increments", () => {
      const { store } = createTestStore();
      store.incrementCounter("users", "1", "views");
      store.incrementCounter("users", "1", "views");
      store.incrementCounter("users", "1", "views");
      const doc = store.getDocument<{ views: number }>("users", "1");
      expect(doc!.views).toBe(3);
    });

    it("returns ChangeRecord with counter_delta", () => {
      const { store } = createTestStore();
      const change = store.incrementCounter("users", "1", "views", 5)!;
      expect(change.crdt_type).toBe("counter");
      expect(change.counter_delta).toEqual({ inc: 5, dec: 0 });
    });
  });

  // ---------------------------------------------------------------------------
  // decrementCounter
  // ---------------------------------------------------------------------------

  describe("decrementCounter", () => {
    it("creates counter field with negative effect", () => {
      const { store } = createTestStore();
      store.decrementCounter("users", "1", "views");
      const doc = store.getDocument<{ views: number }>("users", "1");
      expect(doc!.views).toBe(-1);
    });

    it("accepts custom delta", () => {
      const { store } = createTestStore();
      store.decrementCounter("users", "1", "views", 3);
      const doc = store.getDocument<{ views: number }>("users", "1");
      expect(doc!.views).toBe(-3);
    });

    it("works with mixed increment/decrement", () => {
      const { store } = createTestStore();
      store.incrementCounter("users", "1", "views", 5);
      store.decrementCounter("users", "1", "views", 2);
      const doc = store.getDocument<{ views: number }>("users", "1");
      expect(doc!.views).toBe(3);
    });

    it("returns ChangeRecord with counter_delta", () => {
      const { store } = createTestStore();
      const change = store.decrementCounter("users", "1", "views", 2)!;
      expect(change.counter_delta).toEqual({ inc: 0, dec: 2 });
    });
  });

  // ---------------------------------------------------------------------------
  // addToSet
  // ---------------------------------------------------------------------------

  describe("addToSet", () => {
    it("adds elements to a set field", () => {
      const { store } = createTestStore();
      store.addToSet("users", "1", "tags", ["a", "b"]);
      const doc = store.getDocument<{ tags: string[] }>("users", "1");
      expect(doc!.tags).toContain("a");
      expect(doc!.tags).toContain("b");
    });

    it("returns ChangeRecord with set_op add", () => {
      const { store } = createTestStore();
      const change = store.addToSet("users", "1", "tags", ["x"])!;
      expect(change.crdt_type).toBe("set");
      expect(change.set_op).toEqual({ op: "add", elements: ["x"] });
    });

    it("handles adding to existing set", () => {
      const { store } = createTestStore();
      store.addToSet("users", "1", "tags", ["a"]);
      store.addToSet("users", "1", "tags", ["b"]);
      const doc = store.getDocument<{ tags: string[] }>("users", "1");
      expect(doc!.tags).toContain("a");
      expect(doc!.tags).toContain("b");
    });
  });

  // ---------------------------------------------------------------------------
  // removeFromSet
  // ---------------------------------------------------------------------------

  describe("removeFromSet", () => {
    it("removes elements from a set field", () => {
      const { store } = createTestStore();
      // Add elements in separate operations so they get unique HLC tags
      store.addToSet("users", "1", "tags", ["a"]);
      store.addToSet("users", "1", "tags", ["b"]);
      store.removeFromSet("users", "1", "tags", ["a"]);
      const doc = store.getDocument<{ tags: string[] }>("users", "1");
      expect(doc!.tags).not.toContain("a");
      expect(doc!.tags).toContain("b");
    });

    it("returns ChangeRecord with set_op remove", () => {
      const { store } = createTestStore();
      const change = store.removeFromSet("users", "1", "tags", ["x"])!;
      expect(change.set_op).toEqual({ op: "remove", elements: ["x"] });
    });

    it("is a no-op when element was never added", () => {
      const { store } = createTestStore();
      store.addToSet("users", "1", "tags", ["a"]);
      store.removeFromSet("users", "1", "tags", ["z"]);
      const doc = store.getDocument<{ tags: string[] }>("users", "1");
      expect(doc!.tags).toContain("a");
    });
  });

  // ---------------------------------------------------------------------------
  // deleteDocument
  // ---------------------------------------------------------------------------

  describe("deleteDocument", () => {
    it("tombstones the document", () => {
      const { store } = createTestStore();
      store.setField("users", "1", "name", "Alice");
      store.deleteDocument("users", "1");
      expect(store.getDocument("users", "1")).toBeNull();
    });

    it("returns ChangeRecord with tombstone flag", () => {
      const { store } = createTestStore();
      const change = store.deleteDocument("users", "1");
      expect(change.tombstone).toBe(true);
    });

    it("adds the change to pending", () => {
      const { store } = createTestStore();
      const before = store.pendingCount;
      store.deleteDocument("users", "1");
      expect(store.pendingCount).toBe(before + 1);
    });

    it("uses empty field name", () => {
      const { store } = createTestStore();
      const change = store.deleteDocument("users", "1");
      expect(change.field).toBe("");
    });
  });

  // ---------------------------------------------------------------------------
  // applyChanges
  // ---------------------------------------------------------------------------

  describe("applyChanges", () => {
    it("applies a batch of remote changes", () => {
      const { store } = createTestStore();
      const changes: ChangeRecord[] = [
        {
          table: "users",
          pk: "1",
          field: "name",
          crdt_type: "lww",
          hlc: { ts: 500_000_000_000, c: 0, node: "remote" },
          node_id: "remote",
          value: "Alice",
        },
        {
          table: "users",
          pk: "2",
          field: "name",
          crdt_type: "lww",
          hlc: { ts: 500_000_000_000, c: 1, node: "remote" },
          node_id: "remote",
          value: "Bob",
        },
      ];
      store.applyChanges(changes);
      expect(store.getDocument<{ name: string }>("users", "1")!.name).toBe("Alice");
      expect(store.getDocument<{ name: string }>("users", "2")!.name).toBe("Bob");
    });

    it("merges LWW fields correctly (remote wins)", () => {
      const { store } = createTestStore();
      store.setField("users", "1", "name", "Local");

      const change: ChangeRecord = {
        table: "users",
        pk: "1",
        field: "name",
        crdt_type: "lww",
        hlc: { ts: 999_000_000_000, c: 0, node: "remote" },
        node_id: "remote",
        value: "Remote",
      };
      store.applyChanges([change]);
      const doc = store.getDocument<{ name: string }>("users", "1");
      expect(doc!.name).toBe("Remote");
    });

    it("merges counter fields correctly", () => {
      const { store } = createTestStore();
      store.incrementCounter("users", "1", "views", 3);

      const change: ChangeRecord = {
        table: "users",
        pk: "1",
        field: "views",
        crdt_type: "counter",
        hlc: { ts: 999_000_000_000, c: 0, node: "remote" },
        node_id: "remote",
        counter_delta: { inc: 5, dec: 0 },
      };
      store.applyChanges([change]);
      const doc = store.getDocument<{ views: number }>("users", "1");
      expect(doc!.views).toBe(8); // 3 + 5
    });

    it("merges set fields correctly", () => {
      const { store } = createTestStore();
      store.addToSet("users", "1", "tags", ["local"]);

      const change: ChangeRecord = {
        table: "users",
        pk: "1",
        field: "tags",
        crdt_type: "set",
        hlc: { ts: 999_000_000_000, c: 0, node: "remote" },
        node_id: "remote",
        set_op: { op: "add", elements: ["remote"] },
      };
      store.applyChanges([change]);
      const doc = store.getDocument<{ tags: string[] }>("users", "1");
      expect(doc!.tags).toContain("local");
      expect(doc!.tags).toContain("remote");
    });

    it("handles tombstone changes", () => {
      const { store } = createTestStore();
      store.setField("users", "1", "name", "Alice");

      const change: ChangeRecord = {
        table: "users",
        pk: "1",
        field: "",
        crdt_type: "lww",
        hlc: { ts: 999_000_000_000, c: 0, node: "remote" },
        node_id: "remote",
        tombstone: true,
      };
      store.applyChanges([change]);
      expect(store.getDocument("users", "1")).toBeNull();
    });

    it("batch-notifies listeners once per affected document", () => {
      const { store } = createTestStore();
      const listener = vi.fn();
      store.subscribeDocument("users", "1", listener);

      const changes: ChangeRecord[] = [
        {
          table: "users",
          pk: "1",
          field: "name",
          crdt_type: "lww",
          hlc: { ts: 500_000_000_000, c: 0, node: "r" },
          node_id: "r",
          value: "A",
        },
        {
          table: "users",
          pk: "1",
          field: "email",
          crdt_type: "lww",
          hlc: { ts: 500_000_000_000, c: 1, node: "r" },
          node_id: "r",
          value: "a@b.c",
        },
      ];
      store.applyChanges(changes);
      // Both changes are to the same doc, so batch-notify fires once
      expect(listener).toHaveBeenCalledTimes(1);
    });
  });

  // ---------------------------------------------------------------------------
  // getPendingChanges / clearPendingChanges / pendingCount
  // ---------------------------------------------------------------------------

  describe("getPendingChanges / clearPendingChanges / pendingCount", () => {
    it("returns empty array initially", () => {
      const { store } = createTestStore();
      expect(store.getPendingChanges()).toEqual([]);
    });

    it("accumulates changes from local mutations", () => {
      const { store } = createTestStore();
      store.setField("users", "1", "a", 1);
      store.setField("users", "1", "b", 2);
      store.incrementCounter("users", "1", "c");
      expect(store.pendingCount).toBe(3);
      expect(store.getPendingChanges()).toHaveLength(3);
    });

    it("returns a copy (not reference) of pending", () => {
      const { store } = createTestStore();
      store.setField("users", "1", "a", 1);
      const p1 = store.getPendingChanges();
      const p2 = store.getPendingChanges();
      expect(p1).not.toBe(p2);
      expect(p1).toEqual(p2);
    });

    it("clearPendingChanges resets to empty", () => {
      const { store } = createTestStore();
      store.setField("users", "1", "a", 1);
      store.clearPendingChanges();
      expect(store.pendingCount).toBe(0);
      expect(store.getPendingChanges()).toEqual([]);
    });
  });

  // ---------------------------------------------------------------------------
  // subscribe (global)
  // ---------------------------------------------------------------------------

  describe("subscribe (global)", () => {
    it("notifies listener on any state change", () => {
      const { store } = createTestStore();
      const listener = vi.fn();
      store.subscribe(listener);
      store.setField("users", "1", "name", "Alice");
      expect(listener).toHaveBeenCalled();
    });

    it("returns an unsubscribe function", () => {
      const { store } = createTestStore();
      const listener = vi.fn();
      const unsub = store.subscribe(listener);
      unsub();
      store.setField("users", "1", "name", "Alice");
      expect(listener).not.toHaveBeenCalled();
    });

    it("supports multiple listeners", () => {
      const { store } = createTestStore();
      const listener1 = vi.fn();
      const listener2 = vi.fn();
      store.subscribe(listener1);
      store.subscribe(listener2);
      store.setField("users", "1", "name", "Alice");
      expect(listener1).toHaveBeenCalled();
      expect(listener2).toHaveBeenCalled();
    });
  });

  // ---------------------------------------------------------------------------
  // subscribeDocument
  // ---------------------------------------------------------------------------

  describe("subscribeDocument", () => {
    it("notifies listener only for matching table and pk", () => {
      const { store } = createTestStore();
      const listener = vi.fn();
      store.subscribeDocument("users", "1", listener);

      store.setField("users", "1", "name", "Alice");
      expect(listener).toHaveBeenCalledTimes(1);

      store.setField("users", "2", "name", "Bob");
      expect(listener).toHaveBeenCalledTimes(1); // not called again
    });

    it("returns an unsubscribe function", () => {
      const { store } = createTestStore();
      const listener = vi.fn();
      const unsub = store.subscribeDocument("users", "1", listener);
      unsub();
      store.setField("users", "1", "name", "Alice");
      expect(listener).not.toHaveBeenCalled();
    });

    it("cleans up internal map when last listener unsubscribes", () => {
      const { store } = createTestStore();
      const listener = vi.fn();
      const unsub = store.subscribeDocument("users", "1", listener);
      unsub();
      // After unsub, internal docListeners map entry should be removed.
      // We verify indirectly: subscribing again and mutating should still work.
      const listener2 = vi.fn();
      store.subscribeDocument("users", "1", listener2);
      store.setField("users", "1", "name", "Alice");
      expect(listener2).toHaveBeenCalledTimes(1);
    });
  });

  // ---------------------------------------------------------------------------
  // subscribeCollection
  // ---------------------------------------------------------------------------

  describe("subscribeCollection", () => {
    it("notifies listener for any change in the table", () => {
      const { store } = createTestStore();
      const listener = vi.fn();
      store.subscribeCollection("users", listener);
      store.setField("users", "1", "name", "Alice");
      store.setField("users", "2", "name", "Bob");
      expect(listener).toHaveBeenCalledTimes(2);
    });

    it("does not notify for changes in other tables", () => {
      const { store } = createTestStore();
      const listener = vi.fn();
      store.subscribeCollection("users", listener);
      store.setField("posts", "1", "title", "Hi");
      expect(listener).not.toHaveBeenCalled();
    });

    it("returns an unsubscribe function", () => {
      const { store } = createTestStore();
      const listener = vi.fn();
      const unsub = store.subscribeCollection("users", listener);
      unsub();
      store.setField("users", "1", "name", "Alice");
      expect(listener).not.toHaveBeenCalled();
    });

    it("cleans up internal map when last listener unsubscribes", () => {
      const { store } = createTestStore();
      const listener = vi.fn();
      const unsub = store.subscribeCollection("users", listener);
      unsub();
      const listener2 = vi.fn();
      store.subscribeCollection("users", listener2);
      store.setField("users", "1", "name", "Alice");
      expect(listener2).toHaveBeenCalledTimes(1);
    });
  });

  // ---------------------------------------------------------------------------
  // resolveDocument (via getDocument)
  // ---------------------------------------------------------------------------

  describe("resolveDocument (via getDocument)", () => {
    it("resolves LWW fields to their value", () => {
      const { store } = createTestStore();
      store.setField("t", "1", "name", "Alice");
      const doc = store.getDocument<Record<string, unknown>>("t", "1");
      expect(doc!.name).toBe("Alice");
    });

    it("resolves counter fields to their numeric value", () => {
      const { store } = createTestStore();
      store.incrementCounter("t", "1", "views", 7);
      const doc = store.getDocument<Record<string, unknown>>("t", "1");
      expect(doc!.views).toBe(7);
    });

    it("resolves set fields to their element array", () => {
      const { store } = createTestStore();
      store.addToSet("t", "1", "tags", ["a", "b"]);
      const doc = store.getDocument<Record<string, unknown>>("t", "1");
      expect(Array.isArray(doc!.tags)).toBe(true);
      expect(doc!.tags).toContain("a");
      expect(doc!.tags).toContain("b");
    });

    it("falls through to value for unknown field types", () => {
      const { store } = createTestStore();
      // Apply a change with unknown type via applyChanges
      const change: ChangeRecord = {
        table: "t",
        pk: "1",
        field: "custom",
        crdt_type: "unknown" as any,
        hlc: { ts: 999_000_000_000, c: 0, node: "r" },
        node_id: "r",
        value: "custom-val",
      };
      store.applyChanges([change]);
      const doc = store.getDocument<Record<string, unknown>>("t", "1");
      expect(doc!.custom).toBe("custom-val");
    });
  });

  // ---------------------------------------------------------------------------
  // Plugins (via store API)
  // ---------------------------------------------------------------------------

  describe("CRDTStore - Plugins", () => {
    it("registers plugins via use()", () => {
      const { store } = createTestStore();
      const init = vi.fn();
      store.use({ name: "test-plugin", init });
      expect(init).toHaveBeenCalledTimes(1);
    });

    it("removes plugins via removePlugin()", () => {
      const { store } = createTestStore();
      const destroy = vi.fn();
      store.use({ name: "test-plugin", destroy });
      store.removePlugin("test-plugin");
      expect(destroy).toHaveBeenCalledTimes(1);
    });

    it("gets plugins via getPlugin()", () => {
      const { store } = createTestStore();
      const plugin = { name: "my-plugin" };
      store.use(plugin);
      expect(store.getPlugin("my-plugin")).toBe(plugin);
      expect(store.getPlugin("non-existent")).toBeUndefined();
    });

    it("runs WriteHook.beforeWrite on setField", () => {
      const { store } = createTestStore();
      const beforeWrite = vi.fn((ev: any) => ev);
      store.use({ name: "w", beforeWrite } as StorePlugin & WriteHook);

      store.setField("users", "1", "name", "Alice");
      expect(beforeWrite).toHaveBeenCalledTimes(1);
      expect(beforeWrite.mock.calls[0][0].table).toBe("users");
      expect(beforeWrite.mock.calls[0][0].field).toBe("name");
    });

    it("rejects writes when plugin returns null", () => {
      const { store } = createTestStore();
      store.use({
        name: "blocker",
        beforeWrite() {
          return null;
        },
      } as StorePlugin & WriteHook);

      const result = store.setField("users", "1", "name", "Alice");
      expect(result).toBeNull();
      expect(store.getDocument("users", "1")).toBeNull();
      expect(store.pendingCount).toBe(0);
    });

    it("runs MergeHook.beforeMerge on applyChanges", () => {
      const { store } = createTestStore();
      const beforeMerge = vi.fn((ev: any) => ev.remote);
      store.use({ name: "m", beforeMerge } as StorePlugin & MergeHook);

      const change: ChangeRecord = {
        table: "users",
        pk: "1",
        field: "name",
        crdt_type: "lww",
        hlc: { ts: 500_000_000_000, c: 0, node: "remote" },
        node_id: "remote",
        value: "Alice",
      };
      store.applyChanges([change]);
      expect(beforeMerge).toHaveBeenCalledTimes(1);
    });

    it("runs ReadHook.transformDocument on getDocument", () => {
      const { store } = createTestStore();
      store.use({
        name: "read",
        transformDocument<T>(_table: string, _pk: string, doc: T): T {
          return { ...(doc as object), computed: true } as T;
        },
      } as StorePlugin & ReadHook);

      store.setField("users", "1", "name", "Alice");
      const doc = store.getDocument<Record<string, unknown>>("users", "1");
      expect(doc!.computed).toBe(true);
      expect(doc!.name).toBe("Alice");
    });

    it("runs ReadHook.transformCollection on getCollection", () => {
      const { store } = createTestStore();
      store.use({
        name: "read",
        transformCollection<T>(_table: string, docs: T[]): T[] {
          return docs.slice(0, 1); // Only return first doc
        },
      } as StorePlugin & ReadHook);

      store.setField("users", "1", "name", "Alice");
      store.setField("users", "2", "name", "Bob");
      const col = store.getCollection("users");
      expect(col).toHaveLength(1);
    });
  });

  // ---------------------------------------------------------------------------
  // Batch Writes
  // ---------------------------------------------------------------------------

  describe("CRDTStore - Batch Writes", () => {
    it("creates a batch writer via batch()", () => {
      const { store } = createTestStore();
      const batch = store.batch("users", "1");
      expect(batch).toBeDefined();
      expect(typeof batch.setField).toBe("function");
      expect(typeof batch.commit).toBe("function");
    });

    it("commits multiple field changes atomically", () => {
      const { store } = createTestStore();
      const changes = store.batch("users", "1")
        .setField("name", "Alice")
        .setField("email", "alice@example.com")
        .commit();

      expect(changes).toHaveLength(2);
      const doc = store.getDocument<{ name: string; email: string }>("users", "1");
      expect(doc!.name).toBe("Alice");
      expect(doc!.email).toBe("alice@example.com");
    });

    it("batch setField + incrementCounter together", () => {
      const { store } = createTestStore();
      store.batch("users", "1")
        .setField("name", "Alice")
        .incrementCounter("views", 5)
        .commit();

      const doc = store.getDocument<{ name: string; views: number }>("users", "1");
      expect(doc!.name).toBe("Alice");
      expect(doc!.views).toBe(5);
    });

    it("batch changes appear in pending", () => {
      const { store } = createTestStore();
      expect(store.pendingCount).toBe(0);

      store.batch("users", "1")
        .setField("name", "Alice")
        .setField("email", "alice@example.com")
        .commit();

      expect(store.pendingCount).toBe(2);
    });

    it("batch notifies listeners once", () => {
      const { store } = createTestStore();
      const listener = vi.fn();
      store.subscribeDocument("users", "1", listener);

      store.batch("users", "1")
        .setField("name", "Alice")
        .setField("email", "alice@example.com")
        .commit();

      // commitBatch calls notifyListeners once for the whole batch.
      expect(listener).toHaveBeenCalledTimes(1);
    });
  });

  // ---------------------------------------------------------------------------
  // State Export/Import
  // ---------------------------------------------------------------------------

  describe("CRDTStore - State Export/Import", () => {
    it("exports state as snapshot", () => {
      const { store } = createTestStore();
      store.setField("users", "1", "name", "Alice");

      const snapshot = store.exportState();
      expect(snapshot.version).toBe(1);
      expect(snapshot.nodeId).toBe("test-node");
      expect(snapshot.timestamp).toBeGreaterThan(0);
      expect(snapshot.tables["users"]).toBeDefined();
      expect(snapshot.tables["users"]["1"]).toBeDefined();
    });

    it("imports state from snapshot", () => {
      const { store: store1 } = createTestStore("node-1");
      store1.setField("users", "1", "name", "Alice");
      const snapshot = store1.exportState();

      const { store: store2 } = createTestStore("node-2");
      store2.importState(snapshot);

      const doc = store2.getDocument<{ name: string }>("users", "1");
      expect(doc!.name).toBe("Alice");
    });

    it("import replaces existing state", () => {
      const { store } = createTestStore();
      store.setField("users", "1", "name", "Alice");
      store.setField("posts", "1", "title", "Hello");

      // Import a snapshot with only users table.
      const snapshot = store.exportState();
      // Remove posts from snapshot.
      delete snapshot.tables["posts"];

      store.importState(snapshot);

      expect(store.getDocument("users", "1")).not.toBeNull();
      expect(store.getCollection("posts")).toEqual([]);
    });

    it("export includes pending changes", () => {
      const { store } = createTestStore();
      store.setField("users", "1", "name", "Alice");
      store.setField("users", "1", "email", "alice@example.com");

      const snapshot = store.exportState();
      expect(snapshot.pending).toHaveLength(2);
      expect(snapshot.pending[0].field).toBe("name");
      expect(snapshot.pending[1].field).toBe("email");
    });

    it("exportTable returns single table", () => {
      const { store } = createTestStore();
      store.setField("users", "1", "name", "Alice");
      store.setField("posts", "1", "title", "Hello");

      const usersTable = store.exportTable("users");
      expect(Object.keys(usersTable)).toEqual(["1"]);
      expect(usersTable["1"].table).toBe("users");

      const emptyTable = store.exportTable("nonexistent");
      expect(Object.keys(emptyTable)).toHaveLength(0);
    });
  });
});
