import { describe, it, expect, vi } from "vitest";
import { PluginManager } from "../plugin.js";
import type {
  StorePlugin,
  WriteHook,
  MergeHook,
  ReadHook,
  SyncHook,
  PresenceHook,
  StorageHook,
  WriteEvent,
  MergeEvent,
  PullEvent,
} from "../plugin.js";
import type { ChangeRecord, FieldState, DocumentState } from "../types.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeChange(overrides?: Partial<ChangeRecord>): ChangeRecord {
  return {
    table: "t",
    pk: "1",
    field: "f",
    crdt_type: "lww",
    hlc: { ts: 100, c: 0, node: "n" },
    node_id: "n",
    value: "v",
    ...overrides,
  };
}

function makeWriteEvent(overrides?: Partial<WriteEvent>): WriteEvent {
  return {
    table: "t",
    pk: "1",
    field: "f",
    crdtType: "lww",
    value: "v",
    change: makeChange(),
    previousState: null,
    ...overrides,
  };
}

function makeMergeEvent(overrides?: Partial<MergeEvent>): MergeEvent {
  return {
    table: "t",
    pk: "1",
    field: "f",
    local: null,
    remote: makeChange(),
    conflictDetected: false,
    ...overrides,
  };
}

function makeDocState(): DocumentState {
  return {
    table: "t",
    pk: "1",
    fields: {},
    tombstone: false,
  };
}

// ---------------------------------------------------------------------------
// PluginManager
// ---------------------------------------------------------------------------

describe("PluginManager", () => {
  it("registers and retrieves plugins by name", () => {
    const pm = new PluginManager();
    const plugin: StorePlugin = { name: "test" };
    pm.use(plugin);
    expect(pm.get("test")).toBe(plugin);
  });

  it("removes plugins by name and calls destroy", () => {
    const pm = new PluginManager();
    const destroy = vi.fn();
    const plugin: StorePlugin = { name: "test", destroy };
    pm.use(plugin);

    pm.remove("test");

    expect(destroy).toHaveBeenCalledTimes(1);
    expect(pm.get("test")).toBeUndefined();
  });

  it("calls init on registration", () => {
    const pm = new PluginManager();
    const init = vi.fn();
    const plugin: StorePlugin = { name: "test", init };
    pm.use(plugin);

    expect(init).toHaveBeenCalledTimes(1);
  });

  // ---------------------------------------------------------------------------
  // WriteHook
  // ---------------------------------------------------------------------------

  describe("WriteHook", () => {
    it("calls beforeWrite on registered plugins", () => {
      const pm = new PluginManager();
      const beforeWrite = vi.fn((ev: WriteEvent) => ev);
      const plugin: StorePlugin & WriteHook = { name: "w", beforeWrite };
      pm.use(plugin);

      const event = makeWriteEvent();
      pm.dispatchBeforeWrite(event);

      expect(beforeWrite).toHaveBeenCalledTimes(1);
      expect(beforeWrite).toHaveBeenCalledWith(event);
    });

    it("allows plugins to transform write events", () => {
      const pm = new PluginManager();
      const plugin: StorePlugin & WriteHook = {
        name: "w",
        beforeWrite(ev) {
          return { ...ev, value: "transformed" };
        },
      };
      pm.use(plugin);

      const result = pm.dispatchBeforeWrite(makeWriteEvent());
      expect(result).not.toBeNull();
      expect(result!.value).toBe("transformed");
    });

    it("allows plugins to reject writes by returning null", () => {
      const pm = new PluginManager();
      const plugin: StorePlugin & WriteHook = {
        name: "w",
        beforeWrite() {
          return null;
        },
      };
      pm.use(plugin);

      const result = pm.dispatchBeforeWrite(makeWriteEvent());
      expect(result).toBeNull();
    });

    it("calls afterWrite after successful writes", () => {
      const pm = new PluginManager();
      const afterWrite = vi.fn();
      const plugin: StorePlugin & WriteHook = { name: "w", afterWrite };
      pm.use(plugin);

      const event = makeWriteEvent();
      pm.dispatchAfterWrite(event);

      expect(afterWrite).toHaveBeenCalledTimes(1);
      expect(afterWrite).toHaveBeenCalledWith(event);
    });

    it("chains multiple write hooks in order", () => {
      const pm = new PluginManager();
      const order: number[] = [];

      const plugin1: StorePlugin & WriteHook = {
        name: "w1",
        beforeWrite(ev) {
          order.push(1);
          return { ...ev, value: "from-1" };
        },
      };
      const plugin2: StorePlugin & WriteHook = {
        name: "w2",
        beforeWrite(ev) {
          order.push(2);
          return { ...ev, value: "from-2" };
        },
      };
      pm.use(plugin1);
      pm.use(plugin2);

      const result = pm.dispatchBeforeWrite(makeWriteEvent());

      expect(order).toEqual([1, 2]);
      expect(result!.value).toBe("from-2");
    });
  });

  // ---------------------------------------------------------------------------
  // MergeHook
  // ---------------------------------------------------------------------------

  describe("MergeHook", () => {
    it("calls beforeMerge on incoming changes", () => {
      const pm = new PluginManager();
      const beforeMerge = vi.fn((ev: MergeEvent) => ev.remote);
      const plugin: StorePlugin & MergeHook = { name: "m", beforeMerge };
      pm.use(plugin);

      const event = makeMergeEvent();
      pm.dispatchBeforeMerge(event);

      expect(beforeMerge).toHaveBeenCalledTimes(1);
    });

    it("allows plugins to reject changes by returning null", () => {
      const pm = new PluginManager();
      const plugin: StorePlugin & MergeHook = {
        name: "m",
        beforeMerge() {
          return null;
        },
      };
      pm.use(plugin);

      const result = pm.dispatchBeforeMerge(makeMergeEvent());
      expect(result).toBeNull();
    });

    it("calls afterMerge with conflict info", () => {
      const pm = new PluginManager();
      const afterMerge = vi.fn();
      const plugin: StorePlugin & MergeHook = { name: "m", afterMerge };
      pm.use(plugin);

      const event = makeMergeEvent({ conflictDetected: true });
      pm.dispatchAfterMerge(event);

      expect(afterMerge).toHaveBeenCalledTimes(1);
      expect(afterMerge.mock.calls[0][0].conflictDetected).toBe(true);
    });

    it("sets conflictDetected when local state exists", () => {
      const pm = new PluginManager();
      const afterMerge = vi.fn();
      const plugin: StorePlugin & MergeHook = { name: "m", afterMerge };
      pm.use(plugin);

      const localFS: FieldState = {
        type: "lww",
        hlc: { ts: 50, c: 0, node: "a" },
        node_id: "a",
        value: "old",
      };
      const event = makeMergeEvent({ local: localFS, conflictDetected: true });
      pm.dispatchAfterMerge(event);

      expect(afterMerge.mock.calls[0][0].conflictDetected).toBe(true);
      expect(afterMerge.mock.calls[0][0].local).toBe(localFS);
    });
  });

  // ---------------------------------------------------------------------------
  // ReadHook
  // ---------------------------------------------------------------------------

  describe("ReadHook", () => {
    it("transforms documents via transformDocument", () => {
      const pm = new PluginManager();
      const plugin: StorePlugin & ReadHook = {
        name: "r",
        transformDocument<T>(_table: string, _pk: string, doc: T): T {
          return { ...(doc as object), extra: true } as T;
        },
      };
      pm.use(plugin);

      const result = pm.dispatchTransformDocument("t", "1", { name: "Alice" });
      expect(result).toEqual({ name: "Alice", extra: true });
    });

    it("filters documents returning null", () => {
      const pm = new PluginManager();
      const plugin: StorePlugin & ReadHook = {
        name: "r",
        transformDocument() {
          return null;
        },
      };
      pm.use(plugin);

      const result = pm.dispatchTransformDocument("t", "1", { name: "Alice" });
      expect(result).toBeNull();
    });

    it("transforms collections via transformCollection", () => {
      const pm = new PluginManager();
      const plugin: StorePlugin & ReadHook = {
        name: "r",
        transformCollection<T>(_table: string, docs: T[]): T[] {
          return docs.slice(0, 1);
        },
      };
      pm.use(plugin);

      const docs = [{ id: 1 }, { id: 2 }, { id: 3 }];
      const result = pm.dispatchTransformCollection("t", docs);
      expect(result).toHaveLength(1);
      expect(result[0]).toEqual({ id: 1 });
    });
  });

  // ---------------------------------------------------------------------------
  // SyncHook
  // ---------------------------------------------------------------------------

  describe("SyncHook", () => {
    it("calls beforePull and afterPull", () => {
      const pm = new PluginManager();
      const beforePull = vi.fn((ev: PullEvent) => ev);
      const afterPull = vi.fn();
      const plugin: StorePlugin & SyncHook = { name: "s", beforePull, afterPull };
      pm.use(plugin);

      const pullEvent: PullEvent = { tables: ["users"] };
      pm.dispatchBeforePull(pullEvent);
      expect(beforePull).toHaveBeenCalledTimes(1);

      pm.dispatchAfterPull({ ...pullEvent, changes: [] });
      expect(afterPull).toHaveBeenCalledTimes(1);
    });

    it("calls beforePush and afterPush", () => {
      const pm = new PluginManager();
      const beforePush = vi.fn((changes: ChangeRecord[]) => changes);
      const afterPush = vi.fn();
      const plugin: StorePlugin & SyncHook = { name: "s", beforePush, afterPush };
      pm.use(plugin);

      const changes = [makeChange()];
      pm.dispatchBeforePush(changes);
      expect(beforePush).toHaveBeenCalledTimes(1);

      pm.dispatchAfterPush({ pushed: 1, changes });
      expect(afterPush).toHaveBeenCalledTimes(1);
    });

    it("allows plugins to cancel pull by returning null", () => {
      const pm = new PluginManager();
      const plugin: StorePlugin & SyncHook = {
        name: "s",
        beforePull() {
          return null;
        },
      };
      pm.use(plugin);

      const result = pm.dispatchBeforePull({ tables: ["users"] });
      expect(result).toBeNull();
    });

    it("allows plugins to filter push changes", () => {
      const pm = new PluginManager();
      const plugin: StorePlugin & SyncHook = {
        name: "s",
        beforePush(changes) {
          return changes.filter((c) => c.table !== "secret");
        },
      };
      pm.use(plugin);

      const changes = [makeChange({ table: "users" }), makeChange({ table: "secret" })];
      const result = pm.dispatchBeforePush(changes);
      expect(result).toHaveLength(1);
      expect(result![0].table).toBe("users");
    });
  });

  // ---------------------------------------------------------------------------
  // PresenceHook
  // ---------------------------------------------------------------------------

  describe("PresenceHook", () => {
    it("calls beforePresenceUpdate", () => {
      const pm = new PluginManager();
      const beforePresenceUpdate = vi.fn((_topic: string, data: unknown) => data);
      const plugin: StorePlugin & PresenceHook = { name: "p", beforePresenceUpdate };
      pm.use(plugin);

      pm.dispatchBeforePresenceUpdate("room", { cursor: { x: 10, y: 20 } });
      expect(beforePresenceUpdate).toHaveBeenCalledTimes(1);
      expect(beforePresenceUpdate).toHaveBeenCalledWith("room", { cursor: { x: 10, y: 20 } });
    });

    it("calls onPresenceEvent", () => {
      const pm = new PluginManager();
      const onPresenceEvent = vi.fn();
      const plugin: StorePlugin & PresenceHook = { name: "p", onPresenceEvent };
      pm.use(plugin);

      const event = { type: "join" as const, nodeId: "n1", topic: "room" };
      pm.dispatchOnPresenceEvent(event);
      expect(onPresenceEvent).toHaveBeenCalledTimes(1);
      expect(onPresenceEvent).toHaveBeenCalledWith(event);
    });

    it("allows plugins to reject presence updates", () => {
      const pm = new PluginManager();
      const plugin: StorePlugin & PresenceHook = {
        name: "p",
        beforePresenceUpdate() {
          return null;
        },
      };
      pm.use(plugin);

      const result = pm.dispatchBeforePresenceUpdate("room", { cursor: { x: 0, y: 0 } });
      expect(result).toBeNull();
    });
  });

  // ---------------------------------------------------------------------------
  // StorageHook
  // ---------------------------------------------------------------------------

  describe("StorageHook", () => {
    it("transforms documents before persist", () => {
      const pm = new PluginManager();
      const plugin: StorePlugin & StorageHook = {
        name: "st",
        beforePersist(_table, _pk, doc) {
          return { ...doc, tombstone: true }; // e.g., encrypt
        },
      };
      pm.use(plugin);

      const doc = makeDocState();
      const result = pm.dispatchBeforePersist("t", "1", doc);
      expect(result.tombstone).toBe(true);
    });

    it("transforms documents after hydrate", () => {
      const pm = new PluginManager();
      const plugin: StorePlugin & StorageHook = {
        name: "st",
        afterHydrate(_table, _pk, doc) {
          return {
            ...doc,
            fields: { ...doc.fields, injected: { type: "lww" as const, hlc: { ts: 1, c: 0, node: "n" }, node_id: "n", value: "hydrated" } },
          };
        },
      };
      pm.use(plugin);

      const doc = makeDocState();
      const result = pm.dispatchAfterHydrate("t", "1", doc);
      expect(result.fields["injected"]).toBeDefined();
      expect(result.fields["injected"].value).toBe("hydrated");
    });
  });
});
