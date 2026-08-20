import { describe, it, expect } from "vitest";
import { SyncEngine } from "../sync.js";
import { CRDTClient } from "../client.js";
import { CRDTStore } from "../store.js";
import type { Transport, PushRequest, ChangeRecord } from "../types.js";
import type { StorePlugin, SyncHook } from "../plugin.js";

const HLC0 = { ts: "0", c: 0, node: "" };

function mk(onPush?: (req: PushRequest) => void) {
  const transport: Transport = {
    async pull() { return { changes: [], latest_hlc: HLC0 }; },
    async push(req) {
      onPush?.(req);
      return { merged: req.changes.length, latest_hlc: HLC0 };
    },
  };
  const client = new CRDTClient({ nodeID: "n1", transport });
  const store = new CRDTStore("n1", client.clock, undefined, { persistDebounceMs: 0 });
  return { client, store, engine: new SyncEngine(client, store) };
}

describe("SyncEngine", () => {
  it("pushes pending changes and clears them", async () => {
    const { store, engine } = mk();
    store.setField("t", "p", "f", 1);
    const report = await engine.sync();
    expect(report.pushed).toBe(1);
    expect(store.pendingCount).toBe(0);
  });

  it("does not lose writes made while a push is in flight (D11)", async () => {
    let release!: () => void;
    const gate = new Promise<void>((r) => { release = r; });
    const transport: Transport = {
      async pull() { return { changes: [], latest_hlc: HLC0 }; },
      async push(req) {
        await gate;
        return { merged: req.changes.length, latest_hlc: HLC0 };
      },
    };
    const client = new CRDTClient({ nodeID: "n1", transport });
    const store = new CRDTStore("n1", client.clock, undefined, { persistDebounceMs: 0 });
    const engine = new SyncEngine(client, store);

    store.setField("t", "p", "a", 1);
    const inFlight = engine.sync();
    // This write lands while the push is still awaiting the gate.
    store.setField("t", "p", "b", 2);
    release();
    await inFlight;

    expect(store.pendingCount).toBe(1);
    expect(store.getPendingChanges()[0].field).toBe("b");
  });

  it("runs beforePush and afterPush hooks (D5)", async () => {
    const { store, engine } = mk();
    const calls: string[] = [];
    const plugin: StorePlugin & SyncHook = {
      name: "sync-spy",
      beforePush(changes: ChangeRecord[]) { calls.push(`before:${changes.length}`); return changes; },
      afterPush(ev: { pushed: number }) { calls.push(`after:${ev.pushed}`); },
    };
    store.use(plugin);
    store.setField("t", "p", "f", 1);
    await engine.sync();
    expect(calls).toEqual(["before:1", "after:1"]);
  });

  it("runs beforePull and afterPull hooks (D5)", async () => {
    const { store, engine } = mk();
    const calls: string[] = [];
    const plugin: StorePlugin & SyncHook = {
      name: "pull-spy",
      beforePull(ev) { calls.push("before"); return ev; },
      afterPull(ev) { calls.push(`after:${ev.changes.length}`); },
    };
    store.use(plugin);
    await engine.sync();
    expect(calls).toEqual(["before", "after:0"]);
  });

  it("beforePush returning null cancels the push and keeps pending", async () => {
    let pushes = 0;
    const { store, engine } = mk(() => { pushes++; });
    const plugin: StorePlugin & SyncHook = { name: "veto", beforePush() { return null; } };
    store.use(plugin);
    store.setField("t", "p", "f", 1);
    await engine.sync();
    expect(pushes).toBe(0);
    expect(store.pendingCount).toBe(1);
  });
});
