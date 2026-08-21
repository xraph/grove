import { describe, it, expect } from "vitest";
import { CRDTStore } from "../store.js";
import { SyncEngine } from "../sync.js";
import { CRDTClient } from "../client.js";
import { CRDTErrorCode, CRDTError } from "../errors.js";
import type { Transport, ChangeRecord } from "../types.js";

const HLC0 = { ts: "0", c: 0, node: "" };

const inert: Transport = {
  async pull() { return { changes: [], latest_hlc: HLC0 }; },
  async push(req) { return { merged: req.changes.length, latest_hlc: HLC0 }; },
};

const mkStore = (opts: Record<string, unknown>) =>
  new CRDTStore(
    "n1",
    new CRDTClient({ nodeID: "n1", transport: inert }).clock,
    undefined,
    { persistDebounceMs: 0, ...opts }
  );

describe("offline queue", () => {
  it("bounds the pending queue and reports the overflow", () => {
    const store = mkStore({ maxPendingChanges: 10 });
    const dropped: ChangeRecord[] = [];
    store.onPendingOverflow((d) => { dropped.push(...d); });

    for (let i = 0; i < 15; i++) store.setField("t", "p", `f${i}`, i);

    expect(store.pendingCount).toBe(10);
    expect(dropped).toHaveLength(5);
    // The OLDEST are dropped: recent writes reflect what the user most
    // recently intended, and older ones are likelier superseded.
    expect(dropped[0].field).toBe("f0");
    expect(store.getPendingChanges()[0].field).toBe("f5");
  });

  it("an unbounded queue keeps everything", () => {
    const store = mkStore({ maxPendingChanges: 0 });
    for (let i = 0; i < 50; i++) store.setField("t", "p", `f${i}`, i);
    expect(store.pendingCount).toBe(50);
  });

  it("throws OfflineQueueFull when throwOnOverflow is set", () => {
    const store = mkStore({ maxPendingChanges: 2, throwOnOverflow: true });
    store.setField("t", "p", "a", 1);
    store.setField("t", "p", "b", 2);
    let caught: unknown;
    try {
      store.setField("t", "p", "c", 3);
    } catch (e) {
      caught = e;
    }
    expect(caught).toBeInstanceOf(CRDTError);
    expect((caught as CRDTError).code).toBe(CRDTErrorCode.OfflineQueueFull);
    // The rejected change must not be left half-queued.
    expect(store.pendingCount).toBe(2);
  });

  it("start() syncs on an interval and stop() halts it", async () => {
    let pushes = 0;
    const transport: Transport = {
      async pull() { return { changes: [], latest_hlc: HLC0 }; },
      async push(req) { pushes++; return { merged: req.changes.length, latest_hlc: HLC0 }; },
    };
    const client = new CRDTClient({ nodeID: "n1", transport });
    const store = new CRDTStore("n1", client.clock, undefined, { persistDebounceMs: 0 });
    const engine = new SyncEngine(client, store);

    const stop = engine.start({ intervalMs: 10 });
    store.setField("t", "p", "f", 1);
    await new Promise((r) => setTimeout(r, 50));
    stop();
    const settled = pushes;
    await new Promise((r) => setTimeout(r, 50));

    expect(settled).toBeGreaterThan(0);
    expect(pushes).toBe(settled); // no syncs after stop()
  });
});
