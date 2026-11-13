import { afterEach, describe, expect, it, vi } from "vitest";
import { CRDTClient } from "../client.js";
import { CollaborationSession, CursorSmoother } from "../collaboration.js";
import type { Transport, PresenceUpdate, StreamEventHandler, StreamSubscription } from "../types.js";

const zero = { ts: "0", c: 0, node: "" };
function setup(options: { updateInterval?: number; typingTimeout?: number; stream?: StreamSubscription } = {}) {
  const writes: PresenceUpdate[] = [];
  const transport: Transport = {
    pull: async () => ({ changes: [], latest_hlc: zero }),
    push: async () => ({ merged: 0, latest_hlc: zero }),
    updatePresence: vi.fn(async update => { writes.push(update); }),
    getPresence: vi.fn(async () => [{ node_id: "peer", topic: "room", data: { name: "Bob" }, updated_at: Date.now() }]),
  };
  const client = new CRDTClient({ nodeID: "me", transport });
  const session = new CollaborationSession(client, "room", { name: "Alice" }, options);
  return { client, session, transport, writes };
}

afterEach(() => vi.useRealTimers());

describe("CollaborationSession", () => {
  it("seeds idle peers and coalesces 100 movements into the latest snapshot", async () => {
    vi.useFakeTimers();
    const { session, client, writes } = setup();
    await session.start();
    expect(client.presence.getPresence("room")[0].node_id).toBe("peer");
    for (let i = 0; i < 100; i++) session.updateCursor({ x: i, y: 0 });
    expect(writes).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(49);
    expect(writes).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(writes).toHaveLength(2);
    expect(writes[1].data).toEqual({ name: "Alice", cursor: { x: 99, y: 0 } });
    await session.close();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("bounds pending movement behind a slow write and leaves last", async () => {
    vi.useFakeTimers();
    const { session, transport, writes } = setup();
    await session.start();
    let release!: () => void;
    const original = transport.updatePresence!;
    transport.updatePresence = async update => {
      await original(update);
      if (update.data !== null) await new Promise<void>(resolve => { release = resolve; });
    };
    session.updateCursor({ x: 1, y: 1 });
    await vi.advanceTimersByTimeAsync(50);
    for (let i = 0; i < 100; i++) session.updateCursor({ x: i, y: i });
    await vi.advanceTimersByTimeAsync(1000);
    expect(writes).toHaveLength(2);
    const closing = session.close();
    release();
    await closing;
    expect(writes.map(w => w.data)).toEqual([
      { name: "Alice" }, { name: "Alice", cursor: { x: 1, y: 1 } }, null,
    ]);
    expect(vi.getTimerCount()).toBe(0);
    await session.close();
    expect(writes).toHaveLength(3);
  });

  it("extends typing deadlines and clears typing without pointer movement", async () => {
    vi.useFakeTimers();
    const { session, writes } = setup({ typingTimeout: 100 });
    await session.start();
    session.setTyping(true);
    await vi.advanceTimersByTimeAsync(75);
    session.setTyping(true);
    await vi.advanceTimersByTimeAsync(75);
    expect(writes[writes.length - 1]?.data.is_typing).toBe(true);
    await vi.advanceTimersByTimeAsync(50);
    expect(writes[writes.length - 1]?.data.is_typing).toBe(false);
    await session.close();
  });

  it("republishes and refreshes after stream reconnect without owning the stream", async () => {
    vi.useFakeTimers();
    const handlers = new Set<StreamEventHandler>();
    const stream: StreamSubscription = {
      connected: true, lastHLC: null, connect: vi.fn(), disconnect: vi.fn(),
      on: handler => { handlers.add(handler); return () => { handlers.delete(handler); }; },
    };
    const { session, transport, writes } = setup({ stream });
    await session.start();
    for (const handler of handlers) handler({ type: "connected" });
    await session.refresh();
    expect(transport.getPresence).toHaveBeenCalledTimes(2);
    expect(writes).toHaveLength(2);
    await session.close();
    expect(handlers.size).toBe(0);
    expect(stream.disconnect).not.toHaveBeenCalled();
  });

  it("does not mutate queued data and exposes stable immutable snapshots", async () => {
    vi.useFakeTimers();
    const { session, writes } = setup();
    await session.start();
    const cursor = { x: 10, y: 20 };
    session.updateCursor(cursor);
    const first = session.getSnapshot();
    expect(session.getSnapshot()).toBe(first);
    cursor.x = 999;
    await session.flush();
    expect(writes[writes.length - 1]?.data.cursor).toEqual({ x: 10, y: 20 });
    expect(Object.isFrozen(first)).toBe(true);
    await session.close();
  });

  it("serializes direct writes and prevents late heartbeats after leave", async () => {
    vi.useFakeTimers();
    const { client, transport, writes } = setup();
    let release!: () => void;
    const original = transport.updatePresence!;
    transport.updatePresence = async update => {
      await original(update);
      if (update.data !== null) await new Promise<void>(resolve => { release = resolve; });
    };
    const update = client.updatePresence("room", { x: 1 });
    await vi.advanceTimersByTimeAsync(0);
    const leave = client.leavePresence("room");
    release();
    await Promise.all([update, leave]);
    await vi.advanceTimersByTimeAsync(30_000);
    expect(writes.map(w => w.data)).toEqual([{ x: 1 }, null]);
    expect(vi.getTimerCount()).toBe(0);
  });
});

describe("CursorSmoother", () => {
  it("interpolates arrivals without overshoot and retargets without jumping", () => {
    const smoother = new CursorSmoother(100);
    smoother.push("p", { x: 0, y: 0 }, 0);
    smoother.push("p", { x: 100, y: 50 }, 10);
    expect(smoother.sample("p", 60)).toEqual({ x: 50, y: 25 });
    smoother.push("p", { x: 200, y: 100 }, 60);
    expect(smoother.sample("p", 60)).toEqual({ x: 50, y: 25 });
    expect(smoother.sample("p", 1000)).toEqual({ x: 200, y: 100 });
    smoother.push("p", { x: -1, y: -1 }, 50); // stale local sample
    expect(smoother.sample("p", 1000)).toEqual({ x: 200, y: 100 });
    smoother.push("p", { x: 1, y: 2, field: "another-canvas" }, 1001);
    expect(smoother.sample("p", 1001)).toEqual({ x: 1, y: 2, field: "another-canvas" });
    smoother.remove("p");
    expect(smoother.sample("p")).toBeNull();
  });
});

it("does not starve peer discovery when movement continues during the initial write", async () => {
  vi.useFakeTimers();
  const { session, transport, writes } = setup();
  let release!: () => void;
  const original = transport.updatePresence!;
  transport.updatePresence = async update => {
    await original(update);
    if (writes.length === 1) await new Promise<void>(resolve => { release = resolve; });
  };
  const started = session.start();
  await vi.advanceTimersByTimeAsync(0);
  session.updateCursor({ x: 42, y: 0 });
  release();
  await started;
  expect(transport.getPresence).toHaveBeenCalledTimes(1);
  // Movement remains queued at the configured cadence, not lost or awaited forever.
  await vi.advanceTimersByTimeAsync(50);
  expect(writes[1].data.cursor).toEqual({ x: 42, y: 0 });
  await session.close();
});
