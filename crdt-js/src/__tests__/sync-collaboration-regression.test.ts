import { afterEach, expect, it, vi } from "vitest";
import { WebSocketTransport } from "../transport/websocket.js";
import { CRDTStream } from "../stream.js";
import { CRDTClient } from "../client.js";
import { CRDTStore } from "../store.js";
import { SyncEngine } from "../sync.js";
import { HybridClock } from "../hlc.js";
import type { Transport } from "../types.js";

class Socket {
  readyState = 0;
  onopen: (() => void) | null = null;
  onclose: (() => void) | null = null;
  onmessage: ((e: { data: string }) => void) | null = null;
  onerror: (() => void) | null = null;
  sent: any[] = [];
  constructor(autoOpen = true) {
    if (!autoOpen) return;
    queueMicrotask(() => {
      this.readyState = 1;
      this.onopen?.();
    });
  }
  send(data: string) {
    this.sent.push(JSON.parse(data));
  }
  close() {
    this.readyState = 3;
    this.onclose?.();
  }
}
function setup() {
  const sockets: Socket[] = [];
  const Impl = function () {
    const s = new Socket();
    sockets.push(s);
    return s;
  } as unknown as typeof WebSocket;
  const transport = new WebSocketTransport({
    url: "ws://test",
    WebSocketImpl: Impl,
    backoff: { initialDelay: 1, maxDelay: 1, jitter: false },
  });
  return { transport, sockets };
}
const tick = async () => {
  for (let i = 0; i < 12; i++) await Promise.resolve();
};
afterEach(() => vi.useRealTimers());

it("regression: edits after streamed remote state are causally newer", async () => {
  const clock = new HybridClock("me", { nowFn: () => 1000 });
  const store = new CRDTStore("me", clock, undefined, { persistDebounceMs: 0 });
  await store.ready;
  store.applyChanges([
    {
      table: "docs",
      pk: "1",
      field: "title",
      crdt_type: "lww",
      node_id: "peer",
      hlc: { ts: "1001000000", c: 0, node: "peer" },
      value: "remote",
    },
  ]);
  store.setField("docs", "1", "title", "my later edit");
  expect(store.getDocument("docs", "1")?.title).toBe("my later edit");
});

it("regression: removing an unseen set element does not delete a concurrent add", async () => {
  const a = new CRDTStore(
    "a",
    new HybridClock("a", { nowFn: () => 1000 }),
    undefined,
    { persistDebounceMs: 0 },
  );
  const b = new CRDTStore(
    "b",
    new HybridClock("b", { nowFn: () => 1001 }),
    undefined,
    { persistDebounceMs: 0 },
  );
  await Promise.all([a.ready, b.ready]);
  const add = a.addToSet("docs", "1", "tags", ["x"])!;
  const remove = b.removeFromSet("docs", "1", "tags", ["x"])!;
  a.applyChanges([remove]);
  b.applyChanges([add]);
  expect(a.getDocument("docs", "1")?.tags).toEqual(
    b.getDocument("docs", "1")?.tags,
  );
});

it("regression: undo during pull prevents the cancelled deletion from being pushed", async () => {
  const zero = { ts: "0", c: 0, node: "" };
  let releasePull!: () => void;
  const pullGate = new Promise<void>((r) => {
    releasePull = r;
  });
  const sent: any[] = [];
  const tr: Transport = {
    pull: async () => {
      await pullGate;
      return { changes: [], latest_hlc: zero };
    },
    push: async (req) => {
      sent.push(...req.changes);
      return { merged: req.changes.length, latest_hlc: zero };
    },
  };
  const client = new CRDTClient({ nodeID: "me", transport: tr });
  const store = new CRDTStore("me", client.clock, undefined, {
    persistDebounceMs: 0,
  });
  await store.ready;
  store.setField("docs", "1", "title", "kept");
  store.clearPendingChanges();
  store.deleteDocument("docs", "1");
  const engine = new SyncEngine(client, store);
  const sync = engine.sync();
  expect(store.undo()).toBe(true);
  expect(store.pendingCount).toBe(0);
  releasePull();
  await sync;
  expect(sent.filter((change) => change.tombstone)).toHaveLength(0);
});

it("regression: presence-only WebSocket reconnects after a dropped socket", async () => {
  vi.useFakeTimers();
  const { transport, sockets } = setup();
  try {
    const sub = transport.subscribe({ tables: [] });
    sub.connect();
    await tick();
    expect(sub.connected).toBe(true);
    sockets[0].close();
    await vi.advanceTimersByTimeAsync(100);
    expect(sockets).toHaveLength(2);
  } finally {
    transport.close();
  }
});
it("regression: disconnect stops delivering database changes", async () => {
  const { transport, sockets } = setup();
  try {
    const sub = transport.subscribe({ tables: ["docs"] });
    const events: any[] = [];
    sub.on((e) => events.push(e));
    sub.connect();
    await tick();
    sub.disconnect();
    events.length = 0;
    sockets[0].onmessage?.({
      data: JSON.stringify({
        type: "changes",
        payload: [
          {
            table: "docs",
            pk: "1",
            field: "title",
            crdt_type: "lww",
            node_id: "peer",
            hlc: { ts: "1", c: 0, node: "peer" },
            value: "after disconnect",
          },
        ],
      }),
    });
    expect(events.filter((e) => e.type === "changes")).toHaveLength(0);
    expect(sockets[0].sent.some((m) => m.type === "unsubscribe")).toBe(true);
  } finally {
    transport.close();
  }
});
it("regression: WebSocket carries its requested resume cursor", async () => {
  const { transport, sockets } = setup();
  const since = { ts: "99", c: 0, node: "peer" };
  try {
    transport.subscribe({ tables: ["docs"], since }).connect();
    await tick();
    expect(
      sockets[0].sent.find((m) => m.type === "subscribe").payload.since,
    ).toEqual(since);
  } finally {
    transport.close();
  }
});
it("regression: an empty pull does not reset incremental sync", async () => {
  const zero = { ts: "0", c: 0, node: "" };
  const cursor = { ts: "99", c: 0, node: "peer" };
  let round = 0;
  const seen: any[] = [];
  const tr: Transport = {
    pull: async (req) => {
      seen.push(req.since);
      return { changes: [], latest_hlc: ++round === 1 ? cursor : zero };
    },
    push: async () => ({ merged: 0, latest_hlc: zero }),
  };
  const client = new CRDTClient({ nodeID: "me", transport: tr });
  const store = new CRDTStore("me", client.clock);
  await store.ready;
  const engine = new SyncEngine(client, store);
  await engine.sync();
  await engine.sync();
  await engine.sync();
  expect(seen[2]).toEqual(cursor);
});
it("regression: stale async auth cannot open a second SSE request on reconnect", async () => {
  let resolveOld!: (v: Record<string, string>) => void;
  let count = 0;
  const auth = {
    getHeaders: () =>
      ++count === 1
        ? new Promise<Record<string, string>>((r) => {
            resolveOld = r;
          })
        : Promise.resolve({ Authorization: "new" }),
  };
  const calls: { headers: Headers; signal: AbortSignal | null | undefined }[] =
    [];
  const fetcher = vi.fn(async (_u: any, init?: RequestInit) => {
    calls.push({ headers: new Headers(init?.headers), signal: init?.signal });
    const body = new ReadableStream<Uint8Array>({
      start(c) {
        init?.signal?.addEventListener("abort", () => c.close());
      },
    });
    return new Response(body);
  });
  const stream = new CRDTStream(
    "https://test",
    { idleTimeout: 0 },
    {},
    fetcher as typeof fetch,
    auth,
  );
  try {
    stream.connect();
    stream.disconnect();
    stream.connect();
    await tick();
    resolveOld({ Authorization: "old" });
    await tick();
    expect(calls).toHaveLength(1);
  } finally {
    stream.disconnect();
  }
});

it("regression: a tombstone remains non-undoable after a lost push acknowledgement", async () => {
  const zero = { ts: "0", c: 0, node: "" };
  let started!: () => void;
  const accepted = new Promise<void>((resolve) => {
    started = resolve;
  });
  let fail!: () => void;
  const lostReply = new Promise<void>((_, reject) => {
    fail = () => reject(new Error("lost acknowledgement"));
  });
  const client = new CRDTClient({
    nodeID: "me",
    transport: {
      pull: async () => ({ changes: [], latest_hlc: zero }),
      push: async () => {
        started();
        await lostReply;
        return { merged: 1, latest_hlc: zero };
      },
    },
  });
  const store = new CRDTStore("me", client.clock);
  await store.ready;
  store.applyChanges([
    {
      table: "docs",
      pk: "1",
      field: "title",
      crdt_type: "lww",
      hlc: zero,
      node_id: "peer",
      value: "kept",
    },
  ]);
  store.clearPendingChanges();
  store.deleteDocument("docs", "1");
  const run = new SyncEngine(client, store).sync();
  await accepted;
  expect(store.undo()).toBe(false);
  fail();
  await expect(run).rejects.toThrow("lost acknowledgement");
  expect(store.pendingCount).toBe(1);
  expect(store.undo()).toBe(false);
  expect(store.getDocument("docs", "1")).toBeNull();
});

it("regression: active WebSocket scopes share the socket and resume independently", async () => {
  vi.useFakeTimers();
  const { transport, sockets } = setup();
  const docs = transport.subscribe({
    tables: ["docs"],
    since: { ts: "10", c: 0, node: "peer" },
  });
  const tasks = transport.subscribe({
    tables: ["tasks"],
    since: { ts: "5", c: 0, node: "peer" },
  });
  const events: any[] = [];
  tasks.on((event) => events.push(event));
  try {
    docs.connect();
    await tick();
    tasks.connect();
    await tick();
    expect(
      sockets[0].sent.filter((m) => m.type === "subscribe").slice(-1)[0]
        .payload,
    ).toEqual({
      tables: ["docs", "tasks"],
      since: { ts: "5", c: 0, node: "peer" },
    });
    sockets[0].onmessage?.({
      data: JSON.stringify({
        type: "changes",
        payload: [
          {
            table: "docs",
            pk: "1",
            field: "title",
            crdt_type: "lww",
            node_id: "peer",
            hlc: { ts: "20", c: 0, node: "peer" },
            value: "docs",
          },
          {
            table: "tasks",
            pk: "1",
            field: "title",
            crdt_type: "lww",
            node_id: "peer",
            hlc: { ts: "15", c: 0, node: "peer" },
            value: "tasks",
          },
        ],
      }),
    });
    expect(
      events
        .filter((e) => e.type === "changes")[0]
        .data.map((c: any) => c.table),
    ).toEqual(["tasks"]);
    docs.disconnect();
    expect(tasks.connected).toBe(true);
    sockets[0].close();
    await vi.advanceTimersByTimeAsync(10);
    expect(sockets[1].sent.find((m) => m.type === "subscribe").payload).toEqual(
      {
        tables: ["tasks"],
        since: { ts: "15", c: 0, node: "peer" },
      },
    );
    // Late callbacks from a replaced socket cannot update the resume cursor.
    sockets[0].onmessage?.({
      data: JSON.stringify({
        type: "change",
        payload: {
          table: "tasks",
          hlc: { ts: "999", c: 0, node: "peer" },
        },
      }),
    });
    expect(tasks.lastHLC?.ts).toBe("15");
    tasks.disconnect();
    expect(sockets[1].sent.slice(-1)[0].type).toBe("unsubscribe");
    sockets[1].close();
    await vi.advanceTimersByTimeAsync(100);
    expect(sockets).toHaveLength(2);
  } finally {
    transport.close();
  }
});

it("regression: presence reconnect retries handshake failures and cancels on disconnect", async () => {
  vi.useFakeTimers();
  const sockets: Socket[] = [];
  const Impl = function () {
    const attempt = sockets.length;
    const socket = new Socket(attempt !== 1);
    sockets.push(socket);
    if (attempt === 1) queueMicrotask(() => socket.onerror?.());
    return socket;
  } as unknown as typeof WebSocket;
  const transport = new WebSocketTransport({
    url: "ws://test",
    WebSocketImpl: Impl,
    backoff: { initialDelay: 1, maxDelay: 1, jitter: false },
  });
  const sub = transport.subscribe({ tables: [] });
  try {
    sub.connect();
    await tick();
    sockets[0].close();
    await vi.advanceTimersByTimeAsync(10);
    expect(sockets).toHaveLength(3);
    expect(sub.connected).toBe(true);
    sockets[2].close();
    sub.disconnect();
    await vi.advanceTimersByTimeAsync(100);
    expect(sockets).toHaveLength(3);
  } finally {
    transport.close();
  }
});
