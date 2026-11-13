import { afterEach, expect, it, vi } from "vitest";
import { CRDTClient } from "../client.js";
import { CRDTStore } from "../store.js";
import { HybridClock, hlcCompare } from "../hlc.js";
import { CRDTStream } from "../stream.js";
import { RoomClient } from "../room.js";
import { WebSocketTransport } from "../transport/websocket.js";
import { CollaborationSession } from "../collaboration.js";
import type {
  PresenceState,
  StreamSubscription,
  StreamEventHandler,
  ChangeRecord,
  DocumentState,
  StorageAdapter,
  Transport,
} from "../types.js";

// Opt-in review probes assert the required behavior and currently fail.
// Run with GROVE_CRDT_REVIEW=1 until the production defects are fixed.
const review = it.runIf(process.env.GROVE_CRDT_REVIEW === "1");
const zero = { ts: "0", c: 0, node: "" };
const tick = async () => {
  for (let i = 0; i < 16; i++) await Promise.resolve();
};
afterEach(() => vi.useRealTimers());

review(
  "room metadata follows the Go PUT route and raw body contract",
  async () => {
    const calls: RequestInit[] = [];
    const rooms = new RoomClient({
      baseURL: "https://test/sync",
      fetch: async (_url, init) => {
        calls.push(init!);
        return new Response(null, { status: 204 });
      },
    });
    await rooms.updateMetadata("room", { title: "changed" });
    expect(calls[0].method).toBe("PUT");
    expect(JSON.parse(calls[0].body as string)).toEqual({ title: "changed" });
  },
);

review(
  "getRoom surfaces an authorization failure instead of reporting absence",
  async () => {
    const rooms = new RoomClient({
      baseURL: "https://test/sync",
      fetch: async () => new Response("denied", { status: 403 }),
    });
    await expect(rooms.getRoom("room")).rejects.toThrow();
  },
);

review(
  "leaveAllPresence attempts every topic even when one leave fails",
  async () => {
    vi.useFakeTimers();
    const leaves: string[] = [];
    const transport: Transport = {
      pull: async () => ({ changes: [], latest_hlc: zero }),
      push: async () => ({ merged: 0, latest_hlc: zero }),
      updatePresence: async (u) => {
        if (u.data === null) {
          leaves.push(u.topic);
          if (u.topic === "a") throw new Error("offline");
        }
      },
    };
    const client = new CRDTClient({ nodeID: "me", transport });
    try {
      await client.updatePresence("a", {});
      await client.updatePresence("b", {});
      await expect(client.leaveAllPresence()).rejects.toThrow("offline");
      expect(leaves).toEqual(["a", "b"]);
      expect(vi.getTimerCount()).toBe(0);
    } finally {
      await client.leavePresence("b");
    }
  },
);

review(
  "a delayed persistence write cannot overwrite a newer document",
  async () => {
    let finishFirst!: () => void;
    let count = 0;
    let disk: DocumentState | undefined;
    const storage: StorageAdapter = {
      loadState: async () => new Map(),
      loadPendingChanges: async () => [],
      deleteDocument: async () => {},
      savePendingChanges: async () => {},
      saveDocument: async (_t, _p, d) => {
        if (++count === 1)
          await new Promise<void>((r) => {
            finishFirst = r;
          });
        disk = d;
      },
    };
    const store = new CRDTStore("me", new HybridClock("me"), storage, {
      persistDebounceMs: 0,
    });
    await store.ready;
    store.setField("docs", "1", "title", "old");
    store.setField("docs", "1", "title", "new");
    finishFirst();
    await tick();
    expect(disk?.fields.title.value).toBe("new");
  },
);

review(
  "flushPersistence waits for writes already started by immediate persistence",
  async () => {
    let release!: () => void;
    const gate = new Promise<void>((r) => {
      release = r;
    });
    const storage: StorageAdapter = {
      loadState: async () => new Map(),
      loadPendingChanges: async () => [],
      deleteDocument: async () => {},
      savePendingChanges: async () => {},
      saveDocument: async () => gate,
    };
    const store = new CRDTStore("me", new HybridClock("me"), storage, {
      persistDebounceMs: 0,
    });
    await store.ready;
    store.setField("docs", "1", "title", "pending");
    let flushed = false;
    const flushing = store.flushPersistence().then(() => {
      flushed = true;
    });
    await tick();
    const prematurelyFlushed = flushed;
    release();
    await flushing;
    expect(prematurelyFlushed).toBe(false);
  },
);

review(
  "disconnect inside an SSE handler stops later events in the same chunk",
  async () => {
    const change = (pk: string): ChangeRecord => ({
      table: "docs",
      pk,
      field: "title",
      crdt_type: "lww",
      hlc: { ts: pk, c: 0, node: "peer" },
      node_id: "peer",
      value: pk,
    });
    const body = new ReadableStream<Uint8Array>({
      start(c) {
        c.enqueue(
          new TextEncoder().encode(
            ["1", "2"]
              .map(
                (pk) =>
                  `event: change\ndata: ${JSON.stringify(change(pk))}\n\n`,
              )
              .join(""),
          ),
        );
        c.close();
      },
    });
    const stream = new CRDTStream(
      "https://test",
      { idleTimeout: 0 },
      {},
      async () => new Response(body),
    );
    const seen: string[] = [];
    stream.on((e) => {
      if (e.type === "change") {
        seen.push(e.data.pk);
        stream.disconnect();
      }
    });
    stream.connect();
    await tick();
    stream.disconnect();
    expect(seen).toEqual(["1"]);
  },
);

review("HLC node ties use the same UTF-8 order as Go and Dart", () => {
  expect(
    hlcCompare(
      { ts: "1", c: 0, node: "\u{1f600}" },
      { ts: "1", c: 0, node: "\ue000" },
    ),
  ).toBeGreaterThan(0);
});

review(
  "an older replayed tombstone cannot make a remote deletion undoable",
  async () => {
    const clock = new HybridClock("me", { nowFn: () => 1000 });
    const store = new CRDTStore("me", clock);
    await store.ready;
    store.setField("docs", "1", "title", "saved");
    store.clearPendingChanges();
    const deletion = store.deleteDocument("docs", "1")!;
    store.applyChanges([
      {
        ...deletion,
        hlc: { ts: "1001000000", c: 0, node: "peer" },
        node_id: "peer",
      },
    ]);
    store.applyChanges([deletion]);
    expect(store.undo()).toBe(false);
    expect(store.getDocument("docs", "1")).toBeNull();
  },
);

review(
  "WebSocket request timeout also bounds a handshake that never opens",
  async () => {
    vi.useFakeTimers();
    class NeverOpens {
      readyState = 0;
      onopen: (() => void) | null = null;
      onerror: (() => void) | null = null;
      onclose: (() => void) | null = null;
      onmessage: unknown;
      send() {}
      close() {
        this.readyState = 3;
        this.onclose?.();
      }
    }
    const transport = new WebSocketTransport({
      url: "ws://test",
      requestTimeout: 10,
      WebSocketImpl: NeverOpens as unknown as typeof WebSocket,
    });
    let settled = false;
    const request = transport.pull({ tables: ["docs"], node_id: "me" }).then(
      () => {
        settled = true;
      },
      () => {
        settled = true;
      },
    );
    await vi.advanceTimersByTimeAsync(100);
    const bounded = settled;
    transport.close();
    await request;
    expect(bounded).toBe(true);
  },
);

review(
  "a reconnect during a presence snapshot schedules a fresh snapshot",
  async () => {
    vi.useFakeTimers();
    let release!: (states: PresenceState[]) => void;
    let snapshots = 0;
    const handlers = new Set<StreamEventHandler>();
    const stream: StreamSubscription = {
      connected: true,
      lastHLC: null,
      on: (h) => {
        handlers.add(h);
        return () => {
          handlers.delete(h);
        };
      },
      connect() {},
      disconnect() {},
    };
    const transport: Transport = {
      pull: async () => ({ changes: [], latest_hlc: zero }),
      push: async () => ({ merged: 0, latest_hlc: zero }),
      updatePresence: async () => {},
      getPresence: async () =>
        ++snapshots === 1
          ? new Promise((r) => {
              release = r;
            })
          : [],
    };
    const client = new CRDTClient({ nodeID: "me", transport });
    const session = new CollaborationSession(client, "room", {}, { stream });
    try {
      const starting = session.start();
      await tick();
      for (const h of handlers) h({ type: "disconnected" });
      // The peer leaves while no stream is connected. The first HTTP response
      // contains the snapshot captured before that departure.
      for (const h of handlers) h({ type: "connected" });
      release([
        { topic: "room", node_id: "gone", data: {}, updated_at: Date.now() },
      ]);
      await starting;
      await tick();
      expect(client.presence.getPresence("room")).toEqual([]);
      expect(snapshots).toBe(2);
    } finally {
      await session.close();
    }
  },
);
