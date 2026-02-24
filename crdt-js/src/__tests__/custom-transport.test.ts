import { describe, it, expect, vi } from "vitest";
import { CRDTClient, CRDTError } from "../client.js";
import type {
  Transport,
  StreamTransport,
  StreamSubscription,
  StreamConfig,
  PullRequest,
  PullResponse,
  PushRequest,
  PushResponse,
  HLC,
  StreamEventHandler,
} from "../types.js";

// ---------------------------------------------------------------------------
// Mock transport (non-HTTP)
// ---------------------------------------------------------------------------

function createMockTransport(): Transport & {
  pullCalls: PullRequest[];
  pushCalls: PushRequest[];
} {
  return {
    pullCalls: [],
    pushCalls: [],

    async pull(req: PullRequest): Promise<PullResponse> {
      this.pullCalls.push(req);
      return {
        changes: [],
        latest_hlc: { ts: 1000, c: 0, node: "mock-server" },
      };
    },

    async push(req: PushRequest): Promise<PushResponse> {
      this.pushCalls.push(req);
      return {
        merged: req.changes.length,
        latest_hlc: { ts: 2000, c: 0, node: "mock-server" },
      };
    },
  };
}

function createMockStreamSubscription(): StreamSubscription {
  let _connected = false;
  const handlers = new Set<StreamEventHandler>();

  return {
    get connected() {
      return _connected;
    },
    get lastHLC(): HLC | null {
      return null;
    },
    on(handler: StreamEventHandler) {
      handlers.add(handler);
      return () => handlers.delete(handler);
    },
    connect() {
      _connected = true;
      for (const h of handlers) h({ type: "connected" });
    },
    disconnect() {
      _connected = false;
      for (const h of handlers) h({ type: "disconnected" });
    },
  };
}

function createMockStreamTransport(): StreamTransport & {
  pullCalls: PullRequest[];
  pushCalls: PushRequest[];
  subscribeCalls: StreamConfig[];
  subscription: StreamSubscription;
} {
  const subscription = createMockStreamSubscription();
  return {
    pullCalls: [],
    pushCalls: [],
    subscribeCalls: [],
    subscription,

    async pull(req: PullRequest): Promise<PullResponse> {
      this.pullCalls.push(req);
      return {
        changes: [],
        latest_hlc: { ts: 1000, c: 0, node: "mock-server" },
      };
    },

    async push(req: PushRequest): Promise<PushResponse> {
      this.pushCalls.push(req);
      return {
        merged: req.changes.length,
        latest_hlc: { ts: 2000, c: 0, node: "mock-server" },
      };
    },

    subscribe(config: StreamConfig): StreamSubscription {
      this.subscribeCalls.push(config);
      return this.subscription;
    },
  };
}

// ---------------------------------------------------------------------------
// CRDTClient with custom Transport
// ---------------------------------------------------------------------------

describe("CRDTClient with custom Transport", () => {
  it("pull() delegates to transport.pull()", async () => {
    const transport = createMockTransport();
    const client = new CRDTClient({ nodeID: "n1", transport, tables: ["users"] });

    await client.pull();
    expect(transport.pullCalls).toHaveLength(1);
    expect(transport.pullCalls[0].tables).toEqual(["users"]);
    expect(transport.pullCalls[0].node_id).toBe("n1");
  });

  it("push() delegates to transport.push()", async () => {
    const transport = createMockTransport();
    const client = new CRDTClient({ nodeID: "n1", transport });

    const changes = [
      {
        table: "users",
        pk: "1",
        field: "name",
        crdt_type: "lww" as const,
        hlc: { ts: 100, c: 0, node: "n1" },
        node_id: "n1",
        value: "Alice",
      },
    ];
    const result = await client.push(changes);
    expect(transport.pushCalls).toHaveLength(1);
    expect(transport.pushCalls[0].changes).toEqual(changes);
    expect(result.merged).toBe(1);
  });

  it("clock still updated from transport response", async () => {
    const transport = createMockTransport();
    const client = new CRDTClient({ nodeID: "n1", transport });

    await client.pull();
    // Transport returns latest_hlc with ts=1000
    const next = client.clock.now();
    expect(next.ts >= 1000 || next.c > 0).toBe(true);
  });

  it("pull() passes tables and since correctly", async () => {
    const transport = createMockTransport();
    const client = new CRDTClient({
      nodeID: "n1",
      transport,
      tables: ["default"],
    });

    const since: HLC = { ts: 500, c: 0, node: "n1" };
    await client.pull(["override"], since);
    expect(transport.pullCalls[0].tables).toEqual(["override"]);
    expect(transport.pullCalls[0].since).toEqual(since);
  });

  it("push() short-circuits on empty changes", async () => {
    const transport = createMockTransport();
    const client = new CRDTClient({ nodeID: "n1", transport });

    const result = await client.push([]);
    expect(transport.pushCalls).toHaveLength(0);
    expect(result.merged).toBe(0);
  });

  it("stream() throws when no stream transport", () => {
    const transport = createMockTransport();
    const client = new CRDTClient({ nodeID: "n1", transport });

    expect(() => client.stream()).toThrow(
      "No stream transport available"
    );
  });

  it("stream() works with StreamTransport", () => {
    const transport = createMockStreamTransport();
    const client = new CRDTClient({ nodeID: "n1", transport, tables: ["users"] });

    const sub = client.stream();
    expect(sub).toBeDefined();
    expect(sub.connected).toBe(false);
    expect(transport.subscribeCalls).toHaveLength(1);
    expect(transport.subscribeCalls[0].tables).toEqual(["users"]);
  });

  it("stream() passes config overrides", () => {
    const transport = createMockStreamTransport();
    const client = new CRDTClient({ nodeID: "n1", transport, tables: ["default"] });

    const since: HLC = { ts: 100, c: 0, node: "n1" };
    client.stream({ tables: ["override"], reconnectDelay: 1000, since });
    expect(transport.subscribeCalls[0].tables).toEqual(["override"]);
    expect(transport.subscribeCalls[0].reconnectDelay).toBe(1000);
    expect(transport.subscribeCalls[0].since).toEqual(since);
  });

  it("stream subscription connect/disconnect works", () => {
    const transport = createMockStreamTransport();
    const client = new CRDTClient({ nodeID: "n1", transport });

    const sub = client.stream({ tables: [] });
    expect(sub.connected).toBe(false);

    sub.connect();
    expect(sub.connected).toBe(true);

    sub.disconnect();
    expect(sub.connected).toBe(false);
  });

  it("stream subscription emits events", () => {
    const transport = createMockStreamTransport();
    const client = new CRDTClient({ nodeID: "n1", transport });

    const sub = client.stream({ tables: [] });
    const events: string[] = [];

    sub.on((e) => events.push(e.type));
    sub.connect();
    sub.disconnect();

    expect(events).toEqual(["connected", "disconnected"]);
  });

  it("streamTransport config overrides transport", () => {
    const transport = createMockTransport();
    const streamTransport = createMockStreamTransport();
    const client = new CRDTClient({
      nodeID: "n1",
      transport,
      streamTransport,
      tables: ["users"],
    });

    // stream() should use streamTransport, not transport.
    const sub = client.stream();
    expect(sub).toBeDefined();
    expect(streamTransport.subscribeCalls).toHaveLength(1);
  });
});

// ---------------------------------------------------------------------------
// Constructor validation
// ---------------------------------------------------------------------------

describe("CRDTClient constructor", () => {
  it("throws when neither baseURL nor transport is provided", () => {
    expect(() => new CRDTClient({ nodeID: "n1" })).toThrow(
      "CRDTClient requires either `baseURL` or `transport`."
    );
  });

  it("accepts baseURL without transport (backward compat)", async () => {
    // Mock globalThis.fetch for this test.
    const pullResponse: PullResponse = {
      changes: [],
      latest_hlc: { ts: 1, c: 0, node: "s" },
    };
    const origFetch = globalThis.fetch;
    globalThis.fetch = vi.fn().mockResolvedValue({
      ok: true,
      json: () => Promise.resolve(pullResponse),
    }) as unknown as typeof fetch;

    try {
      const client = new CRDTClient({
        baseURL: "https://api.example.com/sync",
        nodeID: "n1",
        tables: ["users"],
      });
      const result = await client.pull();
      expect(result.changes).toEqual([]);
    } finally {
      globalThis.fetch = origFetch;
    }
  });

  it("accepts custom transport without baseURL", async () => {
    const transport = createMockTransport();
    const client = new CRDTClient({ nodeID: "n1", transport });
    const result = await client.pull();
    expect(result.changes).toEqual([]);
  });
});
