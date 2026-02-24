import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { PresenceManager } from "../presence.js";
import { CRDTClient } from "../client.js";
import { HttpTransport } from "../transport.js";
import type {
  PresenceEvent,
  PresenceState,
  PresenceSnapshot,
  PullResponse,
  PushResponse,
  Transport,
} from "../types.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function mockFetch(body: unknown, status = 200) {
  return vi.fn().mockResolvedValue({
    ok: status >= 200 && status < 300,
    status,
    json: () => Promise.resolve(body),
    text: () =>
      Promise.resolve(typeof body === "string" ? body : JSON.stringify(body)),
  }) as unknown as typeof fetch;
}

function createClient(
  fetchImpl: typeof fetch,
  overrides: Record<string, unknown> = {}
) {
  return new CRDTClient({
    baseURL: "https://api.example.com/sync",
    nodeID: "test-node",
    tables: ["users"],
    fetch: fetchImpl,
    ...overrides,
  });
}

// ---------------------------------------------------------------------------
// PresenceManager (local store)
// ---------------------------------------------------------------------------

describe("PresenceManager", () => {
  let manager: PresenceManager;

  beforeEach(() => {
    manager = new PresenceManager("local-node");
  });

  describe("getPresence()", () => {
    it("returns empty array for unknown topic", () => {
      expect(manager.getPresence("unknown")).toEqual([]);
    });

    it("returns peers after join event", () => {
      manager.applyEvent({
        type: "join",
        node_id: "peer-1",
        topic: "docs:1",
        data: { name: "Alice" },
      });

      const peers = manager.getPresence("docs:1");
      expect(peers).toHaveLength(1);
      expect(peers[0].node_id).toBe("peer-1");
      expect(peers[0].data).toEqual({ name: "Alice" });
    });

    it("excludes the local node from results", () => {
      manager.applyEvent({
        type: "join",
        node_id: "local-node",
        topic: "docs:1",
        data: { name: "Me" },
      });
      manager.applyEvent({
        type: "join",
        node_id: "peer-1",
        topic: "docs:1",
        data: { name: "Alice" },
      });

      const peers = manager.getPresence("docs:1");
      expect(peers).toHaveLength(1);
      expect(peers[0].node_id).toBe("peer-1");
    });

    it("returns new array reference on each call (useSyncExternalStore compat)", () => {
      manager.applyEvent({
        type: "join",
        node_id: "peer-1",
        topic: "docs:1",
        data: {},
      });

      const a = manager.getPresence("docs:1");
      const b = manager.getPresence("docs:1");
      expect(a).not.toBe(b);
      expect(a).toEqual(b);
    });

    it("returns typed presence state", () => {
      interface CursorData {
        x: number;
        y: number;
      }

      manager.applyEvent({
        type: "join",
        node_id: "peer-1",
        topic: "canvas:1",
        data: { x: 10, y: 20 },
      });

      const peers = manager.getPresence<CursorData>("canvas:1");
      expect(peers[0].data.x).toBe(10);
      expect(peers[0].data.y).toBe(20);
    });
  });

  describe("getPeer()", () => {
    it("returns null for unknown topic", () => {
      expect(manager.getPeer("unknown", "peer-1")).toBeNull();
    });

    it("returns null for unknown node", () => {
      manager.applyEvent({
        type: "join",
        node_id: "peer-1",
        topic: "docs:1",
        data: {},
      });
      expect(manager.getPeer("docs:1", "peer-2")).toBeNull();
    });

    it("returns the peer state", () => {
      manager.applyEvent({
        type: "join",
        node_id: "peer-1",
        topic: "docs:1",
        data: { name: "Alice" },
      });

      const peer = manager.getPeer("docs:1", "peer-1");
      expect(peer).not.toBeNull();
      expect(peer!.node_id).toBe("peer-1");
      expect(peer!.data).toEqual({ name: "Alice" });
    });
  });

  describe("applyEvent()", () => {
    it("handles join event", () => {
      manager.applyEvent({
        type: "join",
        node_id: "peer-1",
        topic: "room",
        data: { name: "Alice" },
      });

      expect(manager.getPresence("room")).toHaveLength(1);
    });

    it("handles update event", () => {
      manager.applyEvent({
        type: "join",
        node_id: "peer-1",
        topic: "room",
        data: { name: "Alice" },
      });
      manager.applyEvent({
        type: "update",
        node_id: "peer-1",
        topic: "room",
        data: { name: "Alice", typing: true },
      });

      const peers = manager.getPresence("room");
      expect(peers).toHaveLength(1);
      expect(peers[0].data).toEqual({ name: "Alice", typing: true });
    });

    it("handles leave event", () => {
      manager.applyEvent({
        type: "join",
        node_id: "peer-1",
        topic: "room",
        data: {},
      });
      manager.applyEvent({
        type: "leave",
        node_id: "peer-1",
        topic: "room",
      });

      expect(manager.getPresence("room")).toHaveLength(0);
    });

    it("leave for non-existent peer is a no-op", () => {
      manager.applyEvent({
        type: "leave",
        node_id: "non-existent",
        topic: "room",
      });

      expect(manager.getPresence("room")).toHaveLength(0);
    });

    it("cleans up empty topic map after last leave", () => {
      manager.applyEvent({
        type: "join",
        node_id: "peer-1",
        topic: "room",
        data: {},
      });
      manager.applyEvent({
        type: "leave",
        node_id: "peer-1",
        topic: "room",
      });

      // getPresence returns empty for cleaned-up topic
      expect(manager.getPresence("room")).toEqual([]);
    });

    it("sets updated_at on join/update", () => {
      const before = Date.now();
      manager.applyEvent({
        type: "join",
        node_id: "peer-1",
        topic: "room",
        data: {},
      });
      const after = Date.now();

      const peer = manager.getPeer("room", "peer-1");
      expect(peer!.updated_at).toBeGreaterThanOrEqual(before);
      expect(peer!.updated_at).toBeLessThanOrEqual(after);
    });

    it("defaults data to empty object when undefined", () => {
      manager.applyEvent({
        type: "join",
        node_id: "peer-1",
        topic: "room",
      } as PresenceEvent);

      const peer = manager.getPeer("room", "peer-1");
      expect(peer!.data).toEqual({});
    });

    it("handles multiple peers on same topic", () => {
      manager.applyEvent({
        type: "join",
        node_id: "peer-1",
        topic: "room",
        data: { name: "Alice" },
      });
      manager.applyEvent({
        type: "join",
        node_id: "peer-2",
        topic: "room",
        data: { name: "Bob" },
      });

      const peers = manager.getPresence("room");
      expect(peers).toHaveLength(2);
      const names = peers.map((p) => (p.data as Record<string, unknown>).name);
      expect(names).toContain("Alice");
      expect(names).toContain("Bob");
    });

    it("handles same peer on multiple topics", () => {
      manager.applyEvent({
        type: "join",
        node_id: "peer-1",
        topic: "room-a",
        data: { context: "a" },
      });
      manager.applyEvent({
        type: "join",
        node_id: "peer-1",
        topic: "room-b",
        data: { context: "b" },
      });

      expect(manager.getPresence("room-a")).toHaveLength(1);
      expect(manager.getPresence("room-b")).toHaveLength(1);
    });
  });

  describe("subscribe()", () => {
    it("notifies listener on event for subscribed topic", () => {
      const listener = vi.fn();
      manager.subscribe("room", listener);

      manager.applyEvent({
        type: "join",
        node_id: "peer-1",
        topic: "room",
        data: {},
      });

      expect(listener).toHaveBeenCalledTimes(1);
    });

    it("does not notify listener for other topics", () => {
      const listener = vi.fn();
      manager.subscribe("room-a", listener);

      manager.applyEvent({
        type: "join",
        node_id: "peer-1",
        topic: "room-b",
        data: {},
      });

      expect(listener).not.toHaveBeenCalled();
    });

    it("unsubscribes correctly", () => {
      const listener = vi.fn();
      const unsub = manager.subscribe("room", listener);

      unsub();

      manager.applyEvent({
        type: "join",
        node_id: "peer-1",
        topic: "room",
        data: {},
      });

      expect(listener).not.toHaveBeenCalled();
    });

    it("supports multiple listeners on same topic", () => {
      const l1 = vi.fn();
      const l2 = vi.fn();
      manager.subscribe("room", l1);
      manager.subscribe("room", l2);

      manager.applyEvent({
        type: "join",
        node_id: "peer-1",
        topic: "room",
        data: {},
      });

      expect(l1).toHaveBeenCalledTimes(1);
      expect(l2).toHaveBeenCalledTimes(1);
    });

    it("cleans up listener set when last listener unsubscribes", () => {
      const listener = vi.fn();
      const unsub = manager.subscribe("room", listener);

      unsub();

      // Verify no error occurs with no listeners
      manager.applyEvent({
        type: "join",
        node_id: "peer-1",
        topic: "room",
        data: {},
      });
    });
  });

  describe("subscribeAll()", () => {
    it("notifies on events for any topic", () => {
      const listener = vi.fn();
      manager.subscribeAll(listener);

      manager.applyEvent({
        type: "join",
        node_id: "peer-1",
        topic: "room-a",
        data: {},
      });
      manager.applyEvent({
        type: "join",
        node_id: "peer-2",
        topic: "room-b",
        data: {},
      });

      expect(listener).toHaveBeenCalledTimes(2);
    });

    it("unsubscribes correctly", () => {
      const listener = vi.fn();
      const unsub = manager.subscribeAll(listener);

      unsub();

      manager.applyEvent({
        type: "join",
        node_id: "peer-1",
        topic: "room",
        data: {},
      });

      expect(listener).not.toHaveBeenCalled();
    });

    it("notifies both topic and global listeners", () => {
      const topicListener = vi.fn();
      const globalListener = vi.fn();
      manager.subscribe("room", topicListener);
      manager.subscribeAll(globalListener);

      manager.applyEvent({
        type: "join",
        node_id: "peer-1",
        topic: "room",
        data: {},
      });

      expect(topicListener).toHaveBeenCalledTimes(1);
      expect(globalListener).toHaveBeenCalledTimes(1);
    });
  });

  describe("clear()", () => {
    it("removes all presence state", () => {
      manager.applyEvent({
        type: "join",
        node_id: "peer-1",
        topic: "room-a",
        data: {},
      });
      manager.applyEvent({
        type: "join",
        node_id: "peer-2",
        topic: "room-b",
        data: {},
      });

      manager.clear();

      expect(manager.getPresence("room-a")).toEqual([]);
      expect(manager.getPresence("room-b")).toEqual([]);
    });

    it("notifies topic listeners on clear", () => {
      const listener = vi.fn();
      manager.subscribe("room-a", listener);

      manager.applyEvent({
        type: "join",
        node_id: "peer-1",
        topic: "room-a",
        data: {},
      });
      listener.mockClear();

      manager.clear();

      expect(listener).toHaveBeenCalledTimes(1);
    });

    it("notifies global listeners on clear", () => {
      const listener = vi.fn();
      manager.subscribeAll(listener);

      manager.applyEvent({
        type: "join",
        node_id: "peer-1",
        topic: "room-a",
        data: {},
      });
      listener.mockClear();

      manager.clear();

      expect(listener).toHaveBeenCalledTimes(1);
    });
  });
});

// ---------------------------------------------------------------------------
// HttpTransport presence methods
// ---------------------------------------------------------------------------

describe("HttpTransport presence", () => {
  describe("updatePresence()", () => {
    it("sends POST to /presence with correct body", async () => {
      const fetchFn = mockFetch({});
      const transport = new HttpTransport({
        baseURL: "https://api.example.com/sync",
        fetch: fetchFn,
      });

      await transport.updatePresence({
        node_id: "n1",
        topic: "docs:1",
        data: { name: "Alice" },
      });

      const [url, opts] = (fetchFn as ReturnType<typeof vi.fn>).mock.calls[0];
      expect(url).toBe("https://api.example.com/sync/presence");
      expect(opts.method).toBe("POST");
      const body = JSON.parse(opts.body);
      expect(body.node_id).toBe("n1");
      expect(body.topic).toBe("docs:1");
      expect(body.data).toEqual({ name: "Alice" });
    });

    it("throws TransportError on non-ok response", async () => {
      const fetchFn = mockFetch("error", 500);
      const transport = new HttpTransport({
        baseURL: "https://api.example.com/sync",
        fetch: fetchFn,
      });

      await expect(
        transport.updatePresence({
          node_id: "n1",
          topic: "docs:1",
          data: {},
        })
      ).rejects.toThrow(/500/);
    });

    it("includes auth headers", async () => {
      const fetchFn = mockFetch({});
      const transport = new HttpTransport({
        baseURL: "https://api.example.com/sync",
        fetch: fetchFn,
        auth: {
          getHeaders: () => ({ Authorization: "Bearer token123" }),
        },
      });

      await transport.updatePresence({
        node_id: "n1",
        topic: "docs:1",
        data: {},
      });

      const opts = (fetchFn as ReturnType<typeof vi.fn>).mock.calls[0][1];
      expect(opts.headers.Authorization).toBe("Bearer token123");
    });
  });

  describe("getPresence()", () => {
    it("sends GET to /presence with topic query param", async () => {
      const snapshot: PresenceSnapshot = {
        topic: "docs:1",
        states: [
          {
            node_id: "peer-1",
            topic: "docs:1",
            data: { name: "Alice" },
            updated_at: Date.now(),
          },
        ],
      };
      const fetchFn = mockFetch(snapshot);
      const transport = new HttpTransport({
        baseURL: "https://api.example.com/sync",
        fetch: fetchFn,
      });

      const result = await transport.getPresence("docs:1");

      const [url, opts] = (fetchFn as ReturnType<typeof vi.fn>).mock.calls[0];
      expect(url).toBe(
        "https://api.example.com/sync/presence?topic=docs%3A1"
      );
      expect(opts.method).toBe("GET");
      expect(result).toHaveLength(1);
      expect(result[0].node_id).toBe("peer-1");
    });

    it("throws TransportError on non-ok response", async () => {
      const fetchFn = mockFetch("not found", 404);
      const transport = new HttpTransport({
        baseURL: "https://api.example.com/sync",
        fetch: fetchFn,
      });

      await expect(transport.getPresence("docs:1")).rejects.toThrow(/404/);
    });

    it("includes auth headers", async () => {
      const snapshot: PresenceSnapshot = { topic: "t", states: [] };
      const fetchFn = mockFetch(snapshot);
      const transport = new HttpTransport({
        baseURL: "https://api.example.com/sync",
        fetch: fetchFn,
        auth: {
          getHeaders: () => ({ Authorization: "Bearer xyz" }),
        },
      });

      await transport.getPresence("t");

      const opts = (fetchFn as ReturnType<typeof vi.fn>).mock.calls[0][1];
      expect(opts.headers.Authorization).toBe("Bearer xyz");
    });

    it("encodes special characters in topic", async () => {
      const snapshot: PresenceSnapshot = { topic: "a b&c", states: [] };
      const fetchFn = mockFetch(snapshot);
      const transport = new HttpTransport({
        baseURL: "https://api.example.com/sync",
        fetch: fetchFn,
      });

      await transport.getPresence("a b&c");

      const url = (fetchFn as ReturnType<typeof vi.fn>).mock.calls[0][0];
      expect(url).toContain("topic=a%20b%26c");
    });
  });
});

// ---------------------------------------------------------------------------
// CRDTClient presence methods
// ---------------------------------------------------------------------------

describe("CRDTClient presence", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    vi.useRealTimers();
  });

  describe("updatePresence()", () => {
    it("sends presence update via transport", async () => {
      const fetchFn = mockFetch({});
      const client = createClient(fetchFn, { presence: {} });

      await client.updatePresence("docs:1", { name: "Alice" });

      const calls = (fetchFn as ReturnType<typeof vi.fn>).mock.calls;
      // Find the POST to /presence
      const presenceCall = calls.find(
        (args) => typeof args[0] === "string" && args[0].includes("/presence")
      );
      expect(presenceCall).toBeDefined();
      const body = JSON.parse(presenceCall![1].body);
      expect(body.node_id).toBe("test-node");
      expect(body.topic).toBe("docs:1");
      expect(body.data).toEqual({ name: "Alice" });
    });

    it("throws when transport does not support presence", async () => {
      const transport: Transport = {
        pull: vi.fn().mockResolvedValue({ changes: [], latest_hlc: { ts: 1, c: 0, node: "s" } }),
        push: vi.fn().mockResolvedValue({ merged: 0, latest_hlc: { ts: 1, c: 0, node: "s" } }),
        // no updatePresence
      };

      const client = new CRDTClient({
        nodeID: "test-node",
        transport,
      });

      await expect(
        client.updatePresence("docs:1", { name: "Alice" })
      ).rejects.toThrow(/does not support presence/);
    });

    it("starts heartbeat after update", async () => {
      vi.useFakeTimers();
      const fetchFn = mockFetch({});
      const client = createClient(fetchFn, {
        presence: { heartbeatInterval: 100 },
      });

      await client.updatePresence("docs:1", { cursor: { x: 10, y: 20 } });
      (fetchFn as ReturnType<typeof vi.fn>).mockClear();

      // Advance past one heartbeat interval
      vi.advanceTimersByTime(105);
      // Flush the microtask queue for the promise inside the interval
      await new Promise((r) => Promise.resolve().then(r));

      const calls = (fetchFn as ReturnType<typeof vi.fn>).mock.calls;
      expect(calls.length).toBeGreaterThanOrEqual(1);

      // Clean up to prevent leaking timers
      await client.leavePresence("docs:1");
    });

    it("heartbeat re-sends last presence data", async () => {
      vi.useFakeTimers();
      const fetchFn = mockFetch({});
      const client = createClient(fetchFn, {
        presence: { heartbeatInterval: 100 },
      });

      await client.updatePresence("docs:1", { cursor: { x: 5 } });
      (fetchFn as ReturnType<typeof vi.fn>).mockClear();

      vi.advanceTimersByTime(105);
      await new Promise((r) => Promise.resolve().then(r));

      const presenceCalls = (fetchFn as ReturnType<typeof vi.fn>).mock.calls.filter(
        (args) => typeof args[0] === "string" && args[0].includes("/presence")
      );
      expect(presenceCalls.length).toBeGreaterThanOrEqual(1);
      const body = JSON.parse(presenceCalls[0][1].body);
      expect(body.data).toEqual({ cursor: { x: 5 } });

      // Clean up
      await client.leavePresence("docs:1");
    });
  });

  describe("leavePresence()", () => {
    it("sends null data to server", async () => {
      const fetchFn = mockFetch({});
      const client = createClient(fetchFn, { presence: {} });

      await client.updatePresence("docs:1", { name: "Alice" });
      (fetchFn as ReturnType<typeof vi.fn>).mockClear();

      await client.leavePresence("docs:1");

      const calls = (fetchFn as ReturnType<typeof vi.fn>).mock.calls;
      const presenceCall = calls.find(
        (args) => typeof args[0] === "string" && args[0].includes("/presence")
      );
      expect(presenceCall).toBeDefined();
      const body = JSON.parse(presenceCall![1].body);
      expect(body.data).toBeNull();
    });

    it("stops heartbeat", async () => {
      vi.useFakeTimers();
      const fetchFn = mockFetch({});
      const client = createClient(fetchFn, {
        presence: { heartbeatInterval: 100 },
      });

      await client.updatePresence("docs:1", { name: "Alice" });
      await client.leavePresence("docs:1");
      (fetchFn as ReturnType<typeof vi.fn>).mockClear();

      // Advance past heartbeat interval — should NOT fire
      vi.advanceTimersByTime(200);
      await vi.runAllTimersAsync();

      const presenceCalls = (fetchFn as ReturnType<typeof vi.fn>).mock.calls.filter(
        (args) => typeof args[0] === "string" && args[0].includes("/presence")
      );
      expect(presenceCalls).toHaveLength(0);
    });
  });

  describe("getPresence()", () => {
    it("fetches presence from server via transport", async () => {
      const snapshot: PresenceSnapshot = {
        topic: "docs:1",
        states: [
          {
            node_id: "peer-1",
            topic: "docs:1",
            data: { name: "Alice" },
            updated_at: 1000,
          },
        ],
      };
      // First call will be for pull (not used), subsequent for getPresence
      const fetchFn = mockFetch(snapshot);
      const client = createClient(fetchFn);

      const result = await client.getPresence("docs:1");
      expect(result).toHaveLength(1);
      expect(result[0].node_id).toBe("peer-1");
    });

    it("throws when transport does not support getPresence", async () => {
      const transport: Transport = {
        pull: vi.fn().mockResolvedValue({ changes: [], latest_hlc: { ts: 1, c: 0, node: "s" } }),
        push: vi.fn().mockResolvedValue({ merged: 0, latest_hlc: { ts: 1, c: 0, node: "s" } }),
        // no getPresence
      };

      const client = new CRDTClient({
        nodeID: "test-node",
        transport,
      });

      await expect(client.getPresence("docs:1")).rejects.toThrow(
        /does not support presence/
      );
    });
  });

  describe("leaveAllPresence()", () => {
    it("leaves all active topics", async () => {
      vi.useFakeTimers();
      const fetchFn = mockFetch({});
      const client = createClient(fetchFn, {
        presence: { heartbeatInterval: 100 },
      });

      await client.updatePresence("docs:1", { name: "Alice" });
      await client.updatePresence("docs:2", { name: "Alice" });
      (fetchFn as ReturnType<typeof vi.fn>).mockClear();

      await client.leaveAllPresence();

      // Should have sent leave for both topics
      const presenceCalls = (fetchFn as ReturnType<typeof vi.fn>).mock.calls.filter(
        (args) => typeof args[0] === "string" && args[0].includes("/presence")
      );
      expect(presenceCalls).toHaveLength(2);

      // All should have null data
      for (const call of presenceCalls) {
        const body = JSON.parse(call[1].body);
        expect(body.data).toBeNull();
      }

      // Heartbeats should be stopped
      (fetchFn as ReturnType<typeof vi.fn>).mockClear();
      vi.advanceTimersByTime(200);
      await vi.runAllTimersAsync();

      const heartbeatCalls = (fetchFn as ReturnType<typeof vi.fn>).mock.calls.filter(
        (args) => typeof args[0] === "string" && args[0].includes("/presence")
      );
      expect(heartbeatCalls).toHaveLength(0);
    });
  });

  describe("presence field", () => {
    it("exposes PresenceManager on client", () => {
      const fetchFn = mockFetch({});
      const client = createClient(fetchFn);
      expect(client.presence).toBeInstanceOf(PresenceManager);
    });

    it("PresenceManager uses client's nodeID", () => {
      const fetchFn = mockFetch({});
      const client = createClient(fetchFn);

      // Apply an event for the local node — should be excluded
      client.presence.applyEvent({
        type: "join",
        node_id: "test-node",
        topic: "room",
        data: {},
      });

      expect(client.presence.getPresence("room")).toHaveLength(0);
    });

    it("PresenceManager returns remote peers", () => {
      const fetchFn = mockFetch({});
      const client = createClient(fetchFn);

      client.presence.applyEvent({
        type: "join",
        node_id: "remote-peer",
        topic: "room",
        data: { name: "Bob" },
      });

      const peers = client.presence.getPresence("room");
      expect(peers).toHaveLength(1);
      expect(peers[0].node_id).toBe("remote-peer");
    });
  });
});
