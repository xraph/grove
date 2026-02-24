import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { CRDTStream } from "../stream.js";
import type { CRDTStreamEvent } from "../stream.js";
import type { HLC, ChangeRecord } from "../types.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function createSSEStream(chunks: string[]): ReadableStream<Uint8Array> {
  const encoder = new TextEncoder();
  let index = 0;
  return new ReadableStream({
    pull(controller) {
      if (index < chunks.length) {
        controller.enqueue(encoder.encode(chunks[index++]));
      } else {
        controller.close();
      }
    },
  });
}

function mockSSEFetch(
  chunks: string[],
  status = 200
): typeof fetch {
  return vi.fn().mockResolvedValue({
    ok: status >= 200 && status < 300,
    status,
    body: createSSEStream(chunks),
    text: () => Promise.resolve("error body"),
  }) as unknown as typeof fetch;
}

function sampleChange(ts = 100): ChangeRecord {
  return {
    table: "users",
    pk: "1",
    field: "name",
    crdt_type: "lww",
    hlc: { ts, c: 0, node: "server" },
    node_id: "server",
    value: "Alice",
  };
}

function sseEvent(type: string, data: unknown): string {
  return `event:${type}\ndata:${JSON.stringify(data)}\n\n`;
}

/** Wait for all microtasks to settle. */
function flush(): Promise<void> {
  return new Promise((r) => setTimeout(r, 10));
}

// ---------------------------------------------------------------------------
// Constructor / initial state
// ---------------------------------------------------------------------------

describe("CRDTStream", () => {
  describe("constructor and initial state", () => {
    it("initializes as disconnected", () => {
      const stream = new CRDTStream("https://api.example.com");
      expect(stream.connected).toBe(false);
    });

    it("initializes lastHLC as null", () => {
      const stream = new CRDTStream("https://api.example.com");
      expect(stream.lastHLC).toBeNull();
    });
  });

  // ---------------------------------------------------------------------------
  // on()
  // ---------------------------------------------------------------------------

  describe("on()", () => {
    it("registers an event handler and returns unsubscribe", () => {
      const change = sampleChange();
      const chunks = [sseEvent("change", change)];
      const fetchFn = mockSSEFetch(chunks);
      const stream = new CRDTStream("https://api.example.com", {}, {}, fetchFn);

      const events: CRDTStreamEvent[] = [];
      const unsub = stream.on((e) => events.push(e));
      expect(typeof unsub).toBe("function");
    });

    it("supports multiple handlers", async () => {
      const change = sampleChange();
      const chunks = [sseEvent("change", change)];
      const fetchFn = mockSSEFetch(chunks);
      const stream = new CRDTStream(
        "https://api.example.com",
        { reconnectDelay: 50 },
        {},
        fetchFn
      );

      const events1: CRDTStreamEvent[] = [];
      const events2: CRDTStreamEvent[] = [];
      stream.on((e) => events1.push(e));
      stream.on((e) => events2.push(e));
      stream.connect();
      await flush();
      stream.disconnect();

      // Both should have received the "connected" event
      expect(events1.some((e) => e.type === "connected")).toBe(true);
      expect(events2.some((e) => e.type === "connected")).toBe(true);
    });
  });

  // ---------------------------------------------------------------------------
  // connect()
  // ---------------------------------------------------------------------------

  describe("connect()", () => {
    it("initiates SSE connection via fetch GET", async () => {
      const chunks = [": keepalive\n\n"];
      const fetchFn = mockSSEFetch(chunks);
      const stream = new CRDTStream(
        "https://api.example.com",
        { reconnectDelay: 50 },
        {},
        fetchFn
      );

      stream.connect();
      await flush();
      stream.disconnect();

      expect(fetchFn).toHaveBeenCalled();
      const [, opts] = (fetchFn as any).mock.calls[0];
      expect(opts.method).toBe("GET");
      expect(opts.headers["Accept"]).toBe("text/event-stream");
    });

    it("emits 'connected' event on successful connection", async () => {
      const chunks = [": keepalive\n\n"];
      const fetchFn = mockSSEFetch(chunks);
      const stream = new CRDTStream(
        "https://api.example.com",
        { reconnectDelay: 50 },
        {},
        fetchFn
      );

      const events: CRDTStreamEvent[] = [];
      stream.on((e) => events.push(e));
      stream.connect();
      await flush();
      stream.disconnect();

      expect(events.some((e) => e.type === "connected")).toBe(true);
    });

    it("sets connected to true", async () => {
      const chunks: string[] = [];
      // Create a fetch that returns a stream that hangs (never closes)
      const fetchFn = vi.fn().mockResolvedValue({
        ok: true,
        status: 200,
        body: new ReadableStream({
          start() {
            // Never close — keeps stream open
          },
        }),
        text: () => Promise.resolve(""),
      }) as unknown as typeof fetch;

      const stream = new CRDTStream(
        "https://api.example.com",
        { reconnectDelay: 50 },
        {},
        fetchFn
      );

      const events: CRDTStreamEvent[] = [];
      stream.on((e) => events.push(e));
      stream.connect();
      await flush();

      expect(stream.connected).toBe(true);
      stream.disconnect();
    });
  });

  // ---------------------------------------------------------------------------
  // disconnect()
  // ---------------------------------------------------------------------------

  describe("disconnect()", () => {
    it("emits 'disconnected' event when connected", async () => {
      const fetchFn = vi.fn().mockResolvedValue({
        ok: true,
        status: 200,
        body: new ReadableStream({ start() {} }),
        text: () => Promise.resolve(""),
      }) as unknown as typeof fetch;

      const stream = new CRDTStream(
        "https://api.example.com",
        { reconnectDelay: 50 },
        {},
        fetchFn
      );

      const events: CRDTStreamEvent[] = [];
      stream.on((e) => events.push(e));
      stream.connect();
      await flush();
      stream.disconnect();

      expect(events.some((e) => e.type === "disconnected")).toBe(true);
    });

    it("sets connected to false", async () => {
      const fetchFn = vi.fn().mockResolvedValue({
        ok: true,
        status: 200,
        body: new ReadableStream({ start() {} }),
        text: () => Promise.resolve(""),
      }) as unknown as typeof fetch;

      const stream = new CRDTStream(
        "https://api.example.com",
        { reconnectDelay: 50 },
        {},
        fetchFn
      );

      stream.connect();
      await flush();
      stream.disconnect();
      expect(stream.connected).toBe(false);
    });

    it("is safe to call when not connected", () => {
      const stream = new CRDTStream("https://api.example.com");
      expect(() => stream.disconnect()).not.toThrow();
    });
  });

  // ---------------------------------------------------------------------------
  // SSE parsing
  // ---------------------------------------------------------------------------

  describe("SSE parsing", () => {
    it("parses single 'change' event", async () => {
      const change = sampleChange(100);
      const chunks = [sseEvent("change", change)];
      const fetchFn = mockSSEFetch(chunks);
      const stream = new CRDTStream(
        "https://api.example.com",
        { reconnectDelay: 50 },
        {},
        fetchFn
      );

      const events: CRDTStreamEvent[] = [];
      stream.on((e) => events.push(e));
      stream.connect();
      await flush();
      stream.disconnect();

      const changeEvents = events.filter((e) => e.type === "change");
      expect(changeEvents).toHaveLength(1);
      expect((changeEvents[0] as any).data.value).toBe("Alice");
    });

    it("parses 'changes' event with array", async () => {
      const changes = [sampleChange(100), sampleChange(200)];
      const chunks = [sseEvent("changes", changes)];
      const fetchFn = mockSSEFetch(chunks);
      const stream = new CRDTStream(
        "https://api.example.com",
        { reconnectDelay: 50 },
        {},
        fetchFn
      );

      const events: CRDTStreamEvent[] = [];
      stream.on((e) => events.push(e));
      stream.connect();
      await flush();
      stream.disconnect();

      const changesEvents = events.filter((e) => e.type === "changes");
      expect(changesEvents).toHaveLength(1);
      expect((changesEvents[0] as any).data).toHaveLength(2);
    });

    it("ignores SSE comments (lines starting with :)", async () => {
      const chunks = [": keepalive\n\n"];
      const fetchFn = mockSSEFetch(chunks);
      const stream = new CRDTStream(
        "https://api.example.com",
        { reconnectDelay: 50 },
        {},
        fetchFn
      );

      const events: CRDTStreamEvent[] = [];
      stream.on((e) => events.push(e));
      stream.connect();
      await flush();
      stream.disconnect();

      // Should only have connected + disconnected, no change/changes/error
      const dataEvents = events.filter(
        (e) => e.type === "change" || e.type === "changes"
      );
      expect(dataEvents).toHaveLength(0);
    });

    it("handles partial chunks correctly", async () => {
      const change = sampleChange(300);
      const full = sseEvent("change", change);
      // Split in the middle
      const mid = Math.floor(full.length / 2);
      const chunks = [full.slice(0, mid), full.slice(mid)];
      const fetchFn = mockSSEFetch(chunks);
      const stream = new CRDTStream(
        "https://api.example.com",
        { reconnectDelay: 50 },
        {},
        fetchFn
      );

      const events: CRDTStreamEvent[] = [];
      stream.on((e) => events.push(e));
      stream.connect();
      await flush();
      stream.disconnect();

      const changeEvents = events.filter((e) => e.type === "change");
      expect(changeEvents).toHaveLength(1);
    });

    it("emits error event on malformed JSON", async () => {
      const chunks = ["event:change\ndata:not-valid-json\n\n"];
      const fetchFn = mockSSEFetch(chunks);
      const stream = new CRDTStream(
        "https://api.example.com",
        { reconnectDelay: 50 },
        {},
        fetchFn
      );

      const events: CRDTStreamEvent[] = [];
      stream.on((e) => events.push(e));
      stream.connect();
      await flush();
      stream.disconnect();

      const errorEvents = events.filter((e) => e.type === "error");
      expect(errorEvents.length).toBeGreaterThanOrEqual(1);
    });

    it("ignores unknown event types", async () => {
      const chunks = ['event:unknown\ndata:{"foo":"bar"}\n\n'];
      const fetchFn = mockSSEFetch(chunks);
      const stream = new CRDTStream(
        "https://api.example.com",
        { reconnectDelay: 50 },
        {},
        fetchFn
      );

      const events: CRDTStreamEvent[] = [];
      stream.on((e) => events.push(e));
      stream.connect();
      await flush();
      stream.disconnect();

      const dataEvents = events.filter(
        (e) => e.type === "change" || e.type === "changes"
      );
      expect(dataEvents).toHaveLength(0);
    });
  });

  // ---------------------------------------------------------------------------
  // lastHLC tracking
  // ---------------------------------------------------------------------------

  describe("lastHLC tracking", () => {
    it("updates lastHLC from single change event", async () => {
      const change = sampleChange(500);
      const chunks = [sseEvent("change", change)];
      const fetchFn = mockSSEFetch(chunks);
      const stream = new CRDTStream(
        "https://api.example.com",
        { reconnectDelay: 50 },
        {},
        fetchFn
      );

      stream.on(() => {});
      stream.connect();
      await flush();
      stream.disconnect();

      expect(stream.lastHLC).not.toBeNull();
      expect(stream.lastHLC!.ts).toBe(500);
    });

    it("updates lastHLC to highest from changes event", async () => {
      const changes = [sampleChange(100), sampleChange(500), sampleChange(300)];
      const chunks = [sseEvent("changes", changes)];
      const fetchFn = mockSSEFetch(chunks);
      const stream = new CRDTStream(
        "https://api.example.com",
        { reconnectDelay: 50 },
        {},
        fetchFn
      );

      stream.on(() => {});
      stream.connect();
      await flush();
      stream.disconnect();

      expect(stream.lastHLC!.ts).toBe(500);
    });

    it("does not regress lastHLC", async () => {
      const chunks = [
        sseEvent("change", sampleChange(500)),
        sseEvent("change", sampleChange(100)),
      ];
      const fetchFn = mockSSEFetch(chunks);
      const stream = new CRDTStream(
        "https://api.example.com",
        { reconnectDelay: 50 },
        {},
        fetchFn
      );

      stream.on(() => {});
      stream.connect();
      await flush();
      stream.disconnect();

      expect(stream.lastHLC!.ts).toBe(500);
    });
  });

  // ---------------------------------------------------------------------------
  // buildStreamURL (tested indirectly via connect)
  // ---------------------------------------------------------------------------

  describe("buildStreamURL", () => {
    it("appends /stream to baseURL", async () => {
      const chunks = [": keep\n\n"];
      const fetchFn = mockSSEFetch(chunks);
      const stream = new CRDTStream(
        "https://api.example.com",
        { reconnectDelay: 50 },
        {},
        fetchFn
      );

      stream.connect();
      await flush();
      stream.disconnect();

      const url = (fetchFn as any).mock.calls[0][0] as string;
      expect(url).toContain("/stream");
    });

    it("includes tables as comma-separated query param", async () => {
      const chunks = [": keep\n\n"];
      const fetchFn = mockSSEFetch(chunks);
      const stream = new CRDTStream(
        "https://api.example.com",
        { tables: ["users", "posts"], reconnectDelay: 50 },
        {},
        fetchFn
      );

      stream.connect();
      await flush();
      stream.disconnect();

      const url = (fetchFn as any).mock.calls[0][0] as string;
      expect(url).toContain("tables=users%2Cposts");
    });

    it("includes since params from initial config.since", async () => {
      const chunks = [": keep\n\n"];
      const fetchFn = mockSSEFetch(chunks);
      const since: HLC = { ts: 100, c: 5, node: "n1" };
      const stream = new CRDTStream(
        "https://api.example.com",
        { since, reconnectDelay: 50 },
        {},
        fetchFn
      );

      stream.connect();
      await flush();
      stream.disconnect();

      const url = (fetchFn as any).mock.calls[0][0] as string;
      expect(url).toContain("since_ts=100");
      expect(url).toContain("since_count=5");
      expect(url).toContain("since_node=n1");
    });

    it("omits since params when HLC is zero", async () => {
      const chunks = [": keep\n\n"];
      const fetchFn = mockSSEFetch(chunks);
      const stream = new CRDTStream(
        "https://api.example.com",
        { reconnectDelay: 50 },
        {},
        fetchFn
      );

      stream.connect();
      await flush();
      stream.disconnect();

      const url = (fetchFn as any).mock.calls[0][0] as string;
      expect(url).not.toContain("since_ts");
    });

    it("omits query string when no params needed", async () => {
      const chunks = [": keep\n\n"];
      const fetchFn = mockSSEFetch(chunks);
      const stream = new CRDTStream(
        "https://api.example.com",
        { reconnectDelay: 50 },
        {},
        fetchFn
      );

      stream.connect();
      await flush();
      stream.disconnect();

      const url = (fetchFn as any).mock.calls[0][0] as string;
      expect(url).toBe("https://api.example.com/stream");
    });
  });

  // ---------------------------------------------------------------------------
  // Reconnection
  // ---------------------------------------------------------------------------

  describe("reconnection", () => {
    it("emits error on non-ok response status", async () => {
      const fetchFn = mockSSEFetch([], 500);
      const stream = new CRDTStream(
        "https://api.example.com",
        { reconnectDelay: 50 },
        {},
        fetchFn
      );

      const events: CRDTStreamEvent[] = [];
      stream.on((e) => events.push(e));
      stream.connect();
      await flush();
      stream.disconnect();

      const errorEvents = events.filter((e) => e.type === "error");
      expect(errorEvents.length).toBeGreaterThanOrEqual(1);
    });

    it("handler errors do not crash the stream", async () => {
      const change = sampleChange(100);
      const chunks = [sseEvent("change", change)];
      const fetchFn = mockSSEFetch(chunks);
      const stream = new CRDTStream(
        "https://api.example.com",
        { reconnectDelay: 50 },
        {},
        fetchFn
      );

      // Register handler that throws
      stream.on(() => {
        throw new Error("handler error");
      });

      // Register a second handler to verify it still gets called
      const events: CRDTStreamEvent[] = [];
      stream.on((e) => events.push(e));

      stream.connect();
      await flush();
      stream.disconnect();

      // Second handler should still have received events despite first one throwing
      expect(events.length).toBeGreaterThan(0);
    });

    it("includes custom headers in fetch request", async () => {
      const chunks = [": keep\n\n"];
      const fetchFn = mockSSEFetch(chunks);
      const stream = new CRDTStream(
        "https://api.example.com",
        { reconnectDelay: 50 },
        { Authorization: "Bearer token" },
        fetchFn
      );

      stream.connect();
      await flush();
      stream.disconnect();

      const opts = (fetchFn as any).mock.calls[0][1];
      expect(opts.headers["Authorization"]).toBe("Bearer token");
    });
  });
});
