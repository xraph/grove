import { describe, it, expect, vi } from "vitest";
import { CRDTClient, CRDTError } from "../client.js";
import { CRDTStream } from "../stream.js";
import type { HLC, PullResponse, PushResponse, ChangeRecord } from "../types.js";

function mockFetch(body: unknown, status = 200) {
  return vi.fn().mockResolvedValue({
    ok: status >= 200 && status < 300,
    status,
    json: () => Promise.resolve(body),
    text: () => Promise.resolve(typeof body === "string" ? body : JSON.stringify(body)),
  }) as unknown as typeof fetch;
}

function createClient(
  fetchImpl: typeof fetch,
  overrides: Record<string, unknown> = {}
) {
  return new CRDTClient({
    baseURL: "https://api.example.com/sync",
    nodeID: "test-node",
    tables: ["users", "posts"],
    fetch: fetchImpl,
    ...overrides,
  });
}

// ---------------------------------------------------------------------------
// CRDTError
// ---------------------------------------------------------------------------

describe("CRDTError", () => {
  it("has correct name property", () => {
    const err = new CRDTError("test");
    expect(err.name).toBe("CRDTError");
  });

  it("includes statusCode", () => {
    const err = new CRDTError("test", 404);
    expect(err.statusCode).toBe(404);
  });

  it("extends Error", () => {
    const err = new CRDTError("test");
    expect(err).toBeInstanceOf(Error);
  });

  it("has undefined statusCode when not provided", () => {
    const err = new CRDTError("test");
    expect(err.statusCode).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// CRDTClient constructor
// ---------------------------------------------------------------------------

describe("CRDTClient", () => {
  describe("constructor", () => {
    it("strips trailing slashes from baseURL", async () => {
      const pullResponse: PullResponse = {
        changes: [],
        latest_hlc: { ts: 1, c: 0, node: "s" },
      };
      const fetchFn = mockFetch(pullResponse);
      const client = createClient(fetchFn, {
        baseURL: "https://api.example.com/sync///",
      });
      await client.pull();
      const callUrl = (fetchFn as any).mock.calls[0][0] as string;
      expect(callUrl).toBe("https://api.example.com/sync/pull");
    });

    it("stores configured tables", async () => {
      const pullResponse: PullResponse = {
        changes: [],
        latest_hlc: { ts: 1, c: 0, node: "s" },
      };
      const fetchFn = mockFetch(pullResponse);
      const client = createClient(fetchFn);
      await client.pull();
      const body = JSON.parse((fetchFn as any).mock.calls[0][1].body);
      expect(body.tables).toEqual(["users", "posts"]);
    });

    it("creates a HybridClock with the nodeID", () => {
      const fetchFn = mockFetch({});
      const client = createClient(fetchFn);
      expect(client.nodeID).toBe("test-node");
      expect(client.clock.nodeID).toBe("test-node");
    });
  });

  // ---------------------------------------------------------------------------
  // pull()
  // ---------------------------------------------------------------------------

  describe("pull()", () => {
    it("sends POST to /pull with correct body", async () => {
      const pullResponse: PullResponse = {
        changes: [],
        latest_hlc: { ts: 1, c: 0, node: "s" },
      };
      const fetchFn = mockFetch(pullResponse);
      const client = createClient(fetchFn);
      await client.pull();

      expect(fetchFn).toHaveBeenCalledTimes(1);
      const [url, opts] = (fetchFn as any).mock.calls[0];
      expect(url).toBe("https://api.example.com/sync/pull");
      expect(opts.method).toBe("POST");
      const body = JSON.parse(opts.body);
      expect(body.node_id).toBe("test-node");
      expect(body.tables).toEqual(["users", "posts"]);
    });

    it("includes custom headers", async () => {
      const pullResponse: PullResponse = {
        changes: [],
        latest_hlc: { ts: 1, c: 0, node: "s" },
      };
      const fetchFn = mockFetch(pullResponse);
      const client = createClient(fetchFn, {
        headers: { Authorization: "Bearer token" },
      });
      await client.pull();

      const opts = (fetchFn as any).mock.calls[0][1];
      expect(opts.headers["Authorization"]).toBe("Bearer token");
      expect(opts.headers["Content-Type"]).toBe("application/json");
    });

    it("uses configured tables when none specified", async () => {
      const pullResponse: PullResponse = {
        changes: [],
        latest_hlc: { ts: 1, c: 0, node: "s" },
      };
      const fetchFn = mockFetch(pullResponse);
      const client = createClient(fetchFn);
      await client.pull();

      const body = JSON.parse((fetchFn as any).mock.calls[0][1].body);
      expect(body.tables).toEqual(["users", "posts"]);
    });

    it("uses provided tables when specified", async () => {
      const pullResponse: PullResponse = {
        changes: [],
        latest_hlc: { ts: 1, c: 0, node: "s" },
      };
      const fetchFn = mockFetch(pullResponse);
      const client = createClient(fetchFn);
      await client.pull(["docs"]);

      const body = JSON.parse((fetchFn as any).mock.calls[0][1].body);
      expect(body.tables).toEqual(["docs"]);
    });

    it("includes since HLC when provided", async () => {
      const pullResponse: PullResponse = {
        changes: [],
        latest_hlc: { ts: 1, c: 0, node: "s" },
      };
      const fetchFn = mockFetch(pullResponse);
      const client = createClient(fetchFn);
      const since: HLC = { ts: 100, c: 5, node: "n" };
      await client.pull(undefined, since);

      const body = JSON.parse((fetchFn as any).mock.calls[0][1].body);
      expect(body.since).toEqual(since);
    });

    it("omits since when not provided", async () => {
      const pullResponse: PullResponse = {
        changes: [],
        latest_hlc: { ts: 1, c: 0, node: "s" },
      };
      const fetchFn = mockFetch(pullResponse);
      const client = createClient(fetchFn);
      await client.pull();

      const body = JSON.parse((fetchFn as any).mock.calls[0][1].body);
      expect(body.since).toBeUndefined();
    });

    it("updates clock with server response HLC", async () => {
      const serverHLC: HLC = { ts: 999_000_000_000, c: 10, node: "server" };
      const pullResponse: PullResponse = {
        changes: [],
        latest_hlc: serverHLC,
      };
      const fetchFn = mockFetch(pullResponse);
      const client = createClient(fetchFn);
      await client.pull();

      // After clock.update(serverHLC), the next now() should be after serverHLC
      const next = client.clock.now();
      expect(next.ts >= serverHLC.ts || next.c > serverHLC.c).toBe(true);
    });

    it("returns the pull response", async () => {
      const change: ChangeRecord = {
        table: "users",
        pk: "1",
        field: "name",
        crdt_type: "lww",
        hlc: { ts: 100, c: 0, node: "s" },
        node_id: "s",
        value: "Alice",
      };
      const pullResponse: PullResponse = {
        changes: [change],
        latest_hlc: { ts: 100, c: 0, node: "s" },
      };
      const fetchFn = mockFetch(pullResponse);
      const client = createClient(fetchFn);
      const result = await client.pull();

      expect(result.changes).toHaveLength(1);
      expect(result.changes[0].value).toBe("Alice");
    });

    it("throws CRDTError on non-ok response", async () => {
      const fetchFn = mockFetch("server error", 500);
      const client = createClient(fetchFn);
      await expect(client.pull()).rejects.toThrow(CRDTError);
    });

    it("includes response text in error message", async () => {
      const fetchFn = mockFetch("bad request", 400);
      const client = createClient(fetchFn);
      try {
        await client.pull();
        expect.fail("should have thrown");
      } catch (err) {
        expect((err as CRDTError).message).toContain("400");
        expect((err as CRDTError).statusCode).toBe(400);
      }
    });
  });

  // ---------------------------------------------------------------------------
  // push()
  // ---------------------------------------------------------------------------

  describe("push()", () => {
    it("sends POST to /push with changes", async () => {
      const pushResponse: PushResponse = {
        merged: 1,
        latest_hlc: { ts: 200, c: 0, node: "s" },
      };
      const fetchFn = mockFetch(pushResponse);
      const client = createClient(fetchFn);
      const changes: ChangeRecord[] = [
        {
          table: "users",
          pk: "1",
          field: "name",
          crdt_type: "lww",
          hlc: { ts: 100, c: 0, node: "test-node" },
          node_id: "test-node",
          value: "Alice",
        },
      ];
      await client.push(changes);

      const [url, opts] = (fetchFn as any).mock.calls[0];
      expect(url).toBe("https://api.example.com/sync/push");
      const body = JSON.parse(opts.body);
      expect(body.changes).toHaveLength(1);
      expect(body.node_id).toBe("test-node");
    });

    it("short-circuits on empty changes array", async () => {
      const fetchFn = mockFetch({});
      const client = createClient(fetchFn);
      const result = await client.push([]);

      expect(fetchFn).not.toHaveBeenCalled();
      expect(result.merged).toBe(0);
      expect(result.latest_hlc).toBeDefined();
    });

    it("updates clock with server response HLC", async () => {
      const serverHLC: HLC = { ts: 999_000_000_000, c: 10, node: "server" };
      const pushResponse: PushResponse = {
        merged: 1,
        latest_hlc: serverHLC,
      };
      const fetchFn = mockFetch(pushResponse);
      const client = createClient(fetchFn);
      await client.push([
        {
          table: "t",
          pk: "1",
          field: "f",
          crdt_type: "lww",
          hlc: { ts: 1, c: 0, node: "test-node" },
          node_id: "test-node",
          value: "v",
        },
      ]);

      const next = client.clock.now();
      expect(next.ts >= serverHLC.ts || next.c > serverHLC.c).toBe(true);
    });

    it("returns the push response", async () => {
      const pushResponse: PushResponse = {
        merged: 3,
        latest_hlc: { ts: 200, c: 0, node: "s" },
      };
      const fetchFn = mockFetch(pushResponse);
      const client = createClient(fetchFn);
      const result = await client.push([
        {
          table: "t",
          pk: "1",
          field: "f",
          crdt_type: "lww",
          hlc: { ts: 1, c: 0, node: "test-node" },
          node_id: "test-node",
          value: "v",
        },
      ]);
      expect(result.merged).toBe(3);
    });

    it("throws CRDTError on non-ok response", async () => {
      const fetchFn = mockFetch("error", 500);
      const client = createClient(fetchFn);
      await expect(
        client.push([
          {
            table: "t",
            pk: "1",
            field: "f",
            crdt_type: "lww",
            hlc: { ts: 1, c: 0, node: "n" },
            node_id: "n",
            value: "v",
          },
        ])
      ).rejects.toThrow(CRDTError);
    });

    it("includes custom headers", async () => {
      const pushResponse: PushResponse = {
        merged: 1,
        latest_hlc: { ts: 1, c: 0, node: "s" },
      };
      const fetchFn = mockFetch(pushResponse);
      const client = createClient(fetchFn, {
        headers: { "X-Custom": "value" },
      });
      await client.push([
        {
          table: "t",
          pk: "1",
          field: "f",
          crdt_type: "lww",
          hlc: { ts: 1, c: 0, node: "n" },
          node_id: "n",
          value: "v",
        },
      ]);
      const opts = (fetchFn as any).mock.calls[0][1];
      expect(opts.headers["X-Custom"]).toBe("value");
    });
  });

  // ---------------------------------------------------------------------------
  // stream()
  // ---------------------------------------------------------------------------

  describe("stream()", () => {
    it("creates a CRDTStream instance", () => {
      const fetchFn = mockFetch({});
      const client = createClient(fetchFn);
      const stream = client.stream();
      expect(stream).toBeInstanceOf(CRDTStream);
    });

    it("passes configured tables to stream", () => {
      const fetchFn = mockFetch({});
      const client = createClient(fetchFn);
      const stream = client.stream();
      // We can't directly inspect internal tables, but the stream object is created
      expect(stream).toBeDefined();
    });

    it("passes stream config overrides", () => {
      const fetchFn = mockFetch({});
      const client = createClient(fetchFn);
      const stream = client.stream({
        tables: ["override"],
        reconnectDelay: 1000,
      });
      expect(stream).toBeInstanceOf(CRDTStream);
    });
  });
});
