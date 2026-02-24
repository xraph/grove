import { describe, it, expect, vi } from "vitest";
import { HttpTransport, HttpStreamTransport } from "../transport.js";
import { TransportError, CRDTError } from "../errors.js";
import { CRDTStream } from "../stream.js";
import type { PullResponse, PushResponse, AuthProvider } from "../types.js";

function mockFetch(body: unknown, status = 200) {
  return vi.fn().mockResolvedValue({
    ok: status >= 200 && status < 300,
    status,
    json: () => Promise.resolve(body),
    text: () =>
      Promise.resolve(typeof body === "string" ? body : JSON.stringify(body)),
  }) as unknown as typeof fetch;
}

// ---------------------------------------------------------------------------
// TransportError
// ---------------------------------------------------------------------------

describe("TransportError", () => {
  it("has correct name property", () => {
    const err = new TransportError("test");
    expect(err.name).toBe("TransportError");
  });

  it("includes statusCode", () => {
    const err = new TransportError("test", 404);
    expect(err.statusCode).toBe(404);
  });

  it("extends Error", () => {
    const err = new TransportError("test");
    expect(err).toBeInstanceOf(Error);
  });

  it("extends CRDTError for backward compatibility", () => {
    const err = new TransportError("test", 500);
    expect(err).toBeInstanceOf(CRDTError);
    expect(err.statusCode).toBe(500);
  });
});

// ---------------------------------------------------------------------------
// HttpTransport
// ---------------------------------------------------------------------------

describe("HttpTransport", () => {
  it("sends POST to /pull with correct body", async () => {
    const pullResponse: PullResponse = {
      changes: [],
      latest_hlc: { ts: 1, c: 0, node: "s" },
    };
    const fetchFn = mockFetch(pullResponse);
    const transport = new HttpTransport({
      baseURL: "https://api.example.com/sync",
      fetch: fetchFn,
    });

    await transport.pull({ tables: ["users"], node_id: "n1" });

    expect(fetchFn).toHaveBeenCalledTimes(1);
    const [url, opts] = (fetchFn as any).mock.calls[0];
    expect(url).toBe("https://api.example.com/sync/pull");
    expect(opts.method).toBe("POST");
    const body = JSON.parse(opts.body);
    expect(body.tables).toEqual(["users"]);
    expect(body.node_id).toBe("n1");
  });

  it("sends POST to /push with correct body", async () => {
    const pushResponse: PushResponse = {
      merged: 1,
      latest_hlc: { ts: 1, c: 0, node: "s" },
    };
    const fetchFn = mockFetch(pushResponse);
    const transport = new HttpTransport({
      baseURL: "https://api.example.com/sync",
      fetch: fetchFn,
    });

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
    await transport.push({ changes, node_id: "n1" });

    const [url, opts] = (fetchFn as any).mock.calls[0];
    expect(url).toBe("https://api.example.com/sync/push");
    const body = JSON.parse(opts.body);
    expect(body.changes).toHaveLength(1);
    expect(body.node_id).toBe("n1");
  });

  it("strips trailing slashes from baseURL", async () => {
    const pullResponse: PullResponse = {
      changes: [],
      latest_hlc: { ts: 1, c: 0, node: "s" },
    };
    const fetchFn = mockFetch(pullResponse);
    const transport = new HttpTransport({
      baseURL: "https://api.example.com/sync///",
      fetch: fetchFn,
    });

    await transport.pull({ tables: [], node_id: "n1" });
    const url = (fetchFn as any).mock.calls[0][0];
    expect(url).toBe("https://api.example.com/sync/pull");
  });

  it("includes static headers", async () => {
    const pullResponse: PullResponse = {
      changes: [],
      latest_hlc: { ts: 1, c: 0, node: "s" },
    };
    const fetchFn = mockFetch(pullResponse);
    const transport = new HttpTransport({
      baseURL: "https://api.example.com/sync",
      fetch: fetchFn,
      headers: { "X-Custom": "value" },
    });

    await transport.pull({ tables: [], node_id: "n1" });
    const opts = (fetchFn as any).mock.calls[0][1];
    expect(opts.headers["X-Custom"]).toBe("value");
    expect(opts.headers["Content-Type"]).toBe("application/json");
  });

  it("resolves auth headers dynamically", async () => {
    const pullResponse: PullResponse = {
      changes: [],
      latest_hlc: { ts: 1, c: 0, node: "s" },
    };
    const fetchFn = mockFetch(pullResponse);
    const auth: AuthProvider = {
      getHeaders: vi.fn(() => ({ Authorization: "Bearer dynamic" })),
    };
    const transport = new HttpTransport({
      baseURL: "https://api.example.com/sync",
      fetch: fetchFn,
      auth,
    });

    await transport.pull({ tables: [], node_id: "n1" });
    expect(auth.getHeaders).toHaveBeenCalledTimes(1);
    const opts = (fetchFn as any).mock.calls[0][1];
    expect(opts.headers["Authorization"]).toBe("Bearer dynamic");
  });

  it("auth headers take precedence over static headers", async () => {
    const pullResponse: PullResponse = {
      changes: [],
      latest_hlc: { ts: 1, c: 0, node: "s" },
    };
    const fetchFn = mockFetch(pullResponse);
    const auth: AuthProvider = {
      getHeaders: () => ({ Authorization: "Bearer auth-wins" }),
    };
    const transport = new HttpTransport({
      baseURL: "https://api.example.com/sync",
      fetch: fetchFn,
      headers: { Authorization: "Bearer static-loses" },
      auth,
    });

    await transport.pull({ tables: [], node_id: "n1" });
    const opts = (fetchFn as any).mock.calls[0][1];
    expect(opts.headers["Authorization"]).toBe("Bearer auth-wins");
  });

  it("supports async auth getHeaders()", async () => {
    const pullResponse: PullResponse = {
      changes: [],
      latest_hlc: { ts: 1, c: 0, node: "s" },
    };
    const fetchFn = mockFetch(pullResponse);
    const auth: AuthProvider = {
      getHeaders: async () => ({ Authorization: "Bearer async" }),
    };
    const transport = new HttpTransport({
      baseURL: "https://api.example.com/sync",
      fetch: fetchFn,
      auth,
    });

    await transport.pull({ tables: [], node_id: "n1" });
    const opts = (fetchFn as any).mock.calls[0][1];
    expect(opts.headers["Authorization"]).toBe("Bearer async");
  });

  it("throws TransportError on non-ok response", async () => {
    const fetchFn = mockFetch("server error", 500);
    const transport = new HttpTransport({
      baseURL: "https://api.example.com/sync",
      fetch: fetchFn,
    });

    await expect(
      transport.pull({ tables: [], node_id: "n1" })
    ).rejects.toThrow(TransportError);
  });

  it("error includes status code and message", async () => {
    const fetchFn = mockFetch("bad request", 400);
    const transport = new HttpTransport({
      baseURL: "https://api.example.com/sync",
      fetch: fetchFn,
    });

    try {
      await transport.pull({ tables: [], node_id: "n1" });
      expect.fail("should have thrown");
    } catch (err) {
      expect((err as TransportError).message).toContain("400");
      expect((err as TransportError).statusCode).toBe(400);
    }
  });

  it("auth getHeaders called per request", async () => {
    const pullResponse: PullResponse = {
      changes: [],
      latest_hlc: { ts: 1, c: 0, node: "s" },
    };
    const pushResponse: PushResponse = {
      merged: 0,
      latest_hlc: { ts: 1, c: 0, node: "s" },
    };
    const fetchFn = vi
      .fn()
      .mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve(pullResponse),
      })
      .mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve(pushResponse),
      }) as unknown as typeof fetch;

    const auth: AuthProvider = {
      getHeaders: vi.fn(() => ({ Authorization: "Bearer token" })),
    };
    const transport = new HttpTransport({
      baseURL: "https://api.example.com/sync",
      fetch: fetchFn,
      auth,
    });

    await transport.pull({ tables: [], node_id: "n1" });
    await transport.push({ changes: [], node_id: "n1" });
    expect(auth.getHeaders).toHaveBeenCalledTimes(2);
  });
});

// ---------------------------------------------------------------------------
// HttpStreamTransport
// ---------------------------------------------------------------------------

describe("HttpStreamTransport", () => {
  it("subscribe returns a StreamSubscription (CRDTStream instance)", () => {
    const fetchFn = mockFetch({});
    const transport = new HttpStreamTransport({
      baseURL: "https://api.example.com/sync",
      fetch: fetchFn,
    });

    const sub = transport.subscribe({ tables: ["users"] });
    expect(sub).toBeInstanceOf(CRDTStream);
    expect(sub.connected).toBe(false);
    expect(sub.lastHLC).toBeNull();
  });

  it("inherits pull/push from HttpTransport", async () => {
    const pullResponse: PullResponse = {
      changes: [],
      latest_hlc: { ts: 1, c: 0, node: "s" },
    };
    const fetchFn = mockFetch(pullResponse);
    const transport = new HttpStreamTransport({
      baseURL: "https://api.example.com/sync",
      fetch: fetchFn,
    });

    const resp = await transport.pull({ tables: [], node_id: "n1" });
    expect(resp.changes).toEqual([]);
  });
});
