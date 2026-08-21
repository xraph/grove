import { describe, it, expect } from "vitest";
import { withRetry } from "../transport/retry.js";
import { isStreamTransport } from "../transport.js";
import { TransportError } from "../errors.js";
import type { Transport, StreamTransport, PullRequest, PullResponse } from "../types.js";

const HLC0 = { ts: "0", c: 0, node: "" };
const opts = { retries: 3, backoff: { initialDelay: 1, jitter: false } };

describe("withRetry", () => {
  it("retries a retryable failure from ANY transport, not just HTTP", async () => {
    let calls = 0;
    const flaky: Transport = {
      async pull(): Promise<PullResponse> {
        calls++;
        if (calls < 3) throw new TransportError("boom", 503);
        return { changes: [], latest_hlc: HLC0 };
      },
      async push() { return { merged: 0, latest_hlc: HLC0 }; },
    };
    const resp = await withRetry(flaky, opts).pull({ tables: [], node_id: "n1" });
    expect(calls).toBe(3);
    expect(resp.changes).toEqual([]);
  });

  it("does not retry a non-retryable failure", async () => {
    let calls = 0;
    const bad: Transport = {
      async pull(): Promise<PullResponse> {
        calls++;
        throw new TransportError("bad request", 400);
      },
      async push() { return { merged: 0, latest_hlc: HLC0 }; },
    };
    await expect(
      withRetry(bad, opts).pull({ tables: [], node_id: "n1" })
    ).rejects.toThrow();
    expect(calls).toBe(1);
  });

  it("preserves streaming capability", () => {
    const ws: StreamTransport = {
      async pull() { return { changes: [], latest_hlc: HLC0 }; },
      async push() { return { merged: 0, latest_hlc: HLC0 }; },
      subscribe() {
        return {
          on() { return () => {}; }, connect() {}, disconnect() {},
          connected: false, lastHLC: null,
        };
      },
    };
    const wrapped = withRetry(ws, opts);
    expect(isStreamTransport(wrapped)).toBe(true);
    expect(typeof wrapped.subscribe).toBe("function");
  });

  it("keeps members outside the Transport interface reachable", async () => {
    // withRetry<T>(inner: T): T used to build a fresh object literal and
    // cast it to T, so everything that is not pull/push/presence/subscribe
    // vanished at runtime while TypeScript still typed it as callable:
    // withRetry(ws).close() compiled and then threw. Composing resilience
    // over a pluggable transport is exactly what that breaks.
    class FakeWs implements StreamTransport {
      closed = false;
      subscribedTables: string[] | null = null;
      async pull(_req: PullRequest): Promise<PullResponse> { return { changes: [], latest_hlc: HLC0 }; }
      async push() { return { merged: 0, latest_hlc: HLC0 }; }
      subscribe(config: { tables?: string[] }) {
        this.subscribedTables = config.tables ?? [];
        return {
          on() { return () => {}; }, connect() {}, disconnect() {},
          connected: false, lastHLC: null,
        };
      }
      /** Not part of Transport — the whole point of this test. */
      close(): void { this.closed = true; }
    }

    const inner = new FakeWs();
    const wrapped = withRetry(inner, opts);

    // No cast: the declared type says close() exists, and now it does.
    expect(typeof wrapped.close).toBe("function");
    wrapped.close();
    expect(inner.closed).toBe(true);

    // Prototype methods are bound to the real instance, not the wrapper.
    wrapped.subscribe({ tables: ["docs"] });
    expect(inner.subscribedTables).toEqual(["docs"]);

    // The properties the wrapper must keep honoring.
    expect(isStreamTransport(wrapped)).toBe(true);
    expect((wrapped as Transport).updatePresence).toBeUndefined();
    expect((wrapped as Transport).getPresence).toBeUndefined();

    // Plain data members read through too.
    expect(wrapped.closed).toBe(true);

    // pull is still the retrying wrapper, not the raw method.
    expect(wrapped.pull).not.toBe(inner.pull);
    expect((await wrapped.pull({ tables: [], node_id: "n1" })).changes).toEqual([]);
  });

  it("passes optional presence methods through only when present", async () => {
    const bare: Transport = {
      async pull() { return { changes: [], latest_hlc: HLC0 }; },
      async push() { return { merged: 0, latest_hlc: HLC0 }; },
    };
    expect(withRetry(bare, opts).updatePresence).toBeUndefined();

    let seen = 0;
    const withPresence: Transport = {
      ...bare,
      async updatePresence() { seen++; },
      async getPresence() { return []; },
    };
    await withRetry(withPresence, opts).updatePresence!({
      node_id: "n1", topic: "t", data: {},
    });
    expect(seen).toBe(1);
  });
});
