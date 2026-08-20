import { describe, it, expect } from "vitest";
import { withRetry } from "../transport/retry.js";
import { isStreamTransport } from "../transport.js";
import { TransportError } from "../errors.js";
import type { Transport, StreamTransport, PullResponse } from "../types.js";

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
