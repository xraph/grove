import { describe, it, expect, vi } from "vitest";
import { WebSocketTransport } from "../transport/websocket.js";
import type { WebSocketMessage } from "../transport/websocket.js";

const HLC0 = { ts: "0", c: 0, node: "" };

/** Minimal in-memory WebSocket double that echoes canned responses. */
class FakeSocket {
  static OPEN = 1;
  readyState = 0;
  onopen: (() => void) | null = null;
  onmessage: ((ev: { data: string }) => void) | null = null;
  onclose: (() => void) | null = null;
  onerror: (() => void) | null = null;
  sent: WebSocketMessage[] = [];
  respond: (msg: WebSocketMessage) => WebSocketMessage | null = () => null;

  constructor(public url: string) {
    queueMicrotask(() => {
      this.readyState = FakeSocket.OPEN;
      this.onopen?.();
    });
  }
  send(data: string): void {
    const msg = JSON.parse(data) as WebSocketMessage;
    this.sent.push(msg);
    const reply = this.respond(msg);
    if (reply) queueMicrotask(() => this.onmessage?.({ data: JSON.stringify(reply) }));
  }
  close(): void {
    this.readyState = 3;
    this.onclose?.();
  }
}

function mkTransport() {
  let socket!: FakeSocket;
  const Impl = function (url: string) {
    socket = new FakeSocket(url);
    return socket;
  } as unknown as typeof WebSocket;
  const transport = new WebSocketTransport({ url: "ws://x", WebSocketImpl: Impl });
  return { transport, socket: () => socket };
}

describe("WebSocketTransport", () => {
  it("correlates a pull request with its response by request_id", async () => {
    const { transport, socket } = mkTransport();
    await new Promise((r) => queueMicrotask(r as () => void));
    socket().respond = (msg) =>
      msg.type === "pull_request"
        ? { type: "pull_response", request_id: msg.request_id,
            payload: { changes: [], latest_hlc: HLC0 } }
        : null;

    const resp = await transport.pull({ tables: ["a"], node_id: "n1" });
    expect(resp.changes).toEqual([]);
    expect(socket().sent[0].type).toBe("pull_request");
    transport.close();
  });

  it("rejects a request when the server answers with an error frame", async () => {
    const { transport, socket } = mkTransport();
    await new Promise((r) => queueMicrotask(r as () => void));
    socket().respond = (msg) => ({
      type: "error", request_id: msg.request_id, payload: { error: "nope" },
    });
    await expect(transport.push({ changes: [], node_id: "n1" })).rejects.toThrow("nope");
    transport.close();
  });

  it("subscribe sends a subscribe frame and emits inbound changes", async () => {
    const { transport, socket } = mkTransport();
    await new Promise((r) => queueMicrotask(r as () => void));
    const sub = transport.subscribe({ tables: ["docs"] });
    const events: string[] = [];
    sub.on((e) => { events.push(e.type); });
    sub.connect();
    await new Promise((r) => setTimeout(r, 5));

    expect(socket().sent.some((m) => m.type === "subscribe")).toBe(true);
    socket().onmessage?.({ data: JSON.stringify({
      type: "change",
      payload: { table: "docs", pk: "1", field: "f", crdt_type: "lww",
                 hlc: HLC0, node_id: "n2", value: 1 },
    }) });
    expect(events).toContain("change");
    sub.disconnect();
    transport.close();
  });
});
