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

/**
 * Like mkTransport(), but records every socket ever created by
 * WebSocketImpl (plural, for tests that force a reconnect) and exposes a
 * config override for requestTimeout/backoff.
 */
function mkTransportMulti(
  overrides: {
    requestTimeout?: number;
    backoff?: { initialDelay?: number; maxDelay?: number; jitter?: boolean };
  } = {}
) {
  const sockets: FakeSocket[] = [];
  const Impl = function (url: string) {
    const s = new FakeSocket(url);
    sockets.push(s);
    return s;
  } as unknown as typeof WebSocket;
  const transport = new WebSocketTransport({ url: "ws://x", WebSocketImpl: Impl, ...overrides });
  return { transport, sockets, latest: () => sockets[sockets.length - 1] };
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

  it("subscribe sends exactly one subscribe frame on first connect and emits inbound changes", async () => {
    // No tick-wait before subscribe()/connect(): the transport connects
    // eagerly at construction, so calling connect() immediately races
    // against that in-flight connect. That race is exactly what used to
    // cause a duplicate `subscribe` frame (one from onopen, since
    // connect() already set subscribedTables before onopen fired; one
    // from connect()'s own send). Asserting a count, not `.some()`, is
    // what pins this down against a regression.
    const { transport, socket } = mkTransport();
    const sub = transport.subscribe({ tables: ["docs"] });
    const events: string[] = [];
    sub.on((e) => { events.push(e.type); });
    sub.connect();
    await new Promise((r) => setTimeout(r, 5));

    const subscribeFrames = socket().sent.filter((m) => m.type === "subscribe");
    expect(subscribeFrames.length).toBe(1);
    expect(subscribeFrames[0].payload).toEqual({ tables: ["docs"] });

    socket().onmessage?.({ data: JSON.stringify({
      type: "change",
      payload: { table: "docs", pk: "1", field: "f", crdt_type: "lww",
                 hlc: HLC0, node_id: "n2", value: 1 },
    }) });
    expect(events).toContain("change");
    sub.disconnect();
    transport.close();
  });

  it("still sends exactly one subscribe frame when connect() is called after the socket is already open", async () => {
    // The other half of the dedupe fix: here onopen has already fired
    // (with no tables) by the time connect() runs, so onopen will never
    // fire again for this socket — connect() itself must be the one that
    // sends, and only once.
    const { transport, socket } = mkTransport();
    await new Promise((r) => queueMicrotask(r as () => void));
    expect(socket().readyState).toBe(FakeSocket.OPEN);

    const sub = transport.subscribe({ tables: ["docs"] });
    sub.connect();
    await new Promise((r) => setTimeout(r, 5));

    const subscribeFrames = socket().sent.filter((m) => m.type === "subscribe");
    expect(subscribeFrames.length).toBe(1);
    sub.disconnect();
    transport.close();
  });

  it("reconnects after the socket drops and resubscribes exactly once", async () => {
    const { transport, sockets, latest } = mkTransportMulti({
      backoff: { initialDelay: 1, maxDelay: 1, jitter: false },
    });
    const sub = transport.subscribe({ tables: ["docs"] });
    sub.connect();
    await new Promise((r) => setTimeout(r, 10));

    const framesSoFar = () => sockets.flatMap((s) => s.sent).filter((m) => m.type === "subscribe");
    expect(framesSoFar().length).toBe(1);
    expect(sockets.length).toBe(1);

    // Simulate the server dropping the connection (not a client close()).
    latest().onclose?.();
    await new Promise((r) => setTimeout(r, 20));

    expect(sockets.length).toBe(2);
    expect(framesSoFar().length).toBe(2);
    expect(framesSoFar()[1].payload).toEqual({ tables: ["docs"] });

    sub.disconnect();
    transport.close();
  });

  it("re-emits `connected` after an auto-reconnect", async () => {
    // Only subscribe().connect() used to emit `connected`; the reconnect
    // path resubscribed but stayed silent, so the event log read
    // ["connected","disconnected"] forever while the socket was in fact
    // back up and resubscribed. CRDTStream re-emits on every reconnect, so
    // the two transports disagreed on the same StreamSubscription
    // contract, and a React app on WebSocket showed "disconnected" for the
    // rest of its life after the first blip.
    const { transport, sockets, latest } = mkTransportMulti({
      backoff: { initialDelay: 1, maxDelay: 1, jitter: false },
    });
    const sub = transport.subscribe({ tables: ["docs"] });
    const events: string[] = [];
    sub.on((e) => { events.push(e.type); });
    sub.connect();
    await new Promise((r) => setTimeout(r, 10));
    expect(events).toEqual(["connected"]);

    latest().onclose?.();
    await new Promise((r) => setTimeout(r, 20));

    expect(sockets.length).toBe(2);
    expect(events).toEqual(["connected", "disconnected", "connected"]);
    expect(sub.connected).toBe(true);

    sub.disconnect();
    transport.close();
  });

  it("does not emit `connected` for a connection no subscriber asked for", async () => {
    // The transport connects eagerly at construction. That socket has no
    // subscription behind it, so its onopen must stay silent — otherwise
    // every consumer that only ever calls pull/push gets phantom
    // connection events.
    const { transport } = mkTransportMulti();
    const events: string[] = [];
    const sub = transport.subscribe({ tables: ["docs"] });
    sub.on((e) => { events.push(e.type); });
    await new Promise((r) => setTimeout(r, 10));
    expect(events).toEqual([]);
    transport.close();
  });

  it("emits exactly one `connected` when connect() runs against an already-open socket", async () => {
    // The already-open branch is the one onopen will never fire for, so
    // connect() owns the emit there. Asserting a count is what stops the
    // reconnect fix from double-emitting on first connect.
    const { transport } = mkTransportMulti();
    await new Promise((r) => setTimeout(r, 5));

    const sub = transport.subscribe({ tables: ["docs"] });
    const events: string[] = [];
    sub.on((e) => { events.push(e.type); });
    sub.connect();
    await new Promise((r) => setTimeout(r, 10));

    expect(events.filter((e) => e === "connected")).toHaveLength(1);
    sub.disconnect();
    transport.close();
  });

  it("close() during an in-flight connect rejects the caller instead of hanging", async () => {
    // FakeSocket only flips OPEN and fires onopen on a queued microtask, so
    // calling close() synchronously right after pull() catches the
    // transport mid-handshake — exactly the race that used to leave the
    // caller unsettled forever (pending timeout doesn't start until after
    // ensureOpen() resolves).
    const { transport } = mkTransport();
    const pullPromise = transport.pull({ tables: ["a"], node_id: "n1" });
    transport.close();

    const settledInTime = await Promise.race([
      pullPromise.then(
        () => "resolved",
        () => "rejected"
      ),
      new Promise((resolve) => setTimeout(() => resolve("timed-out"), 200)),
    ]);
    expect(settledInTime).toBe("rejected");
    await expect(pullPromise).rejects.toThrow(/closed/i);
  });

  it("times out a request that never gets a reply and cleans up its pending entry", async () => {
    let socket!: FakeSocket;
    const Impl = function (url: string) {
      socket = new FakeSocket(url);
      return socket;
    } as unknown as typeof WebSocket;
    const transport = new WebSocketTransport({ url: "ws://x", WebSocketImpl: Impl, requestTimeout: 10 });
    await new Promise((r) => queueMicrotask(r as () => void));
    socket.respond = () => null; // server never answers

    await expect(
      transport.pull({ tables: ["a"], node_id: "n1" })
    ).rejects.toThrow(/timed out/i);

    // A late reply after the timeout must find nothing pending — no crash,
    // no double-settle.
    expect(() => {
      socket.onmessage?.({ data: JSON.stringify({
        type: "pull_response", request_id: "r1",
        payload: { changes: [], latest_hlc: HLC0 },
      }) });
    }).not.toThrow();

    transport.close();
  });

  it("updatePresence sends a presence_update frame (fire-and-forget)", async () => {
    const { transport, socket } = mkTransport();
    await new Promise((r) => queueMicrotask(r as () => void));

    await transport.updatePresence({ node_id: "n1", topic: "room", data: { x: 1 } });

    expect(socket().sent).toContainEqual({
      type: "presence_update",
      payload: { node_id: "n1", topic: "room", data: { x: 1 } },
    });
    transport.close();
  });
});
