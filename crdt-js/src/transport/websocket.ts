/**
 * WebSocket transport — one socket carrying pull, push, presence, and the
 * change stream, multiplexed by request_id.
 *
 * Wire format mirrors crdt/transport_ws.go exactly. Uses the global
 * WebSocket by default so the package stays dependency-free; pass
 * WebSocketImpl to supply one (e.g. `ws` under Node).
 */

import type {
  StreamTransport, StreamConfig, StreamSubscription, StreamEvent,
  StreamEventHandler, PullRequest, PullResponse, PushRequest, PushResponse,
  PresenceUpdate, ChangeRecord,
  PresenceEvent, PresenceState, PresenceSnapshot, AuthProvider, HLC,
} from "../types.js";
import { TransportError } from "../errors.js";
import { Backoff } from "../backoff.js";
import type { BackoffOptions } from "../backoff.js";
import { hlcAfter, HLC_ZERO } from "../hlc.js";

/** Mirrors WSMessageType in crdt/transport_ws.go. */
export type WSMessageType =
  | "pull_request" | "pull_response"
  | "push_request" | "push_response"
  | "change" | "changes"
  | "presence_update" | "presence_event"
  | "presence_get" | "presence_snapshot"
  | "subscribe" | "unsubscribe"
  | "error" | "ping" | "pong";

/** Mirrors WebSocketMessage in crdt/transport_ws.go. */
export interface WebSocketMessage {
  type: WSMessageType;
  payload?: unknown;
  request_id?: string;
}

export interface WebSocketTransportConfig {
  /** ws:// or wss:// endpoint. */
  url: string;
  /** Subprotocols passed to the WebSocket constructor. */
  protocols?: string | string[];
  /** WebSocket implementation (default: globalThis.WebSocket). */
  WebSocketImpl?: typeof WebSocket;
  /** Per-request timeout in ms (default: 30000). */
  requestTimeout?: number;
  /** Reconnect backoff schedule. */
  backoff?: BackoffOptions;
  /**
   * Auth provider. Headers cannot be set on a browser WebSocket handshake,
   * so each header is appended as a query parameter instead.
   */
  auth?: AuthProvider;
}

interface Pending {
  resolve: (value: unknown) => void;
  reject: (err: Error) => void;
  timer: ReturnType<typeof setTimeout>;
}

interface SubscriptionState {
  tables: string[];
  lastHLC: HLC | null;
  handlers: Set<StreamEventHandler>;
}

export class WebSocketTransport implements StreamTransport {
  private socket: WebSocket | null = null;
  private pending = new Map<string, Pending>();
  private subscriptions = new Set<SubscriptionState>();
  private nextId = 0;
  private closed = false;
  private opening: Promise<WebSocket> | null = null;
  private openingReject: ((err: Error) => void) | null = null;
  private backoff: Backoff;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private announced: string | null = null;

  private impl: typeof WebSocket;
  private requestTimeout: number;

  constructor(private config: WebSocketTransportConfig) {
    const impl = config.WebSocketImpl ?? globalThis.WebSocket;
    if (!impl) {
      throw new TransportError(
        "No WebSocket implementation available. Pass `WebSocketImpl` (e.g. the `ws` package under Node)."
      );
    }
    this.impl = impl;
    this.requestTimeout = config.requestTimeout ?? 30_000;
    this.backoff = new Backoff(config.backoff);

    // Connect eagerly rather than waiting for the first pull/push/subscribe
    // call, so the handshake latency is paid once up front. Failures here
    // are swallowed: pull(), push(), updatePresence() and subscribe().connect()
    // all funnel through ensureOpen(), which retries the connection lazily.
    void this.ensureOpen().catch(() => { /* retried lazily by callers */ });
  }

  // --- Transport ---

  async pull(req: PullRequest): Promise<PullResponse> {
    return this.rpc<PullResponse>("pull_request", req);
  }

  async push(req: PushRequest): Promise<PushResponse> {
    return this.rpc<PushResponse>("push_request", req);
  }

  /**
   * Fire-and-forget: the Go server answers `presence_update` with
   * `sendResponse("", WSPresenceEvent, event)` — an empty request_id — so
   * the reply is a broadcast, not a correlated response. A server-side
   * rejection cannot reach this call's promise.
   */
  async updatePresence(update: PresenceUpdate): Promise<void> {
    const socket = await this.ensureOpen();
    socket.send(JSON.stringify({ type: "presence_update", payload: update }));
  }

  /** Seed idle peers over the same socket used for collaboration updates. */
  async getPresence(topic: string): Promise<PresenceState[]> {
    const snapshot = await this.rpc<PresenceSnapshot>("presence_get", { topic });
    return snapshot.states ?? [];
  }

  // --- StreamTransport ---

  subscribe(config: StreamConfig): StreamSubscription {
    const transport = this;
    const state: SubscriptionState = {
      tables: [...(config.tables ?? [])], lastHLC: config.since ?? null,
      handlers: new Set(),
    };
    return {
      get connected() { return transport.subscriptions.has(state) && transport.socket?.readyState === 1; },
      get lastHLC() { return state.lastHLC; },
      on(handler) {
        state.handlers.add(handler);
        return () => { state.handlers.delete(handler); };
      },
      connect() {
        if (transport.closed || transport.subscriptions.has(state)) return;
        transport.subscriptions.add(state);
        const alreadyOpen = transport.socket?.readyState === 1;
        void transport.ensureOpen().then((socket) => {
          if (!transport.subscriptions.has(state)) return;
          transport.announce(socket);
          if (alreadyOpen) transport.deliver(state, { type: "connected" });
        }).catch((err: unknown) => {
          if (transport.subscriptions.has(state)) transport.deliver(state, {
            type: "error", error: err instanceof Error ? err : new Error(String(err)),
          });
        });
      },
      disconnect() {
        if (!transport.subscriptions.delete(state)) return;
        transport.deliver(state, { type: "disconnected" });
        if (transport.subscriptions.size === 0) transport.cancelReconnect();
        if (transport.socket?.readyState === 1) transport.announce(transport.socket);
      },
    };
  }

  private announce(socket: WebSocket): void {
    if (socket !== this.socket || socket.readyState !== 1) return;
    if (this.subscriptions.size === 0) {
      if (this.announced !== null) socket.send(JSON.stringify({ type: "unsubscribe" }));
      this.announced = null;
      return;
    }
    const active = [...this.subscriptions];
    const tables = [...new Set(active.flatMap((s) => s.tables))].sort();
    // A new table scope starts from its own checkpoint, never another scope's.
    const since = active.reduce((min, s) => {
      const cursor = s.lastHLC ?? HLC_ZERO;
      return hlcAfter(min, cursor) ? cursor : min;
    }, active[0].lastHLC ?? HLC_ZERO);
    const payload = JSON.stringify({ tables, since });
    if (payload === this.announced) return;
    socket.send(JSON.stringify({ type: "subscribe", payload: { tables, since } }));
    this.announced = payload;
  }

  private cancelReconnect(): void {
    if (this.reconnectTimer !== null) clearTimeout(this.reconnectTimer);
    this.reconnectTimer = null;
  }

  private scheduleReconnect(): void {
    if (this.closed || this.subscriptions.size === 0 || this.reconnectTimer !== null) return;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      if (this.closed || this.subscriptions.size === 0) return;
      void this.ensureOpen().catch(() => { this.scheduleReconnect(); });
    }, this.backoff.next());
  }

  /**
   * Close the socket and reject every in-flight request — including a
   * caller still stuck inside ensureOpen() (not yet registered in
   * `pending`, since rpc() only adds its entry after the connect settles).
   * Without this, "construct then close before the handshake finishes" —
   * an ordinary lifecycle, and the common case once the transport connects
   * eagerly — would hang that caller forever: open()'s promise only ever
   * settles from onopen/onerror, and requestTimeout doesn't start until
   * after ensureOpen() resolves.
   */
  close(): void {
    this.closed = true;
    this.cancelReconnect();
    for (const [, p] of this.pending) {
      clearTimeout(p.timer);
      p.reject(new TransportError("WebSocket transport closed"));
    }
    this.pending.clear();
    if (this.openingReject) {
      const reject = this.openingReject;
      this.openingReject = null;
      reject(new TransportError("WebSocket transport closed"));
    }
    this.socket?.close();
    this.socket = null;
  }

  // --- Internals ---

  private deliver(state: SubscriptionState, event: StreamEvent): void {
    for (const handler of state.handlers) {
      try { handler(event); } catch { /* An observer cannot stop delivery. */ }
    }
  }

  private emit(event: StreamEvent): void {
    for (const state of this.subscriptions) {
      const accepts = (c: ChangeRecord) => state.tables.includes(c.table);
      const observe = (c: ChangeRecord) => {
        if (!state.lastHLC || hlcAfter(c.hlc, state.lastHLC)) state.lastHLC = c.hlc;
      };
      if (event.type === "change") {
        if (!accepts(event.data)) continue;
        observe(event.data);
        this.deliver(state, event);
      } else if (event.type === "changes") {
        const changes = event.data.filter(accepts);
        if (changes.length === 0) continue;
        changes.forEach(observe);
        this.deliver(state, { type: "changes", data: changes });
      } else { this.deliver(state, event); }
    }
  }

  private async rpc<T>(type: WSMessageType, payload: unknown): Promise<T> {
    const socket = await this.ensureOpen();
    const id = `r${++this.nextId}`;

    return new Promise<T>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new TransportError(`CRDT ws ${type} timed out after ${this.requestTimeout}ms`));
      }, this.requestTimeout);

      this.pending.set(id, {
        resolve: resolve as (v: unknown) => void,
        reject,
        timer,
      });
      try { socket.send(JSON.stringify({ type, payload, request_id: id })); }
      catch (err) { clearTimeout(timer); this.pending.delete(id); reject(err); }
    });
  }

  private async ensureOpen(): Promise<WebSocket> {
    if (this.closed) throw new TransportError("WebSocket transport closed");
    if (this.socket?.readyState === 1) return this.socket;
    if (!this.opening) {
      this.opening = new Promise<WebSocket>((resolve, reject) => {
        this.openingReject = reject;
        void this.attemptConnect(resolve, reject);
      }).finally(() => {
        this.opening = null;
        this.openingReject = null;
        if (this.socket?.readyState !== 1) this.scheduleReconnect();
      });
    }

    const socket = await this.opening;
    // close() may have landed between resolve() firing and this
    // continuation running (e.g. it ran synchronously right after onopen,
    // before the `.finally()` above got a chance to clear openingReject —
    // at that point rejecting an already-settled promise is a no-op, so
    // this recheck is what actually stops a stale socket from reaching a
    // caller on a transport that's already shut down).
    if (this.closed) throw new TransportError("WebSocket transport closed");
    return socket;
  }

  private async attemptConnect(
    resolve: (ws: WebSocket) => void,
    reject: (err: Error) => void
  ): Promise<void> {
    let url: string;
    try {
      url = await this.buildURL();
    } catch (err) {
      reject(err instanceof Error ? err : new Error(String(err)));
      return;
    }
    // close() may have fired while buildURL() (e.g. an async auth lookup)
    // was in flight — don't open a socket for a transport that's already
    // shut down.
    if (this.closed) {
      reject(new TransportError("WebSocket transport closed"));
      return;
    }

    let socket: WebSocket;
    try {
      socket = new this.impl(url, this.config.protocols);
    } catch (err) {
      reject(
        err instanceof Error
          ? err
          : new TransportError(`CRDT ws connection failed: ${url}`)
      );
      return;
    }
    this.socket = socket;

    socket.onopen = () => {
      if (this.closed || this.socket !== socket) { socket.close(); return; }
      this.cancelReconnect();
      this.backoff.reset();
      this.announced = null;
      this.announce(socket);
      this.emit({ type: "connected" });
      resolve(socket);
    };
    socket.onmessage = (ev: MessageEvent) => {
      if (!this.closed && this.socket === socket) this.handleMessage(String(ev.data));
    };
    const dropped = () => {
      if (this.socket !== socket) return;
      this.socket = null;
      this.announced = null;
      const err = new TransportError("CRDT ws connection closed");
      reject(err);
      for (const p of this.pending.values()) { clearTimeout(p.timer); p.reject(err); }
      this.pending.clear();
      this.emit({ type: "disconnected" });
      this.scheduleReconnect();
    };
    socket.onerror = () => { dropped(); socket.close(); };
    socket.onclose = dropped;
  }

  private handleMessage(raw: string): void {
    let msg: WebSocketMessage;
    try {
      msg = JSON.parse(raw) as WebSocketMessage;
    } catch (err) {
      this.emit({ type: "error", error: new Error(`Failed to parse ws frame: ${err}`) });
      return;
    }

    // Correlated reply to an in-flight rpc().
    if (msg.request_id) {
      const p = this.pending.get(msg.request_id);
      if (p) {
        this.pending.delete(msg.request_id);
        clearTimeout(p.timer);
        if (msg.type === "error") {
          const detail = (msg.payload as { error?: string })?.error ?? "unknown error";
          p.reject(new TransportError(`CRDT ws error: ${detail}`));
        } else {
          p.resolve(msg.payload);
        }
        return;
      }
    }

    switch (msg.type) {
      case "change": {
        const change = msg.payload as ChangeRecord;
        this.emit({ type: "change", data: change });
        break;
      }
      case "changes": {
        const changes = msg.payload as ChangeRecord[];
        this.emit({ type: "changes", data: changes });
        break;
      }
      case "presence_event":
        this.emit({ type: "presence", data: msg.payload as PresenceEvent });
        break;
      case "ping":
        this.socket?.send(JSON.stringify({ type: "pong" }));
        break;
      case "error":
        this.emit({
          type: "error",
          error: new Error(
            (msg.payload as { error?: string })?.error ?? "unknown ws error"
          ),
        });
        break;
      default:
        // pong and unknown types need no action.
        break;
    }
  }

  private async buildURL(): Promise<string> {
    if (!this.config.auth) return this.config.url;
    // A browser WebSocket handshake cannot carry custom headers, so auth
    // travels as query parameters instead.
    const headers = await Promise.resolve(this.config.auth.getHeaders());
    const url = new URL(this.config.url);
    for (const [k, v] of Object.entries(headers)) {
      url.searchParams.set(k.toLowerCase(), v);
    }
    return url.toString();
  }
}
