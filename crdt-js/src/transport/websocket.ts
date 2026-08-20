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
  PresenceEvent, AuthProvider, HLC,
} from "../types.js";
import { TransportError } from "../errors.js";
import { Backoff } from "../backoff.js";
import type { BackoffOptions } from "../backoff.js";
import { hlcAfter } from "../hlc.js";

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

export class WebSocketTransport implements StreamTransport {
  private socket: WebSocket | null = null;
  private pending = new Map<string, Pending>();
  private handlers = new Set<StreamEventHandler>();
  private nextId = 0;
  private closed = false;
  private opening: Promise<WebSocket> | null = null;
  private backoff: Backoff;
  private subscribedTables: string[] = [];
  private _lastHLC: HLC | null = null;

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

  // --- StreamTransport ---

  subscribe(config: StreamConfig): StreamSubscription {
    const transport = this;
    let connected = false;

    return {
      get connected() { return connected && transport.socket?.readyState === 1; },
      get lastHLC() { return transport._lastHLC; },

      on(handler: StreamEventHandler): () => void {
        transport.handlers.add(handler);
        return () => { transport.handlers.delete(handler); };
      },

      connect(): void {
        if (connected) return;
        connected = true;
        transport.subscribedTables = config.tables ?? [];
        void transport.ensureOpen().then((socket) => {
          socket.send(JSON.stringify({
            type: "subscribe",
            payload: { tables: transport.subscribedTables },
          }));
          transport.emit({ type: "connected" });
        }).catch((err: unknown) => {
          transport.emit({
            type: "error",
            error: err instanceof Error ? err : new Error(String(err)),
          });
        });
      },

      disconnect(): void {
        if (!connected) return;
        connected = false;
        // No `unsubscribe` frame: the Go server does not handle that type
        // and would answer with an error frame, surfacing as a spurious
        // stream error. Dropping the subscription client-side is enough.
        transport.subscribedTables = [];
        transport.emit({ type: "disconnected" });
      },
    };
  }

  /** Close the socket and reject every in-flight request. */
  close(): void {
    this.closed = true;
    for (const [, p] of this.pending) {
      clearTimeout(p.timer);
      p.reject(new TransportError("WebSocket transport closed"));
    }
    this.pending.clear();
    this.socket?.close();
    this.socket = null;
  }

  // --- Internals ---

  private emit(event: StreamEvent): void {
    for (const handler of this.handlers) {
      try { handler(event); } catch { /* Ignore handler errors. */ }
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
      socket.send(JSON.stringify({ type, payload, request_id: id }));
    });
  }

  private async ensureOpen(): Promise<WebSocket> {
    if (this.closed) throw new TransportError("WebSocket transport closed");
    if (this.socket?.readyState === 1) return this.socket;
    if (this.opening) return this.opening;

    this.opening = this.open().finally(() => { this.opening = null; });
    return this.opening;
  }

  private async open(): Promise<WebSocket> {
    const url = await this.buildURL();
    return new Promise<WebSocket>((resolve, reject) => {
      const socket = new this.impl(url, this.config.protocols);
      this.socket = socket;

      socket.onopen = () => {
        this.backoff.reset();
        // Re-subscribe after a reconnect: the server does not remember us.
        if (this.subscribedTables.length > 0) {
          socket.send(JSON.stringify({
            type: "subscribe",
            payload: { tables: this.subscribedTables },
          }));
        }
        resolve(socket);
      };

      socket.onmessage = (ev: MessageEvent) => {
        this.handleMessage(String(ev.data));
      };

      socket.onerror = () => {
        reject(new TransportError(`CRDT ws connection failed: ${url}`));
      };

      socket.onclose = () => {
        this.socket = null;
        this.emit({ type: "disconnected" });
        if (!this.closed && this.subscribedTables.length > 0) {
          setTimeout(() => { void this.ensureOpen().catch(() => {}); },
            this.backoff.next());
        }
      };
    });
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
        this.updateLastHLC(change.hlc);
        this.emit({ type: "change", data: change });
        break;
      }
      case "changes": {
        const changes = msg.payload as ChangeRecord[];
        for (const c of changes) this.updateLastHLC(c.hlc);
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

  private updateLastHLC(hlc: HLC): void {
    if (!this._lastHLC || hlcAfter(hlc, this._lastHLC)) this._lastHLC = hlc;
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
