/**
 * SSE streaming client for real-time CRDT change propagation.
 *
 * Uses fetch() + ReadableStream instead of EventSource to support
 * custom headers (for auth tokens). Implements auto-reconnect with
 * configurable delay.
 */

import type {
  HLC,
  ChangeRecord,
  StreamConfig,
  AuthProvider,
  StreamEvent,
  StreamEventHandler,
  PresenceEvent,
} from "./types.js";
import { hlcAfter, hlcIsZero } from "./hlc.js";
import { Backoff } from "./backoff.js";

/** Event types emitted by the CRDT stream (alias for StreamEvent). */
export type CRDTStreamEvent = StreamEvent;

/** Re-export StreamEventHandler for backward compatibility. */
export type { StreamEventHandler } from "./types.js";

/**
 * SSE streaming client with auto-reconnect.
 *
 * Connects to the CRDT sync server's SSE endpoint and receives
 * real-time change events. Automatically reconnects on disconnection.
 */
export class CRDTStream {
  private baseURL: string;
  private tables: string[];
  private backoff: Backoff;
  private headers: Record<string, string>;
  private handlers: Set<StreamEventHandler> = new Set();
  private abortController: AbortController | null = null;
  private _connected = false;
  private _lastHLC: HLC | null = null;
  private _since: HLC | null;
  private shouldReconnect = false;
  private fetchImpl: typeof fetch;
  private auth?: AuthProvider;
  private idleTimeout: number;
  private idleTimer: ReturnType<typeof setTimeout> | null = null;
  private idleTimerGen = -1;
  // Identifies which connectLoop()/connectOnce() invocation is current.
  // Bumped by connect() and disconnect() so a loop that is still in
  // flight (e.g. blocked on a reader.read() that hasn't rejected yet)
  // can recognize itself as stale and stop touching shared state, even
  // when disconnect() is immediately followed by connect() in the same
  // synchronous block.
  private generation = 0;

  constructor(
    baseURL: string,
    config?: StreamConfig,
    headers?: Record<string, string>,
    fetchImpl?: typeof fetch,
    auth?: AuthProvider
  ) {
    this.baseURL = baseURL;
    this.tables = config?.tables ?? [];
    this.backoff = new Backoff({
      initialDelay: config?.reconnectDelay ?? 5000,
      maxDelay: config?.maxReconnectDelay ?? 30_000,
    });
    this._since = config?.since ?? null;
    this.headers = headers ?? {};
    this.fetchImpl = fetchImpl ?? globalThis.fetch.bind(globalThis);
    this.auth = auth;
    this.idleTimeout = config?.idleTimeout ?? 45_000;
  }

  /** Whether the stream is currently connected. */
  get connected(): boolean {
    return this._connected;
  }

  /** The last HLC received from the stream. */
  get lastHLC(): HLC | null {
    return this._lastHLC;
  }

  /** Subscribe to stream events. Returns an unsubscribe function. */
  on(handler: StreamEventHandler): () => void {
    this.handlers.add(handler);
    return () => {
      this.handlers.delete(handler);
    };
  }

  /** Start the SSE connection. Idempotent while a loop is running. */
  connect(): void {
    // Guard on shouldReconnect, not _connected: _connected stays false
    // until response headers arrive, so two quick calls would otherwise
    // start two loops and leak the first AbortController.
    if (this.shouldReconnect) return;
    this.shouldReconnect = true;
    const gen = ++this.generation;
    void this.connectLoop(gen);
  }

  /** Disconnect and stop reconnecting. */
  disconnect(): void {
    this.clearIdleTimer();
    this.shouldReconnect = false;
    // Bump the generation so any loop still in flight recognizes itself as
    // stale on its next check and stops touching abortController,
    // _connected, or events — even if a connect() right after this
    // disconnect() starts a new loop before the old one notices it was
    // aborted (its pending reader.read() rejection hasn't landed yet).
    this.generation++;
    this.abortController?.abort();
    this.abortController = null;
    if (this._connected) {
      this._connected = false;
      this.emit({ type: "disconnected" });
    }
  }

  /**
   * Abort the connection if nothing arrives for idleTimeout ms.
   *
   * A TCP connection can die without the read ever completing, which leaves
   * the reader parked forever and no reconnect is ever attempted. The
   * server's SSE keep-alive comments are enough to keep this armed.
   *
   * Scoped to `gen`: a stale generation's timer must never abort a newer
   * generation's connection, and a newer generation's armed timer must
   * never be wiped out by a stale generation's cleanup.
   */
  private armIdleTimer(gen: number): void {
    if (this.idleTimeout <= 0) return;
    this.clearIdleTimerFor(gen);
    this.idleTimerGen = gen;
    this.idleTimer = setTimeout(() => {
      if (this.idleTimerGen === gen) {
        this.idleTimer = null;
        this.idleTimerGen = -1;
      }
      if (gen === this.generation) {
        this.abortController?.abort();
      }
    }, this.idleTimeout);
  }

  /** Clear the idle timer unconditionally, regardless of which generation armed it. */
  private clearIdleTimer(): void {
    if (this.idleTimer) {
      clearTimeout(this.idleTimer);
      this.idleTimer = null;
      this.idleTimerGen = -1;
    }
  }

  /** Clear the idle timer only if `gen` is the generation that armed it. */
  private clearIdleTimerFor(gen: number): void {
    if (this.idleTimer && this.idleTimerGen === gen) {
      clearTimeout(this.idleTimer);
      this.idleTimer = null;
      this.idleTimerGen = -1;
    }
  }

  private emit(event: CRDTStreamEvent): void {
    for (const handler of this.handlers) {
      try {
        handler(event);
      } catch {
        // Ignore handler errors.
      }
    }
  }

  private async connectLoop(gen: number): Promise<void> {
    while (gen === this.generation) {
      let threw = false;
      let caught: unknown;
      try {
        await this.connectOnce(gen);
      } catch (err) {
        threw = true;
        caught = err;
      }

      // A newer generation may have taken over while we were awaiting
      // connectOnce (e.g. disconnect() immediately followed by connect()).
      // This loop instance is stale: it must not touch abortController,
      // _connected, or emit events — those belong to the current
      // generation now.
      if (gen !== this.generation) break;

      if (threw) {
        this.emit({
          type: "error",
          error: caught instanceof Error ? caught : new Error(String(caught)),
        });
      }

      if (this._connected) {
        this._connected = false;
        this.emit({ type: "disconnected" });
      }

      if (gen !== this.generation) break;

      // Wait before reconnecting, with jittered exponential backoff.
      const delay = this.backoff.next();
      await new Promise((resolve) => setTimeout(resolve, delay));
    }
  }

  private async connectOnce(gen: number): Promise<void> {
    // Last line of defense: connectLoop already checks this before calling
    // in, but a stale generation must never be able to assign
    // this.abortController (or anything else downstream of it).
    if (gen !== this.generation) return;

    this.abortController = new AbortController();
    const url = this.buildStreamURL();

    const authHeaders = this.auth
      ? await Promise.resolve(this.auth.getHeaders())
      : {};

    const response = await this.fetchImpl(url, {
      method: "GET",
      headers: {
        Accept: "text/event-stream",
        "Cache-Control": "no-cache",
        ...this.headers,
        ...authHeaders,
      },
      signal: this.abortController.signal,
    });

    // A newer generation may have taken over while the request was in
    // flight. Don't touch _connected or emit "connected"/"error" on
    // behalf of a stale generation.
    if (gen !== this.generation) return;

    if (!response.ok) {
      const text = await response.text().catch(() => "");
      throw new Error(
        `CRDT stream returned ${response.status}: ${text}`
      );
    }

    this._connected = true;
    this.backoff.reset();
    this.armIdleTimer(gen);
    this.emit({ type: "connected" });

    const decoder = new TextDecoder();
    let buffer = "";
    let eventType = "";
    let dataLines: string[] = [];
    let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;

    try {
      reader = response.body?.getReader();
      if (!reader) {
        throw new Error("CRDT stream: response has no body");
      }

      while (true) {
        const { done, value } = await reader.read();
        // Same as above: a newer generation may have taken over while
        // this read was pending. Stop processing without touching shared
        // state; connectLoop's own gen check handles the rest.
        if (gen !== this.generation) break;
        if (done) break;
        this.armIdleTimer(gen);

        buffer += decoder.decode(value, { stream: true });

        // Process complete lines.
        const lines = buffer.split("\n");
        // Keep the last incomplete line in the buffer.
        buffer = lines.pop() ?? "";

        for (const line of lines) {
          // Empty line = end of event.
          if (line === "") {
            if (dataLines.length > 0) {
              const data = dataLines.join("\n");
              this.processEvent(eventType, data);
            }
            eventType = "";
            dataLines = [];
            continue;
          }

          // SSE comment (keep-alive).
          if (line.startsWith(":")) {
            continue;
          }

          // Parse SSE fields.
          if (line.startsWith("event:")) {
            eventType = line.slice(6).trim();
          } else if (line.startsWith("data:")) {
            dataLines.push(line.slice(5).trim());
          }
        }
      }
    } finally {
      this.clearIdleTimerFor(gen);
      reader?.releaseLock();
    }
  }

  private processEvent(eventType: string, data: string): void {
    try {
      if (eventType === "change") {
        const change = JSON.parse(data) as ChangeRecord;
        this.updateLastHLC(change.hlc);
        this.emit({ type: "change", data: change });
      } else if (eventType === "changes") {
        const changes = JSON.parse(data) as ChangeRecord[];
        for (const change of changes) {
          this.updateLastHLC(change.hlc);
        }
        this.emit({ type: "changes", data: changes });
      } else if (eventType === "presence") {
        const event = JSON.parse(data) as PresenceEvent;
        this.emit({ type: "presence", data: event });
      }
    } catch (err) {
      this.emit({
        type: "error",
        error: new Error(`Failed to parse SSE event: ${err}`),
      });
    }
  }

  private updateLastHLC(hlc: HLC): void {
    if (!this._lastHLC || hlcAfter(hlc, this._lastHLC)) {
      this._lastHLC = hlc;
    }
  }

  /**
   * Build the SSE endpoint URL with query parameters.
   * Matches Go's StreamingTransport.buildStreamURL().
   */
  private buildStreamURL(): string {
    const params = new URLSearchParams();

    if (this.tables.length > 0) {
      params.set("tables", this.tables.join(","));
    }

    const since = this._lastHLC ?? this._since;
    if (since && !hlcIsZero(since)) {
      params.set("since_ts", String(since.ts));
      params.set("since_count", String(since.c));
      params.set("since_node", since.node);
    }

    const query = params.toString();
    return query ? `${this.baseURL}/stream?${query}` : `${this.baseURL}/stream`;
  }
}
