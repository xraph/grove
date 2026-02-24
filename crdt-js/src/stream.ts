/**
 * SSE streaming client for real-time CRDT change propagation.
 *
 * Uses fetch() + ReadableStream instead of EventSource to support
 * custom headers (for auth tokens). Implements auto-reconnect with
 * configurable delay.
 */

import type { HLC, ChangeRecord, StreamConfig } from "./types.js";
import { hlcAfter, hlcIsZero } from "./hlc.js";

/** Event types emitted by the CRDT stream. */
export type CRDTStreamEvent =
  | { type: "change"; data: ChangeRecord }
  | { type: "changes"; data: ChangeRecord[] }
  | { type: "error"; error: Error }
  | { type: "connected" }
  | { type: "disconnected" };

/** Callback for stream events. */
export type StreamEventHandler = (event: CRDTStreamEvent) => void;

/**
 * SSE streaming client with auto-reconnect.
 *
 * Connects to the CRDT sync server's SSE endpoint and receives
 * real-time change events. Automatically reconnects on disconnection.
 */
export class CRDTStream {
  private baseURL: string;
  private tables: string[];
  private reconnectDelay: number;
  private headers: Record<string, string>;
  private handlers: Set<StreamEventHandler> = new Set();
  private abortController: AbortController | null = null;
  private _connected = false;
  private _lastHLC: HLC | null = null;
  private _since: HLC | null;
  private shouldReconnect = false;
  private fetchImpl: typeof fetch;

  constructor(
    baseURL: string,
    config?: StreamConfig,
    headers?: Record<string, string>,
    fetchImpl?: typeof fetch
  ) {
    this.baseURL = baseURL;
    this.tables = config?.tables ?? [];
    this.reconnectDelay = config?.reconnectDelay ?? 5000;
    this._since = config?.since ?? null;
    this.headers = headers ?? {};
    this.fetchImpl = fetchImpl ?? globalThis.fetch.bind(globalThis);
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

  /** Start the SSE connection. */
  connect(): void {
    if (this._connected) return;
    this.shouldReconnect = true;
    this.connectLoop();
  }

  /** Disconnect and stop reconnecting. */
  disconnect(): void {
    this.shouldReconnect = false;
    this.abortController?.abort();
    this.abortController = null;
    if (this._connected) {
      this._connected = false;
      this.emit({ type: "disconnected" });
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

  private async connectLoop(): Promise<void> {
    while (this.shouldReconnect) {
      try {
        await this.connectOnce();
      } catch (err) {
        if (!this.shouldReconnect) break;
        this.emit({
          type: "error",
          error: err instanceof Error ? err : new Error(String(err)),
        });
      }

      if (this._connected) {
        this._connected = false;
        this.emit({ type: "disconnected" });
      }

      if (!this.shouldReconnect) break;

      // Wait before reconnecting.
      await new Promise((resolve) =>
        setTimeout(resolve, this.reconnectDelay)
      );
    }
  }

  private async connectOnce(): Promise<void> {
    this.abortController = new AbortController();
    const url = this.buildStreamURL();

    const response = await this.fetchImpl(url, {
      method: "GET",
      headers: {
        Accept: "text/event-stream",
        "Cache-Control": "no-cache",
        ...this.headers,
      },
      signal: this.abortController.signal,
    });

    if (!response.ok) {
      const text = await response.text().catch(() => "");
      throw new Error(
        `CRDT stream returned ${response.status}: ${text}`
      );
    }

    this._connected = true;
    this.emit({ type: "connected" });

    const reader = response.body?.getReader();
    if (!reader) {
      throw new Error("CRDT stream: response has no body");
    }

    const decoder = new TextDecoder();
    let buffer = "";
    let eventType = "";
    let dataLines: string[] = [];

    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

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
      reader.releaseLock();
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
