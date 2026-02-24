/**
 * HTTP client for CRDT sync operations (pull/push).
 */

import type {
  CRDTClientConfig,
  StreamConfig,
  PullRequest,
  PullResponse,
  PushRequest,
  PushResponse,
  ChangeRecord,
  HLC,
} from "./types.js";
import { HybridClock } from "./hlc.js";
import { CRDTStream } from "./stream.js";

/** Error thrown by CRDT client operations. */
export class CRDTError extends Error {
  constructor(
    message: string,
    public readonly statusCode?: number
  ) {
    super(message);
    this.name = "CRDTError";
  }
}

/**
 * HTTP client for Grove CRDT sync operations.
 *
 * Provides typed methods for pull/push sync and SSE stream creation.
 * Maintains an internal HLC clock that stays synchronized with the server.
 */
export class CRDTClient {
  readonly nodeID: string;
  readonly clock: HybridClock;

  private baseURL: string;
  private tables: string[];
  private fetchImpl: typeof fetch;
  private headers: Record<string, string>;

  constructor(config: CRDTClientConfig) {
    this.nodeID = config.nodeID;
    this.baseURL = config.baseURL.replace(/\/+$/, ""); // Strip trailing slashes
    this.tables = config.tables ?? [];
    this.fetchImpl = config.fetch ?? globalThis.fetch.bind(globalThis);
    this.headers = config.headers ?? {};
    this.clock = new HybridClock(config.nodeID);
  }

  /**
   * Pull changes from the server since the given HLC.
   *
   * @param tables - Tables to pull changes for (defaults to client's configured tables).
   * @param since - HLC after which to return changes (optional).
   * @returns Pull response with changes and latest HLC.
   */
  async pull(tables?: string[], since?: HLC): Promise<PullResponse> {
    const req: PullRequest = {
      tables: tables ?? this.tables,
      node_id: this.nodeID,
      ...(since && { since }),
    };

    const response = await this.request<PullResponse>("/pull", req);

    // Update local clock with server's latest HLC.
    if (response.latest_hlc) {
      this.clock.update(response.latest_hlc);
    }

    return response;
  }

  /**
   * Push local changes to the server.
   *
   * @param changes - Array of change records to push.
   * @returns Push response with merged count and latest HLC.
   */
  async push(changes: ChangeRecord[]): Promise<PushResponse> {
    if (changes.length === 0) {
      return { merged: 0, latest_hlc: this.clock.now() };
    }

    const req: PushRequest = {
      changes,
      node_id: this.nodeID,
    };

    const response = await this.request<PushResponse>("/push", req);

    // Update local clock with server's latest HLC.
    if (response.latest_hlc) {
      this.clock.update(response.latest_hlc);
    }

    return response;
  }

  /**
   * Create an SSE stream connection for real-time changes.
   *
   * @param config - Stream configuration (tables, reconnect delay, since HLC).
   * @returns A CRDTStream instance (call .connect() to start).
   */
  stream(config?: StreamConfig): CRDTStream {
    return new CRDTStream(
      this.baseURL,
      {
        tables: config?.tables ?? this.tables,
        reconnectDelay: config?.reconnectDelay,
        since: config?.since,
      },
      this.headers,
      this.fetchImpl
    );
  }

  private async request<T>(path: string, body: unknown): Promise<T> {
    const response = await this.fetchImpl(`${this.baseURL}${path}`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...this.headers,
      },
      body: JSON.stringify(body),
    });

    if (!response.ok) {
      const text = await response.text().catch(() => "");
      throw new CRDTError(
        `CRDT ${path} returned ${response.status}: ${text}`,
        response.status
      );
    }

    return (await response.json()) as T;
  }
}
