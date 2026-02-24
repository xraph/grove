/**
 * CRDT client for sync operations (pull/push/stream).
 *
 * Delegates transport to pluggable Transport/StreamTransport implementations.
 * Defaults to HTTP transport when baseURL is provided.
 */

import type {
  CRDTClientConfig,
  StreamConfig,
  Transport,
  StreamTransport,
  StreamSubscription,
  PullRequest,
  PullResponse,
  PushResponse,
  ChangeRecord,
  HLC,
} from "./types.js";
import { HybridClock } from "./hlc.js";
import { HttpStreamTransport, isStreamTransport } from "./transport.js";

// Re-export CRDTError for backward compatibility.
export { CRDTError } from "./errors.js";

/**
 * CRDT client for Grove sync operations.
 *
 * Provides typed methods for pull/push sync and stream creation.
 * Maintains an internal HLC clock that stays synchronized with the server.
 * Supports pluggable transports — defaults to HTTP when baseURL is provided.
 */
export class CRDTClient {
  readonly nodeID: string;
  readonly clock: HybridClock;

  private tables: string[];
  private transport: Transport;
  private streamTransport: StreamTransport | null;

  constructor(config: CRDTClientConfig) {
    this.nodeID = config.nodeID;
    this.tables = config.tables ?? [];
    this.clock = new HybridClock(config.nodeID);

    // Resolve transport.
    if (config.transport) {
      this.transport = config.transport;
    } else if (config.baseURL) {
      this.transport = new HttpStreamTransport({
        baseURL: config.baseURL,
        fetch: config.fetch,
        headers: config.headers,
        auth: config.auth,
      });
    } else {
      throw new Error("CRDTClient requires either `baseURL` or `transport`.");
    }

    // Resolve stream transport.
    if (config.streamTransport) {
      this.streamTransport = config.streamTransport;
    } else if (isStreamTransport(this.transport)) {
      this.streamTransport = this.transport;
    } else {
      this.streamTransport = null;
    }
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

    const response = await this.transport.pull(req);

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

    const response = await this.transport.push({
      changes,
      node_id: this.nodeID,
    });

    // Update local clock with server's latest HLC.
    if (response.latest_hlc) {
      this.clock.update(response.latest_hlc);
    }

    return response;
  }

  /**
   * Create a stream subscription for real-time changes.
   *
   * @param config - Stream configuration (tables, reconnect delay, since HLC).
   * @returns A StreamSubscription instance (call .connect() to start).
   * @throws Error if no stream transport is available.
   */
  stream(config?: StreamConfig): StreamSubscription {
    if (!this.streamTransport) {
      throw new Error(
        "No stream transport available. Provide a `streamTransport` or use a transport that supports streaming."
      );
    }

    return this.streamTransport.subscribe({
      tables: config?.tables ?? this.tables,
      reconnectDelay: config?.reconnectDelay,
      since: config?.since,
    });
  }
}
