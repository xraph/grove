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
  PresenceState,
  PresenceConfig,
  HLC,
} from "./types.js";
import { HybridClock } from "./hlc.js";
import { HttpStreamTransport, isStreamTransport } from "./transport.js";
import { PresenceManager } from "./presence.js";

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
  readonly presence: PresenceManager;

  private tables: string[];
  private transport: Transport;
  private streamTransport: StreamTransport | null;
  private presenceConfig: PresenceConfig | undefined;
  private heartbeatTimers = new Map<string, ReturnType<typeof setInterval>>();
  private lastPresenceData = new Map<string, unknown>();

  constructor(config: CRDTClientConfig) {
    this.nodeID = config.nodeID;
    this.tables = config.tables ?? [];
    this.clock = new HybridClock(config.nodeID);
    this.presence = new PresenceManager(config.nodeID);
    this.presenceConfig = config.presence;

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

  // --- Presence ---

  /**
   * Update your presence for a topic (e.g., "documents:doc-1").
   * Automatically starts a heartbeat to keep presence alive on the server.
   *
   * @param topic - Presence scope (typically "table:pk" or a room name).
   * @param data - User-defined presence payload (cursor, typing state, user info, etc.).
   */
  async updatePresence<T = Record<string, unknown>>(
    topic: string,
    data: T
  ): Promise<void> {
    if (!this.transport.updatePresence) {
      throw new Error(
        "Transport does not support presence. Use HttpTransport or implement updatePresence()."
      );
    }

    this.lastPresenceData.set(topic, data);

    await this.transport.updatePresence({
      node_id: this.nodeID,
      topic,
      data: data as Record<string, unknown>,
    });

    // Start or reset heartbeat for this topic.
    this.startHeartbeat(topic);
  }

  /**
   * Leave a topic — stops the heartbeat and notifies the server.
   *
   * @param topic - The topic to leave.
   */
  async leavePresence(topic: string): Promise<void> {
    this.stopHeartbeat(topic);
    this.lastPresenceData.delete(topic);

    if (this.transport.updatePresence) {
      await this.transport.updatePresence({
        node_id: this.nodeID,
        topic,
        data: null as unknown as Record<string, unknown>,
      });
    }
  }

  /**
   * Get all current presence entries for a topic from the server.
   *
   * @param topic - The topic to query.
   * @returns Array of presence states.
   */
  async getPresence<T = Record<string, unknown>>(
    topic: string
  ): Promise<PresenceState<T>[]> {
    if (!this.transport.getPresence) {
      throw new Error(
        "Transport does not support presence. Use HttpTransport or implement getPresence()."
      );
    }

    return this.transport.getPresence(topic) as Promise<PresenceState<T>[]>;
  }

  /**
   * Leave all topics and stop all heartbeats. Call on cleanup/unmount.
   */
  async leaveAllPresence(): Promise<void> {
    const topics = [...this.heartbeatTimers.keys()];
    for (const topic of topics) {
      await this.leavePresence(topic);
    }
  }

  private startHeartbeat(topic: string): void {
    this.stopHeartbeat(topic);

    const interval = this.presenceConfig?.heartbeatInterval ?? 10_000;

    const timer = setInterval(() => {
      const lastData = this.lastPresenceData.get(topic);
      if (lastData !== undefined && this.transport.updatePresence) {
        this.transport
          .updatePresence({
            node_id: this.nodeID,
            topic,
            data: lastData as Record<string, unknown>,
          })
          .catch(() => {});
      }
    }, interval);

    this.heartbeatTimers.set(topic, timer);
  }

  private stopHeartbeat(topic: string): void {
    const timer = this.heartbeatTimers.get(topic);
    if (timer) {
      clearInterval(timer);
      this.heartbeatTimers.delete(topic);
    }
  }
}
