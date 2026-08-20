/**
 * Client-side presence manager for tracking remote peers' ephemeral state.
 *
 * Stores presence data received via SSE events and provides fine-grained
 * subscriptions compatible with React's useSyncExternalStore.
 */

import type { PresenceState, PresenceEvent } from "./types.js";

type Listener = () => void;

/** Shared empty — a fresh [] per call breaks useSyncExternalStore. */
const EMPTY_PRESENCE: PresenceState[] = [];

/**
 * In-memory store for remote peers' presence state.
 *
 * This is a read-only store — it holds presence state received from
 * the server via SSE events. The user's own presence updates are
 * sent through CRDTClient.updatePresence().
 */
export class PresenceManager {
  /** topic → nodeID → state */
  private peers = new Map<string, Map<string, PresenceState>>();

  /** Cached per-topic snapshots. Cleared for a topic when it changes. */
  private snapshots = new Map<string, PresenceState[]>();

  /** Per-topic listeners: topic → listeners */
  private topicListeners = new Map<string, Set<Listener>>();

  /** Global listeners notified on any presence change. */
  private globalListeners = new Set<Listener>();

  /** The local node ID (excluded from getPresence results). */
  private localNodeID: string;

  constructor(localNodeID: string) {
    this.localNodeID = localNodeID;
  }

  /**
   * All presence states for a topic, excluding the local node.
   *
   * The returned array is CACHED and referentially stable until the topic
   * changes, which is what useSyncExternalStore requires — returning a
   * fresh array per call causes an unbounded render loop.
   */
  getPresence<T = Record<string, unknown>>(topic: string): PresenceState<T>[] {
    const hit = this.snapshots.get(topic);
    if (hit) return hit as PresenceState<T>[];

    const topicMap = this.peers.get(topic);
    if (!topicMap) return EMPTY_PRESENCE as PresenceState<T>[];

    const result: PresenceState[] = [];
    for (const [nodeID, state] of topicMap) {
      if (nodeID !== this.localNodeID) result.push(state);
    }
    const frozen = result.length === 0 ? EMPTY_PRESENCE : result;
    this.snapshots.set(topic, frozen);
    return frozen as PresenceState<T>[];
  }

  /**
   * Get a specific peer's presence for a topic.
   */
  getPeer<T = Record<string, unknown>>(
    topic: string,
    nodeID: string
  ): PresenceState<T> | null {
    const topicMap = this.peers.get(topic);
    if (!topicMap) return null;
    return (topicMap.get(nodeID) as PresenceState<T>) ?? null;
  }

  /**
   * Apply a presence event from an SSE stream.
   * Updates local state and notifies listeners.
   */
  applyEvent(event: PresenceEvent): void {
    const { topic, node_id: nodeID } = event;

    switch (event.type) {
      case "join":
      case "update": {
        let topicMap = this.peers.get(topic);
        if (!topicMap) {
          topicMap = new Map();
          this.peers.set(topic, topicMap);
        }
        topicMap.set(nodeID, {
          node_id: nodeID,
          topic,
          data: event.data ?? {},
          updated_at: Date.now(),
        });
        break;
      }
      case "leave": {
        const topicMap = this.peers.get(topic);
        if (topicMap) {
          topicMap.delete(nodeID);
          if (topicMap.size === 0) {
            this.peers.delete(topic);
          }
        }
        break;
      }
    }

    this.notifyListeners(topic);
  }

  /**
   * Subscribe to presence changes for a specific topic.
   * Returns an unsubscribe function. Compatible with useSyncExternalStore.
   */
  subscribe(topic: string, listener: Listener): () => void {
    let listeners = this.topicListeners.get(topic);
    if (!listeners) {
      listeners = new Set();
      this.topicListeners.set(topic, listeners);
    }
    listeners.add(listener);

    return () => {
      listeners!.delete(listener);
      if (listeners!.size === 0) {
        this.topicListeners.delete(topic);
      }
    };
  }

  /**
   * Subscribe to all presence changes across all topics.
   * Returns an unsubscribe function.
   */
  subscribeAll(listener: Listener): () => void {
    this.globalListeners.add(listener);
    return () => {
      this.globalListeners.delete(listener);
    };
  }

  /**
   * Replace a topic's peers from a server snapshot.
   *
   * Needed on join and after every stream reconnect: SSE only delivers
   * CHANGES, so peers that were already idle when you subscribed are
   * otherwise invisible until they happen to move.
   */
  seed(topic: string, states: PresenceState[]): void {
    const topicMap = new Map<string, PresenceState>();
    for (const state of states) {
      topicMap.set(state.node_id, state);
    }
    if (topicMap.size === 0) {
      this.peers.delete(topic);
    } else {
      this.peers.set(topic, topicMap);
    }
    this.notifyListeners(topic);
  }

  /**
   * Drop peers whose last update is older than maxAgeMs.
   *
   * The server expires presence on its own TTL and broadcasts a leave, but
   * that only reaches clients with a live stream. A client that was
   * disconnected across the expiry never sees the leave.
   */
  prune(maxAgeMs: number): void {
    const cutoff = Date.now() - maxAgeMs;
    for (const [topic, topicMap] of [...this.peers]) {
      let dropped = false;
      for (const [nodeID, state] of [...topicMap]) {
        if (state.updated_at < cutoff) {
          topicMap.delete(nodeID);
          dropped = true;
        }
      }
      if (topicMap.size === 0) this.peers.delete(topic);
      if (dropped) this.notifyListeners(topic);
    }
  }

  /**
   * Clear all presence state and notify listeners.
   */
  clear(): void {
    const topics = [...this.peers.keys()];
    this.peers.clear();
    for (const topic of topics) {
      this.notifyListeners(topic);
    }
  }

  private notifyListeners(topic: string): void {
    this.snapshots.delete(topic);

    // Topic-level listeners.
    const topicListeners = this.topicListeners.get(topic);
    if (topicListeners) {
      for (const listener of topicListeners) listener();
    }

    // Global listeners.
    for (const listener of this.globalListeners) listener();
  }
}
