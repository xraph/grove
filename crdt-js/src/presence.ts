/**
 * Client-side presence manager for tracking remote peers' ephemeral state.
 *
 * Stores presence data received via SSE events and provides fine-grained
 * subscriptions compatible with React's useSyncExternalStore.
 */

import type { PresenceState, PresenceEvent } from "./types.js";

type Listener = () => void;

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
   * Get all presence states for a topic, excluding the local node.
   * Returns a new array on each call for useSyncExternalStore compatibility.
   */
  getPresence<T = Record<string, unknown>>(
    topic: string
  ): PresenceState<T>[] {
    const topicMap = this.peers.get(topic);
    if (!topicMap) return [];

    const result: PresenceState<T>[] = [];
    for (const [nodeID, state] of topicMap) {
      if (nodeID !== this.localNodeID) {
        result.push(state as PresenceState<T>);
      }
    }
    return result;
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
    // Topic-level listeners.
    const topicListeners = this.topicListeners.get(topic);
    if (topicListeners) {
      for (const listener of topicListeners) listener();
    }

    // Global listeners.
    for (const listener of this.globalListeners) listener();
  }
}
