/**
 * Plugin and hook system for the Grove CRDT client.
 *
 * Provides interception points for reads, writes, merges, sync,
 * presence, and lifecycle events. Plugins implement any subset
 * of the specialized interfaces — only implement what you need.
 *
 * @example
 * ```ts
 * import { CRDTStore } from "@grove-js/crdt";
 * import type { StorePlugin, WriteHook, MergeHook } from "@grove-js/crdt";
 *
 * class AuditPlugin implements StorePlugin, WriteHook, MergeHook {
 *   name = "audit";
 *   beforeWrite(ev) { console.log("writing", ev.table, ev.pk, ev.field); return ev; }
 *   afterMerge(ev) { console.log("merged", ev.field, "winner:", ev.winnerNodeId); }
 * }
 *
 * const store = new CRDTStore("node-1", clock);
 * store.use(new AuditPlugin());
 * ```
 */

import type { ChangeRecord, FieldState, DocumentState, HLC } from "./types.js";

// --- Core Plugin Interface ---

/**
 * Base interface for all CRDT store plugins.
 * Implement any combination of the hook interfaces below.
 */
export interface StorePlugin {
  /** Unique name for this plugin. */
  name: string;

  /**
   * Called when the plugin is registered with a store.
   * Use for one-time setup like subscribing to store events.
   */
  init?(): void;

  /**
   * Called when the store is being torn down.
   * Use for cleanup like removing event listeners.
   */
  destroy?(): void;
}

// --- Hook Interfaces ---

/**
 * Intercepts local write operations (setField, incrementCounter, etc.).
 * Use for validation, transformation, access control, or audit logging.
 */
export interface WriteHook {
  /**
   * Called before a local mutation is applied to the store.
   * Return the event to proceed, a modified event to transform,
   * or null to reject the write.
   */
  beforeWrite?(event: WriteEvent): WriteEvent | null;

  /**
   * Called after a local mutation has been applied and persisted.
   * Use for audit logging or triggering side effects.
   */
  afterWrite?(event: WriteEvent): void;
}

/**
 * Intercepts merge operations when remote changes are applied.
 * Use for custom conflict resolution, analytics, or notifications.
 */
export interface MergeHook {
  /**
   * Called before a remote change is merged with local state.
   * Return the change to proceed, a modified change to transform,
   * or null to skip this change.
   */
  beforeMerge?(event: MergeEvent): ChangeRecord | null;

  /**
   * Called after a merge completes. Useful for conflict logging,
   * analytics, or notification triggers.
   */
  afterMerge?(event: MergeEvent): void;
}

/**
 * Intercepts sync operations (pull/push).
 * Use for network-level transformations, batching, or error handling.
 */
export interface SyncHook {
  /** Called before a pull request is sent. Return modified tables/since or null to cancel. */
  beforePull?(event: PullEvent): PullEvent | null;

  /** Called after a pull completes with the received changes. */
  afterPull?(event: PullEvent & { changes: ChangeRecord[] }): void;

  /** Called before local changes are pushed. Return modified changes or null to cancel. */
  beforePush?(changes: ChangeRecord[]): ChangeRecord[] | null;

  /** Called after a push completes. */
  afterPush?(event: { pushed: number; changes: ChangeRecord[] }): void;
}

/**
 * Intercepts document read operations.
 * Use for computed fields, transformations, or access control.
 */
export interface ReadHook {
  /**
   * Called when a document is resolved for reading.
   * Return a transformed document or null to hide it.
   */
  transformDocument?<T>(table: string, pk: string, doc: T): T | null;

  /**
   * Called when a collection is resolved for reading.
   * Return a filtered/transformed array.
   */
  transformCollection?<T>(table: string, docs: T[]): T[];
}

/**
 * Intercepts presence events.
 * Use for presence enrichment, filtering, or analytics.
 */
export interface PresenceHook {
  /** Called before a presence update is sent. Return modified data or null to cancel. */
  beforePresenceUpdate?(topic: string, data: unknown): unknown | null;

  /** Called when a remote presence event is received. */
  onPresenceEvent?(event: {
    type: "join" | "update" | "leave";
    nodeId: string;
    topic: string;
    data?: unknown;
  }): void;
}

/**
 * Intercepts storage operations (IndexedDB, memory, etc.).
 * Use for encryption, compression, or custom serialization.
 */
export interface StorageHook {
  /** Called before a document is persisted to storage. Return transformed state. */
  beforePersist?(table: string, pk: string, doc: DocumentState): DocumentState;

  /** Called after a document is loaded from storage. Return transformed state. */
  afterHydrate?(table: string, pk: string, doc: DocumentState): DocumentState;
}

// --- Event Types ---

/** Event data for write operations. */
export interface WriteEvent {
  table: string;
  pk: string;
  field: string;
  crdtType: string;
  value?: unknown;
  change: ChangeRecord;
  previousState: FieldState | null;
}

/** Event data for merge operations. */
export interface MergeEvent {
  table: string;
  pk: string;
  field: string;
  local: FieldState | null;
  remote: ChangeRecord;
  result?: FieldState;
  winnerNodeId?: string;
  conflictDetected: boolean;
}

/** Event data for pull operations. */
export interface PullEvent {
  tables: string[];
  since?: HLC;
}

// --- Plugin Manager ---

/**
 * Manages registered plugins and dispatches events through hook chains.
 * Used internally by CRDTStore — access via `store.use()`.
 */
export class PluginManager {
  private plugins: StorePlugin[] = [];

  /** Register a plugin. */
  use(plugin: StorePlugin): void {
    this.plugins.push(plugin);
    plugin.init?.();
  }

  /** Remove a plugin by name. */
  remove(name: string): void {
    const idx = this.plugins.findIndex((p) => p.name === name);
    if (idx >= 0) {
      this.plugins[idx].destroy?.();
      this.plugins.splice(idx, 1);
    }
  }

  /** Get a plugin by name. */
  get<T extends StorePlugin>(name: string): T | undefined {
    return this.plugins.find((p) => p.name === name) as T | undefined;
  }

  /** Get all registered plugins. */
  all(): readonly StorePlugin[] {
    return this.plugins;
  }

  /** Destroy all plugins. */
  destroy(): void {
    for (const p of this.plugins) {
      p.destroy?.();
    }
    this.plugins = [];
  }

  // --- Dispatch Methods ---

  /** Dispatch beforeWrite through all WriteHook plugins. */
  dispatchBeforeWrite(event: WriteEvent): WriteEvent | null {
    let current: WriteEvent | null = event;
    for (const p of this.plugins) {
      if (current === null) break;
      if (isWriteHook(p) && p.beforeWrite) {
        current = p.beforeWrite(current);
      }
    }
    return current;
  }

  /** Dispatch afterWrite through all WriteHook plugins. */
  dispatchAfterWrite(event: WriteEvent): void {
    for (const p of this.plugins) {
      if (isWriteHook(p) && p.afterWrite) {
        p.afterWrite(event);
      }
    }
  }

  /** Dispatch beforeMerge through all MergeHook plugins. */
  dispatchBeforeMerge(event: MergeEvent): ChangeRecord | null {
    let current: ChangeRecord | null = event.remote;
    for (const p of this.plugins) {
      if (current === null) break;
      if (isMergeHook(p) && p.beforeMerge) {
        event.remote = current;
        current = p.beforeMerge(event);
      }
    }
    return current;
  }

  /** Dispatch afterMerge through all MergeHook plugins. */
  dispatchAfterMerge(event: MergeEvent): void {
    for (const p of this.plugins) {
      if (isMergeHook(p) && p.afterMerge) {
        p.afterMerge(event);
      }
    }
  }

  /** Dispatch beforePull through all SyncHook plugins. */
  dispatchBeforePull(event: PullEvent): PullEvent | null {
    let current: PullEvent | null = event;
    for (const p of this.plugins) {
      if (current === null) break;
      if (isSyncHook(p) && p.beforePull) {
        current = p.beforePull(current);
      }
    }
    return current;
  }

  /** Dispatch afterPull through all SyncHook plugins. */
  dispatchAfterPull(
    event: PullEvent & { changes: ChangeRecord[] }
  ): void {
    for (const p of this.plugins) {
      if (isSyncHook(p) && p.afterPull) {
        p.afterPull(event);
      }
    }
  }

  /** Dispatch beforePush through all SyncHook plugins. */
  dispatchBeforePush(changes: ChangeRecord[]): ChangeRecord[] | null {
    let current: ChangeRecord[] | null = changes;
    for (const p of this.plugins) {
      if (current === null) break;
      if (isSyncHook(p) && p.beforePush) {
        current = p.beforePush(current);
      }
    }
    return current;
  }

  /** Dispatch afterPush through all SyncHook plugins. */
  dispatchAfterPush(event: {
    pushed: number;
    changes: ChangeRecord[];
  }): void {
    for (const p of this.plugins) {
      if (isSyncHook(p) && p.afterPush) {
        p.afterPush(event);
      }
    }
  }

  /** Dispatch transformDocument through all ReadHook plugins. */
  dispatchTransformDocument<T>(
    table: string,
    pk: string,
    doc: T
  ): T | null {
    let current: T | null = doc;
    for (const p of this.plugins) {
      if (current === null) break;
      if (isReadHook(p) && p.transformDocument) {
        current = p.transformDocument(table, pk, current);
      }
    }
    return current;
  }

  /** Dispatch transformCollection through all ReadHook plugins. */
  dispatchTransformCollection<T>(table: string, docs: T[]): T[] {
    let current = docs;
    for (const p of this.plugins) {
      if (isReadHook(p) && p.transformCollection) {
        current = p.transformCollection(table, current);
      }
    }
    return current;
  }

  /** Dispatch beforePresenceUpdate through all PresenceHook plugins. */
  dispatchBeforePresenceUpdate(
    topic: string,
    data: unknown
  ): unknown | null {
    let current: unknown | null = data;
    for (const p of this.plugins) {
      if (current === null) break;
      if (isPresenceHook(p) && p.beforePresenceUpdate) {
        current = p.beforePresenceUpdate(topic, current);
      }
    }
    return current;
  }

  /** Dispatch onPresenceEvent through all PresenceHook plugins. */
  dispatchOnPresenceEvent(event: {
    type: "join" | "update" | "leave";
    nodeId: string;
    topic: string;
    data?: unknown;
  }): void {
    for (const p of this.plugins) {
      if (isPresenceHook(p) && p.onPresenceEvent) {
        p.onPresenceEvent(event);
      }
    }
  }

  /** Dispatch beforePersist through all StorageHook plugins. */
  dispatchBeforePersist(
    table: string,
    pk: string,
    doc: DocumentState
  ): DocumentState {
    let current = doc;
    for (const p of this.plugins) {
      if (isStorageHook(p) && p.beforePersist) {
        current = p.beforePersist(table, pk, current);
      }
    }
    return current;
  }

  /** Dispatch afterHydrate through all StorageHook plugins. */
  dispatchAfterHydrate(
    table: string,
    pk: string,
    doc: DocumentState
  ): DocumentState {
    let current = doc;
    for (const p of this.plugins) {
      if (isStorageHook(p) && p.afterHydrate) {
        current = p.afterHydrate(table, pk, current);
      }
    }
    return current;
  }
}

// --- Type Guards ---

function isWriteHook(p: StorePlugin): p is StorePlugin & WriteHook {
  return "beforeWrite" in p || "afterWrite" in p;
}

function isMergeHook(p: StorePlugin): p is StorePlugin & MergeHook {
  return "beforeMerge" in p || "afterMerge" in p;
}

function isSyncHook(p: StorePlugin): p is StorePlugin & SyncHook {
  return (
    "beforePull" in p ||
    "afterPull" in p ||
    "beforePush" in p ||
    "afterPush" in p
  );
}

function isReadHook(p: StorePlugin): p is StorePlugin & ReadHook {
  return "transformDocument" in p || "transformCollection" in p;
}

function isPresenceHook(p: StorePlugin): p is StorePlugin & PresenceHook {
  return "beforePresenceUpdate" in p || "onPresenceEvent" in p;
}

function isStorageHook(p: StorePlugin): p is StorePlugin & StorageHook {
  return "beforePersist" in p || "afterHydrate" in p;
}
