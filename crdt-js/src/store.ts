/**
 * In-memory CRDT state store with dirty tracking and subscriptions.
 *
 * Manages local CRDT state, applies remote changes via merge functions,
 * and provides fine-grained subscriptions for React integration
 * (compatible with useSyncExternalStore).
 */

import type {
  HLC,
  ChangeRecord,
  FieldState,
  DocumentState,
  PNCounterState,
  ORSetState,
  ORSetTag,
  CRDTType,
} from "./types.js";
import { HybridClock } from "./hlc.js";
import {
  mergeFieldState,
  counterValue,
  setElements,
  tagKey,
} from "./merge.js";

type Listener = () => void;

/**
 * In-memory CRDT state store.
 *
 * Tracks documents (table + pk) with per-field CRDT state. Supports:
 * - Reading resolved document values
 * - Local mutations (LWW set, counter inc/dec, set add/remove)
 * - Applying remote changes via merge
 * - Dirty tracking for pending push
 * - Fine-grained subscriptions for React hooks
 */
export class CRDTStore {
  private nodeID: string;
  private clock: HybridClock;

  /** Internal state: table → pk → DocumentState */
  private state = new Map<string, Map<string, DocumentState>>();

  /** Pending local changes that need to be pushed to the server. */
  private pending: ChangeRecord[] = [];

  /** Global listeners notified on any state change. */
  private globalListeners = new Set<Listener>();

  /** Per-document listeners: "table:pk" → listeners */
  private docListeners = new Map<string, Set<Listener>>();

  /** Per-table listeners: table → listeners */
  private tableListeners = new Map<string, Set<Listener>>();

  constructor(nodeID: string, clock: HybridClock) {
    this.nodeID = nodeID;
    this.clock = clock;
  }

  // --- Read ---

  /**
   * Get the resolved state of a document.
   * Returns null if the document doesn't exist or is tombstoned.
   */
  getDocument<T = Record<string, unknown>>(
    table: string,
    pk: string
  ): T | null {
    const doc = this.state.get(table)?.get(pk);
    if (!doc || doc.tombstone) return null;
    return this.resolveDocument(doc) as T;
  }

  /**
   * Get all documents in a table.
   * Excludes tombstoned documents.
   */
  getCollection<T = Record<string, unknown>>(table: string): T[] {
    const tableMap = this.state.get(table);
    if (!tableMap) return [];

    const result: T[] = [];
    for (const doc of tableMap.values()) {
      if (!doc.tombstone) {
        const resolved = this.resolveDocument(doc);
        result.push(resolved as T);
      }
    }
    return result;
  }

  // --- Write (Local Mutations) ---

  /**
   * Update a single LWW field locally. Returns the ChangeRecord for push.
   */
  setField(
    table: string,
    pk: string,
    field: string,
    value: unknown
  ): ChangeRecord {
    const hlc = this.clock.now();
    const change: ChangeRecord = {
      table,
      pk,
      field,
      crdt_type: "lww",
      hlc,
      node_id: this.nodeID,
      value,
    };

    this.applyChangeInternal(change);
    this.pending.push(change);
    this.notifyListeners(table, pk);
    return change;
  }

  /**
   * Increment a counter field. Returns the ChangeRecord for push.
   */
  incrementCounter(
    table: string,
    pk: string,
    field: string,
    delta = 1
  ): ChangeRecord {
    const hlc = this.clock.now();
    const change: ChangeRecord = {
      table,
      pk,
      field,
      crdt_type: "counter",
      hlc,
      node_id: this.nodeID,
      counter_delta: { inc: delta, dec: 0 },
    };

    this.applyChangeInternal(change);
    this.pending.push(change);
    this.notifyListeners(table, pk);
    return change;
  }

  /**
   * Decrement a counter field. Returns the ChangeRecord for push.
   */
  decrementCounter(
    table: string,
    pk: string,
    field: string,
    delta = 1
  ): ChangeRecord {
    const hlc = this.clock.now();
    const change: ChangeRecord = {
      table,
      pk,
      field,
      crdt_type: "counter",
      hlc,
      node_id: this.nodeID,
      counter_delta: { inc: 0, dec: delta },
    };

    this.applyChangeInternal(change);
    this.pending.push(change);
    this.notifyListeners(table, pk);
    return change;
  }

  /**
   * Add elements to a set field. Returns the ChangeRecord for push.
   */
  addToSet(
    table: string,
    pk: string,
    field: string,
    elements: unknown[]
  ): ChangeRecord {
    const hlc = this.clock.now();
    const change: ChangeRecord = {
      table,
      pk,
      field,
      crdt_type: "set",
      hlc,
      node_id: this.nodeID,
      set_op: { op: "add", elements },
    };

    this.applyChangeInternal(change);
    this.pending.push(change);
    this.notifyListeners(table, pk);
    return change;
  }

  /**
   * Remove elements from a set field. Returns the ChangeRecord for push.
   */
  removeFromSet(
    table: string,
    pk: string,
    field: string,
    elements: unknown[]
  ): ChangeRecord {
    const hlc = this.clock.now();
    const change: ChangeRecord = {
      table,
      pk,
      field,
      crdt_type: "set",
      hlc,
      node_id: this.nodeID,
      set_op: { op: "remove", elements },
    };

    this.applyChangeInternal(change);
    this.pending.push(change);
    this.notifyListeners(table, pk);
    return change;
  }

  /**
   * Delete a document (tombstone). Returns the ChangeRecord for push.
   */
  deleteDocument(table: string, pk: string): ChangeRecord {
    const hlc = this.clock.now();
    const change: ChangeRecord = {
      table,
      pk,
      field: "",
      crdt_type: "lww",
      hlc,
      node_id: this.nodeID,
      tombstone: true,
    };

    const doc = this.ensureDocument(table, pk);
    doc.tombstone = true;
    doc.tombstone_hlc = hlc;
    this.notifyListeners(table, pk);

    this.pending.push(change);
    return change;
  }

  // --- Sync ---

  /**
   * Apply remote changes from a pull or stream event.
   * Uses merge functions to resolve conflicts.
   */
  applyChanges(changes: ChangeRecord[]): void {
    const affected = new Set<string>();

    for (const change of changes) {
      this.applyChangeInternal(change);
      affected.add(`${change.table}:${change.pk}`);
    }

    // Batch-notify all affected documents.
    for (const key of affected) {
      const [table, pk] = key.split(":", 2);
      this.notifyListeners(table, pk);
    }
  }

  /** Get all pending (dirty) local changes that need to be pushed. */
  getPendingChanges(): ChangeRecord[] {
    return [...this.pending];
  }

  /** Clear pending changes after a successful push. */
  clearPendingChanges(): void {
    this.pending = [];
  }

  /** Get the number of pending changes. */
  get pendingCount(): number {
    return this.pending.length;
  }

  // --- Subscriptions ---

  /**
   * Subscribe to any state change. Returns an unsubscribe function.
   * Compatible with React's useSyncExternalStore.
   */
  subscribe(listener: Listener): () => void {
    this.globalListeners.add(listener);
    return () => {
      this.globalListeners.delete(listener);
    };
  }

  /**
   * Subscribe to changes for a specific document.
   * Returns an unsubscribe function.
   */
  subscribeDocument(
    table: string,
    pk: string,
    listener: Listener
  ): () => void {
    const key = `${table}:${pk}`;
    let listeners = this.docListeners.get(key);
    if (!listeners) {
      listeners = new Set();
      this.docListeners.set(key, listeners);
    }
    listeners.add(listener);
    return () => {
      listeners!.delete(listener);
      if (listeners!.size === 0) {
        this.docListeners.delete(key);
      }
    };
  }

  /**
   * Subscribe to changes for a specific table.
   * Returns an unsubscribe function.
   */
  subscribeCollection(table: string, listener: Listener): () => void {
    let listeners = this.tableListeners.get(table);
    if (!listeners) {
      listeners = new Set();
      this.tableListeners.set(table, listeners);
    }
    listeners.add(listener);
    return () => {
      listeners!.delete(listener);
      if (listeners!.size === 0) {
        this.tableListeners.delete(table);
      }
    };
  }

  // --- Internals ---

  private applyChangeInternal(change: ChangeRecord): void {
    if (change.tombstone) {
      const doc = this.ensureDocument(change.table, change.pk);
      doc.tombstone = true;
      doc.tombstone_hlc = change.hlc;
      return;
    }

    const doc = this.ensureDocument(change.table, change.pk);
    const existing = doc.fields[change.field] ?? null;
    doc.fields[change.field] = mergeFieldState(existing, change);
  }

  private ensureDocument(table: string, pk: string): DocumentState {
    let tableMap = this.state.get(table);
    if (!tableMap) {
      tableMap = new Map();
      this.state.set(table, tableMap);
    }

    let doc = tableMap.get(pk);
    if (!doc) {
      doc = {
        table,
        pk,
        fields: {},
        tombstone: false,
      };
      tableMap.set(pk, doc);
    }

    return doc;
  }

  /**
   * Resolve a DocumentState into a plain object with field values.
   * Includes _table and _pk metadata fields.
   */
  private resolveDocument(doc: DocumentState): Record<string, unknown> {
    const result: Record<string, unknown> = {
      _table: doc.table,
      _pk: doc.pk,
    };

    for (const [field, state] of Object.entries(doc.fields)) {
      switch (state.type) {
        case "lww":
          result[field] = state.value;
          break;
        case "counter":
          result[field] = state.counter_state
            ? counterValue(state.counter_state)
            : 0;
          break;
        case "set":
          result[field] = state.set_state
            ? setElements(state.set_state)
            : [];
          break;
        default:
          result[field] = state.value;
      }
    }

    return result;
  }

  private notifyListeners(table: string, pk: string): void {
    // Document-level listeners.
    const docKey = `${table}:${pk}`;
    const docListeners = this.docListeners.get(docKey);
    if (docListeners) {
      for (const listener of docListeners) listener();
    }

    // Table-level listeners.
    const tableListeners = this.tableListeners.get(table);
    if (tableListeners) {
      for (const listener of tableListeners) listener();
    }

    // Global listeners.
    for (const listener of this.globalListeners) listener();
  }
}
