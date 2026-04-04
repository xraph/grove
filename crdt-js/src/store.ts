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
  StorageAdapter,
  RGAListState,
  DocumentCRDTState,
} from "./types.js";
import { HybridClock } from "./hlc.js";
import {
  mergeFieldState,
  counterValue,
  setElements,
  listElements,
  documentResolve,
  tagKey,
} from "./merge.js";
import { MemoryStorage } from "./storage.js";
import { UndoManager } from "./undo.js";
import { PluginManager } from "./plugin.js";
import type { StorePlugin, WriteEvent, MergeEvent } from "./plugin.js";

type Listener = () => void;

/** Serializable snapshot of store state. */
export interface StateSnapshot {
  version: number;
  nodeId: string;
  timestamp: number;
  tables: Record<string, Record<string, DocumentState>>;
  pending: ChangeRecord[];
}

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
  private storage: StorageAdapter;
  private undoManager: UndoManager;
  private plugins: PluginManager;

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

  /**
   * Resolves when persisted state has been hydrated.
   * The store is usable immediately (starts empty), but consumers
   * should await `ready` before relying on persisted data.
   */
  readonly ready: Promise<void>;

  constructor(nodeID: string, clock: HybridClock, storage?: StorageAdapter) {
    this.nodeID = nodeID;
    this.clock = clock;
    this.storage = storage ?? new MemoryStorage();
    this.undoManager = new UndoManager();
    this.plugins = new PluginManager();
    this.ready = this.hydrate();
  }

  // --- Plugins ---

  /**
   * Register a plugin to intercept store operations.
   * Plugins can implement any combination of hook interfaces:
   * WriteHook, MergeHook, SyncHook, ReadHook, PresenceHook, StorageHook.
   *
   * @example
   * ```ts
   * store.use({
   *   name: "logger",
   *   afterWrite(ev) { console.log("wrote", ev.field, ev.value); },
   *   afterMerge(ev) { console.log("merged", ev.field, "conflict:", ev.conflictDetected); },
   * });
   * ```
   */
  use(plugin: StorePlugin): void {
    this.plugins.use(plugin);
  }

  /**
   * Remove a plugin by name.
   */
  removePlugin(name: string): void {
    this.plugins.remove(name);
  }

  /**
   * Get a registered plugin by name.
   */
  getPlugin<T extends StorePlugin>(name: string): T | undefined {
    return this.plugins.get<T>(name);
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
    const resolved = this.resolveDocument(doc) as T;
    return this.plugins.dispatchTransformDocument(table, pk, resolved);
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
        const resolved = this.resolveDocument(doc) as T;
        const transformed = this.plugins.dispatchTransformDocument(table, doc.pk, resolved);
        if (transformed !== null) {
          result.push(transformed);
        }
      }
    }
    return this.plugins.dispatchTransformCollection(table, result);
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
  ): ChangeRecord | null {
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

    const previousState = this.captureFieldState(table, pk, field);

    // Run beforeWrite plugins.
    const writeEvent: WriteEvent = { table, pk, field, crdtType: "lww", value, change, previousState };
    const allowed = this.plugins.dispatchBeforeWrite(writeEvent);
    if (!allowed) return null;

    this.applyChangeInternal(allowed.change);
    this.undoManager.record(allowed.change, previousState);
    this.pending.push(allowed.change);
    this.persistDocument(table, pk);
    this.persistPending();
    this.notifyListeners(table, pk);

    // Run afterWrite plugins.
    this.plugins.dispatchAfterWrite(allowed);

    return allowed.change;
  }

  /**
   * Increment a counter field. Returns the ChangeRecord for push.
   */
  incrementCounter(
    table: string,
    pk: string,
    field: string,
    delta = 1
  ): ChangeRecord | null {
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

    const previousState = this.captureFieldState(table, pk, field);
    const writeEvent: WriteEvent = { table, pk, field, crdtType: "counter", value: delta, change, previousState };
    const allowed = this.plugins.dispatchBeforeWrite(writeEvent);
    if (!allowed) return null;

    this.applyChangeInternal(allowed.change);
    this.undoManager.record(allowed.change, previousState);
    this.pending.push(allowed.change);
    this.persistDocument(table, pk);
    this.persistPending();
    this.notifyListeners(table, pk);
    this.plugins.dispatchAfterWrite(allowed);
    return allowed.change;
  }

  /**
   * Decrement a counter field. Returns the ChangeRecord for push.
   */
  decrementCounter(
    table: string,
    pk: string,
    field: string,
    delta = 1
  ): ChangeRecord | null {
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

    const previousState = this.captureFieldState(table, pk, field);
    const writeEvent: WriteEvent = { table, pk, field, crdtType: "counter", value: -delta, change, previousState };
    const allowed = this.plugins.dispatchBeforeWrite(writeEvent);
    if (!allowed) return null;

    this.applyChangeInternal(allowed.change);
    this.undoManager.record(allowed.change, previousState);
    this.pending.push(allowed.change);
    this.persistDocument(table, pk);
    this.persistPending();
    this.notifyListeners(table, pk);
    this.plugins.dispatchAfterWrite(allowed);
    return allowed.change;
  }

  /**
   * Add elements to a set field. Returns the ChangeRecord for push.
   */
  addToSet(
    table: string,
    pk: string,
    field: string,
    elements: unknown[]
  ): ChangeRecord | null {
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

    const previousState = this.captureFieldState(table, pk, field);
    const writeEvent: WriteEvent = { table, pk, field, crdtType: "set", value: elements, change, previousState };
    const allowed = this.plugins.dispatchBeforeWrite(writeEvent);
    if (!allowed) return null;

    this.applyChangeInternal(allowed.change);
    this.undoManager.record(allowed.change, previousState);
    this.pending.push(allowed.change);
    this.persistDocument(table, pk);
    this.persistPending();
    this.notifyListeners(table, pk);
    this.plugins.dispatchAfterWrite(allowed);
    return allowed.change;
  }

  /**
   * Remove elements from a set field. Returns the ChangeRecord for push.
   */
  removeFromSet(
    table: string,
    pk: string,
    field: string,
    elements: unknown[]
  ): ChangeRecord | null {
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

    const previousState = this.captureFieldState(table, pk, field);
    const writeEvent: WriteEvent = { table, pk, field, crdtType: "set", value: elements, change, previousState };
    const allowed = this.plugins.dispatchBeforeWrite(writeEvent);
    if (!allowed) return null;

    this.applyChangeInternal(allowed.change);
    this.undoManager.record(allowed.change, previousState);
    this.pending.push(allowed.change);
    this.persistDocument(table, pk);
    this.persistPending();
    this.notifyListeners(table, pk);
    this.plugins.dispatchAfterWrite(allowed);
    return allowed.change;
  }

  /**
   * Insert a value into a list field. Returns the ChangeRecord for push.
   * If afterId is provided, inserts after that node; otherwise appends to the end.
   */
  insertIntoList(
    table: string,
    pk: string,
    field: string,
    value: unknown,
    afterId?: HLC
  ): ChangeRecord {
    const hlc = this.clock.now();
    const parentId = afterId ?? { ts: 0, c: 0, node: "" };
    const change: ChangeRecord = {
      table,
      pk,
      field,
      crdt_type: "list",
      hlc,
      node_id: this.nodeID,
      list_op: {
        op: "insert",
        node_id: hlc,
        parent_id: parentId,
        value,
      },
    };

    const previousState = this.captureFieldState(table, pk, field);
    this.applyChangeInternal(change);
    this.undoManager.record(change, previousState);
    this.pending.push(change);
    this.persistDocument(table, pk);
    this.persistPending();
    this.notifyListeners(table, pk);
    return change;
  }

  /**
   * Delete a node from a list field by its node ID. Returns the ChangeRecord for push.
   */
  deleteFromList(
    table: string,
    pk: string,
    field: string,
    nodeId: HLC
  ): ChangeRecord {
    const hlc = this.clock.now();
    const change: ChangeRecord = {
      table,
      pk,
      field,
      crdt_type: "list",
      hlc,
      node_id: this.nodeID,
      list_op: {
        op: "delete",
        node_id: nodeId,
      },
    };

    const previousState = this.captureFieldState(table, pk, field);
    this.applyChangeInternal(change);
    this.undoManager.record(change, previousState);
    this.pending.push(change);
    this.persistDocument(table, pk);
    this.persistPending();
    this.notifyListeners(table, pk);
    return change;
  }

  /**
   * Set a field within a nested document CRDT. Returns the ChangeRecord for push.
   * The path identifies the sub-field within the nested document.
   */
  setDocumentField(
    table: string,
    pk: string,
    field: string,
    path: string,
    value: unknown
  ): ChangeRecord {
    const hlc = this.clock.now();
    // For nested document fields, we store the path as the field and
    // the value in the change. The merge function handles applying it
    // to the nested document state.
    const change: ChangeRecord = {
      table,
      pk,
      field,
      crdt_type: "document",
      hlc,
      node_id: this.nodeID,
      value: { path, value },
    };

    const previousState = this.captureFieldState(table, pk, field);
    this.applyDocumentFieldChange(table, pk, field, path, value, hlc);
    this.undoManager.record(change, previousState);
    this.pending.push(change);
    this.persistDocument(table, pk);
    this.persistPending();
    this.notifyListeners(table, pk);
    return change;
  }

  /**
   * Delete a field within a nested document CRDT. Returns the ChangeRecord for push.
   */
  deleteDocumentField(
    table: string,
    pk: string,
    field: string,
    path: string
  ): ChangeRecord {
    const hlc = this.clock.now();
    const change: ChangeRecord = {
      table,
      pk,
      field,
      crdt_type: "document",
      hlc,
      node_id: this.nodeID,
      tombstone: true,
      value: { path },
    };

    const previousState = this.captureFieldState(table, pk, field);
    this.applyDocumentFieldDelete(table, pk, field, path, hlc);
    this.undoManager.record(change, previousState);
    this.pending.push(change);
    this.persistDocument(table, pk);
    this.persistPending();
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

    // For tombstone, capture a synthetic "previous state" that records whether
    // the document was already tombstoned. We store the tombstone flag in the
    // value field so undo can restore it.
    const doc = this.ensureDocument(table, pk);
    const previousState: FieldState | null = doc.tombstone
      ? { type: "lww", hlc: doc.tombstone_hlc!, node_id: this.nodeID, value: true }
      : null;

    doc.tombstone = true;
    doc.tombstone_hlc = hlc;

    this.undoManager.record(change, previousState);
    this.pending.push(change);
    this.persistDocument(table, pk);
    this.persistPending();
    this.notifyListeners(table, pk);
    return change;
  }

  // --- Undo / Redo ---

  /** Whether an undo operation is available. */
  get canUndo(): boolean {
    return this.undoManager.canUndo;
  }

  /** Whether a redo operation is available. */
  get canRedo(): boolean {
    return this.undoManager.canRedo;
  }

  /**
   * Undo the last local mutation by restoring the previous field state.
   * Returns true if an undo was performed, false if the stack was empty.
   */
  undo(): boolean {
    const entry = this.undoManager.undo();
    if (!entry) return false;

    const { change, previousState } = entry;

    if (change.tombstone) {
      // Undo a delete: restore the document (un-tombstone).
      const doc = this.ensureDocument(change.table, change.pk);
      // previousState is null means the document was NOT tombstoned before.
      doc.tombstone = previousState !== null;
      doc.tombstone_hlc = previousState?.hlc;
    } else {
      const doc = this.ensureDocument(change.table, change.pk);
      if (previousState === null) {
        // Field didn't exist before; remove it.
        delete doc.fields[change.field];
      } else {
        doc.fields[change.field] = previousState;
      }
    }

    this.persistDocument(change.table, change.pk);
    this.notifyListeners(change.table, change.pk);
    return true;
  }

  /**
   * Redo the last undone mutation by re-applying the change.
   * Returns true if a redo was performed, false if the stack was empty.
   */
  redo(): boolean {
    const entry = this.undoManager.redo();
    if (!entry) return false;

    const { change } = entry;

    if (change.tombstone) {
      const doc = this.ensureDocument(change.table, change.pk);
      doc.tombstone = true;
      doc.tombstone_hlc = change.hlc;
    } else {
      this.applyChangeInternal(change);
    }

    this.persistDocument(change.table, change.pk);
    this.notifyListeners(change.table, change.pk);
    return true;
  }

  // --- Sync ---

  /**
   * Apply remote changes from a pull or stream event.
   * Uses merge functions to resolve conflicts.
   */
  applyChanges(changes: ChangeRecord[]): void {
    const affected = new Set<string>();

    for (const change of changes) {
      // Run beforeMerge plugins.
      const doc = this.ensureDocument(change.table, change.pk);
      const localFS = doc.fields[change.field] ?? null;
      const mergeEvent: MergeEvent = {
        table: change.table,
        pk: change.pk,
        field: change.field,
        local: localFS,
        remote: change,
        conflictDetected: localFS !== null,
      };

      const allowed = this.plugins.dispatchBeforeMerge(mergeEvent);
      if (!allowed) continue; // Plugin rejected this change.

      const changeCopy = allowed === change ? change : { ...change, ...allowed };
      this.applyChangeInternal(changeCopy);
      affected.add(`${change.table}:${change.pk}`);

      // Run afterMerge plugins.
      const resultFS = doc.fields[change.field] ?? null;
      mergeEvent.result = resultFS ?? undefined;
      mergeEvent.winnerNodeId = resultFS?.node_id;
      this.plugins.dispatchAfterMerge(mergeEvent);
    }

    // Batch-persist and notify all affected documents.
    for (const key of affected) {
      const [table, pk] = key.split(":", 2);
      this.persistDocument(table, pk);
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
    this.persistPending();
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

  // --- Batch Write API ---

  /**
   * Create a BatchWriter for atomic multi-field updates on a single document.
   * Changes are collected without being applied until `commit()` is called.
   *
   * @example
   * ```ts
   * store.batch("users", "user-1")
   *   .setField("name", "Alice")
   *   .setField("email", "alice@example.com")
   *   .incrementCounter("login_count")
   *   .commit();
   * ```
   */
  batch(table: string, pk: string): BatchWriter {
    return new BatchWriter(this, table, pk);
  }

  /**
   * Apply a batch of changes atomically.
   * All changes are applied in order, then persisted and notified once.
   * @internal Used by BatchWriter.
   */
  commitBatch(table: string, pk: string, changes: ChangeRecord[]): ChangeRecord[] {
    for (const change of changes) {
      this.applyChangeInternal(change);
      this.pending.push(change);
    }
    this.persistDocument(table, pk);
    this.persistPending();
    this.notifyListeners(table, pk);
    return changes;
  }

  // --- State Export/Import ---

  /**
   * Export the entire store state as a serializable snapshot.
   * Useful for backups, debugging, or transferring state between stores.
   */
  exportState(): StateSnapshot {
    const tables: Record<string, Record<string, DocumentState>> = {};
    for (const [tableName, tableMap] of this.state) {
      const docs: Record<string, DocumentState> = {};
      for (const [pk, doc] of tableMap) {
        docs[pk] = JSON.parse(JSON.stringify(doc));
      }
      tables[tableName] = docs;
    }
    return {
      version: 1,
      nodeId: this.nodeID,
      timestamp: Date.now(),
      tables,
      pending: JSON.parse(JSON.stringify(this.pending)),
    };
  }

  /**
   * Import a state snapshot, replacing current state. Fires all listeners.
   */
  importState(snapshot: StateSnapshot): void {
    // Clear current state.
    this.state.clear();

    // Load tables from snapshot.
    for (const [tableName, docs] of Object.entries(snapshot.tables)) {
      const tableMap = new Map<string, DocumentState>();
      for (const [pk, doc] of Object.entries(docs)) {
        tableMap.set(pk, JSON.parse(JSON.stringify(doc)));
      }
      this.state.set(tableName, tableMap);
    }

    // Replace pending changes.
    this.pending = JSON.parse(JSON.stringify(snapshot.pending));
    this.persistPending();

    // Persist all imported documents.
    for (const [tableName, tableMap] of this.state) {
      for (const [pk] of tableMap) {
        this.persistDocument(tableName, pk);
      }
    }

    // Notify all listeners.
    for (const listener of this.globalListeners) listener();
    for (const [, listeners] of this.docListeners) {
      for (const listener of listeners) listener();
    }
    for (const [, listeners] of this.tableListeners) {
      for (const listener of listeners) listener();
    }
  }

  /**
   * Export a single table's state as a record of document states.
   */
  exportTable(table: string): Record<string, DocumentState> {
    const tableMap = this.state.get(table);
    if (!tableMap) return {};
    const result: Record<string, DocumentState> = {};
    for (const [pk, doc] of tableMap) {
      result[pk] = JSON.parse(JSON.stringify(doc));
    }
    return result;
  }

  // --- Internals ---

  /**
   * Hydrate state from the storage adapter.
   * Loads persisted state and pending changes, merging them into
   * the in-memory maps (preferring any mutations that happened
   * between construction and hydration completion).
   */
  private async hydrate(): Promise<void> {
    const [loadedState, loadedPending] = await Promise.all([
      this.storage.loadState(),
      this.storage.loadPendingChanges(),
    ]);

    // Merge loaded state into in-memory maps.
    for (const [table, docs] of loadedState) {
      let tableMap = this.state.get(table);
      if (!tableMap) {
        tableMap = new Map();
        this.state.set(table, tableMap);
      }
      for (const [pk, doc] of docs) {
        // Only set if no in-memory mutation happened during hydration.
        if (!tableMap.has(pk)) {
          const hydrated = this.plugins.dispatchAfterHydrate(table, pk, doc);
          tableMap.set(pk, hydrated);
        }
      }
    }

    // Prepend loaded pending changes (before any new ones added during hydration).
    if (loadedPending.length > 0) {
      this.pending = [...loadedPending, ...this.pending];
    }

    // Notify all listeners that state may have changed.
    for (const listener of this.globalListeners) listener();
  }

  /** Fire-and-forget: persist a document to storage. */
  private persistDocument(table: string, pk: string): void {
    const doc = this.state.get(table)?.get(pk);
    if (doc) {
      const transformed = this.plugins.dispatchBeforePersist(table, pk, doc);
      this.storage.saveDocument(table, pk, transformed).catch(() => {});
    }
  }

  /** Fire-and-forget: persist pending changes to storage. */
  private persistPending(): void {
    this.storage.savePendingChanges([...this.pending]).catch(() => {});
  }

  /**
   * Capture a deep copy of the current field state for undo purposes.
   * Returns null if the field doesn't exist yet.
   */
  private captureFieldState(
    table: string,
    pk: string,
    field: string
  ): FieldState | null {
    const doc = this.state.get(table)?.get(pk);
    if (!doc) return null;
    const fs = doc.fields[field];
    if (!fs) return null;
    // Deep clone to avoid shared references.
    return JSON.parse(JSON.stringify(fs)) as FieldState;
  }

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
        case "list":
          result[field] = state.list_state
            ? listElements(state.list_state)
            : [];
          break;
        case "document":
          result[field] = state.doc_state
            ? documentResolve(state.doc_state)
            : {};
          break;
        default:
          result[field] = state.value;
      }
    }

    return result;
  }

  /**
   * Apply a nested document field set directly to the document state.
   */
  private applyDocumentFieldChange(
    table: string,
    pk: string,
    field: string,
    path: string,
    value: unknown,
    hlc: HLC
  ): void {
    const doc = this.ensureDocument(table, pk);
    let fieldState = doc.fields[field];
    if (!fieldState || fieldState.type !== "document") {
      fieldState = {
        type: "document",
        hlc,
        node_id: this.nodeID,
        doc_state: { fields: {} },
      };
      doc.fields[field] = fieldState;
    }
    if (!fieldState.doc_state) {
      fieldState.doc_state = { fields: {} };
    }
    fieldState.hlc = hlc;
    fieldState.doc_state.fields[path] = {
      type: "lww",
      hlc,
      node_id: this.nodeID,
      value,
    };
  }

  /**
   * Apply a nested document field delete directly to the document state.
   */
  private applyDocumentFieldDelete(
    table: string,
    pk: string,
    field: string,
    path: string,
    hlc: HLC
  ): void {
    const doc = this.ensureDocument(table, pk);
    const fieldState = doc.fields[field];
    if (fieldState?.type === "document" && fieldState.doc_state) {
      delete fieldState.doc_state.fields[path];
      fieldState.hlc = hlc;
    }
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

/**
 * Batch writer for atomic multi-field updates on a single document.
 *
 * Collects changes without applying them. On `commit()`, all changes
 * are applied in order with a single persist and notification cycle.
 *
 * @example
 * ```ts
 * const changes = store.batch("users", "user-1")
 *   .setField("name", "Alice")
 *   .setField("email", "alice@example.com")
 *   .incrementCounter("login_count")
 *   .addToSet("tags", ["admin", "active"])
 *   .commit();
 * ```
 */
export class BatchWriter {
  private store: CRDTStore;
  private table: string;
  private pk: string;
  private changes: ChangeRecord[] = [];

  constructor(store: CRDTStore, table: string, pk: string) {
    this.store = store;
    this.table = table;
    this.pk = pk;
  }

  /** Set an LWW field. */
  setField(field: string, value: unknown): this {
    this.changes.push({
      table: this.table,
      pk: this.pk,
      field,
      crdt_type: "lww",
      hlc: { ts: 0, c: 0, node: "" }, // placeholder, assigned at commit
      node_id: "",
      value,
    });
    return this;
  }

  /** Increment a counter field. */
  incrementCounter(field: string, delta = 1): this {
    this.changes.push({
      table: this.table,
      pk: this.pk,
      field,
      crdt_type: "counter",
      hlc: { ts: 0, c: 0, node: "" },
      node_id: "",
      counter_delta: { inc: delta, dec: 0 },
    });
    return this;
  }

  /** Add elements to a set field. */
  addToSet(field: string, elements: unknown[]): this {
    this.changes.push({
      table: this.table,
      pk: this.pk,
      field,
      crdt_type: "set",
      hlc: { ts: 0, c: 0, node: "" },
      node_id: "",
      set_op: { op: "add", elements },
    });
    return this;
  }

  /**
   * Commit all queued changes atomically.
   * All changes get a fresh HLC timestamp for causal consistency.
   * Returns the list of committed changes.
   */
  commit(): ChangeRecord[] {
    if (this.changes.length === 0) return [];

    // Access the store's clock to assign real HLCs.
    // We use the store's internal clock via the commitBatch path,
    // but first assign proper HLC and node_id to each change.
    const clock: HybridClock = (this.store as unknown as { clock: HybridClock }).clock;
    const nodeID: string = (this.store as unknown as { nodeID: string }).nodeID;

    for (const change of this.changes) {
      change.hlc = clock.now();
      change.node_id = nodeID;
    }

    const committed = this.store.commitBatch(this.table, this.pk, this.changes);
    this.changes = [];
    return committed;
  }
}
