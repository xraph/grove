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
  TextState,
  TextOperation,
  TextRef,
  TextDeltaSegment,
} from "./types.js";
import { HybridClock, hlcString } from "./hlc.js";
import {
  mergeFieldState,
  counterValue,
  setElements,
  listElements,
  listNodeIds,
  documentResolve,
  tagKey,
} from "./merge.js";
import {
  newTextState,
  cloneTextState,
  textInsert,
  textDeleteOp,
  textFormat,
  textValue,
  textDelta,
  textRefAt,
  textIndexOf,
} from "./text.js";
import { withEntry, withoutKeys } from "./immutable.js";
import { MemoryStorage } from "./storage.js";
import { UndoManager } from "./undo.js";
import { PluginManager } from "./plugin.js";
import type { StorePlugin, WriteEvent, MergeEvent } from "./plugin.js";

type Listener = () => void;

/** Shared empties — a fresh [] each call breaks useSyncExternalStore. */
const EMPTY_HLCS: HLC[] = [];
const EMPTY_DELTA: TextDeltaSegment[] = [];

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
/**
 * Migrate persisted CRDT states whose RGA list node maps were keyed with
 * the pre-parity `${ts}:${c}:${node}` format to the Go HLC.String()
 * format. Keys are authoritative-rebuilt from each node's id, so the
 * migration is idempotent and format-agnostic.
 */
function normalizeHLCKeys(doc: DocumentState): void {
  for (const fs of Object.values(doc.fields)) {
    const nodes = fs.list_state?.nodes;
    if (!nodes) continue;
    for (const [key, node] of Object.entries(nodes)) {
      const canonical = hlcString(node.id);
      if (key !== canonical) {
        delete nodes[key];
        nodes[canonical] = node;
      }
    }
  }
}

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

  /** Per-document listeners: table → pk → listeners. Nested, never a
   *  delimited string key — a pk may legally contain ":". */
  private docListeners = new Map<string, Map<string, Set<Listener>>>();

  /** Per-table listeners: table → listeners */
  private tableListeners = new Map<string, Set<Listener>>();

  /** Bumped on every document replacement. Keys the collection cache. */
  private tableVersions = new Map<string, number>();

  /**
   * Resolved-document cache keyed on DocumentState IDENTITY. Safe because
   * every write replaces the document object (see setDocument), so a hit
   * can never be stale. WeakMap so evicted documents are collectable.
   */
  private docCache = new WeakMap<DocumentState, unknown>();

  /** Ordered live node IDs per list field, keyed on document identity. */
  private listIdCache = new WeakMap<DocumentState, Map<string, HLC[]>>();

  /** Text delta segments per text field, keyed on document identity. */
  private textDeltaCache = new WeakMap<DocumentState, Map<string, TextDeltaSegment[]>>();

  /** Resolved collections, keyed on the table's write version. */
  private collectionCache = new Map<string, { version: number; items: unknown[] }>();

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
    this.invalidateSnapshots();
  }

  /**
   * Remove a plugin by name.
   */
  removePlugin(name: string): void {
    this.plugins.remove(name);
    this.invalidateSnapshots();
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
    const doc = this.getDoc(table, pk);
    if (!doc || doc.tombstone) return null;
    if (this.docCache.has(doc)) return this.docCache.get(doc) as T | null;

    const resolved = this.resolveDocument(doc) as T;
    const transformed = this.plugins.dispatchTransformDocument(table, pk, resolved);
    this.docCache.set(doc, transformed);
    return transformed;
  }

  /**
   * Get all documents in a table.
   * Excludes tombstoned documents.
   */
  getCollection<T = Record<string, unknown>>(table: string): T[] {
    const version = this.tableVersions.get(table) ?? 0;
    const hit = this.collectionCache.get(table);
    if (hit && hit.version === version) return hit.items as T[];

    const tableMap = this.state.get(table);
    const result: T[] = [];
    if (tableMap) {
      for (const doc of tableMap.values()) {
        if (doc.tombstone) continue;
        const transformed = this.getDocument<T>(table, doc.pk);
        if (transformed !== null) result.push(transformed);
      }
    }
    const items = this.plugins.dispatchTransformCollection(table, result);
    this.collectionCache.set(table, { version, items });
    return items;
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
    // The wire delta is this node's CUMULATIVE totals (merged max-per-node
    // on every replica) — idempotent under redelivery, matching Go.
    const totals = this.counterTotals(table, pk, field);
    const change: ChangeRecord = {
      table,
      pk,
      field,
      crdt_type: "counter",
      hlc,
      node_id: this.nodeID,
      counter_delta: { inc: totals.inc + delta, dec: totals.dec },
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
    const totals = this.counterTotals(table, pk, field);
    const change: ChangeRecord = {
      table,
      pk,
      field,
      crdt_type: "counter",
      hlc,
      node_id: this.nodeID,
      counter_delta: { inc: totals.inc, dec: totals.dec + delta },
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
    // Name the observed tags so the remove is exact everywhere (true
    // observed-remove semantics; receivers don't guess by HLC).
    const setState = this.getDoc(table, pk)?.fields[field]?.set_state;
    const tags = elements.flatMap(
      (elem) => setState?.entries[JSON.stringify(elem)] ?? []
    );
    const change: ChangeRecord = {
      table,
      pk,
      field,
      crdt_type: "set",
      hlc,
      node_id: this.nodeID,
      set_op: { op: "remove", elements, ...(tags.length > 0 ? { tags } : {}) },
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
   * Ordered RGA node IDs for a list field's live elements (parallel to the
   * resolved array) — needed for insert-after and delete addressing.
   */
  getListNodeIds(table: string, pk: string, field: string): HLC[] {
    const doc = this.getDoc(table, pk);
    if (!doc) return EMPTY_HLCS;
    let byField = this.listIdCache.get(doc);
    if (!byField) { byField = new Map(); this.listIdCache.set(doc, byField); }
    const hit = byField.get(field);
    if (hit) return hit;

    const fs = doc.fields[field];
    const ids = fs?.list_state ? listNodeIds(fs.list_state) : EMPTY_HLCS;
    byField.set(field, ids);
    return ids;
  }

  // --- Text (collaborative rich text) ---

  /** Current text state for a field, without creating anything. */
  private textStateOf(table: string, pk: string, field: string): TextState {
    const fs = this.getDoc(table, pk)?.fields[field];
    return fs?.type === "text" && fs.text_state ? fs.text_state : newTextState();
  }

  private emitTextChange(
    table: string,
    pk: string,
    field: string,
    hlc: HLC,
    op: TextOperation,
    previousState: FieldState | null,
    nextState: TextState
  ): ChangeRecord {
    const change: ChangeRecord = {
      table,
      pk,
      field,
      crdt_type: "text",
      hlc,
      node_id: this.nodeID,
      text_op: op,
    };
    const doc = this.docOrEmpty(table, pk);
    this.setDocument(table, pk, this.withField(doc, field, {
      type: "text",
      hlc,
      node_id: this.nodeID,
      text_state: nextState,
    }));
    this.undoManager.record(change, previousState);
    this.pending.push(change);
    this.persistDocument(table, pk);
    this.persistPending();
    this.notifyListeners(table, pk);
    return change;
  }

  /**
   * Insert text at a visible character index. Returns the ChangeRecord
   * for push. Sequential typing coalesces into one origin span.
   */
  insertText(
    table: string,
    pk: string,
    field: string,
    index: number,
    content: string
  ): ChangeRecord {
    const hlc = this.clock.now();
    const current = this.textStateOf(table, pk, field);
    const previousState = this.captureFieldState(table, pk, field);
    const ref = index > 0 ? textRefAt(current, index - 1) : null;
    if (index > 0 && !ref) {
      throw new Error(`crdt: no text character at index ${index - 1}`);
    }
    // The builders apply the op as they build it, so build against a clone
    // and keep the clone as the new state.
    const next = cloneTextState(current);
    const op = textInsert(next, ref, content, this.nodeID, hlc);
    return this.emitTextChange(table, pk, field, hlc, op, previousState, next);
  }

  /** Delete `length` visible characters starting at index. */
  deleteText(
    table: string,
    pk: string,
    field: string,
    index: number,
    length: number
  ): ChangeRecord {
    const hlc = this.clock.now();
    const current = this.textStateOf(table, pk, field);
    const previousState = this.captureFieldState(table, pk, field);
    const ref = textRefAt(current, index);
    if (!ref) throw new Error(`crdt: no text character at index ${index}`);
    const next = cloneTextState(current);
    const op = textDeleteOp(next, ref, length);
    return this.emitTextChange(table, pk, field, hlc, op, previousState, next);
  }

  /**
   * Apply formatting attributes to `length` visible characters starting
   * at index. A null attribute value clears the attribute.
   */
  formatText(
    table: string,
    pk: string,
    field: string,
    index: number,
    length: number,
    attrs: Record<string, unknown>
  ): ChangeRecord {
    const hlc = this.clock.now();
    const current = this.textStateOf(table, pk, field);
    const previousState = this.captureFieldState(table, pk, field);
    const ref = textRefAt(current, index);
    if (!ref) throw new Error(`crdt: no text character at index ${index}`);
    const next = cloneTextState(current);
    const op = textFormat(next, ref, length, attrs, this.nodeID, hlc);
    return this.emitTextChange(table, pk, field, hlc, op, previousState, next);
  }

  /** Visible text of a text field ("" when absent). */
  getText(table: string, pk: string, field: string): string {
    const fs = this.getDoc(table, pk)?.fields[field];
    if (!fs?.text_state) return "";
    return textValue(fs.text_state);
  }

  /** Quill-style attribute-run segments of a text field. */
  getTextDelta(table: string, pk: string, field: string): TextDeltaSegment[] {
    const doc = this.getDoc(table, pk);
    if (!doc) return EMPTY_DELTA;
    let byField = this.textDeltaCache.get(doc);
    if (!byField) { byField = new Map(); this.textDeltaCache.set(doc, byField); }
    const hit = byField.get(field);
    if (hit) return hit;

    const fs = doc.fields[field];
    const delta = fs?.text_state ? textDelta(fs.text_state) : EMPTY_DELTA;
    byField.set(field, delta);
    return delta;
  }

  /**
   * Stable address of the character at a visible index — a relative
   * position that survives concurrent edits (cursor anchoring).
   */
  getTextRefAt(table: string, pk: string, field: string, index: number): TextRef | null {
    const fs = this.getDoc(table, pk)?.fields[field];
    if (!fs?.text_state) return null;
    return textRefAt(fs.text_state, index);
  }

  /** Current visible index of a stable text address. */
  getTextIndexOf(table: string, pk: string, field: string, ref: TextRef): number | null {
    const fs = this.getDoc(table, pk)?.fields[field];
    if (!fs?.text_state) return null;
    return textIndexOf(fs.text_state, ref);
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
    const doc = this.docOrEmpty(table, pk);
    const previousState: FieldState | null = doc.tombstone
      ? { type: "lww", hlc: doc.tombstone_hlc!, node_id: this.nodeID, value: true }
      : null;

    this.setDocument(table, pk, {
      ...doc,
      tombstone: true,
      tombstone_hlc: hlc,
    });

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

    const doc = this.docOrEmpty(change.table, change.pk);
    if (change.tombstone) {
      // previousState === null means the document was NOT tombstoned before.
      this.setDocument(change.table, change.pk, {
        ...doc,
        tombstone: previousState !== null,
        tombstone_hlc: previousState?.hlc,
      });
    } else if (previousState === null) {
      // Field didn't exist before; remove it.
      this.setDocument(change.table, change.pk, {
        ...doc,
        fields: withoutKeys(doc.fields, (k) => k === change.field),
      });
    } else {
      this.setDocument(
        change.table,
        change.pk,
        this.withField(doc, change.field, previousState)
      );
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
      const doc = this.docOrEmpty(change.table, change.pk);
      this.setDocument(change.table, change.pk, {
        ...doc,
        tombstone: true,
        tombstone_hlc: change.hlc,
      });
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
    const affected = new Map<string, Set<string>>();

    for (const change of changes) {
      // Run beforeMerge plugins.
      const localFS = this.getDoc(change.table, change.pk)?.fields[change.field] ?? null;
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

      let pks = affected.get(change.table);
      if (!pks) {
        pks = new Set();
        affected.set(change.table, pks);
      }
      pks.add(change.pk);

      // Run afterMerge plugins. The merge installed a NEW document object,
      // so re-read from the store rather than from the pre-merge document.
      const resultFS =
        this.getDoc(change.table, change.pk)?.fields[change.field] ?? null;
      mergeEvent.result = resultFS ?? undefined;
      mergeEvent.winnerNodeId = resultFS?.node_id;
      this.plugins.dispatchAfterMerge(mergeEvent);
    }

    // Batch-persist and notify all affected documents.
    for (const [table, pkSet] of affected) {
      for (const pk of pkSet) {
        this.persistDocument(table, pk);
        this.notifyListeners(table, pk);
      }
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
    let byPk = this.docListeners.get(table);
    if (!byPk) {
      byPk = new Map();
      this.docListeners.set(table, byPk);
    }
    let listeners = byPk.get(pk);
    if (!listeners) {
      listeners = new Set();
      byPk.set(pk, listeners);
    }
    listeners.add(listener);

    return () => {
      listeners!.delete(listener);
      if (listeners!.size === 0) {
        byPk!.delete(pk);
        if (byPk!.size === 0) this.docListeners.delete(table);
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
    // Every table about to be dropped must invalidate: a table the snapshot
    // omits is wiped here and would otherwise keep a version that still
    // matches a cached resolution.
    for (const t of this.state.keys()) {
      this.tableVersions.set(t, (this.tableVersions.get(t) ?? 0) + 1);
    }

    // Clear current state.
    this.state.clear();

    // Load tables from snapshot.
    for (const [tableName, docs] of Object.entries(snapshot.tables)) {
      for (const [pk, doc] of Object.entries(docs)) {
        this.setDocument(tableName, pk, JSON.parse(JSON.stringify(doc)));
      }
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
    for (const byPk of this.docListeners.values()) {
      for (const listeners of byPk.values()) {
        for (const listener of listeners) listener();
      }
    }
    for (const listeners of this.tableListeners.values()) {
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
      for (const [pk, doc] of docs) {
        // Only set if no in-memory mutation happened during hydration.
        if (!this.getDoc(table, pk)) {
          normalizeHLCKeys(doc);
          const hydrated = this.plugins.dispatchAfterHydrate(table, pk, doc);
          this.setDocument(table, pk, hydrated);
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
    const doc = this.getDoc(table, pk);
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
   * Current field state for undo. State is immutable, so a reference is a
   * safe snapshot — no clone needed. Returns null if the field is absent.
   */
  private captureFieldState(
    table: string,
    pk: string,
    field: string
  ): FieldState | null {
    return this.getDoc(table, pk)?.fields[field] ?? null;
  }

  /** This node's current cumulative counter totals for a field. */
  private counterTotals(
    table: string,
    pk: string,
    field: string
  ): { inc: number; dec: number } {
    const cs = this.getDoc(table, pk)?.fields[field]?.counter_state;
    return {
      inc: cs?.inc[this.nodeID] ?? 0,
      dec: cs?.dec[this.nodeID] ?? 0,
    };
  }

  private applyChangeInternal(change: ChangeRecord): void {
    // A tombstoned document-type change carrying a value is a PATH delete
    // inside the nested document, not a record delete.
    const isDocPathDelete =
      change.crdt_type === "document" && change.value !== undefined;
    const doc = this.docOrEmpty(change.table, change.pk);

    if (change.tombstone && !isDocPathDelete) {
      this.setDocument(change.table, change.pk, {
        ...doc,
        tombstone: true,
        tombstone_hlc: change.hlc,
      });
      return;
    }

    const existing = doc.fields[change.field] ?? null;
    this.setDocument(
      change.table,
      change.pk,
      this.withField(doc, change.field, mergeFieldState(existing, change))
    );
  }

  private getDoc(table: string, pk: string): DocumentState | undefined {
    return this.state.get(table)?.get(pk);
  }

  /** Current document, or a fresh empty one. Never inserts into state. */
  private docOrEmpty(table: string, pk: string): DocumentState {
    return this.getDoc(table, pk) ?? { table, pk, fields: {}, tombstone: false };
  }

  /**
   * Install a new document object. The ONLY write path into `state`.
   * Replacing rather than mutating is what makes identity-keyed snapshot
   * caching correct (see getDocument).
   */
  private setDocument(table: string, pk: string, doc: DocumentState): void {
    let tableMap = this.state.get(table);
    if (!tableMap) {
      tableMap = new Map();
      this.state.set(table, tableMap);
    }
    tableMap.set(pk, doc);
    this.tableVersions.set(table, (this.tableVersions.get(table) ?? 0) + 1);
  }

  /** Drop every cached snapshot. Called when plugin output may change. */
  private invalidateSnapshots(): void {
    this.docCache = new WeakMap();
    this.listIdCache = new WeakMap();
    this.textDeltaCache = new WeakMap();
    this.collectionCache.clear();
  }

  /** Document with one field replaced; every other field is shared. */
  private withField(
    doc: DocumentState,
    field: string,
    fs: FieldState
  ): DocumentState {
    return { ...doc, fields: withEntry(doc.fields, field, fs) };
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
    const doc = this.docOrEmpty(table, pk);
    const existing = doc.fields[field];
    const docState =
      existing?.type === "document" && existing.doc_state
        ? existing.doc_state
        : { fields: {} };

    this.setDocument(table, pk, this.withField(doc, field, {
      type: "document",
      hlc,
      node_id: this.nodeID,
      doc_state: {
        fields: withEntry(docState.fields, path, {
          type: "lww",
          hlc,
          node_id: this.nodeID,
          value,
        }),
      },
    }));
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
    const doc = this.docOrEmpty(table, pk);
    const existing = doc.fields[field];
    if (existing?.type !== "document" || !existing.doc_state) return;

    this.setDocument(table, pk, this.withField(doc, field, {
      ...existing,
      hlc,
      doc_state: {
        fields: withoutKeys(existing.doc_state.fields, (k) => k === path),
      },
    }));
  }

  private notifyListeners(table: string, pk: string): void {
    // Document-level listeners.
    const docListeners = this.docListeners.get(table)?.get(pk);
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
