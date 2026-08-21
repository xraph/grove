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
import { HybridClock, hlcString, hlcIsZero } from "./hlc.js";
import { compactDocument } from "./compact.js";
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
import { CRDTError, CRDTErrorCode } from "./errors.js";

type Listener = () => void;

/** Shared empties — a fresh [] each call breaks useSyncExternalStore. Frozen
 *  because getListNodeIds/getTextDelta hand these out through non-readonly
 *  public return types; without the freeze a consumer mutating an empty
 *  result would silently corrupt every other empty snapshot for the
 *  module's lifetime. Freezing turns that silent corruption into an
 *  immediate TypeError. */
const EMPTY_HLCS: HLC[] = Object.freeze([]) as unknown as HLC[];
const EMPTY_DELTA: TextDeltaSegment[] = Object.freeze([]) as unknown as TextDeltaSegment[];

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

  /** Max queued unpushed changes (0 disables the bound). */
  private maxPendingChanges: number;

  /** Throw instead of dropping when the bound is hit. */
  private throwOnOverflow: boolean;

  /** Notified with the dropped records whenever the pending bound evicts. */
  private overflowHandlers = new Set<(dropped: ChangeRecord[]) => void>();

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

  /** Depth of nested transact() calls. Zero means notify immediately. */
  private txDepth = 0;

  /** Documents touched while suspended: table → pks. */
  private txTouched = new Map<string, Set<string>>();

  /** True when pending changes were written while suspended. */
  private txPendingDirty = false;

  /** Milliseconds a burst of writes is coalesced over before hitting the
   *  storage adapter. `0` disables debouncing and writes synchronously. */
  private persistDebounceMs: number;

  /** Shared timer for the debounced document + pending-change flush. Null
   *  when no flush is currently scheduled. Tests that construct a store with
   *  the default (non-zero) debounce and never call flushPersistence() may
   *  leave this armed past the end of the test — that's safe, not a leak:
   *  each such test builds its own fresh storage/mock, so a late fire only
   *  ever touches an object private to that already-finished test, and the
   *  write itself is .catch()-wrapped. Do NOT "fix" this with an afterEach
   *  that clears it globally — that would change drain semantics for tests
   *  that deliberately assert on the debounce's timing. */
  private pendingPersistTimer: ReturnType<typeof setTimeout> | null = null;

  /** Documents touched since the last debounced flush: table → pks. Drained
   *  (and read at fire time, not schedule time) by the debounce timer. */
  private docPersistQueue = new Map<string, Set<string>>();

  /** True when `this.pending` was written since the last debounced drain.
   *  Gates the shared drain's `savePendingChanges` call so document-only
   *  operations (undo, redo, applyChanges — none of which touch `pending`)
   *  don't re-write an unchanged pending array on every debounce window. */
  private pendingDebounceDirty = false;

  /**
   * Resolves when persisted state has been hydrated.
   * The store is usable immediately (starts empty), but consumers
   * should await `ready` before relying on persisted data.
   */
  readonly ready: Promise<void>;

  constructor(
    nodeID: string,
    clock: HybridClock,
    storage?: StorageAdapter,
    options?: {
      persistDebounceMs?: number;
      /** Max queued unpushed changes (default: 10000). 0 disables. */
      maxPendingChanges?: number;
      /** Throw instead of dropping when the bound is hit (default: false). */
      throwOnOverflow?: boolean;
    }
  ) {
    this.nodeID = nodeID;
    this.clock = clock;
    this.storage = storage ?? new MemoryStorage();
    this.undoManager = new UndoManager();
    this.plugins = new PluginManager();
    this.persistDebounceMs = options?.persistDebounceMs ?? 50;
    this.maxPendingChanges = options?.maxPendingChanges ?? 10_000;
    this.throwOnOverflow = options?.throwOnOverflow ?? false;
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
    this.assertPendingCapacity();
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
    this.enqueuePending(allowed.change);
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
    this.assertPendingCapacity();
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
    this.enqueuePending(allowed.change);
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
    this.assertPendingCapacity();
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
    this.enqueuePending(allowed.change);
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
    this.assertPendingCapacity();
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
    this.enqueuePending(allowed.change);
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
    this.assertPendingCapacity();
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
    this.enqueuePending(allowed.change);
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
    this.assertPendingCapacity();
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
    this.enqueuePending(change);
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
    this.assertPendingCapacity();
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
    this.enqueuePending(change);
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
    this.enqueuePending(change);
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
    this.assertPendingCapacity();
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
    this.assertPendingCapacity();
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
    this.assertPendingCapacity();
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
    this.assertPendingCapacity();
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
    this.enqueuePending(change);
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
    this.assertPendingCapacity();
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
    this.enqueuePending(change);
    this.persistDocument(table, pk);
    this.persistPending();
    this.notifyListeners(table, pk);
    return change;
  }

  /**
   * Delete a document (tombstone). Returns the ChangeRecord for push.
   */
  deleteDocument(table: string, pk: string): ChangeRecord {
    this.assertPendingCapacity();
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
    this.enqueuePending(change);
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

  /**
   * Clear pending changes after a successful push.
   *
   * Pass the exact records that were pushed to clear only those. Clearing
   * everything would also discard writes made WHILE the push was in
   * flight, silently losing them.
   */
  clearPendingChanges(pushed?: readonly ChangeRecord[]): void {
    if (!pushed) {
      this.pending = [];
    } else {
      const sent = new Set(pushed);
      this.pending = this.pending.filter((c) => !sent.has(c));
    }
    this.persistPending();
  }

  /** Get the number of pending changes. */
  get pendingCount(): number {
    return this.pending.length;
  }

  /**
   * Notified when the pending queue overflows and changes are dropped.
   * Returns an unsubscribe function.
   *
   * Overflow means unsynced local work was discarded — surface it to the
   * user rather than swallowing it.
   */
  onPendingOverflow(handler: (dropped: ChangeRecord[]) => void): () => void {
    this.overflowHandlers.add(handler);
    return () => { this.overflowHandlers.delete(handler); };
  }

  /**
   * Throws OfflineQueueFull when throwOnOverflow is set and the bound is
   * already at capacity. Every public mutator calls this as its very
   * FIRST statement — before it even reads the clock — so a rejected
   * write has no observable effect anywhere: no HLC consumed, no document
   * mutation, no undo entry, no pending entry. Checking only inside
   * enqueuePending() would be too late: by the time a mutator reaches its
   * push call, applyChangeInternal() and undoManager.record() have
   * already run against live store state.
   */
  private assertPendingCapacity(): void {
    if (
      this.throwOnOverflow &&
      this.maxPendingChanges > 0 &&
      this.pending.length >= this.maxPendingChanges
    ) {
      throw new CRDTError(
        `crdt: pending queue full (${this.maxPendingChanges} changes)`,
        undefined,
        CRDTErrorCode.OfflineQueueFull,
        false
      );
    }
  }

  /**
   * Queue a change for push, enforcing the offline bound.
   *
   * Repeats the assertPendingCapacity() check as a backstop: every
   * current call site already checks capacity before doing anything
   * observable (see above), so this repeat should never fire in
   * practice. It exists so a future call site that forgets the up-front
   * check still can't grow `pending` past the bound, even though by then
   * it's too late to undo whatever that call site already mutated.
   */
  private enqueuePending(change: ChangeRecord): void {
    this.assertPendingCapacity();

    this.pending.push(change);

    if (this.maxPendingChanges <= 0) return;
    if (this.pending.length <= this.maxPendingChanges) return;

    // Drop the OLDEST: recent writes reflect what the user most recently
    // intended, and older ones are likelier to have been superseded.
    const dropped = this.pending.splice(
      0,
      this.pending.length - this.maxPendingChanges
    );
    for (const handler of this.overflowHandlers) handler(dropped);
  }

  /** @internal Sync orchestration needs the hook chain. */
  get pluginManager(): PluginManager {
    return this.plugins;
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

  /**
   * Run `fn` as one transaction: persistence and listener notification are
   * suspended until the outermost call returns, so a multi-write batch
   * produces exactly one render instead of one per field.
   *
   * Re-entrant — nested calls join the outer transaction.
   */
  transact<T>(fn: () => T): T {
    this.txDepth++;
    try {
      return fn();
    } finally {
      this.txDepth--;
      if (this.txDepth === 0) this.flushTransaction();
    }
  }

  private flushTransaction(): void {
    const touched = this.txTouched;
    const pendingDirty = this.txPendingDirty;
    this.txTouched = new Map();
    this.txPendingDirty = false;

    for (const [table, pks] of touched) {
      for (const pk of pks) {
        this.persistDocumentNow(table, pk);
        this.notifyListenersNow(table, pk);
      }
    }
    if (pendingDirty) this.persistPendingNow();
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
   * Compact tombstones across every document, dropping state older than
   * the horizon. Returns the total units dropped.
   *
   * The horizon is a stability floor YOU guarantee: every replica has seen
   * everything older than it, and nothing in flight references an older
   * address. A horizon that does not hold will diverge replicas. When in
   * doubt use the server's last acknowledged HLC minus a safety margin.
   *
   * Compacted documents are persisted and their subscribers notified.
   * `setDocument` alone only rewrites the in-memory map, so without the
   * explicit persist the storage adapter would keep serving the
   * uncompacted document and a reload would restore every tombstone this
   * just dropped. The calls go through the transaction-aware wrappers, so
   * a whole-store compaction still coalesces into one flush.
   */
  compact(before: HLC): number {
    if (hlcIsZero(before)) return 0;

    let dropped = 0;
    this.transact(() => {
      for (const [table, tableMap] of this.state) {
        for (const [pk, doc] of tableMap) {
          const result = compactDocument(doc, before);
          if (result.dropped > 0) {
            dropped += result.dropped;
            this.setDocument(table, pk, result.doc);
            this.persistDocument(table, pk);
            this.notifyListeners(table, pk);
          }
        }
      }
    });
    return dropped;
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

  /**
   * Fire-and-forget: persist a document to storage, debounced.
   * `persistDebounceMs <= 0` writes immediately and synchronously (relative
   * to the caller); several tests depend on that for assertions right after
   * a mutation. Otherwise the (table, pk) is queued and a shared timer is
   * (re)armed if one isn't already pending — the queue is a Set, so a burst
   * of edits to the same document still produces a single saveDocument.
   */
  private persistDocumentNow(table: string, pk: string): void {
    if (this.persistDebounceMs <= 0) {
      const doc = this.getDoc(table, pk);
      if (doc) {
        const transformed = this.plugins.dispatchBeforePersist(table, pk, doc);
        this.storage.saveDocument(table, pk, transformed).catch(() => {});
      }
      return;
    }
    let pks = this.docPersistQueue.get(table);
    if (!pks) {
      pks = new Set();
      this.docPersistQueue.set(table, pks);
    }
    pks.add(pk);
    this.scheduleDebouncedPersist();
  }

  /**
   * Fire-and-forget: persist pending changes to storage, debounced.
   * `persistDebounceMs <= 0` writes immediately and synchronously. Otherwise
   * it marks `pending` dirty and arms the shared timer — the timer's own
   * callback re-reads `this.pending` when it fires, so it always captures
   * the latest array regardless of how many writes landed after scheduling.
   */
  private persistPendingNow(): void {
    if (this.persistDebounceMs <= 0) {
      this.storage.savePendingChanges([...this.pending]).catch(() => {});
      return;
    }
    this.pendingDebounceDirty = true;
    this.scheduleDebouncedPersist();
  }

  /**
   * Arm the shared debounce timer if one isn't already running. Both
   * persistDocumentNow and persistPendingNow funnel through here so a
   * document write and a pending-change write in the same burst share one
   * timer and drain together — whichever call arms it first, the other
   * just adds to the queue the eventual drain will read.
   */
  private scheduleDebouncedPersist(): void {
    if (this.pendingPersistTimer) return;
    this.pendingPersistTimer = setTimeout(() => {
      this.pendingPersistTimer = null;
      this.drainDebouncedPersist();
    }, this.persistDebounceMs);
  }

  /** Fire-and-forget: write everything queued by the debounce — every
   *  touched document, plus the pending-changes array but ONLY if it was
   *  actually written since the last drain (see pendingDebounceDirty) —
   *  read fresh at fire time so the final write in a burst is never lost.
   *  The dirty flag is captured, then cleared, then acted on — never
   *  cleared before it's read — so a write that lands mid-drain still
   *  marks the *next* drain dirty rather than being silently absorbed. */
  private drainDebouncedPersist(): void {
    const queued = this.docPersistQueue;
    this.docPersistQueue = new Map();
    for (const [table, pks] of queued) {
      for (const pk of pks) {
        const doc = this.getDoc(table, pk);
        if (doc) {
          const transformed = this.plugins.dispatchBeforePersist(table, pk, doc);
          this.storage.saveDocument(table, pk, transformed).catch(() => {});
        }
      }
    }
    const pendingDirty = this.pendingDebounceDirty;
    this.pendingDebounceDirty = false;
    if (pendingDirty) {
      this.storage.savePendingChanges([...this.pending]).catch(() => {});
    }
  }

  /**
   * Force any debounced writes to storage and wait for them. Cancels the
   * pending timer first (rather than letting it race this call) so the
   * write happens exactly once. Call before unload, or in tests that need
   * to assert on the adapter synchronously.
   *
   * Mirrors drainDebouncedPersist's gating: `savePendingChanges` is only
   * written if `pending` actually changed since the last drain/flush. A
   * document-only session (undo/redo/applyChanges, no local writes) that
   * flushes before unload has nothing new to say about `pending`, so
   * flushing does not manufacture a write for it — same reasoning as the
   * debounce fix this method exists to support, not a special case of it.
   */
  async flushPersistence(): Promise<void> {
    if (this.pendingPersistTimer) {
      clearTimeout(this.pendingPersistTimer);
      this.pendingPersistTimer = null;
    }
    const queued = this.docPersistQueue;
    this.docPersistQueue = new Map();
    const writes: Promise<void>[] = [];
    for (const [table, pks] of queued) {
      for (const pk of pks) {
        const doc = this.getDoc(table, pk);
        if (doc) {
          const transformed = this.plugins.dispatchBeforePersist(table, pk, doc);
          writes.push(this.storage.saveDocument(table, pk, transformed));
        }
      }
    }
    const pendingDirty = this.pendingDebounceDirty;
    this.pendingDebounceDirty = false;
    if (pendingDirty) {
      writes.push(this.storage.savePendingChanges([...this.pending]));
    }
    await Promise.allSettled(writes);
  }

  /**
   * Persist a document to storage, or defer until the outermost transact()
   * flushes if a transaction is in progress.
   */
  private persistDocument(table: string, pk: string): void {
    if (this.txDepth > 0) {
      let pks = this.txTouched.get(table);
      if (!pks) { pks = new Set(); this.txTouched.set(table, pks); }
      pks.add(pk);
      return;
    }
    this.persistDocumentNow(table, pk);
  }

  /**
   * Persist pending changes to storage, or defer until the outermost
   * transact() flushes if a transaction is in progress.
   */
  private persistPending(): void {
    if (this.txDepth > 0) { this.txPendingDirty = true; return; }
    this.persistPendingNow();
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
        case "text":
          result[field] = state.text_state ? textValue(state.text_state) : "";
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

  private notifyListenersNow(table: string, pk: string): void {
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

  /**
   * Notify listeners for a document, or defer until the outermost
   * transact() flushes if a transaction is in progress.
   */
  private notifyListeners(table: string, pk: string): void {
    if (this.txDepth > 0) {
      let pks = this.txTouched.get(table);
      if (!pks) { pks = new Set(); this.txTouched.set(table, pks); }
      pks.add(pk);
      return;
    }
    this.notifyListenersNow(table, pk);
  }
}

/**
 * Batch writer for atomic multi-field updates on a single document.
 *
 * Queues calls to the store's own mutators and replays them inside a
 * single transact(), so batched writes get identical semantics to direct
 * ones — plugin hooks, undo recording, and cumulative counter totals —
 * with one persist and one notification.
 *
 * @example
 * ```ts
 * const changes = store.batch("users", "user-1")
 *   .setField("name", "Alice")
 *   .incrementCounter("login_count")
 *   .addToSet("tags", ["admin"])
 *   .commit();
 * ```
 */
export class BatchWriter {
  private ops: Array<() => ChangeRecord | null> = [];

  constructor(
    private store: CRDTStore,
    private table: string,
    private pk: string
  ) {}

  setField(field: string, value: unknown): this {
    this.ops.push(() => this.store.setField(this.table, this.pk, field, value));
    return this;
  }

  incrementCounter(field: string, delta = 1): this {
    this.ops.push(() =>
      this.store.incrementCounter(this.table, this.pk, field, delta));
    return this;
  }

  decrementCounter(field: string, delta = 1): this {
    this.ops.push(() =>
      this.store.decrementCounter(this.table, this.pk, field, delta));
    return this;
  }

  addToSet(field: string, elements: unknown[]): this {
    this.ops.push(() => this.store.addToSet(this.table, this.pk, field, elements));
    return this;
  }

  removeFromSet(field: string, elements: unknown[]): this {
    this.ops.push(() =>
      this.store.removeFromSet(this.table, this.pk, field, elements));
    return this;
  }

  insertIntoList(field: string, value: unknown, afterId?: HLC): this {
    this.ops.push(() =>
      this.store.insertIntoList(this.table, this.pk, field, value, afterId));
    return this;
  }

  setDocumentField(field: string, path: string, value: unknown): this {
    this.ops.push(() =>
      this.store.setDocumentField(this.table, this.pk, field, path, value));
    return this;
  }

  insertText(field: string, index: number, content: string): this {
    this.ops.push(() =>
      this.store.insertText(this.table, this.pk, field, index, content));
    return this;
  }

  /**
   * Commit all queued writes as one transaction.
   * Returns the changes that were actually applied — a write a plugin
   * rejected is omitted, and does not abort the rest of the batch.
   */
  commit(): ChangeRecord[] {
    const ops = this.ops;
    this.ops = [];
    if (ops.length === 0) return [];

    return this.store.transact(() => {
      const applied: ChangeRecord[] = [];
      for (const op of ops) {
        const change = op();
        if (change) applied.push(change);
      }
      return applied;
    });
  }
}
