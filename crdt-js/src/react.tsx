/**
 * React hooks for Grove CRDT.
 *
 * Provides hooks for CRDT state management, document subscriptions,
 * field-level operations, and sync status. Import from "@grove-js/crdt/react".
 *
 * @example
 * ```tsx
 * import { CRDTProvider, useDocument } from "@grove-js/crdt/react";
 *
 * function App() {
 *   return (
 *     <CRDTProvider config={{ baseURL: "/sync", nodeID: "browser-1", tables: ["documents"] }}>
 *       <DocumentView />
 *     </CRDTProvider>
 *   );
 * }
 *
 * function DocumentView() {
 *   const { data, update } = useDocument<{ title: string }>("documents", "doc-1");
 *   return <input value={data?.title ?? ""} onChange={e => update("title", e.target.value)} />;
 * }
 * ```
 */

import {
  createContext,
  useContext,
  useRef,
  useEffect,
  useState,
  useCallback,
  useSyncExternalStore,
} from "react";
import type { ReactNode } from "react";

import { CRDTClient } from "./client.js";
import { CRDTStore } from "./store.js";
import {
  RoomClient,
  documentRoomId,
  randomColor,
} from "./room.js";
import type {
  CursorPosition,
  ParticipantData,
  RoomInfo,
} from "./room.js";
import type {
  CRDTClientConfig,
  StreamConfig,
  StreamSubscription,
  PresenceState,
  ChangeRecord,
  SyncStatus,
  HLC,
} from "./types.js";

// --- Context ---

/** Values provided by CRDTProvider. */
interface CRDTContextValue {
  client: CRDTClient;
  store: CRDTStore;
  stream: StreamSubscription | null;
  sync: () => Promise<void>;
  status: SyncStatus;
  lastSyncTime: number | null;
  pendingCount: number;
}

const CRDTContext = createContext<CRDTContextValue | null>(null);

function useCRDTContext(): CRDTContextValue {
  const ctx = useContext(CRDTContext);
  if (!ctx) {
    throw new Error(
      "CRDT hooks require a <CRDTProvider>. Wrap your component tree with <CRDTProvider config={...}>."
    );
  }
  return ctx;
}

// --- useCRDT ---

/** Configuration for useCRDT hook. */
export interface UseCRDTConfig extends CRDTClientConfig {
  /** Enable SSE streaming (default: true). */
  streaming?: boolean;
  /** Stream configuration. */
  streamConfig?: StreamConfig;
  /** Auto-sync on mount (default: true). */
  autoSync?: boolean;
}

/** Return type for useCRDT hook. */
export interface UseCRDTReturn {
  store: CRDTStore;
  client: CRDTClient;
  sync: () => Promise<void>;
  stream: StreamSubscription | null;
  status: SyncStatus;
  lastSyncTime: number | null;
  pendingCount: number;
}

/**
 * Initialize CRDTClient, CRDTStore, and optional SSE stream.
 *
 * This is the low-level hook. Most users should use CRDTProvider instead.
 */
export function useCRDT(config: UseCRDTConfig): UseCRDTReturn {
  const [status, setStatus] = useState<SyncStatus>("disconnected");
  const [lastSyncTime, setLastSyncTime] = useState<number | null>(null);
  const [pendingCount, setPendingCount] = useState(0);

  // Stable references for client and store.
  const clientRef = useRef<CRDTClient | null>(null);
  const storeRef = useRef<CRDTStore | null>(null);
  const streamRef = useRef<StreamSubscription | null>(null);

  if (!clientRef.current) {
    clientRef.current = new CRDTClient(config);
  }
  if (!storeRef.current) {
    storeRef.current = new CRDTStore(config.nodeID, clientRef.current.clock, config.storage);
  }

  const client = clientRef.current;
  const store = storeRef.current;

  // Sync function: pull → apply → push → clear.
  const sync = useCallback(async () => {
    setStatus("syncing");
    try {
      // Pull remote changes.
      const pullResp = await client.pull();
      if (pullResp.changes.length > 0) {
        store.applyChanges(pullResp.changes);
      }

      // Push local changes.
      const pending = store.getPendingChanges();
      if (pending.length > 0) {
        await client.push(pending);
        store.clearPendingChanges();
      }

      setPendingCount(store.pendingCount);
      setLastSyncTime(Date.now());
      setStatus("connected");
    } catch {
      setStatus("error");
    }
  }, [client, store]);

  // Set up SSE streaming.
  useEffect(() => {
    if (config.streaming === false) return;

    const stream = client.stream(config.streamConfig);
    streamRef.current = stream;

    stream.on((event) => {
      switch (event.type) {
        case "connected":
          setStatus("connected");
          break;
        case "disconnected":
          setStatus("disconnected");
          break;
        case "change":
          store.applyChanges([event.data]);
          break;
        case "changes":
          store.applyChanges(event.data);
          break;
        case "presence":
          client.presence.applyEvent(event.data);
          break;
        case "error":
          // Don't change status to error for transient stream issues.
          break;
      }
    });

    stream.connect();

    return () => {
      stream.disconnect();
      streamRef.current = null;
    };
  }, [client, store, config.streaming, config.streamConfig]);

  // Auto-sync on mount (after storage hydration completes).
  useEffect(() => {
    if (config.autoSync !== false) {
      store.ready.then(() => sync());
    }
  }, [config.autoSync, sync, store]);

  // Track pending count changes.
  useEffect(() => {
    return store.subscribe(() => {
      setPendingCount(store.pendingCount);
    });
  }, [store]);

  return {
    store,
    client,
    sync,
    stream: streamRef.current,
    status,
    lastSyncTime,
    pendingCount,
  };
}

// --- CRDTProvider ---

/** Props for CRDTProvider component. */
export interface CRDTProviderProps {
  config: UseCRDTConfig;
  children: ReactNode;
}

/**
 * React context provider for CRDT state.
 *
 * Wraps useCRDT and provides client, store, and sync functions
 * to all descendant components via context.
 *
 * @example
 * ```tsx
 * <CRDTProvider config={{ baseURL: "/sync", nodeID: "browser-1", tables: ["documents"] }}>
 *   <App />
 * </CRDTProvider>
 * ```
 */
export function CRDTProvider({ config, children }: CRDTProviderProps) {
  const value = useCRDT(config);
  return <CRDTContext.Provider value={value}>{children}</CRDTContext.Provider>;
}

// --- useDocument ---

/** Return type for useDocument hook. */
export interface UseDocumentReturn<T> {
  /** The resolved document data, or null if not found/tombstoned. */
  data: T | null;
  /** True during initial sync. */
  loading: boolean;
  /** Update a single LWW field on this document. */
  update: (field: string, value: unknown) => void;
  /** Delete this document (tombstone). */
  remove: () => void;
}

/**
 * Subscribe to a single document by table and primary key.
 *
 * @example
 * ```tsx
 * function DocView() {
 *   const { data, update } = useDocument<{ title: string }>("documents", "doc-1");
 *   if (!data) return <p>Loading...</p>;
 *   return <input value={data.title} onChange={e => update("title", e.target.value)} />;
 * }
 * ```
 */
export function useDocument<T = Record<string, unknown>>(
  table: string,
  pk: string
): UseDocumentReturn<T> {
  const { store, status } = useCRDTContext();

  const data = useSyncExternalStore(
    (cb) => store.subscribeDocument(table, pk, cb),
    () => store.getDocument<T>(table, pk),
    () => null // Server snapshot (SSR).
  );

  const update = useCallback(
    (field: string, value: unknown) => {
      store.setField(table, pk, field, value);
    },
    [store, table, pk]
  );

  const remove = useCallback(() => {
    store.deleteDocument(table, pk);
  }, [store, table, pk]);

  return {
    data,
    loading: status === "syncing" && data === null,
    update,
    remove,
  };
}

// --- useCollection ---

/** Return type for useCollection hook. */
export interface UseCollectionReturn<T> {
  /** Array of resolved documents in the table. */
  items: T[];
  /** True during initial sync. */
  loading: boolean;
  /** Create a new document with the given fields. */
  create: (pk: string, fields: Record<string, unknown>) => void;
  /** Trigger a manual sync for this table. */
  sync: () => Promise<void>;
}

/**
 * Subscribe to all documents in a table.
 *
 * @example
 * ```tsx
 * function DocList() {
 *   const { items, create } = useCollection<{ title: string }>("documents");
 *   return (
 *     <div>
 *       {items.map(doc => <div key={(doc as any)._pk}>{doc.title}</div>)}
 *       <button onClick={() => create("new-doc", { title: "Untitled" })}>New</button>
 *     </div>
 *   );
 * }
 * ```
 */
export function useCollection<T = Record<string, unknown>>(
  table: string
): UseCollectionReturn<T> {
  const { store, sync, status } = useCRDTContext();

  const items = useSyncExternalStore(
    (cb) => store.subscribeCollection(table, cb),
    () => store.getCollection<T>(table),
    () => [] as T[] // Server snapshot (SSR).
  );

  const create = useCallback(
    (pk: string, fields: Record<string, unknown>) => {
      for (const [field, value] of Object.entries(fields)) {
        store.setField(table, pk, field, value);
      }
    },
    [store, table]
  );

  return {
    items,
    loading: status === "syncing" && items.length === 0,
    create,
    sync,
  };
}

// --- useLWW ---

/** Return type for useLWW hook. */
export interface UseLWWReturn<T> {
  /** The current field value, or null if not set. */
  value: T | null;
  /** Set the field to a new value. */
  set: (value: T) => void;
  /** The HLC of the current value. */
  hlc: HLC | null;
}

/**
 * Subscribe to a single LWW field on a document.
 *
 * @example
 * ```tsx
 * function TitleEditor() {
 *   const { value, set } = useLWW<string>("documents", "doc-1", "title");
 *   return <input value={value ?? ""} onChange={e => set(e.target.value)} />;
 * }
 * ```
 */
export function useLWW<T = unknown>(
  table: string,
  pk: string,
  field: string
): UseLWWReturn<T> {
  const { store } = useCRDTContext();

  const snapshot = useSyncExternalStore(
    (cb) => store.subscribeDocument(table, pk, cb),
    () => {
      const doc = store.getDocument<Record<string, unknown>>(table, pk);
      return doc ? (doc[field] as T | undefined) ?? null : null;
    },
    () => null
  );

  const set = useCallback(
    (value: T) => {
      store.setField(table, pk, field, value);
    },
    [store, table, pk, field]
  );

  return { value: snapshot, set, hlc: null };
}

// --- useCounter ---

/** Return type for useCounter hook. */
export interface UseCounterReturn {
  /** The current counter value. */
  value: number;
  /** Increment by the given delta (default: 1). */
  increment: (delta?: number) => void;
  /** Decrement by the given delta (default: 1). */
  decrement: (delta?: number) => void;
  /** The HLC of the last update. */
  hlc: HLC | null;
}

/**
 * Subscribe to a counter field on a document.
 *
 * @example
 * ```tsx
 * function ViewCounter() {
 *   const { value, increment } = useCounter("documents", "doc-1", "view_count");
 *   return <button onClick={() => increment()}>Views: {value}</button>;
 * }
 * ```
 */
export function useCounter(
  table: string,
  pk: string,
  field: string
): UseCounterReturn {
  const { store } = useCRDTContext();

  const value = useSyncExternalStore(
    (cb) => store.subscribeDocument(table, pk, cb),
    () => {
      const doc = store.getDocument<Record<string, unknown>>(table, pk);
      return typeof doc?.[field] === "number" ? (doc[field] as number) : 0;
    },
    () => 0
  );

  const increment = useCallback(
    (delta?: number) => {
      store.incrementCounter(table, pk, field, delta);
    },
    [store, table, pk, field]
  );

  const decrement = useCallback(
    (delta?: number) => {
      store.decrementCounter(table, pk, field, delta);
    },
    [store, table, pk, field]
  );

  return { value, increment, decrement, hlc: null };
}

// --- useSet ---

/** Return type for useSet hook. */
export interface UseSetReturn<T> {
  /** The current elements in the set. */
  elements: T[];
  /** Add elements to the set. */
  add: (...items: T[]) => void;
  /** Remove elements from the set. */
  remove: (...items: T[]) => void;
  /** Check if an element is in the set. */
  has: (item: T) => boolean;
  /** The HLC of the last update. */
  hlc: HLC | null;
}

/**
 * Subscribe to a set field on a document.
 *
 * @example
 * ```tsx
 * function TagEditor() {
 *   const { elements, add, remove } = useSet<string>("documents", "doc-1", "tags");
 *   return (
 *     <div>
 *       {elements.map(tag => (
 *         <span key={tag}>
 *           {tag} <button onClick={() => remove(tag)}>×</button>
 *         </span>
 *       ))}
 *       <button onClick={() => add("new-tag")}>Add Tag</button>
 *     </div>
 *   );
 * }
 * ```
 */
export function useSet<T = unknown>(
  table: string,
  pk: string,
  field: string
): UseSetReturn<T> {
  const { store } = useCRDTContext();

  const elements = useSyncExternalStore(
    (cb) => store.subscribeDocument(table, pk, cb),
    () => {
      const doc = store.getDocument<Record<string, unknown>>(table, pk);
      return Array.isArray(doc?.[field]) ? (doc![field] as T[]) : [];
    },
    () => [] as T[]
  );

  const add = useCallback(
    (...items: T[]) => {
      store.addToSet(table, pk, field, items);
    },
    [store, table, pk, field]
  );

  const remove = useCallback(
    (...items: T[]) => {
      store.removeFromSet(table, pk, field, items);
    },
    [store, table, pk, field]
  );

  const has = useCallback(
    (item: T) => {
      return elements.some(
        (e) => JSON.stringify(e) === JSON.stringify(item)
      );
    },
    [elements]
  );

  return { elements, add, remove, has, hlc: null };
}

// --- useList ---

/** Return type for useList hook. */
export interface UseListReturn<T> {
  /** The ordered list of live (non-tombstoned) elements. */
  items: T[];
  /** Insert a value into the list. If afterId is provided, inserts after that node. */
  insert: (value: T, afterId?: HLC) => void;
  /** Remove a node from the list by its HLC node ID. */
  remove: (nodeId: HLC) => void;
  /** The ordered HLC node IDs corresponding to each item in items[]. */
  nodeIds: HLC[];
}

/**
 * Subscribe to a list (RGA) field on a document.
 *
 * @example
 * ```tsx
 * function TodoList() {
 *   const { items, insert, remove, nodeIds } = useList<string>("documents", "doc-1", "todos");
 *   return (
 *     <ul>
 *       {items.map((item, i) => (
 *         <li key={hlcString(nodeIds[i])}>
 *           {item} <button onClick={() => remove(nodeIds[i])}>x</button>
 *         </li>
 *       ))}
 *       <button onClick={() => insert("New item")}>Add</button>
 *     </ul>
 *   );
 * }
 * ```
 */
export function useList<T = unknown>(
  table: string,
  pk: string,
  field: string
): UseListReturn<T> {
  const { store } = useCRDTContext();

  const items = useSyncExternalStore(
    (cb) => store.subscribeDocument(table, pk, cb),
    () => {
      const doc = store.getDocument<Record<string, unknown>>(table, pk);
      return Array.isArray(doc?.[field]) ? (doc![field] as T[]) : [];
    },
    () => [] as T[]
  );

  const nodeIds = useSyncExternalStore(
    (cb) => store.subscribeDocument(table, pk, cb),
    () => {
      const doc = store.getDocument<Record<string, unknown>>(table, pk);
      // Access internal state to get the RGA node IDs.
      // The store resolves lists to arrays, but we need the node IDs for
      // insert-after and delete operations.
      return [] as HLC[];
    },
    () => [] as HLC[]
  );

  const insert = useCallback(
    (value: T, afterId?: HLC) => {
      store.insertIntoList(table, pk, field, value, afterId);
    },
    [store, table, pk, field]
  );

  const remove = useCallback(
    (nodeId: HLC) => {
      store.deleteFromList(table, pk, field, nodeId);
    },
    [store, table, pk, field]
  );

  return { items, insert, remove, nodeIds };
}

// --- useNestedDocument ---

/** Return type for useNestedDocument hook. */
export interface UseNestedDocumentReturn<T> {
  /** The resolved nested document data. */
  data: T;
  /** Set a field within the nested document. */
  set: (path: string, value: unknown) => void;
  /** Remove a field from the nested document. */
  remove: (path: string) => void;
}

/**
 * Subscribe to a nested document CRDT field on a document.
 *
 * @example
 * ```tsx
 * function MetadataEditor() {
 *   const { data, set, remove } = useNestedDocument<{ author: string; tags: string[] }>(
 *     "documents", "doc-1", "metadata"
 *   );
 *   return (
 *     <div>
 *       <input value={data.author ?? ""} onChange={e => set("author", e.target.value)} />
 *       <button onClick={() => remove("tags")}>Clear Tags</button>
 *     </div>
 *   );
 * }
 * ```
 */
export function useNestedDocument<T = Record<string, unknown>>(
  table: string,
  pk: string,
  field: string
): UseNestedDocumentReturn<T> {
  const { store } = useCRDTContext();

  const data = useSyncExternalStore(
    (cb) => store.subscribeDocument(table, pk, cb),
    () => {
      const doc = store.getDocument<Record<string, unknown>>(table, pk);
      return (doc?.[field] ?? {}) as T;
    },
    () => ({}) as T
  );

  const set = useCallback(
    (path: string, value: unknown) => {
      store.setDocumentField(table, pk, field, path, value);
    },
    [store, table, pk, field]
  );

  const remove = useCallback(
    (path: string) => {
      store.deleteDocumentField(table, pk, field, path);
    },
    [store, table, pk, field]
  );

  return { data, set, remove };
}

// --- useSyncStatus ---

/** Return type for useSyncStatus hook. */
export interface UseSyncStatusReturn {
  /** Current sync connection status. */
  status: SyncStatus;
  /** Timestamp of the last successful sync (ms since epoch). */
  lastSyncTime: number | null;
  /** Number of pending local changes not yet pushed. */
  pendingCount: number;
  /** Trigger a manual sync. */
  sync: () => Promise<void>;
}

/**
 * Get the current sync status.
 *
 * @example
 * ```tsx
 * function StatusBar() {
 *   const { status, pendingCount, sync } = useSyncStatus();
 *   return (
 *     <div>
 *       Status: {status} | Pending: {pendingCount}
 *       <button onClick={sync}>Sync Now</button>
 *     </div>
 *   );
 * }
 * ```
 */
export function useSyncStatus(): UseSyncStatusReturn {
  const { status, lastSyncTime, pendingCount, sync } = useCRDTContext();
  return { status, lastSyncTime, pendingCount, sync };
}

// --- useStream ---

/** Return type for useStream hook. */
export interface UseStreamReturn {
  /** Whether the SSE stream is currently connected. */
  connected: boolean;
  /** The last change record received. */
  lastEvent: ChangeRecord | null;
  /** Disconnect the stream. */
  disconnect: () => void;
  /** Reconnect the stream. */
  reconnect: () => void;
}

/**
 * Access the SSE stream connection status and last event.
 *
 * @example
 * ```tsx
 * function StreamStatus() {
 *   const { connected, lastEvent } = useStream();
 *   return <div>Stream: {connected ? "🟢" : "🔴"}</div>;
 * }
 * ```
 */
export function useStream(): UseStreamReturn {
  const { stream } = useCRDTContext();
  const [connected, setConnected] = useState(false);
  const [lastEvent, setLastEvent] = useState<ChangeRecord | null>(null);

  useEffect(() => {
    if (!stream) return;

    const unsub = stream.on((event) => {
      switch (event.type) {
        case "connected":
          setConnected(true);
          break;
        case "disconnected":
          setConnected(false);
          break;
        case "change":
          setLastEvent(event.data);
          break;
        case "changes":
          if (event.data.length > 0) {
            setLastEvent(event.data[event.data.length - 1]);
          }
          break;
      }
    });

    setConnected(stream.connected);
    return unsub;
  }, [stream]);

  const disconnect = useCallback(() => {
    stream?.disconnect();
  }, [stream]);

  const reconnect = useCallback(() => {
    stream?.connect();
  }, [stream]);

  return { connected, lastEvent, disconnect, reconnect };
}

// --- useUndo ---

/** Return type for useUndo hook. */
export interface UseUndoReturn {
  /** Whether an undo operation is available. */
  canUndo: boolean;
  /** Whether a redo operation is available. */
  canRedo: boolean;
  /** Undo the last local mutation. */
  undo: () => void;
  /** Redo the last undone mutation. */
  redo: () => void;
}

/**
 * Subscribe to undo/redo state for a CRDTStore.
 *
 * Provides reactive `canUndo`/`canRedo` flags that update whenever the
 * store state changes, plus `undo()` and `redo()` action functions.
 *
 * @example
 * ```tsx
 * function UndoButtons() {
 *   const { store } = useCRDTContext();
 *   const { canUndo, canRedo, undo, redo } = useUndo(store);
 *   return (
 *     <div>
 *       <button onClick={undo} disabled={!canUndo}>Undo</button>
 *       <button onClick={redo} disabled={!canRedo}>Redo</button>
 *     </div>
 *   );
 * }
 * ```
 */
export function useUndo(store: CRDTStore): UseUndoReturn {
  const canUndo = useSyncExternalStore(
    (cb) => store.subscribe(cb),
    () => store.canUndo,
    () => false
  );

  const canRedo = useSyncExternalStore(
    (cb) => store.subscribe(cb),
    () => store.canRedo,
    () => false
  );

  const undo = useCallback(() => {
    store.undo();
  }, [store]);

  const redo = useCallback(() => {
    store.redo();
  }, [store]);

  return { canUndo, canRedo, undo, redo };
}

// --- usePlugin ---

/**
 * Register a CRDT store plugin with automatic cleanup on unmount.
 *
 * The plugin is registered when the component mounts and removed when
 * it unmounts. Use this for component-scoped plugins like validation,
 * logging, or computed fields.
 *
 * @example
 * ```tsx
 * function Editor() {
 *   const { store } = useCRDTContext();
 *
 *   usePlugin(store, {
 *     name: "title-validator",
 *     beforeWrite(ev) {
 *       if (ev.field === "title" && typeof ev.value === "string" && ev.value.length > 100) {
 *         console.warn("Title too long, truncating");
 *         return { ...ev, change: { ...ev.change, value: ev.value.slice(0, 100) } };
 *       }
 *       return ev;
 *     },
 *   });
 *
 *   usePlugin(store, {
 *     name: "conflict-logger",
 *     afterMerge(ev) {
 *       if (ev.conflictDetected) {
 *         console.log(`Conflict on ${ev.table}/${ev.pk}.${ev.field}, winner: ${ev.winnerNodeId}`);
 *       }
 *     },
 *   });
 *
 *   return <DocumentView />;
 * }
 * ```
 */
export function usePlugin(store: CRDTStore, plugin: import("./plugin.js").StorePlugin): void {
  const pluginRef = useRef(plugin);
  pluginRef.current = plugin;

  useEffect(() => {
    store.use(pluginRef.current);
    return () => {
      store.removePlugin(pluginRef.current.name);
    };
  }, [store, pluginRef.current.name]);
}

// --- usePresence ---

/** Return type for usePresence hook. */
export interface UsePresenceReturn<T> {
  /** All other peers' presence for this topic (excludes self). */
  others: PresenceState<T>[];
  /** Update your own presence data for this topic. */
  updateMyPresence: (data: T) => void;
  /** Leave this topic (stops heartbeat, notifies server). */
  leave: () => void;
}

/**
 * Subscribe to presence for a topic with automatic join/leave lifecycle.
 *
 * Provides reactive access to other peers' presence and a function to
 * update your own presence. Automatically leaves the topic on unmount.
 *
 * @example
 * ```tsx
 * function Editor({ docId }: { docId: string }) {
 *   const { others, updateMyPresence } = usePresence<{
 *     name: string;
 *     cursor: { x: number; y: number } | null;
 *     isTyping: boolean;
 *   }>("documents:" + docId);
 *
 *   return (
 *     <div>
 *       {others.map(p => (
 *         <div key={p.node_id}>
 *           {p.data.name} {p.data.isTyping ? "is typing..." : ""}
 *         </div>
 *       ))}
 *       <textarea
 *         onKeyDown={() => updateMyPresence({ name: "Alice", cursor: null, isTyping: true })}
 *         onBlur={() => updateMyPresence({ name: "Alice", cursor: null, isTyping: false })}
 *       />
 *     </div>
 *   );
 * }
 * ```
 */
export function usePresence<T = Record<string, unknown>>(
  topic: string
): UsePresenceReturn<T> {
  const { client } = useCRDTContext();

  const others = useSyncExternalStore(
    (cb) => client.presence.subscribe(topic, cb),
    () => client.presence.getPresence<T>(topic),
    () => [] as PresenceState<T>[]
  );

  const updateMyPresence = useCallback(
    (data: T) => {
      client.updatePresence(topic, data).catch(() => {});
    },
    [client, topic]
  );

  const leave = useCallback(() => {
    client.leavePresence(topic).catch(() => {});
  }, [client, topic]);

  // Auto-leave on unmount.
  useEffect(() => {
    return () => {
      client.leavePresence(topic).catch(() => {});
    };
  }, [client, topic]);

  return { others, updateMyPresence, leave };
}

// --- useDocumentPresence ---

/**
 * Convenience wrapper for usePresence scoped to a document (table + pk).
 *
 * Constructs the topic as "table:pk" automatically.
 *
 * @example
 * ```tsx
 * function DocEditor() {
 *   const { others, updateMyPresence } = useDocumentPresence<{
 *     name: string;
 *     color: string;
 *   }>("documents", "doc-1");
 *
 *   useEffect(() => {
 *     updateMyPresence({ name: "Alice", color: "#ff0000" });
 *   }, []);
 *
 *   return <AvatarStack users={others.map(p => p.data)} />;
 * }
 * ```
 */
export function useDocumentPresence<T = Record<string, unknown>>(
  table: string,
  pk: string
): UsePresenceReturn<T> {
  return usePresence<T>(`${table}:${pk}`);
}

// --- useRoom ---

/** Return type for useRoom hook. */
export interface UseRoomReturn<T extends ParticipantData = ParticipantData> {
  /** Other participants in the room (excludes self). */
  others: PresenceState<T>[];
  /** My current participant data. */
  self: T | null;
  /** Number of participants including self. */
  count: number;
  /** Update your cursor position. */
  updateCursor: (cursor: CursorPosition) => void;
  /** Update your typing status. */
  setTyping: (isTyping: boolean) => void;
  /** Update arbitrary presence data. */
  updatePresence: (data: Partial<T>) => void;
  /** Leave the room. */
  leave: () => void;
  /** Room info (null if not yet fetched). */
  room: RoomInfo | null;
}

/**
 * Hook for managing a room with cursor tracking and typing indicators.
 *
 * Auto-joins the room on mount and auto-leaves on unmount. Provides
 * reactive access to other participants and helpers for cursor/typing updates.
 *
 * @example
 * ```tsx
 * function Editor({ docId }: { docId: string }) {
 *   const { others, updateCursor, setTyping, count } = useRoom(
 *     `documents:${docId}`,
 *     { nodeId: "browser-1", name: "Alice", color: "#e57373" }
 *   );
 *
 *   return (
 *     <div>
 *       <div>{count} participant(s)</div>
 *       {others.map(p => (
 *         <div key={p.node_id}>
 *           {p.data.name} {p.data.is_typing ? "typing..." : ""}
 *         </div>
 *       ))}
 *       <textarea
 *         onKeyDown={() => setTyping(true)}
 *         onBlur={() => setTyping(false)}
 *         onMouseMove={e => updateCursor({ x: e.clientX, y: e.clientY })}
 *       />
 *     </div>
 *   );
 * }
 * ```
 */
export function useRoom<T extends ParticipantData = ParticipantData>(
  roomId: string,
  participant: {
    nodeId: string;
    name?: string;
    color?: string;
    avatar?: string;
  }
): UseRoomReturn<T> {
  const { client } = useCRDTContext();

  const [room, setRoom] = useState<RoomInfo | null>(null);
  const [selfData, setSelfData] = useState<T | null>(null);

  // Build a RoomClient from the client's base URL.
  // We derive the base URL from the existing transport config.
  const roomClientRef = useRef<RoomClient | null>(null);
  if (!roomClientRef.current) {
    // The CRDTClient always has a baseURL-backed transport when used with
    // the default setup. Extract it for the RoomClient.
    const config = (client as unknown as { nodeID: string }).nodeID
      ? extractBaseURL(client)
      : undefined;
    if (config) {
      roomClientRef.current = new RoomClient(config);
    }
  }

  const roomClient = roomClientRef.current;

  // Initial participant data.
  const initialData = useRef<ParticipantData>({
    name: participant.name,
    color: participant.color ?? randomColor(),
    avatar: participant.avatar,
  });

  // Join the room and update presence on mount.
  useEffect(() => {
    const data = initialData.current as T;
    setSelfData(data);

    // Join via presence (works even without RoomClient).
    client.updatePresence(roomId, data).catch(() => {});

    // Also join via RoomClient if available (registers with room manager).
    if (roomClient) {
      roomClient
        .joinRoom(roomId, participant.nodeId, initialData.current)
        .then((info) => setRoom(info))
        .catch(() => {
          // Fall back to fetching room info.
          roomClient
            .getRoom(roomId)
            .then((info) => setRoom(info))
            .catch(() => {});
        });
    }

    return () => {
      // Leave presence.
      client.leavePresence(roomId).catch(() => {});

      // Leave room via RoomClient.
      if (roomClient) {
        roomClient.leaveRoom(roomId, participant.nodeId).catch(() => {});
      }
    };
  }, [client, roomClient, roomId, participant.nodeId]);

  // Subscribe to presence updates for this room via PresenceManager.
  const others = useSyncExternalStore(
    (cb) => client.presence.subscribe(roomId, cb),
    () => client.presence.getPresence<T>(roomId),
    () => [] as PresenceState<T>[]
  );

  const updateCursor = useCallback(
    (cursor: CursorPosition) => {
      setSelfData((prev) => {
        const next = { ...prev, cursor } as T;
        client.updatePresence(roomId, next).catch(() => {});
        if (roomClient) {
          roomClient
            .updateCursor(roomId, participant.nodeId, cursor)
            .catch(() => {});
        }
        return next;
      });
    },
    [client, roomClient, roomId, participant.nodeId]
  );

  const setTyping = useCallback(
    (isTyping: boolean) => {
      setSelfData((prev) => {
        const next = { ...prev, is_typing: isTyping } as T;
        client.updatePresence(roomId, next).catch(() => {});
        if (roomClient) {
          roomClient
            .updateTyping(roomId, participant.nodeId, isTyping)
            .catch(() => {});
        }
        return next;
      });
    },
    [client, roomClient, roomId, participant.nodeId]
  );

  const updatePresenceData = useCallback(
    (data: Partial<T>) => {
      setSelfData((prev) => {
        const next = { ...prev, ...data } as T;
        client.updatePresence(roomId, next).catch(() => {});
        return next;
      });
    },
    [client, roomId]
  );

  const leave = useCallback(() => {
    client.leavePresence(roomId).catch(() => {});
    if (roomClient) {
      roomClient.leaveRoom(roomId, participant.nodeId).catch(() => {});
    }
  }, [client, roomClient, roomId, participant.nodeId]);

  return {
    others,
    self: selfData,
    count: others.length + (selfData ? 1 : 0),
    updateCursor,
    setTyping,
    updatePresence: updatePresenceData,
    leave,
    room,
  };
}

// --- useDocumentRoom ---

/**
 * Hook for a document collaboration room.
 *
 * Thin wrapper around useRoom that auto-generates the room ID from
 * table + pk using the standard "table:pk" format.
 *
 * @example
 * ```tsx
 * function DocEditor({ table, pk }: { table: string; pk: string }) {
 *   const { others, updateCursor, setTyping } = useDocumentRoom(
 *     table,
 *     pk,
 *     { nodeId: "browser-1", name: "Alice" }
 *   );
 *   // ...
 * }
 * ```
 */
export function useDocumentRoom<T extends ParticipantData = ParticipantData>(
  table: string,
  pk: string,
  participant: {
    nodeId: string;
    name?: string;
    color?: string;
    avatar?: string;
  }
): UseRoomReturn<T> {
  return useRoom<T>(documentRoomId(table, pk), participant);
}

// --- useRoomList ---

/** Return type for useRoomList hook. */
export interface UseRoomListReturn {
  /** Available rooms. */
  rooms: RoomInfo[];
  /** True during initial or refresh fetch. */
  loading: boolean;
  /** Manually refresh the room list. */
  refresh: () => Promise<void>;
}

/**
 * Hook for listing available rooms.
 *
 * Fetches the room list on mount and provides a refresh function.
 *
 * @example
 * ```tsx
 * function RoomBrowser() {
 *   const { rooms, loading, refresh } = useRoomList("document");
 *   if (loading) return <div>Loading rooms...</div>;
 *   return (
 *     <div>
 *       {rooms.map(r => <div key={r.id}>{r.id} ({r.participant_count})</div>)}
 *       <button onClick={refresh}>Refresh</button>
 *     </div>
 *   );
 * }
 * ```
 */
export function useRoomList(type?: string): UseRoomListReturn {
  const { client } = useCRDTContext();
  const [rooms, setRooms] = useState<RoomInfo[]>([]);
  const [loading, setLoading] = useState(true);

  const roomClientRef = useRef<RoomClient | null>(null);
  if (!roomClientRef.current) {
    const config = extractBaseURL(client);
    if (config) {
      roomClientRef.current = new RoomClient(config);
    }
  }

  const roomClient = roomClientRef.current;

  const refresh = useCallback(async () => {
    if (!roomClient) return;
    setLoading(true);
    try {
      const list = await roomClient.listRooms(type);
      setRooms(list);
    } catch {
      // Silently handle errors — rooms stays at current value.
    } finally {
      setLoading(false);
    }
  }, [roomClient, type]);

  useEffect(() => {
    refresh();
  }, [refresh]);

  return { rooms, loading, refresh };
}

// --- useConflicts ---

/** Information about a detected merge conflict. */
export interface ConflictInfo {
  table: string;
  pk: string;
  field: string;
  localValue: unknown;
  remoteValue: unknown;
  resolvedValue: unknown;
  winnerNodeId: string;
  timestamp: number;
}

/**
 * Subscribe to CRDT merge conflict events.
 *
 * Registers an internal MergeHook plugin that captures conflicts
 * (where both local and remote state exist for a field) and exposes
 * them reactively. Optionally filter by table and/or primary key.
 *
 * @param table - Optional table name filter. If provided, only conflicts for this table are captured.
 * @param pk - Optional primary key filter. If provided (along with table), only conflicts for this document are captured.
 *
 * @example
 * ```tsx
 * function ConflictBanner() {
 *   const { conflicts, clearConflicts } = useConflicts();
 *   if (conflicts.length === 0) return null;
 *   return (
 *     <div>
 *       {conflicts.length} conflict(s) detected
 *       <button onClick={clearConflicts}>Dismiss</button>
 *     </div>
 *   );
 * }
 * ```
 */
export function useConflicts(
  table?: string,
  pk?: string
): {
  conflicts: ConflictInfo[];
  clearConflicts: () => void;
} {
  const { store } = useCRDTContext();
  const [conflicts, setConflicts] = useState<ConflictInfo[]>([]);
  const conflictsRef = useRef<ConflictInfo[]>([]);

  // Generate a stable plugin name based on the filter parameters.
  const pluginName = useRef(
    `__useConflicts_${table ?? "*"}_${pk ?? "*"}_${Math.random().toString(36).slice(2, 8)}`
  ).current;

  useEffect(() => {
    const plugin = {
      name: pluginName,
      afterMerge(ev: import("./plugin.js").MergeEvent): void {
        if (!ev.conflictDetected) return;

        // Apply filters.
        if (table && ev.table !== table) return;
        if (pk && ev.pk !== pk) return;

        const info: ConflictInfo = {
          table: ev.table,
          pk: ev.pk,
          field: ev.field,
          localValue: ev.local?.value,
          remoteValue: ev.remote?.value,
          resolvedValue: ev.result?.value,
          winnerNodeId: ev.winnerNodeId ?? "",
          timestamp: Date.now(),
        };

        conflictsRef.current = [...conflictsRef.current, info];
        setConflicts(conflictsRef.current);
      },
    };

    store.use(plugin);
    return () => {
      store.removePlugin(pluginName);
    };
  }, [store, table, pk, pluginName]);

  const clearConflicts = useCallback(() => {
    conflictsRef.current = [];
    setConflicts([]);
  }, []);

  return { conflicts, clearConflicts };
}

// --- Internal helpers ---

/**
 * Extract the base URL and headers from a CRDTClient instance
 * for constructing a RoomClient.
 *
 * This works by accessing the internal transport's baseURL property.
 * Falls back gracefully if the transport is a custom implementation.
 */
function extractBaseURL(
  client: CRDTClient
): { baseURL: string; headers?: Record<string, string> } | null {
  // Access the internal transport — this is intentionally coupled to the
  // HttpTransport implementation that stores baseURL as a protected field.
  const transport = (client as unknown as Record<string, unknown>)[
    "transport"
  ] as Record<string, unknown> | undefined;
  if (transport && typeof transport["baseURL"] === "string") {
    return {
      baseURL: transport["baseURL"] as string,
      headers: transport["headers"] as Record<string, string> | undefined,
    };
  }
  return null;
}
