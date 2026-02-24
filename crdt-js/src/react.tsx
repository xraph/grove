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
import type {
  CRDTClientConfig,
  StreamConfig,
  StreamSubscription,
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
