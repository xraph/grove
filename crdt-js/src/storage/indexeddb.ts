/**
 * IndexedDB-backed storage adapter for offline-first persistence.
 *
 * Uses the raw IndexedDB API (zero external dependencies).
 * Falls back to no-op behavior when IndexedDB is not available
 * (e.g. server-side rendering, older environments).
 */

import type {
  StorageAdapter,
  DocumentState,
  ChangeRecord,
} from "../types.js";

/** Options accepted by {@link IndexedDBStorage}. */
export interface IndexedDBStorageOptions {
  /** Database name (default: `"grove-crdt"`). */
  dbName?: string;
  /** Database schema version (default: `1`). */
  dbVersion?: number;
}

/** Object-store names used inside the database. */
const STORE_DOCUMENTS = "documents";
const STORE_PENDING = "pending";

/** Fixed key used for the single pending-changes record. */
const PENDING_KEY = "changes";

/**
 * IndexedDB implementation of {@link StorageAdapter}.
 *
 * Stores document state in a `"documents"` object store keyed by the
 * compound path `[table, pk]`, and pending change records in a
 * `"pending"` store under the single key `"changes"`.
 *
 * When IndexedDB is unavailable the adapter silently degrades to a
 * no-op so callers never need to feature-detect.
 */
export class IndexedDBStorage implements StorageAdapter {
  private readonly _dbName: string;
  private readonly _dbVersion: number;
  private _db: IDBDatabase | null = null;
  private _dbPromise: Promise<IDBDatabase | null> | null = null;
  private readonly _available: boolean;

  constructor(options: IndexedDBStorageOptions = {}) {
    this._dbName = options.dbName ?? "grove-crdt";
    this._dbVersion = options.dbVersion ?? 1;
    this._available =
      typeof indexedDB !== "undefined" && indexedDB !== null;
  }

  // ---------------------------------------------------------------------------
  // Internal helpers
  // ---------------------------------------------------------------------------

  /**
   * Open (or reuse) the database connection.
   *
   * Returns `null` when IndexedDB is not available so every public
   * method can short-circuit without duplicating the feature check.
   */
  private _open(): Promise<IDBDatabase | null> {
    if (!this._available) {
      return Promise.resolve(null);
    }

    if (this._db) {
      return Promise.resolve(this._db);
    }

    if (this._dbPromise) {
      return this._dbPromise;
    }

    this._dbPromise = new Promise<IDBDatabase | null>((resolve) => {
      try {
        const request = indexedDB.open(this._dbName, this._dbVersion);

        request.onupgradeneeded = () => {
          const db = request.result;

          if (!db.objectStoreNames.contains(STORE_DOCUMENTS)) {
            db.createObjectStore(STORE_DOCUMENTS, {
              keyPath: ["table", "pk"],
            });
          }

          if (!db.objectStoreNames.contains(STORE_PENDING)) {
            db.createObjectStore(STORE_PENDING);
          }
        };

        request.onsuccess = () => {
          this._db = request.result;

          // If the connection is closed externally, clear cached handle.
          this._db.onclose = () => {
            this._db = null;
            this._dbPromise = null;
          };

          resolve(this._db);
        };

        request.onerror = () => {
          this._dbPromise = null;
          resolve(null);
        };
      } catch {
        this._dbPromise = null;
        resolve(null);
      }
    });

    return this._dbPromise;
  }

  /**
   * Run a read-only or read-write callback inside a transaction.
   *
   * Returns `fallback` when the database is unavailable.
   */
  private async _tx<T>(
    storeNames: string | string[],
    mode: IDBTransactionMode,
    fallback: T,
    fn: (tx: IDBTransaction) => IDBRequest<T>
  ): Promise<T> {
    const db = await this._open();
    if (!db) return fallback;

    return new Promise<T>((resolve, reject) => {
      try {
        const tx = db.transaction(storeNames, mode);
        const req = fn(tx);

        req.onsuccess = () => resolve(req.result);
        req.onerror = () => reject(req.error);

        tx.onerror = () => reject(tx.error);
      } catch (err) {
        // Gracefully degrade on unexpected errors (e.g. store missing
        // after an external schema change).
        resolve(fallback);
      }
    });
  }

  /**
   * Run a void read-write operation (put / delete) inside a transaction.
   */
  private async _txVoid(
    storeNames: string | string[],
    fn: (tx: IDBTransaction) => IDBRequest
  ): Promise<void> {
    const db = await this._open();
    if (!db) return;

    return new Promise<void>((resolve, reject) => {
      try {
        const tx = db.transaction(storeNames, "readwrite");
        const req = fn(tx);

        req.onsuccess = () => resolve();
        req.onerror = () => reject(req.error);

        tx.onerror = () => reject(tx.error);
      } catch {
        // Degrade silently.
        resolve();
      }
    });
  }

  // ---------------------------------------------------------------------------
  // StorageAdapter implementation
  // ---------------------------------------------------------------------------

  async loadState(): Promise<Map<string, Map<string, DocumentState>>> {
    const result = new Map<string, Map<string, DocumentState>>();

    const db = await this._open();
    if (!db) return result;

    const docs = await new Promise<DocumentState[]>((resolve, reject) => {
      try {
        const tx = db.transaction(STORE_DOCUMENTS, "readonly");
        const store = tx.objectStore(STORE_DOCUMENTS);
        const req = store.getAll();

        req.onsuccess = () => resolve((req.result ?? []) as DocumentState[]);
        req.onerror = () => reject(req.error);
        tx.onerror = () => reject(tx.error);
      } catch {
        resolve([]);
      }
    });

    for (const doc of docs) {
      let tableMap = result.get(doc.table);
      if (!tableMap) {
        tableMap = new Map<string, DocumentState>();
        result.set(doc.table, tableMap);
      }
      tableMap.set(doc.pk, doc);
    }

    return result;
  }

  async saveDocument(
    _table: string,
    _pk: string,
    doc: DocumentState
  ): Promise<void> {
    return this._txVoid(STORE_DOCUMENTS, (tx) =>
      tx.objectStore(STORE_DOCUMENTS).put(doc)
    );
  }

  async deleteDocument(table: string, pk: string): Promise<void> {
    return this._txVoid(STORE_DOCUMENTS, (tx) =>
      tx.objectStore(STORE_DOCUMENTS).delete([table, pk])
    );
  }

  async loadPendingChanges(): Promise<ChangeRecord[]> {
    return this._tx<ChangeRecord[]>(
      STORE_PENDING,
      "readonly",
      [],
      (tx) => tx.objectStore(STORE_PENDING).get(PENDING_KEY)
    ).then((val) => val ?? []);
  }

  async savePendingChanges(changes: ChangeRecord[]): Promise<void> {
    return this._txVoid(STORE_PENDING, (tx) =>
      tx.objectStore(STORE_PENDING).put(changes, PENDING_KEY)
    );
  }

  // ---------------------------------------------------------------------------
  // Lifecycle
  // ---------------------------------------------------------------------------

  /** Close the underlying database connection (if open). */
  close(): void {
    if (this._db) {
      this._db.close();
      this._db = null;
      this._dbPromise = null;
    }
  }
}
