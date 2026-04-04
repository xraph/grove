/**
 * Default storage adapter implementations.
 */

import type { StorageAdapter, DocumentState, ChangeRecord } from "./types.js";

/**
 * No-op in-memory storage adapter.
 *
 * Returns empty state on load and discards all save operations.
 * CRDTStore always holds in-memory state as source of truth;
 * this adapter is the default when no persistence is needed.
 */
export class MemoryStorage implements StorageAdapter {
  async loadState(): Promise<Map<string, Map<string, DocumentState>>> {
    return new Map();
  }

  async saveDocument(
    _table: string,
    _pk: string,
    _doc: DocumentState
  ): Promise<void> {
    // No-op for in-memory storage.
  }

  async deleteDocument(_table: string, _pk: string): Promise<void> {
    // No-op for in-memory storage.
  }

  async loadPendingChanges(): Promise<ChangeRecord[]> {
    return [];
  }

  async savePendingChanges(_changes: ChangeRecord[]): Promise<void> {
    // No-op for in-memory storage.
  }
}

// Re-export IndexedDB adapter for convenience.
export { IndexedDBStorage } from "./storage/indexeddb.js";
export type { IndexedDBStorageOptions } from "./storage/indexeddb.js";
