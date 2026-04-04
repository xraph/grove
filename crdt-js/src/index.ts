/**
 * @grove-js/crdt
 *
 * TypeScript client for Grove CRDT sync — pull/push, SSE streaming,
 * client-side merge, and in-memory state management.
 *
 * @example
 * ```ts
 * import { CRDTClient, CRDTStore, HybridClock } from "@grove-js/crdt";
 *
 * const client = new CRDTClient({
 *   baseURL: "https://api.example.com/sync",
 *   nodeID: "browser-1",
 *   tables: ["documents"],
 * });
 *
 * const store = new CRDTStore("browser-1", client.clock);
 *
 * // Pull remote changes
 * const { changes } = await client.pull();
 * store.applyChanges(changes);
 *
 * // Make a local change
 * store.setField("documents", "doc-1", "title", "Hello World");
 *
 * // Push local changes
 * await client.push(store.getPendingChanges());
 * store.clearPendingChanges();
 * ```
 *
 * For React hooks, import from "@grove-js/crdt/react":
 * ```ts
 * import { CRDTProvider, useDocument } from "@grove-js/crdt/react";
 * ```
 */

// Wire format types
export type {
  HLC,
  CRDTType,
  SetOpType,
  CounterDelta,
  SetOperation,
  ChangeRecord,
  PullRequest,
  PullResponse,
  PushRequest,
  PushResponse,
  SyncStatus,
  CRDTClientConfig,
  StreamConfig,
  PNCounterState,
  ORSetTag,
  ORSetState,
  FieldState,
  DocumentState,
  SyncReport,
  RGANode,
  RGAListState,
  ListOperation,
  DocumentCRDTState,
} from "./types.js";

// Plugin interfaces
export type {
  Transport,
  StreamTransport,
  StreamSubscription,
  StreamEvent,
  StreamEventHandler,
  StorageAdapter,
  AuthProvider,
} from "./types.js";

// Presence types
export type {
  PresenceState,
  PresenceUpdate,
  PresenceEvent,
  PresenceSnapshot,
  PresenceConfig,
} from "./types.js";

// HLC clock
export {
  HLC_ZERO,
  hlcCompare,
  hlcAfter,
  hlcIsZero,
  hlcMax,
  hlcString,
  HybridClock,
} from "./hlc.js";

// Merge functions
export type { LWWValue } from "./merge.js";
export {
  mergeLWW,
  newPNCounterState,
  mergeCounter,
  counterValue,
  tagKey,
  newORSetState,
  mergeSet,
  setElements,
  newRGAListState,
  mergeListState,
  listElements,
  listNodeIds,
  newDocumentCRDTState,
  mergeDocumentState,
  documentResolve,
  mergeFieldState,
} from "./merge.js";

// Errors
export {
  CRDTErrorCode,
  CRDTError,
  TransportError,
  NetworkError,
  ValidationError,
  SyncError,
  PluginError,
} from "./errors.js";

// Client
export { CRDTClient } from "./client.js";

// SSE streaming
export type { CRDTStreamEvent } from "./stream.js";
export { CRDTStream } from "./stream.js";

// State store
export { CRDTStore, BatchWriter } from "./store.js";
export type { StateSnapshot } from "./store.js";

// Undo/Redo
export type { UndoEntry } from "./undo.js";
export { UndoManager } from "./undo.js";

// Plugin system
export { PluginManager } from "./plugin.js";
export type {
  StorePlugin,
  WriteHook,
  MergeHook,
  SyncHook,
  ReadHook,
  PresenceHook,
  StorageHook,
  WriteEvent,
  MergeEvent,
  PullEvent,
} from "./plugin.js";

// Presence
export { PresenceManager } from "./presence.js";

// Room management
export { RoomClient, documentRoomId, randomColor } from "./room.js";
export type {
  CursorPosition,
  ParticipantData,
  Room,
  RoomInfo,
  RoomClientConfig,
} from "./room.js";

// Default implementations
export { HttpTransport, HttpStreamTransport, isStreamTransport } from "./transport.js";
export { MemoryStorage, IndexedDBStorage } from "./storage.js";
export type { IndexedDBStorageOptions } from "./storage.js";
export { StaticAuthProvider } from "./auth.js";
