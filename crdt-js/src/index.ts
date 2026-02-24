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
  mergeFieldState,
} from "./merge.js";

// HTTP client
export { CRDTClient, CRDTError } from "./client.js";

// SSE streaming
export type { CRDTStreamEvent, StreamEventHandler } from "./stream.js";
export { CRDTStream } from "./stream.js";

// State store
export { CRDTStore } from "./store.js";
