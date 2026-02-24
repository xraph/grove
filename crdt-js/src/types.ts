/**
 * Wire format types for Grove CRDT sync protocol.
 *
 * These types mirror the Go structs in grove/crdt/crdt.go and
 * grove/crdt/transport.go. Field names match the Go JSON tags exactly.
 */

/** Hybrid Logical Clock value. Mirrors Go crdt.HLC. */
export interface HLC {
  /** Physical timestamp in nanoseconds since Unix epoch. */
  ts: number;
  /** Logical counter (incremented when physical clock hasn't advanced). */
  c: number;
  /** Node identifier that produced this clock value. */
  node: string;
}

/** CRDT type identifier. */
export type CRDTType = "lww" | "counter" | "set";

/** Set operation type. */
export type SetOpType = "add" | "remove";

/** Counter delta for a single node's change. Mirrors Go crdt.CounterDelta. */
export interface CounterDelta {
  inc: number;
  dec: number;
}

/** Set operation payload. Mirrors Go crdt.SetOperation. */
export interface SetOperation {
  op: SetOpType;
  elements: unknown[];
}

/**
 * A single field-level change record for sync transport.
 * Mirrors Go crdt.ChangeRecord.
 */
export interface ChangeRecord {
  table: string;
  pk: string;
  field: string;
  crdt_type: CRDTType;
  hlc: HLC;
  node_id: string;
  value?: unknown;
  tombstone?: boolean;
  counter_delta?: CounterDelta;
  set_op?: SetOperation;
}

/** Pull request payload. Mirrors Go crdt.PullRequest. */
export interface PullRequest {
  tables: string[];
  since?: HLC;
  node_id: string;
}

/** Pull response payload. Mirrors Go crdt.PullResponse. */
export interface PullResponse {
  changes: ChangeRecord[];
  latest_hlc: HLC;
}

/** Push request payload. Mirrors Go crdt.PushRequest. */
export interface PushRequest {
  changes: ChangeRecord[];
  node_id: string;
}

/** Push response payload. Mirrors Go crdt.PushResponse. */
export interface PushResponse {
  merged: number;
  latest_hlc: HLC;
}

/** Sync status for UI display. */
export type SyncStatus = "connected" | "disconnected" | "syncing" | "error";

/** Configuration for the CRDT client. */
export interface CRDTClientConfig {
  /** Base URL of the CRDT sync server. Required when no custom transport is provided. */
  baseURL?: string;
  /** Unique node ID for this client. */
  nodeID: string;
  /** Tables to sync. */
  tables?: string[];
  /** Custom fetch implementation (default: globalThis.fetch). Only used by built-in HttpTransport. */
  fetch?: typeof fetch;
  /** Custom headers to include in all requests. */
  headers?: Record<string, string>;
  /** Custom transport for pull/push operations. When provided, baseURL is not required. */
  transport?: Transport;
  /** Custom stream transport for real-time subscriptions. */
  streamTransport?: StreamTransport;
  /** Auth provider for dynamic header injection (e.g., JWT refresh). */
  auth?: AuthProvider;
  /** Storage adapter for persistence (passed through to CRDTStore). */
  storage?: StorageAdapter;
  /** Presence configuration. When provided, enables presence features. */
  presence?: PresenceConfig;
}

/** Configuration for SSE streaming. */
export interface StreamConfig {
  /** Tables to subscribe to. */
  tables?: string[];
  /** Reconnect delay in milliseconds (default: 5000). */
  reconnectDelay?: number;
  /** Starting HLC for the stream. */
  since?: HLC;
}

/** Per-node counter state for PN-Counter. Mirrors Go crdt.PNCounterState. */
export interface PNCounterState {
  /** nodeID → total increments from that node. */
  inc: Record<string, number>;
  /** nodeID → total decrements from that node. */
  dec: Record<string, number>;
}

/** OR-Set tag. Mirrors Go crdt.Tag. */
export interface ORSetTag {
  node: string;
  hlc: HLC;
}

/** OR-Set state. Mirrors Go crdt.ORSetState. */
export interface ORSetState {
  /** Element JSON string → array of tags that added it. */
  entries: Record<string, ORSetTag[]>;
  /** Tag key → removed flag. */
  removed: Record<string, boolean>;
}

/** Field-level CRDT state. Mirrors Go crdt.FieldState. */
export interface FieldState {
  type: CRDTType;
  hlc: HLC;
  node_id: string;
  value?: unknown;
  counter_state?: PNCounterState;
  set_state?: ORSetState;
}

/** Full document state (all fields for a single record). Mirrors Go crdt.State. */
export interface DocumentState {
  table: string;
  pk: string;
  fields: Record<string, FieldState>;
  tombstone: boolean;
  tombstone_hlc?: HLC;
}

/** Sync report summary. Mirrors Go crdt.SyncReport. */
export interface SyncReport {
  pulled: number;
  pushed: number;
  merged: number;
  conflicts: number;
}

// --- Presence Types ---

/** A single client's presence data for a topic. Mirrors Go crdt.PresenceState. */
export interface PresenceState<T = Record<string, unknown>> {
  node_id: string;
  topic: string;
  data: T;
  updated_at: number;
}

/** Request body for updating presence. Mirrors Go crdt.PresenceUpdate. */
export interface PresenceUpdate<T = Record<string, unknown>> {
  node_id: string;
  topic: string;
  data: T;
}

/** Presence event received via SSE. Mirrors Go crdt.PresenceEvent. */
export interface PresenceEvent<T = Record<string, unknown>> {
  type: "join" | "update" | "leave";
  node_id: string;
  topic: string;
  data?: T;
}

/** Presence snapshot response. Mirrors Go crdt.PresenceSnapshot. */
export interface PresenceSnapshot<T = Record<string, unknown>> {
  topic: string;
  states: PresenceState<T>[];
}

/** Configuration for presence behavior. */
export interface PresenceConfig {
  /** Heartbeat interval in ms (default: 10000). */
  heartbeatInterval?: number;
}

// --- Plugin Interfaces ---

/** Stream event handler callback type (re-exported from stream.ts for interface use). */
export type StreamEventHandler = (event: StreamEvent) => void;

/** Stream event types (mirrors CRDTStreamEvent in stream.ts). */
export type StreamEvent =
  | { type: "change"; data: ChangeRecord }
  | { type: "changes"; data: ChangeRecord[] }
  | { type: "presence"; data: PresenceEvent }
  | { type: "error"; error: Error }
  | { type: "connected" }
  | { type: "disconnected" };

/**
 * Transport interface for CRDT sync operations.
 *
 * Implement this to use custom transports (WebSocket, gRPC, etc.)
 * instead of the built-in HTTP transport.
 */
export interface Transport {
  pull(req: PullRequest): Promise<PullResponse>;
  push(req: PushRequest): Promise<PushResponse>;
  /** Update presence for a topic. Optional — only needed when presence is used. */
  updatePresence?(update: PresenceUpdate): Promise<void>;
  /** Get current presence snapshot for a topic. Optional. */
  getPresence?(topic: string): Promise<PresenceState[]>;
}

/**
 * Subscription handle returned by StreamTransport.subscribe().
 *
 * CRDTStream already satisfies this interface, so existing code is compatible.
 */
export interface StreamSubscription {
  on(handler: StreamEventHandler): () => void;
  connect(): void;
  disconnect(): void;
  readonly connected: boolean;
  readonly lastHLC: HLC | null;
}

/**
 * Transport that also supports real-time streaming subscriptions.
 *
 * Extends Transport with a subscribe() method for SSE, WebSocket, or
 * other push-based change propagation.
 */
export interface StreamTransport extends Transport {
  subscribe(config: StreamConfig): StreamSubscription;
}

/**
 * Persistence adapter for CRDTStore state.
 *
 * All methods are async to support IndexedDB, localStorage, remote storage, etc.
 * The built-in MemoryStorage is a no-op implementation.
 */
export interface StorageAdapter {
  loadState(): Promise<Map<string, Map<string, DocumentState>>>;
  saveDocument(table: string, pk: string, doc: DocumentState): Promise<void>;
  deleteDocument(table: string, pk: string): Promise<void>;
  loadPendingChanges(): Promise<ChangeRecord[]>;
  savePendingChanges(changes: ChangeRecord[]): Promise<void>;
}

/**
 * Auth provider for dynamic header injection.
 *
 * Called before every request. Supports both sync and async implementations
 * (e.g., static API keys vs. JWT token refresh).
 */
export interface AuthProvider {
  getHeaders(): Record<string, string> | Promise<Record<string, string>>;
}
