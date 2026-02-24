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
  /** Base URL of the CRDT sync server (e.g., "https://api.example.com/sync"). */
  baseURL: string;
  /** Unique node ID for this client. */
  nodeID: string;
  /** Tables to sync. */
  tables?: string[];
  /** Custom fetch implementation (default: globalThis.fetch). */
  fetch?: typeof fetch;
  /** Custom headers to include in all requests. */
  headers?: Record<string, string>;
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
