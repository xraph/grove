/**
 * Error classes for CRDT operations.
 *
 * Provides structured error types with error codes, retryability hints,
 * and specialized subclasses for common failure scenarios.
 */

/** Structured error codes for CRDT operations. */
export enum CRDTErrorCode {
  NetworkUnreachable = "NETWORK_UNREACHABLE",
  SyncTimeout = "SYNC_TIMEOUT",
  MergeConflict = "MERGE_CONFLICT",
  ValidationFailed = "VALIDATION_FAILED",
  Unauthorized = "UNAUTHORIZED",
  RateLimited = "RATE_LIMITED",
  OfflineQueueFull = "OFFLINE_QUEUE_FULL",
  PluginRejected = "PLUGIN_REJECTED",
  StorageError = "STORAGE_ERROR",
  InvalidState = "INVALID_STATE",
}

/** Error thrown by CRDT client operations. */
export class CRDTError extends Error {
  /** Structured error code for programmatic handling. */
  code: CRDTErrorCode;

  /** Whether this error is retryable. */
  retryable: boolean;

  /**
   * Create a new CRDTError.
   *
   * Supports two call signatures for backward compatibility:
   * - `new CRDTError(message, statusCode?)` — legacy form
   * - `new CRDTError(message, statusCode?, code?, retryable?)` — extended form
   */
  constructor(
    message: string,
    public readonly statusCode?: number,
    code?: CRDTErrorCode,
    retryable?: boolean
  ) {
    super(message);
    this.name = "CRDTError";
    this.code = code ?? CRDTErrorCode.InvalidState;
    this.retryable = retryable ?? false;
  }
}

/**
 * Error thrown by transport operations.
 * Extends CRDTError for backward compatibility — existing
 * `catch (e) { if (e instanceof CRDTError) }` patterns still work.
 */
export class TransportError extends CRDTError {
  constructor(message: string, statusCode?: number) {
    super(message, statusCode, CRDTErrorCode.NetworkUnreachable, true);
    this.name = "TransportError";
  }
}

/**
 * Error thrown for network-level failures (unreachable server, DNS errors, etc.).
 */
export class NetworkError extends CRDTError {
  constructor(message: string, statusCode?: number) {
    super(message, statusCode, CRDTErrorCode.NetworkUnreachable, true);
    this.name = "NetworkError";
  }
}

/**
 * Error thrown when input or state validation fails.
 */
export class ValidationError extends CRDTError {
  /** The field or path that failed validation (if applicable). */
  readonly field?: string;

  constructor(message: string, field?: string) {
    super(message, undefined, CRDTErrorCode.ValidationFailed, false);
    this.name = "ValidationError";
    this.field = field;
  }
}

/**
 * Error thrown during sync operations (pull/push timeouts, merge failures, etc.).
 */
export class SyncError extends CRDTError {
  /** The sync phase where the error occurred. */
  readonly phase?: "pull" | "push" | "merge" | "stream";

  constructor(
    message: string,
    statusCode?: number,
    code?: CRDTErrorCode,
    phase?: "pull" | "push" | "merge" | "stream"
  ) {
    super(message, statusCode, code ?? CRDTErrorCode.SyncTimeout, true);
    this.name = "SyncError";
    this.phase = phase;
  }
}

/**
 * Error thrown when a plugin rejects an operation or encounters an error.
 */
export class PluginError extends CRDTError {
  /** The name of the plugin that produced the error. */
  readonly pluginName: string;

  constructor(message: string, pluginName: string) {
    super(message, undefined, CRDTErrorCode.PluginRejected, false);
    this.name = "PluginError";
    this.pluginName = pluginName;
  }
}
