/// Error types for CRDT operations. Port of crdt-js `errors.ts`.
///
/// Every error carries a structured [CrdtErrorCode], a retryability hint and,
/// where the failure came from an HTTP response, the status code.
library;

import 'clock_skew.dart';

/// Structured error codes for CRDT operations.
enum CrdtErrorCode {
  /// The server could not be reached.
  networkUnreachable('NETWORK_UNREACHABLE'),

  /// A sync request did not finish in time.
  syncTimeout('SYNC_TIMEOUT'),

  /// Two changes could not be merged.
  mergeConflict('MERGE_CONFLICT'),

  /// Input or state failed validation.
  validationFailed('VALIDATION_FAILED'),

  /// The server refused the credentials.
  unauthorized('UNAUTHORIZED'),

  /// The server asked the client to slow down.
  rateLimited('RATE_LIMITED'),

  /// The pending queue is full and the store was told to throw.
  offlineQueueFull('OFFLINE_QUEUE_FULL'),

  /// A plugin rejected an operation or failed.
  pluginRejected('PLUGIN_REJECTED'),

  /// A storage read or write failed.
  storageError('STORAGE_ERROR'),

  /// The operation was cancelled on purpose, for example because the account
  /// it belonged to was switched away. Never retried. New in the Dart port;
  /// crdt-js has no equivalent.
  cancelled('CANCELLED'),

  /// The operation is not valid in the current state.
  invalidState('INVALID_STATE');

  const CrdtErrorCode(this.value);

  /// The crdt-js string for this code.
  final String value;
}

/// Error thrown by CRDT client operations.
base class CrdtError implements Exception {
  /// Creates an error. [code] defaults to [CrdtErrorCode.invalidState] and
  /// [retryable] to false.
  CrdtError(
    this.message, {
    this.statusCode,
    this.code = CrdtErrorCode.invalidState,
    this.retryable = false,
  });

  /// What went wrong.
  final String message;

  /// The HTTP status of the response this error came from, if any.
  final int? statusCode;

  /// Structured code for programmatic handling.
  final CrdtErrorCode code;

  /// Whether trying again could succeed.
  final bool retryable;

  /// The class name crdt-js reports as `error.name`. Stable under minified
  /// and compiled builds, unlike `runtimeType.toString()`.
  String get name => 'CrdtError';

  @override
  String toString() => '$name: $message';
}

/// Whether an HTTP status code represents a transient failure worth retrying.
///
/// 5xx, 429 and 408 are transient; other 4xx responses are the caller's
/// fault. No status at all means the request never reached the server (DNS, a
/// dropped connection), which is typically transient too.
///
/// This is the one place the rule lives: [TransportError] reads it for its
/// [CrdtError.retryable] flag.
///
/// Go parity: a plain 500 is transient by this rule, yet Go answers every
/// deterministic push failure (validation, hook rejection) with a 500, so
/// retrying one only repeats the rejection. The HTTP transport therefore
/// does not auto-retry a plain 500, and the sync engine classifies it with
/// `classifyPushError` instead.
bool isRetryableStatus(int? status) =>
    status == null || status >= 500 || status == 429 || status == 408;

/// The longest wait the transports honour from a `Retry-After` header. A
/// server that asks for more is treated as asking for this much.
const maxRetryAfter = Duration(seconds: 60);

/// Error thrown by transport operations.
///
/// A [CrdtError] subclass, so `on CrdtError` catches it too.
final class TransportError extends CrdtError {
  /// Creates an error for a failed request. [body] is the decoded response
  /// body (JSON, or text when it was not JSON), [serverTime] the response's
  /// `Date` header and [headers] the response headers (keys lower-case), when
  /// they were available.
  TransportError(
    super.message, {
    super.statusCode,
    this.body,
    this.serverTime,
    this.headers = const {},
  }) : super(
         code: CrdtErrorCode.networkUnreachable,
         retryable: isRetryableStatus(statusCode),
       );

  /// The decoded response body, if there was one. `classifyPushError` reads
  /// it.
  final Object? body;

  /// The server's clock, from the response's `Date` header.
  final DateTime? serverTime;

  /// The response headers, with lower-case keys. Empty when the failure has
  /// no response.
  final Map<String, String> headers;

  /// How long the server asked the client to wait, from the `Retry-After`
  /// header: a count of seconds or an HTTP date. Null when the header is
  /// absent or malformed. A date in the past reads as zero. The value is not
  /// capped; see [maxRetryAfter].
  ///
  /// An HTTP date is measured from the response's own `Date` header when it
  /// has one, so a device clock that is off does not skew the wait.
  Duration? get retryAfter {
    final raw = headers['retry-after']?.trim();
    if (raw == null || raw.isEmpty) return null;
    if (RegExp(r'^\d{1,9}$').hasMatch(raw)) {
      return Duration(seconds: int.parse(raw));
    }
    final at = parseHttpDate(raw);
    if (at == null) return null;
    final wait = at.difference(serverTime ?? DateTime.now().toUtc());
    return wait.isNegative ? Duration.zero : wait;
  }

  @override
  String get name => 'TransportError';
}

/// Error thrown for network-level failures: an unreachable server, DNS errors,
/// a timeout.
final class NetworkError extends CrdtError {
  /// Creates an error. A timeout passes `code: CrdtErrorCode.syncTimeout`.
  NetworkError(
    super.message, {
    super.statusCode,
    super.code = CrdtErrorCode.networkUnreachable,
    super.retryable = true,
    this.cause,
  });

  /// The underlying error, when there was one.
  final Object? cause;

  @override
  String get name => 'NetworkError';
}

/// Error thrown when input or state validation fails.
final class ValidationError extends CrdtError {
  /// Creates an error for [field], the field or path that failed.
  ValidationError(super.message, {this.field})
    : super(code: CrdtErrorCode.validationFailed);

  /// The field or path that failed validation, if it applies.
  final String? field;

  @override
  String get name => 'ValidationError';
}

/// The sync phase a [SyncError] happened in.
enum SyncPhase {
  /// Pulling remote changes.
  pull,

  /// Pushing local changes.
  push,

  /// Merging pulled changes.
  merge,

  /// Reading a change stream.
  stream,
}

/// Error thrown during sync operations: pull and push timeouts, merge
/// failures.
final class SyncError extends CrdtError {
  /// Creates an error. [code] defaults to [CrdtErrorCode.syncTimeout] and the
  /// error is retryable, as in crdt-js. [cause] is the error that stopped the
  /// phase.
  SyncError(
    super.message, {
    super.statusCode,
    super.code = CrdtErrorCode.syncTimeout,
    this.phase,
    this.cause,
  }) : super(retryable: true);

  /// The sync phase where the error occurred.
  final SyncPhase? phase;

  /// The error that caused this one, when there was one.
  final Object? cause;

  @override
  String get name => 'SyncError';
}

/// Error thrown when a plugin rejects an operation or fails.
final class PluginError extends CrdtError {
  /// Creates an error for the plugin called [pluginName].
  PluginError(super.message, this.pluginName)
    : super(code: CrdtErrorCode.pluginRejected);

  /// The name of the plugin that produced the error.
  final String pluginName;

  @override
  String get name => 'PluginError';
}
