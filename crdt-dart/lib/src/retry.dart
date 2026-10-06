/// Retry decorator for any [Transport]. Port of crdt-js `transport/retry.ts`.
///
/// Retry lives here rather than inside a transport so that a WebSocket
/// transport, or any transport a consumer plugs in, gets the same resilience.
library;

import 'backoff.dart';
import 'errors.dart';
import 'presence_types.dart';
import 'sync_types.dart';
import 'transport.dart';

/// Whether [t] can be asked for a live subscription. Unlike `t is
/// StreamTransport`, it sees through a [RetryingTransport], which implements
/// the interface for every inner transport but only supports it when the inner
/// one does.
bool isStreamTransport(Transport t) =>
    t is RetryingTransport ? t.supportsStream : t is StreamTransport;

/// Whether [t] supports presence. Sees through a [RetryingTransport] the same
/// way [isStreamTransport] does.
bool isPresenceTransport(Transport t) =>
    t is RetryingTransport ? t.supportsPresence : t is PresenceTransport;

/// The default retry policy: only a failure that says it is transient.
///
/// A [NetworkError] is retried when it is marked retryable, and a
/// [TransportError] when its status is (see `isRetryableStatus`). Nothing else
/// is: not a [CrdtError] with [CrdtErrorCode.cancelled], not any other
/// exception and not a Dart [Error].
///
/// Differs from crdt-js: its default retries any `Error`. Retrying an unknown
/// error would call the auth provider again after it threw a cancellation
/// (the account was switched away), so the Dart default is narrower. Pass
/// `isRetryable` to widen it.
///
/// Go parity: a 500 is not retried, though [TransportError] marks it
/// retryable. Go answers every deterministic push failure (validation, hook
/// rejection) with a 500, so retrying one only repeats the rejection.
bool defaultIsRetryable(Object error) => switch (error) {
  CrdtError(code: CrdtErrorCode.cancelled) => false,
  TransportError(statusCode: 500) => false,
  NetworkError(:final retryable) => retryable,
  TransportError(:final retryable) => retryable,
  _ => false,
};

/// How long to wait before the next attempt: the backoff step, or the server's
/// `Retry-After` on a 429 or 503 when that is longer, capped at
/// [maxRetryAfter].
Duration retryDelay(Duration backoffStep, Object error) {
  if (error is TransportError &&
      (error.statusCode == 429 || error.statusCode == 503)) {
    final asked = error.retryAfter;
    if (asked != null) {
      final capped = asked > maxRetryAfter ? maxRetryAfter : asked;
      return capped > backoffStep ? capped : backoffStep;
    }
  }
  return backoffStep;
}

Future<void> _defaultSleep(Duration d) => Future<void>.delayed(d);

Future<T> _runWithRetry<T>(
  Future<T> Function() op, {
  required int retries,
  required Backoff Function() backoff,
  required bool Function(Object error) isRetryable,
  required Future<void> Function(Duration) sleep,
}) async {
  final schedule = backoff();
  for (var attempt = 0; ; attempt++) {
    try {
      return await op();
    } on Object catch (error) {
      if (attempt >= retries || !isRetryable(error)) rethrow;
      await sleep(retryDelay(schedule.next(), error));
    }
  }
}

/// A transport whose pull, push and presence calls retry on failure.
///
/// Streaming is delegated untouched: a live subscription has its own
/// reconnect loop, and retrying `subscribe` would fight it.
///
/// crdt-js returns a proxy that forwards every other member of the wrapped
/// transport. Dart cannot proxy arbitrary members, so the wrapped transport is
/// reachable as [inner]. [PresenceTransport] and [StreamTransport] are
/// implemented for every inner transport, because a Dart class cannot gain an
/// interface at runtime; [supportsPresence] and [supportsStream] say whether
/// the inner transport has them, and a call the inner transport lacks throws
/// [UnsupportedError].
///
/// An `HttpTransport` already retries internally (its own `retries`, default
/// 2). Wrapping one here stacks the two schedules and multiplies the attempt
/// count, so set `retries: 0` on one of them unless you want that.
final class RetryingTransport
    implements Transport, PresenceTransport, StreamTransport {
  RetryingTransport._(
    this.inner, {
    required this._retries,
    required this._backoff,
    required this._isRetryable,
    required this._sleep,
  });

  /// The wrapped transport, for members outside [Transport].
  final Transport inner;

  final int _retries;
  final Backoff Function() _backoff;
  final bool Function(Object error) _isRetryable;
  final Future<void> Function(Duration) _sleep;

  /// Whether the inner transport supports presence.
  bool get supportsPresence => isPresenceTransport(inner);

  /// Whether the inner transport supports streaming.
  bool get supportsStream => isStreamTransport(inner);

  Future<T> _run<T>(Future<T> Function() op) => _runWithRetry(
    op,
    retries: _retries,
    backoff: _backoff,
    isRetryable: _isRetryable,
    sleep: _sleep,
  );

  @override
  Future<PullResponse> pull(PullRequest req) => _run(() => inner.pull(req));

  @override
  Future<PushResponse> push(PushRequest req) => _run(() => inner.push(req));

  PresenceTransport _presence() {
    final Object t = inner;
    if (supportsPresence && t is PresenceTransport) return t;
    throw UnsupportedError(
      'The wrapped ${t.runtimeType} does not support presence',
    );
  }

  @override
  Future<void> updatePresence(PresenceUpdate update) {
    final t = _presence();
    return _run(() => t.updatePresence(update));
  }

  @override
  Future<List<PresenceState>> getPresence(String topic) {
    final t = _presence();
    return _run(() => t.getPresence(topic));
  }

  @override
  CrdtSubscription subscribe(StreamConfig config) {
    final Object t = inner;
    if (supportsStream && t is StreamTransport) return t.subscribe(config);
    throw UnsupportedError(
      'The wrapped ${t.runtimeType} does not support streaming',
    );
  }
}

/// Wraps [inner] so its pull, push and presence calls retry on retryable
/// failures: up to [retries] more attempts, waiting a step of [backoff]
/// between them (a fresh schedule per call; default [Backoff]). [isRetryable]
/// replaces [defaultIsRetryable]. [sleep] replaces the real wait, for tests.
RetryingTransport withRetry(
  Transport inner, {
  int retries = 2,
  Backoff Function()? backoff,
  bool Function(Object error)? isRetryable,
  Future<void> Function(Duration)? sleep,
}) => RetryingTransport._(
  inner,
  retries: retries,
  backoff: backoff ?? Backoff.new,
  isRetryable: isRetryable ?? defaultIsRetryable,
  sleep: sleep ?? _defaultSleep,
);
