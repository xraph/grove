/// Built-in HTTP transport for pull, push and presence. Port of crdt-js
/// `transport.ts` (`HttpTransport`).
library;

import 'dart:async';
import 'dart:convert';

import 'package:http/http.dart' as http;
import 'package:meta/meta.dart';

import 'auth.dart';
import 'backoff.dart';
import 'clock_skew.dart';
import 'envelope.dart';
import 'errors.dart';
import 'presence_types.dart';
import 'retry.dart';
import 'sse.dart';
import 'sync_types.dart';
import 'transport.dart';

/// HTTP transport for pull, push and presence.
///
/// Sends POST requests to `pullPath`, `pushPath` and `presencePath` under
/// [baseUrl], with bodies framed by an envelope (Grove's own by default).
///
/// Retries a network error, a timeout and a 408, 429, 502, 503 or 504 up to
/// `retries` times, waiting a step of `backoff` between attempts, and at
/// least the server's `Retry-After` (capped at [maxRetryAfter]) on a 429 or
/// 503. A 401 calls `onUnauthorized` once; when it returns true the request
/// goes out once more with fresh auth headers.
///
/// Auth headers are read again for every attempt, never cached. An error from
/// the auth provider or from `onUnauthorized` is never retried, by this
/// transport or by `withRetry`, and is not turned into a 401 refresh. A
/// [CrdtError] with [CrdtErrorCode.cancelled] (the account the provider
/// belongs to was switched away) reaches the caller as thrown. Any other
/// exception is wrapped in an [AuthError] carrying it as `cause`, so a
/// `NetworkError` from an identity provider is not mistaken for a transport
/// failure and retried. A Dart `Error` passes through untouched.
///
/// A failed response throws [TransportError] carrying the status, the decoded
/// body, the response headers and the server's `Date`; no response at all
/// throws [NetworkError]. Whenever a response has a `Date` header that parses,
/// `onServerTime` is called with it, successful or not.
base class HttpTransport implements Transport, PresenceTransport {
  /// Creates a transport for the server at [baseUrl]. Trailing slashes on its
  /// path are dropped.
  ///
  /// [client] defaults to a new `http.Client`, which [close] shuts. [headers]
  /// go on every request; headers from [auth] win over them. [timeout] limits
  /// one attempt, response body included (zero disables it). [backoff] makes
  /// a fresh schedule per request. [sleep] replaces the real wait between
  /// attempts, for tests.
  HttpTransport({
    required Uri baseUrl,
    http.Client? client,
    Map<String, String> headers = const {},
    this._auth,
    this.timeout = const Duration(seconds: 30),
    this.retries = 2,
    Backoff Function()? backoff,
    this.envelope = groveEnvelope,
    this.pullPath = '/pull',
    this.pushPath = '/push',
    this.presencePath = '/presence',
    this._onServerTime,
    this._onUnauthorized,
    Future<void> Function(Duration)? sleep,
  }) : baseUrl = baseUrl.replace(
         path: baseUrl.path.replaceFirst(RegExp(r'/+$'), ''),
       ),
       _client = client ?? http.Client(),
       _ownsClient = client == null,
       _headers = Map.of(headers),
       _backoff = backoff ?? Backoff.new,
       _sleep = sleep ?? _defaultSleep;

  /// The server's base URL, without trailing slashes on its path.
  final Uri baseUrl;

  /// Longest one attempt may take. Zero disables the limit.
  final Duration timeout;

  /// Retries after the first attempt.
  final int retries;

  /// How bodies are framed.
  final SyncEnvelope envelope;

  /// The pull endpoint's path, under [baseUrl].
  final String pullPath;

  /// The push endpoint's path, under [baseUrl].
  final String pushPath;

  /// The presence endpoint's path, under [baseUrl].
  final String presencePath;

  final http.Client _client;
  final bool _ownsClient;
  final Map<String, String> _headers;
  final CrdtAuthProvider? _auth;
  final Backoff Function() _backoff;
  final void Function(DateTime serverTime)? _onServerTime;
  final Future<bool> Function()? _onUnauthorized;
  final Future<void> Function(Duration) _sleep;

  /// The underlying client, for a subclass that opens its own connections.
  @protected
  http.Client get client => _client;

  /// The static headers, for a subclass that opens its own connections.
  @protected
  Map<String, String> get headers => _headers;

  /// The auth provider, for a subclass that opens its own connections.
  @protected
  CrdtAuthProvider? get auth => _auth;

  /// The server-time callback, for a subclass that opens its own connections.
  @protected
  void Function(DateTime serverTime)? get onServerTime => _onServerTime;

  /// Closes the HTTP client if this transport created it.
  void close() {
    if (_ownsClient) _client.close();
  }

  @override
  Future<PullResponse> pull(PullRequest req) => _request(
    'POST',
    pullPath,
    body: envelope.encodePull(req),
    decode: envelope.decodePull,
  );

  @override
  Future<PushResponse> push(PushRequest req) => _request(
    'POST',
    pushPath,
    body: envelope.encodePush(req),
    decode: envelope.decodePush,
  );

  @override
  Future<void> updatePresence(PresenceUpdate update) => _request<void>(
    'POST',
    presencePath,
    body: envelope.encodePresenceUpdate(update),
    allowEmpty: true,
    decode: (_) {},
  );

  @override
  Future<List<PresenceState>> getPresence(String topic) => _request(
    'GET',
    presencePath,
    query: {'topic': topic},
    allowEmpty: true,
    // An empty answer reads as nobody present.
    decode: (j) =>
        j == null ? <PresenceState>[] : PresenceSnapshot.fromJson(j).states,
  );

  Uri _url(String path, Map<String, String>? query) {
    final slash = path.startsWith('/') ? path : '/$path';
    return baseUrl.replace(
      path: '${baseUrl.path}$slash',
      queryParameters: query,
    );
  }

  Future<T> _request<T>(
    String method,
    String path, {
    String? body,
    Map<String, String>? query,
    bool allowEmpty = false,
    required T Function(Object? json) decode,
  }) async {
    final url = _url(path, query);
    final backoff = _backoff();
    var attempt = 0;
    var refreshed = false;
    while (true) {
      // Read for every attempt: a cancellation thrown here ends the request,
      // and nothing from an earlier attempt is reused. Not inside `_attempt`,
      // so it is never mistaken for a network error.
      final authHeaders = await _readAuth();
      final outcome = await _attempt(method, path, url, {
        'accept': 'application/json',
        if (body != null) 'content-type': 'application/json',
        ..._headers,
        ...authHeaders,
      }, body);
      if (outcome is NetworkError) {
        if (!outcome.retryable || attempt >= retries) throw outcome;
        attempt++;
        await _sleep(retryDelay(backoff.next(), outcome));
        continue;
      }
      final response = outcome as http.Response;
      final serverTime = _observeDate(response);
      final text = utf8.decode(response.bodyBytes, allowMalformed: true);
      final status = response.statusCode;
      if (status >= 200 && status < 300) {
        return _decodeSuccess(
          path,
          response,
          text,
          serverTime,
          allowEmpty,
          decode,
        );
      }
      final error = TransportError(
        'CRDT $path returned $status: $text',
        statusCode: status,
        body: _decodeBody(text),
        serverTime: serverTime,
        headers: response.headers,
      );
      final refresh = _onUnauthorized;
      if (status == 401 && !refreshed && refresh != null) {
        refreshed = true;
        // Not inside `_attempt`: an error thrown here propagates, never
        // retried, whatever its type.
        if (await _refresh(refresh)) continue;
      }
      if (!isTransientStatus(status) || attempt >= retries) throw error;
      attempt++;
      await _sleep(retryDelay(backoff.next(), error));
    }
  }

  Future<Map<String, String>> _readAuth() async {
    try {
      return await _auth?.getHeaders() ?? const {};
    } on Exception catch (error, stack) {
      Error.throwWithStackTrace(
        _asAuthError(error, 'reading credentials'),
        stack,
      );
    }
  }

  Future<bool> _refresh(Future<bool> Function() refresh) async {
    try {
      return await refresh();
    } on Exception catch (error, stack) {
      Error.throwWithStackTrace(
        _asAuthError(error, 'refreshing credentials'),
        stack,
      );
    }
  }

  /// A cancellation passes through as thrown. Any other exception from the
  /// credentials or the refresh callback is wrapped in an [AuthError], which
  /// no retry layer retries (a `NetworkError` from an identity provider must
  /// not look like a transport failure to `withRetry`).
  static Exception _asAuthError(Exception error, String doing) =>
      error is CrdtError && error.code == CrdtErrorCode.cancelled
      ? error
      : AuthError('CRDT auth failed while $doing: $error', cause: error);

  /// One attempt: the response, or the [NetworkError] that stopped it.
  ///
  /// The error is returned, not thrown, so the retry loop has no `try`. An
  /// earlier shape of the loop (a `try`/`catch` around the await, a
  /// `continue`, and state declared before the loop) failed the node (dart2js)
  /// test run with `ReferenceError: refreshed0 is not defined`. This shape
  /// passes on the VM and on node. A reduced copy of the earlier shape did not
  /// reproduce the failure, so its cause is unconfirmed.
  Future<Object> _attempt(
    String method,
    String path,
    Uri url,
    Map<String, String> headers,
    String? body,
  ) async {
    try {
      return await _roundTrip(method, path, url, headers, body);
    } on NetworkError catch (error) {
      if (error.code == CrdtErrorCode.cancelled) rethrow;
      return error;
    }
  }

  /// Sends one attempt and reads its whole body, within [timeout].
  Future<http.Response> _roundTrip(
    String method,
    String path,
    Uri url,
    Map<String, String> headers,
    String? body,
  ) async {
    final limited = timeout > Duration.zero;
    final abort = Completer<void>();
    final request = http.AbortableRequest(
      method,
      url,
      abortTrigger: limited ? abort.future : null,
    )..headers.addAll(headers);
    if (body != null) request.bodyBytes = utf8.encode(body);
    Future<http.Response> attempt() async =>
        http.Response.fromStream(await _client.send(request));
    try {
      if (!limited) return await attempt();
      return await attempt().timeout(
        timeout,
        onTimeout: () {
          // Abort the real request, so a slow server does not keep a socket
          // busy after the caller has moved on.
          if (!abort.isCompleted) abort.complete();
          throw NetworkError(
            'CRDT $path timed out after ${timeout.inMilliseconds}ms',
            code: CrdtErrorCode.syncTimeout,
          );
        },
      );
    } on NetworkError {
      rethrow;
    } on Exception catch (e) {
      // A cancellation from the client ends the request; it is not a network
      // failure to retry.
      if (e is CrdtError && e.code == CrdtErrorCode.cancelled) rethrow;
      throw NetworkError('CRDT $path failed: $e', cause: e);
    }
  }

  DateTime? _observeDate(http.Response response) {
    final raw = response.headers['date'];
    final time = raw == null ? null : parseHttpDate(raw);
    if (time != null) _onServerTime?.call(time);
    return time;
  }

  T _decodeSuccess<T>(
    String path,
    http.Response response,
    String text,
    DateTime? serverTime,
    bool allowEmpty,
    T Function(Object? json) decode,
  ) {
    try {
      final empty = response.statusCode == 204 || text.trim().isEmpty;
      if (empty && !allowEmpty) {
        throw const FormatException('the response body is empty');
      }
      return decode(empty ? null : jsonDecode(text));
    } on FormatException catch (e) {
      throw TransportError(
        'CRDT $path returned an unreadable body: ${e.message}',
        statusCode: response.statusCode,
        body: text,
        serverTime: serverTime,
        headers: response.headers,
      );
    }
  }

  /// The error body as JSON when it parses, as text when it does not, and
  /// null when it is empty.
  static Object? _decodeBody(String text) {
    if (text.trim().isEmpty) return null;
    try {
      return jsonDecode(text);
    } on FormatException {
      return text;
    }
  }
}

/// HTTP pull and push plus the SSE change stream.
final class HttpStreamTransport extends HttpTransport
    implements StreamTransport {
  /// Creates the transport. See [HttpTransport] for the shared parameters.
  HttpStreamTransport({
    required super.baseUrl,
    super.client,
    super.headers,
    super.auth,
    super.timeout,
    super.retries,
    super.backoff,
    super.envelope,
    super.pullPath,
    super.pushPath,
    super.presencePath,
    super.onServerTime,
    super.onUnauthorized,
    super.sleep,
    this.streamPath = '/stream',
    this.sseConnect,
  });

  /// Path of the SSE endpoint, appended to [baseUrl].
  final String streamPath;

  /// Overrides how the SSE request is made (tests).
  final SseConnect? sseConnect;

  /// Opens a change stream with this transport's headers, auth and clock
  /// callback. Auth is read again on every connection attempt of the stream.
  @override
  CrdtSubscription subscribe(StreamConfig config) => CrdtStream(
    baseUrl: baseUrl,
    streamPath: streamPath,
    config: config,
    headers: headers,
    auth: auth,
    client: client,
    connect: sseConnect,
    onServerTime: onServerTime,
  );
}

Future<void> _defaultSleep(Duration d) => Future<void>.delayed(d);
