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
import 'sync_types.dart';
import 'transport.dart';

/// The statuses worth another attempt: request timeout, throttling and the
/// gateway failures.
///
/// A plain 500 is not here, and neither is any other 4xx. Go answers every
/// deterministic push failure (validation, hook rejection) with a 500, so
/// retrying one only repeats the rejection; the sync engine classifies it with
/// `classifyPushError` instead.
const _retryStatuses = {408, 429, 502, 503, 504};

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
/// the auth provider or from `onUnauthorized` is never retried and is not
/// turned into a 401 refresh: it reaches the caller unchanged, so a provider
/// that cancels requests (the account it belongs to was switched away) stops
/// the request for good.
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
      final authHeaders = await _auth?.getHeaders() ?? const {};
      final outcome = await _attempt(method, path, url, {
        'accept': 'application/json',
        if (body != null) 'content-type': 'application/json',
        ..._headers,
        ...authHeaders,
      }, body);
      if (outcome is NetworkError) {
        if (attempt >= retries) throw outcome;
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
        if (await refresh()) continue;
      }
      if (!_retryStatuses.contains(status) || attempt >= retries) throw error;
      attempt++;
      await _sleep(retryDelay(backoff.next(), error));
    }
  }

  /// One attempt: the response, or the [NetworkError] that stopped it.
  ///
  /// The error is returned, not thrown, so the retry loop has no `try`:
  /// dart2js (the web and node builds) miscompiles an async loop that mixes a
  /// `try`/`catch` around an await, a `continue` and a variable declared
  /// before the loop, and the compiled code then reads an undeclared name.
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

Future<void> _defaultSleep(Duration d) => Future<void>.delayed(d);
