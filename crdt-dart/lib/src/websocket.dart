/// Multiplexed WebSocket transport. Port of crdt-js `transport/websocket.ts`
/// (lines 1-410), speaking every message type of Go `crdt/transport_ws.go`.
///
/// One socket carries pull, push, presence and the change stream, told apart
/// by `request_id`. Go `transport_ws.go` is authoritative where it differs
/// from crdt-js.
library;

import 'dart:async';
import 'dart:convert';

import 'package:web_socket_channel/web_socket_channel.dart';

import 'auth.dart';
import 'backoff.dart';
import 'errors.dart';
import 'hlc.dart';
import 'presence_types.dart';
import 'rejection.dart';
import 'sync_types.dart';
import 'transport.dart';
import 'types.dart';
import 'wait.dart';
import 'wire_helpers.dart';
import 'ws_connect_io.dart' if (dart.library.js_interop) 'ws_connect_web.dart';
import 'ws_types.dart';

/// An open WebSocket, reduced to what the transport needs.
///
/// [messages] is listened to once. It ends when the socket closes, and a
/// binary frame arrives decoded as UTF-8 text.
abstract interface class WsConnection {
  /// Text frames from the server, as they arrive.
  Stream<String> get messages;

  /// Sends one text frame. Throws when the socket is no longer open.
  void send(String text);

  /// Closes the socket.
  Future<void> close();
}

/// Opens a WebSocket to [url] and completes once it is open and ready to
/// send.
///
/// [headers] are the auth headers read for this attempt. A native connector
/// sends them in the handshake; a web connector cannot (a browser handshake
/// carries no custom headers), so it appends each as a lower-cased query
/// parameter instead. See [defaultWebSocketConnector].
typedef WebSocketConnector = Future<WsConnection> Function(
  Uri url,
  Map<String, String> headers,
  Iterable<String>? protocols,
);

/// The platform's WebSocket connector.
///
/// On native platforms it is `IOWebSocketChannel.connect` with [headers] in
/// the handshake and the connection awaited until it is ready. On the web it
/// is the browser `WebSocket`, with each header added to the URL as a
/// lower-cased query parameter (the crdt-js convention), because a browser
/// handshake cannot carry headers.
///
/// Warning: on the web the auth values travel in the URL. A proxy, a server
/// or a log may record a URL, query string included. Use short-lived tokens
/// there, never a long-lived secret. This package never prints a URL's query
/// values in an error message or `toString`.
WebSocketConnector defaultWebSocketConnector() => platformWebSocketConnector();

/// The URL a browser connects to: [url] with each of [headers] added as a
/// lower-cased query parameter, replacing a parameter of the same name.
///
/// Warning: the values end up in a URL, which a proxy, a server or a log may
/// record. Use short-lived tokens. See [defaultWebSocketConnector].
Uri webSocketUrl(Uri url, Map<String, String> headers) {
  if (headers.isEmpty) return url;
  return url.replace(
    queryParameters: {
      ...url.queryParameters,
      for (final e in headers.entries) e.key.toLowerCase(): e.value,
    },
  );
}

/// Wraps a [WebSocketChannel] as a [WsConnection]. A binary frame is decoded as
/// UTF-8. Not part of the public API.
WsConnection wrapChannel(WebSocketChannel channel) =>
    _ChannelConnection(channel);

final class _ChannelConnection implements WsConnection {
  _ChannelConnection(this._channel);

  final WebSocketChannel _channel;

  @override
  Stream<String> get messages => _channel.stream.map(
    (Object? m) =>
        m is String ? m : utf8.decode(m! as List<int>, allowMalformed: true),
  );

  @override
  void send(String text) => _channel.sink.add(text);

  @override
  Future<void> close() => _channel.sink.close();
}

/// The longest wait for a socket to finish closing.
const _closeTimeout = Duration(seconds: 5);

/// A request waiting for its reply, and the socket it went out on.
final class _Pending {
  _Pending(this.socket, this.completer, this.timer);

  final WsConnection socket;
  final Completer<Object?> completer;
  final Timer? timer;
}

/// WebSocket transport for pull, push, presence and the change stream.
///
/// [pull] and [push] send `pull_request` and `push_request` frames with a
/// fresh `request_id` (`r1`, `r2`, ...) and wait for the frame carrying the
/// same id. A correlated `error` frame rejects with a [TransportError] whose
/// `body` is the frame's payload, so `classifyPushError(0, e.body)` reads it.
/// A request with no reply within `requestTimeout` rejects with a
/// [NetworkError] and is forgotten, and a late reply is ignored.
///
/// [subscribe] returns a [CrdtSubscription]. Its `connect` sends
/// `{"type":"subscribe","payload":{"tables":[...]}}` once per opened socket
/// (the Go server then streams from HLC zero, one `change` frame per record).
/// No `unsubscribe` frame is ever sent: the Go server answers it with an
/// error, so `disconnect` only drops the subscription on this side.
///
/// Every `pingInterval` the transport sends `{"type":"ping","payload":null}`,
/// the Go client's keep-alive (crdt-js sends none). A `ping` from the server
/// is answered with `pong`; a `pong` is ignored.
///
/// [updatePresence] sends `presence_update` with no request id. The server's
/// reply is an uncorrelated `presence_event`, delivered to subscribers as a
/// [StreamPresence]. [getPresence] throws [UnsupportedError]: the Go server
/// answers `presence_get` with an `error` frame. Use the HTTP presence
/// endpoint.
///
/// The transport connects at construction (a failure is swallowed and the
/// next call retries) and, while a subscription is active, reconnects with
/// `backoff` after the socket drops or a connect fails, sending `subscribe`
/// again on each new socket. A subscription with no `tables` reconnects too
/// (crdt-js does not), but note that over the Go WebSocket an empty table list
/// streams nothing: the server waits for tables. Name the tables. One active
/// subscription at a time is supported, as in crdt-js: a second `connect` on
/// an already-subscribed socket does not send its tables.
///
/// A socket never outlives its credentials. Auth headers are read from `auth`
/// before every request ([pull], [push], [updatePresence]) and before
/// [CrdtSubscription.connect] sends, and again for every connection attempt
/// (so a reconnect resubscribes under the credentials of that moment):
///
///  * If the headers differ from the ones the open socket was opened with,
///    that socket is closed and a new one is opened with the new headers, and
///    the request goes out on the new socket. Requests in flight on the old
///    socket fail with a [NetworkError] and are never replayed.
///  * If `auth` throws a [CrdtError] with [CrdtErrorCode.cancelled] (the
///    account was switched away), the request fails with it unwrapped, the
///    open socket is closed, and the subscription ends: one [StreamError]
///    carries the cancellation and nothing reconnects or is scheduled.
///  * Any other error from `auth` fails that request with an [AuthError],
///    which is not retryable, and leaves the socket alone. In the reconnect
///    loop it is reported as a [StreamError] and retried with backoff like a
///    failed connection.
///
/// A request in flight when its socket closes fails with a [NetworkError].
/// Nothing is replayed on the next socket, so a request never reaches a
/// server under credentials other than the ones it was issued with.
///
/// `requestTimeout` also bounds the connect, so a handshake that never
/// finishes fails the request instead of hanging it.
///
/// A frame that does not decode is reported as a [StreamError] and skipped, and
/// a frame of an unknown type is ignored. Neither closes the socket. A reported
/// error never contains the frame text. An `error` frame that names a request
/// nobody waits for any more (it timed out) is dropped.
///
/// Warning: on the web the auth headers are sent as URL query parameters (a
/// browser handshake cannot carry headers), and a URL can be logged by a proxy
/// or a server. Use short-lived tokens. No error message, no `cause` of one
/// and no [toString] of this class shows a query value: every URL in an error
/// text is rebuilt with each query value replaced by `REDACTED`, and the auth
/// values are removed from the text whatever their case.
final class WebSocketTransport implements StreamTransport, PresenceTransport {
  /// Creates a transport for the endpoint [url] (`ws://` or `wss://`).
  ///
  /// [protocols] are the WebSocket subprotocols to offer. [requestTimeout]
  /// limits one request (zero disables it) and [pingInterval] sets the
  /// keep-alive (zero disables it). [backoff] makes the reconnect schedule.
  /// [auth] supplies handshake headers (query parameters on the web, see the
  /// class warning). [connect] replaces the platform connector, and [sleep]
  /// replaces the real wait between reconnects, both for tests.
  WebSocketTransport({
    required this.url,
    this.protocols,
    this.requestTimeout = const Duration(seconds: 30),
    this.pingInterval = const Duration(seconds: 30),
    Backoff Function()? backoff,
    this._auth,
    WebSocketConnector? connect,
    Future<void> Function(Duration)? sleep,
  }) : _backoff = (backoff ?? Backoff.new)(),
       _connector = connect ?? defaultWebSocketConnector(),
       _wait = CancellableWait(sleep) {
    // Connect eagerly rather than waiting for the first call, so the handshake
    // latency is paid once up front. A failure here is swallowed: every call
    // goes through _ensureOpen, which retries the connection lazily.
    unawaited(_ensureOpen().then<void>((_) {}, onError: (Object _) {}));
  }

  /// The `ws://` or `wss://` endpoint.
  final Uri url;

  /// The subprotocols offered in the handshake.
  final Iterable<String>? protocols;

  /// Longest wait for a reply to one request. Zero disables the limit.
  final Duration requestTimeout;

  /// How often a `ping` is sent. Zero disables it.
  final Duration pingInterval;

  final Backoff _backoff;
  final CrdtAuthProvider? _auth;
  final WebSocketConnector _connector;
  final CancellableWait _wait;

  final Map<String, _Pending> _pending = {};
  final Set<void Function(CrdtStreamEvent)> _handlers = {};
  final List<_WsSubscription> _active = [];
  var _nextId = 0;
  var _closed = false;
  var _reconnecting = false;
  WsConnection? _socket;
  Map<String, String>? _socketHeaders;
  StreamSubscription<String>? _socketSub;
  WsConnection? _announced;
  Completer<WsConnection>? _opening;
  Timer? _pingTimer;
  HLC? _lastHlc;

  // --- Transport ---

  @override
  Future<PullResponse> pull(PullRequest req) =>
      _rpc(WsMessageType.pullRequest, req.toJson(), PullResponse.fromJson);

  @override
  Future<PushResponse> push(PushRequest req) =>
      _rpc(WsMessageType.pushRequest, req.toJson(), PushResponse.fromJson);

  // --- PresenceTransport ---

  /// Fire and forget: the Go server answers `presence_update` with an empty
  /// `request_id`, so the reply is a broadcast, not a correlated response, and
  /// a server-side rejection cannot reach this call.
  @override
  Future<void> updatePresence(PresenceUpdate update) async {
    final socket = await _ready();
    final sent = _send(
      socket,
      WebSocketMessage(WsMessageType.presenceUpdate, payload: update.toJson()),
    );
    if (!sent) throw NetworkError('CRDT ws presence_update could not be sent');
  }

  /// Not supported over the socket: the Go server answers `presence_get` with
  /// an `error` frame ("unknown message type"). Use the HTTP presence
  /// endpoint instead.
  ///
  /// Throws [UnsupportedError] at once, not as a failed future.
  @override
  Future<List<PresenceState>> getPresence(
    String topic,
  ) => throw UnsupportedError(
    'presence snapshots are not available over the WebSocket transport; use '
    'the HTTP presence endpoint',
  );

  // --- StreamTransport ---

  @override
  CrdtSubscription subscribe(StreamConfig config) =>
      _WsSubscription(this, List.unmodifiable(config.tables));

  /// Closes the socket and rejects every in-flight request, including a caller
  /// still waiting for the socket to open (it is not in the pending map yet,
  /// because a request registers only after the connect settles). Without
  /// that, constructing and closing before the handshake finishes would hang
  /// the caller.
  Future<void> close() async {
    if (_closed) return;
    _closed = true;
    _wait.cancel();
    _pingTimer?.cancel();
    _pingTimer = null;
    final closing = TransportError('WebSocket transport closed');
    _failPending((p) => true, closing);
    final opening = _opening;
    if (opening != null && !opening.isCompleted) {
      _opening = null;
      opening.completeError(closing);
    }
    final socket = _socket;
    final wasAnnounced = _announced != null;
    _socket = null;
    _announced = null;
    final sub = _socketSub;
    _socketSub = null;
    if (sub != null) _release(sub);
    if (wasAnnounced && _active.isNotEmpty) _emit(const StreamDisconnected());
    for (final s in List.of(_active)) {
      s._connected = false;
    }
    _active.clear();
    if (socket != null) await _closeQuietly(socket);
  }

  /// The endpoint with its query values hidden.
  @override
  String toString() => 'WebSocketTransport(${_redactUrl(url)})';

  // --- Internals ---

  void _emit(CrdtStreamEvent event) {
    for (final handler in List.of(_handlers)) {
      try {
        handler(event);
      } on Object {
        // A handler's error must not stop the transport or other handlers.
      }
    }
  }

  static bool _isCancellation(Object error) =>
      error is CrdtError && error.code == CrdtErrorCode.cancelled;

  TransportError _closedError() => TransportError('WebSocket transport closed');

  static void _release(StreamSubscription<Object?> sub) {
    unawaited(sub.cancel().then<void>((_) {}, onError: (Object _) {}));
  }

  Future<void> _closeQuietly(WsConnection socket) async {
    try {
      await socket.close().timeout(_closeTimeout, onTimeout: () {});
    } on Object {
      // Nothing to do about a socket that fails to close.
    }
  }

  bool _send(WsConnection socket, WebSocketMessage frame) {
    try {
      socket.send(encodeWire(frame.toJson()));
      return true;
    } on Object {
      return false;
    }
  }

  void _failPending(bool Function(_Pending) which, Object error) {
    final ids = [
      for (final e in _pending.entries)
        if (which(e.value)) e.key,
    ];
    for (final id in ids) {
      final p = _pending.remove(id)!;
      p.timer?.cancel();
      if (!p.completer.isCompleted) p.completer.completeError(error);
    }
  }

  Future<T> _rpc<T>(
    WsMessageType type,
    Map<String, Object?> payload,
    T Function(Object? payload) decode,
  ) async {
    final socket = await _ready();
    // The socket may have dropped while this call was resuming.
    if (!identical(_socket, socket)) {
      throw NetworkError('CRDT ws connection closed');
    }
    final id = 'r${++_nextId}';
    final completer = Completer<Object?>();
    Timer? timer;
    if (requestTimeout > Duration.zero) {
      timer = Timer(requestTimeout, () {
        if (_pending.remove(id) == null) return;
        completer.completeError(
          NetworkError(
            'CRDT ws ${type.wire} timed out after '
            '${requestTimeout.inMilliseconds}ms',
            code: CrdtErrorCode.syncTimeout,
          ),
        );
      });
    }
    _pending[id] = _Pending(socket, completer, timer);
    if (!_send(
      socket,
      WebSocketMessage(type, payload: payload, requestId: id),
    )) {
      _pending.remove(id);
      timer?.cancel();
      throw NetworkError('CRDT ws ${type.wire} could not be sent');
    }
    final reply = await completer.future;
    try {
      return decode(reply);
    } on FormatException catch (e) {
      throw TransportError(
        'CRDT ws ${type.wire} returned an unreadable payload: ${e.message}',
        body: reply,
      );
    }
  }

  /// An open socket bound to the credentials the provider returns right now.
  ///
  /// Reads the auth headers first. A socket opened with other headers is
  /// closed (its in-flight requests fail, none is replayed) and a new one is
  /// opened with these. A cancellation closes the socket and ends the
  /// subscription before it is rethrown.
  Future<WsConnection> _ready() async {
    for (var attempt = 0; attempt < 3; attempt++) {
      if (_closed) throw _closedError();
      final Map<String, String> headers;
      try {
        headers = await _readAuth();
      } on Object catch (error) {
        if (_isCancellation(error)) _revoke(error);
        rethrow;
      }
      if (_closed) throw _closedError();
      final open = _socket;
      if (open != null) {
        if (_sameHeaders(headers, _socketHeaders)) return open;
        _dropSocket(
          open,
          NetworkError('CRDT ws connection closed: credentials changed'),
        );
      }
      // An opening already in flight may have read older headers: wait for it
      // and check again. Otherwise open with the headers just read.
      final opened = await _ensureOpen(_opening == null ? headers : null);
      if (identical(_socket, opened) && _sameHeaders(headers, _socketHeaders)) {
        return opened;
      }
    }
    throw NetworkError('CRDT ws credentials kept changing while connecting');
  }

  static bool _sameHeaders(Map<String, String> a, Map<String, String>? b) {
    if (b == null || a.length != b.length) return false;
    final lower = {for (final e in b.entries) e.key.toLowerCase(): e.value};
    for (final e in a.entries) {
      if (lower[e.key.toLowerCase()] != e.value) return false;
    }
    return true;
  }

  /// The open socket, opening one when there is none. Callers share one
  /// opening in flight. [headers], when given, are the credentials to open
  /// with; otherwise the attempt reads them itself.
  Future<WsConnection> _ensureOpen([Map<String, String>? headers]) async {
    if (_closed) throw _closedError();
    final open = _socket;
    if (open != null) return open;
    final opening = _opening ??= _startOpening(headers);
    final socket = await opening.future;
    // close() may have landed between the open completing and this
    // continuation running: do not hand a stale socket to a caller of a
    // transport that is already shut down.
    if (_closed) throw _closedError();
    return socket;
  }

  Completer<WsConnection> _startOpening(Map<String, String>? headers) {
    final completer = Completer<WsConnection>();
    unawaited(_attemptConnect(completer, headers));
    return completer;
  }

  void _openFailed(Completer<WsConnection> c, Object error, [StackTrace? st]) {
    if (identical(_opening, c)) _opening = null;
    if (c.isCompleted) return;
    c.completeError(error, st);
    if (_isCancellation(error)) _endForCancellation(error);
  }

  Future<void> _attemptConnect(
    Completer<WsConnection> c,
    Map<String, String>? given,
  ) async {
    // Read for every attempt: a cancellation thrown here ends the attempt and
    // nothing from an earlier one is reused.
    final Map<String, String> headers;
    try {
      headers = given ?? await _readAuth();
    } on Object catch (error, stack) {
      _openFailed(c, error, stack);
      return;
    }
    // close() may have run while credentials were pending: do not open a
    // socket for a transport that is already shut down.
    if (_closed) {
      _openFailed(c, _closedError());
      return;
    }
    final WsConnection socket;
    try {
      final connecting = _connector(url, headers, protocols);
      socket = requestTimeout > Duration.zero
          ? await connecting.timeout(
              requestTimeout,
              onTimeout: () {
                // A handshake that finishes late must not leave a socket open.
                unawaited(
                  connecting.then<void>(_closeQuietly, onError: (Object _) {}),
                );
                throw NetworkError(
                  'CRDT ws connect timed out after '
                  '${requestTimeout.inMilliseconds}ms',
                  code: CrdtErrorCode.syncTimeout,
                );
              },
            )
          : await connecting;
    } on Object catch (error, stack) {
      _openFailed(c, _connectError(error, headers), stack);
      return;
    }
    if (_closed || c.isCompleted) {
      unawaited(_closeQuietly(socket));
      if (!c.isCompleted) _openFailed(c, _closedError());
      return;
    }
    _adopt(socket, headers);
    if (identical(_opening, c)) _opening = null;
    c.complete(socket);
  }

  /// The credentials. A cancellation passes through as thrown, any other
  /// exception is wrapped in an [AuthError].
  Future<Map<String, String>> _readAuth() async {
    final auth = _auth;
    if (auth == null) return const {};
    try {
      return await auth.getHeaders();
    } on Exception catch (error, stack) {
      Error.throwWithStackTrace(
        _isCancellation(error)
            ? error
            : AuthError(
                'CRDT ws auth failed while reading credentials: $error',
                cause: error,
              ),
        stack,
      );
    }
  }

  /// A connect or socket failure as a [NetworkError] whose message and `cause`
  /// show no secret. Only a cancellation passes through as it is: any other
  /// error, whoever raised it, is reworded, because a connector's message may
  /// name the URL. The cause is a redacted copy, never the raw error.
  Object _connectError(Object error, Map<String, String> headers) {
    if (_isCancellation(error)) return error;
    final text = _redact(
      error is CrdtError ? error.message : '$error',
      headers,
    );
    return NetworkError(
      'CRDT ws connection failed: $text',
      cause: _RedactedCause(
        error is CrdtError ? 'CrdtError' : '${error.runtimeType}',
        text,
      ),
    );
  }

  /// [text] with every secret removed.
  ///
  /// First by parsing: each URL in the text is rebuilt with every query value
  /// replaced by `REDACTED` and its credentials dropped, so a URL the
  /// transport has never seen (a web connect URL carrying the auth values, an
  /// `http://` form of the endpoint) is covered. Then the known secrets, the
  /// endpoint's query values and the auth header values, are removed wherever
  /// they still appear, case-insensitively, raw and form-encoded, longest
  /// first so that a secret that contains another is removed whole. Secrets
  /// shorter than three characters are not removed from free text.
  String _redact(String text, Map<String, String> headers) {
    var out = text.replaceAllMapped(_urlPattern, (m) {
      final raw = m[0]!;
      final core = raw.replaceFirst(RegExp(r'[.,;:)\]}]+$'), '');
      final uri = Uri.tryParse(core);
      final redacted = uri == null ? '<url REDACTED>' : _redactUrl(uri);
      return '$redacted${raw.substring(core.length)}';
    });
    final secrets = <String>{
      for (final values in url.queryParametersAll.values) ...values,
      ...headers.values,
    }.where((s) => s.length >= 3);
    final forms = <String>{
      for (final s in secrets) ...{
        s,
        Uri.encodeQueryComponent(s),
        Uri.encodeComponent(s),
      },
    }.toList()..sort((a, b) => b.length.compareTo(a.length));
    for (final form in forms) {
      out = out.replaceAll(
        RegExp(RegExp.escape(form), caseSensitive: false),
        'REDACTED',
      );
    }
    return out;
  }

  /// The socket is open: listen to it, keep it alive and, if a subscription
  /// is waiting, subscribe on it.
  void _adopt(WsConnection socket, Map<String, String> headers) {
    _socket = socket;
    _socketHeaders = headers;
    _backoff.reset();
    _socketSub = socket.messages.listen(
      (raw) => _handleMessage(socket, raw),
      onError: (Object error) => _socketDropped(socket, error),
      onDone: () => _socketDropped(socket, null),
      cancelOnError: true,
    );
    _startPing(socket);
    _announce(socket);
  }

  /// Sends `subscribe` and emits `connected`, once for each socket however it
  /// got here (the socket opening, or `connect()` finding it already open).
  /// The Go server does not remember a subscription across sockets, so every
  /// new socket needs its own.
  void _announce(WsConnection socket) {
    if (_closed || _active.isEmpty) return;
    if (!identical(_socket, socket) || identical(_announced, socket)) return;
    _announced = socket;
    _send(
      socket,
      WebSocketMessage(
        WsMessageType.subscribe,
        payload: {'tables': _active.last.tables},
      ),
    );
    _emit(const StreamConnected());
  }

  void _startPing(WsConnection socket) {
    _pingTimer?.cancel();
    _pingTimer = null;
    if (pingInterval <= Duration.zero) return;
    _pingTimer = Timer.periodic(pingInterval, (_) {
      if (identical(_socket, socket)) {
        _send(socket, const WebSocketMessage(WsMessageType.ping));
      }
    });
  }

  /// Closes [socket] on purpose and forgets it. Requests in flight on it fail
  /// with [pendingError] and are never sent again: whoever asked decides
  /// whether to ask again, under whatever credentials are current by then.
  /// Does not schedule a reconnect. [streamError], when given, is reported to
  /// an active subscription.
  void _dropSocket(
    WsConnection socket,
    Object pendingError, {
    Object? streamError,
  }) {
    // A socket this transport already replaced or closed is not news.
    if (!identical(_socket, socket)) return;
    _socket = null;
    _socketHeaders = null;
    final sub = _socketSub;
    _socketSub = null;
    if (sub != null) _release(sub);
    _pingTimer?.cancel();
    _pingTimer = null;
    final wasAnnounced = identical(_announced, socket);
    _announced = null;
    unawaited(_closeQuietly(socket));
    _failPending((p) => identical(p.socket, socket), pendingError);
    if (_closed || _active.isEmpty) return;
    if (streamError != null) _emit(StreamError(streamError));
    if (wasAnnounced) _emit(const StreamDisconnected());
  }

  void _socketDropped(WsConnection socket, Object? error) {
    if (!identical(_socket, socket)) return;
    // Scrub with the headers this socket was opened with: on the web they are
    // in the URL it connected to.
    final headers = _socketHeaders ?? const <String, String>{};
    _dropSocket(
      socket,
      NetworkError('CRDT ws connection closed'),
      streamError: error == null ? null : _connectError(error, headers),
    );
    _scheduleReconnect();
  }

  /// The credentials' account was switched away while a socket was open: close
  /// it, end the subscription for good and report [error] once.
  void _revoke(Object error) {
    final open = _socket;
    if (open != null) _dropSocket(open, error);
    _endForCancellation(error);
  }

  void _scheduleReconnect() {
    if (_reconnecting || _closed || _active.isEmpty) return;
    unawaited(_reconnectLoop());
  }

  Future<void> _reconnectLoop() async {
    _reconnecting = true;
    try {
      while (!_closed && _active.isNotEmpty && _socket == null) {
        await _wait.wait(_backoff.next());
        if (_closed || _active.isEmpty || _socket != null) break;
        try {
          // The socket opening re-subscribes and emits `connected`.
          await _ensureOpen();
        } on Object catch (error) {
          if (_closed) break;
          // A cancellation was already reported and ended the subscription.
          if (!_isCancellation(error) && _active.isNotEmpty) {
            _emit(StreamError(error));
          }
        }
      }
    } finally {
      _reconnecting = false;
    }
  }

  /// The credentials' account was switched away: report it once and stop for
  /// good. Nothing reconnects until a caller subscribes again.
  void _endForCancellation(Object error) {
    if (_active.isEmpty) return;
    _wait.cancel();
    _emit(StreamError(error));
    for (final s in List.of(_active)) {
      s._connected = false;
    }
    _active.clear();
    _announced = null;
  }

  void _handleMessage(WsConnection socket, String raw) {
    final String typeText;
    final String requestId;
    final Object? payload;
    try {
      final decoded = jsonDecode(raw);
      if (decoded is! Map) {
        throw FormatException(
          'a frame must be a JSON object, got ${decoded.runtimeType}',
        );
      }
      final frame = wireObj(decoded);
      typeText = wireStr(frame, 'type');
      requestId = wireStr(frame, 'request_id');
      payload = frame['payload'];
    } on Object catch (error) {
      _reportMalformed('frame', error);
      return;
    }
    final type = WsMessageType.fromWire(typeText);

    // A correlated reply to an in-flight request.
    if (requestId.isNotEmpty) {
      final pending = _pending.remove(requestId);
      if (pending != null) {
        pending.timer?.cancel();
        if (pending.completer.isCompleted) return;
        if (type == WsMessageType.error) {
          pending.completer.completeError(_errorFrame(payload));
        } else {
          pending.completer.complete(payload);
        }
        return;
      }
    }

    // An error reply for a request that timed out (or was failed) is not a
    // stream error: nobody is waiting for it.
    if (requestId.isNotEmpty && type == WsMessageType.error) return;

    // Decode before emitting: a bad payload is reported, never thrown, and
    // never ends the socket.
    CrdtStreamEvent? event;
    try {
      event = switch (type) {
        WsMessageType.change => _changeEvent(payload),
        WsMessageType.changes => _changesEvent(payload),
        WsMessageType.presenceEvent => StreamPresence(
          PresenceEvent.fromJson(payload),
        ),
        WsMessageType.error => StreamError(_errorFrame(payload)),
        WsMessageType.ping => null,
        // pong, a reply nobody waits for any more, and an unknown type need
        // no action.
        _ => null,
      };
    } on Object catch (error) {
      _reportMalformed(typeText, error);
      return;
    }
    if (type == WsMessageType.ping) {
      if (identical(_socket, socket)) {
        _send(socket, const WebSocketMessage(WsMessageType.pong));
      }
      return;
    }
    if (event != null) _emit(event);
  }

  TransportError _errorFrame(Object? payload) => TransportError(
    'CRDT ws error: ${serverMessage(payload) ?? 'unknown error'}',
    body: payload,
  );

  StreamChange _changeEvent(Object? payload) {
    final change = ChangeRecord.fromJson(payload);
    _updateLastHlc(change.hlc);
    return StreamChange(change);
  }

  StreamChanges _changesEvent(Object? payload) {
    if (payload is! List) {
      throw FormatException(
        'expected a JSON array, got ${payload.runtimeType}',
      );
    }
    final changes = [for (final c in payload) ChangeRecord.fromJson(c)];
    for (final c in changes) {
      _updateLastHlc(c.hlc);
    }
    return StreamChanges(changes);
  }

  void _reportMalformed(String what, Object error) {
    // Not the frame text: it can carry data the log should not.
    final why = error is FormatException ? error.message : error.runtimeType;
    _emit(
      StreamError(
        SyncError(
          'Failed to parse ws $what: $why',
          code: CrdtErrorCode.validationFailed,
          phase: SyncPhase.stream,
        ),
      ),
    );
  }

  void _updateLastHlc(HLC hlc) {
    final last = _lastHlc;
    if (last == null || hlc.isAfter(last)) _lastHlc = hlc;
  }
}

/// Matches a URL in free text: a scheme, `://`, then anything up to a space or
/// a quote.
final _urlPattern = RegExp(r'''[A-Za-z][A-Za-z0-9+.\-]*://[^\s'"<>]+''');

/// [url] without credentials or fragment, rebuilt with every query value
/// replaced by `REDACTED`.
String _redactUrl(Uri url) {
  final port = url.hasPort ? ':${url.port}' : '';
  final keys = url.hasQuery && url.query.isNotEmpty
      ? url.queryParametersAll.keys
      : const <String>[];
  final query = keys.isEmpty
      ? ''
      : '?${[for (final k in keys) '${Uri.encodeQueryComponent(k)}=REDACTED'].join('&')}';
  return '${url.scheme}://${url.host}$port${url.path}$query';
}

/// A copy of an error that held a secret: its type and its redacted text.
/// Stored as the `cause` of a [NetworkError] in place of the raw error.
final class _RedactedCause {
  const _RedactedCause(this.type, this.text);

  final String type;
  final String text;

  @override
  String toString() => '$type: $text';
}

/// The subscription [WebSocketTransport.subscribe] returns.
final class _WsSubscription implements CrdtSubscription {
  _WsSubscription(this._transport, this.tables);

  final WebSocketTransport _transport;
  final List<String> tables;
  bool _connected = false;

  @override
  bool get connected => _connected && _transport._socket != null;

  @override
  HLC? get lastHlc => _transport._lastHlc;

  @override
  void Function() on(void Function(CrdtStreamEvent event) handler) {
    _transport._handlers.add(handler);
    return () => _transport._handlers.remove(handler);
  }

  @override
  void connect() {
    if (_connected) return;
    _connected = true;
    final t = _transport;
    if (t._closed) {
      _connected = false;
      return;
    }
    t._active.add(this);
    // The socket's opening is one owner of the subscribe frame and this is the
    // other (it will not open again if it is already open). _announce sends
    // at most once per socket, so the two never both fire.
    t._ready().then<void>(
      t._announce,
      onError: (Object error) {
        if (!t._active.contains(this)) return;
        // A cancellation was reported and ended the subscription already.
        if (!WebSocketTransport._isCancellation(error)) {
          t._emit(StreamError(error));
          t._scheduleReconnect();
        }
      },
    );
  }

  @override
  void disconnect() {
    if (!_connected) return;
    _connected = false;
    final t = _transport;
    final wasActive = t._active.remove(this);
    // No `unsubscribe` frame: the Go server does not handle that type and
    // would answer with an error frame, surfacing as a spurious stream error.
    // Dropping the subscription on this side is enough.
    if (wasActive && t._active.isEmpty) {
      final announced = t._announced != null;
      t._announced = null;
      t._wait.cancel();
      if (announced) t._emit(const StreamDisconnected());
    }
  }
}
