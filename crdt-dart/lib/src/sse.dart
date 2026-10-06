/// Server-Sent Events change stream. Port of crdt-js `stream.ts` (lines
/// 1-367).
///
/// Uses a streamed HTTP request rather than `EventSource` so a request can
/// carry headers (auth tokens). Reconnects with jittered, growing delays and
/// resumes from the newest change it has seen.
library;

import 'dart:async';
import 'dart:convert';

import 'package:http/http.dart' as http;

import 'auth.dart';
import 'backoff.dart';
import 'clock_skew.dart';
import 'errors.dart';
import 'hlc.dart';
import 'presence_types.dart';
import 'retry.dart';
import 'sse_io.dart' if (dart.library.js_interop) 'sse_web.dart';
import 'transport.dart';
import 'types.dart';
import 'wait.dart';

/// The answer to an SSE request: the status, the headers and the body bytes
/// as they arrive.
final class SseResponse {
  /// Creates a response. [headers] keys should be lower-case.
  const SseResponse(this.status, this.body, {this.headers = const {}});

  /// The HTTP status code.
  final int status;

  /// The body, as it arrives. It ends when the server closes the stream.
  final Stream<List<int>> body;

  /// The response headers, with lower-case keys.
  final Map<String, String> headers;
}

/// Opens one SSE request: a GET of [url] with [headers] whose connection ends
/// when [abort] completes.
typedef SseConnect = Future<SseResponse> Function(
  Uri url,
  Map<String, String> headers,
  Future<void> abort,
);

/// The platform's SSE connector.
///
/// On native platforms it is a streamed `package:http` GET through [client]
/// (a new client per connection when null). On the web it is `fetch` with an
/// `AbortController`, read through a `ReadableStreamDefaultReader`; [client]
/// is ignored there. `EventSource` is not used on either, because it cannot
/// set request headers.
SseConnect defaultSseConnect({http.Client? client}) =>
    platformSseConnect(client: client);

/// The most of an error response body kept for the error message.
const _maxErrorBody = 4096;

/// The longest wait for an error response body.
const _errorBodyTimeout = Duration(seconds: 10);

/// SSE change stream with auto-reconnect.
///
/// Connects to the sync server's `<baseUrl><streamPath>` endpoint, emits the
/// changes it receives and reconnects after a drop, resuming from the newest
/// change clock it has seen ([lastHlc]).
///
/// Auth headers are read from `auth` again for every connection attempt, never
/// reused. A [CrdtError] with [CrdtErrorCode.cancelled] from `auth` (the
/// account the provider belongs to was switched away) ends the stream: it
/// emits one [StreamError] carrying it and neither retries nor schedules
/// another connection. A later [connect] call starts again on purpose. Any
/// other error from `auth` is reported as a [StreamError] wrapping an
/// [AuthError] and backs off like a failed connection.
///
/// A frame the stream cannot decode is reported as a [StreamError] and
/// skipped; an event type it does not know is ignored. Neither ends the
/// stream. The reported error never contains the frame text.
///
/// Header names are lower-cased before the request is made, so a header from
/// `auth` replaces a static one whatever its case.
final class CrdtStream implements CrdtSubscription {
  /// Creates a stream for the server at [baseUrl], connecting to [streamPath]
  /// under it.
  ///
  /// [config] picks the tables, the resume point, the reconnect schedule and
  /// the idle timeout. [headers] go on every request; headers from [auth] win
  /// over them. [client] is used by the default connector; [connect]
  /// replaces the connector (tests). [onServerTime] is called with the `Date`
  /// of every response that has one. [random] feeds the reconnect jitter and
  /// [sleep] replaces the real wait between reconnects, for tests.
  CrdtStream({
    required Uri baseUrl,
    this.streamPath = '/stream',
    this.config = const StreamConfig(),
    Map<String, String> headers = const {},
    this._auth,
    http.Client? client,
    SseConnect? connect,
    this._onServerTime,
    double Function()? random,
    Future<void> Function(Duration)? sleep,
  }) : baseUrl = baseUrl.replace(
         path: baseUrl.path.replaceFirst(RegExp(r'/+$'), ''),
       ),
       _headers = _lowerKeys(headers),
       _connect = connect ?? defaultSseConnect(client: client),
       _wait = CancellableWait(sleep),
       _backoff = Backoff(
         initialDelay: config.reconnectDelay,
         maxDelay: config.maxReconnectDelay,
         random: random,
       );

  /// The server's base URL, without trailing slashes on its path.
  final Uri baseUrl;

  /// The SSE endpoint's path, under [baseUrl].
  final String streamPath;

  /// The subscription's configuration.
  final StreamConfig config;

  final Map<String, String> _headers;
  final CrdtAuthProvider? _auth;
  final SseConnect _connect;
  final void Function(DateTime serverTime)? _onServerTime;
  final CancellableWait _wait;
  final Backoff _backoff;

  final Set<void Function(CrdtStreamEvent)> _handlers = {};
  Completer<void>? _abort;
  bool _idleFired = false;
  bool _connected = false;
  HLC? _lastHlc;
  bool _shouldReconnect = false;
  Timer? _idleTimer;
  int _idleTimerGen = -1;

  // Identifies which _connectLoop/_connectOnce invocation is current. Bumped
  // by connect() and disconnect() so a loop still in flight (blocked on a read
  // that has not ended yet) can recognise itself as stale and stop touching
  // shared state, even when disconnect() is immediately followed by connect().
  int _generation = 0;

  @override
  bool get connected => _connected;

  @override
  HLC? get lastHlc => _lastHlc;

  @override
  void Function() on(void Function(CrdtStreamEvent event) handler) {
    _handlers.add(handler);
    return () => _handlers.remove(handler);
  }

  /// Starts the SSE connection. Does nothing while a loop is already running.
  @override
  void connect() {
    // Guard on _shouldReconnect, not _connected: _connected stays false until
    // the response arrives, so two quick calls would otherwise start two
    // loops and leak the first connection.
    if (_shouldReconnect) return;
    _shouldReconnect = true;
    final gen = ++_generation;
    unawaited(_connectLoop(gen));
  }

  /// Disconnects and stops reconnecting.
  @override
  void disconnect() {
    _clearIdleTimer();
    _shouldReconnect = false;
    // Bump the generation so any loop still in flight sees it is stale on its
    // next check and stops touching _abort, _connected or the handlers, even
    // if a connect() right after this disconnect() starts a new loop before
    // the old one notices it was aborted.
    _generation++;
    final abort = _abort;
    _abort = null;
    if (abort != null && !abort.isCompleted) abort.complete();
    _wait.cancel();
    if (_connected) {
      _connected = false;
      _emit(const StreamDisconnected());
    }
  }

  /// The endpoint URL with its query parameters.
  ///
  /// The query follows Go's `buildStreamURL`: `tables` (comma-joined), then
  /// `since_ts`, `since_count` and `since_node` when the resume point is not
  /// zero. `node_id` comes last, when the configuration has one. It is an
  /// addition of this port: the grove extension reads it to clean up presence
  /// when the stream drops, and Go's own client never sends it.
  Uri buildStreamUrl() {
    final since = _lastHlc ?? config.since;
    final query = <String, String>{
      if (config.tables.isNotEmpty) 'tables': config.tables.join(','),
      if (since != null && !since.isZero) ...{
        'since_ts': since.ts.toString(),
        'since_count': since.c.toString(),
        'since_node': since.node,
      },
      if (config.nodeId.isNotEmpty) 'node_id': config.nodeId,
    };
    final slash = streamPath.startsWith('/') ? streamPath : '/$streamPath';
    return baseUrl.replace(
      path: '${baseUrl.path}$slash',
      queryParameters: query.isEmpty ? null : query,
    );
  }

  static Map<String, String> _lowerKeys(Map<String, String> headers) => {
    for (final e in headers.entries) e.key.toLowerCase(): e.value,
  };

  /// Aborts the connection if nothing arrives for `idleTimeout`.
  ///
  /// A TCP connection can die without the read ever finishing, which would
  /// leave the reader parked forever with no reconnect. The server's SSE
  /// keep-alive comments are enough to keep this armed.
  ///
  /// Scoped to [gen]: a stale generation's timer never aborts a newer
  /// generation's connection, and a newer generation's armed timer is never
  /// wiped out by a stale generation's cleanup.
  void _armIdleTimer(int gen) {
    if (config.idleTimeout <= Duration.zero) return;
    _clearIdleTimerFor(gen);
    _idleTimerGen = gen;
    _idleTimer = Timer(config.idleTimeout, () {
      if (_idleTimerGen == gen) {
        _idleTimer = null;
        _idleTimerGen = -1;
      }
      if (gen == _generation) {
        _idleFired = true;
        final abort = _abort;
        if (abort != null && !abort.isCompleted) abort.complete();
      }
    });
  }

  /// Clears the idle timer whichever generation armed it.
  void _clearIdleTimer() {
    _idleTimer?.cancel();
    _idleTimer = null;
    _idleTimerGen = -1;
  }

  /// Clears the idle timer only if [gen] is the generation that armed it.
  void _clearIdleTimerFor(int gen) {
    if (_idleTimer != null && _idleTimerGen == gen) _clearIdleTimer();
  }

  void _emit(CrdtStreamEvent event) {
    for (final handler in List.of(_handlers)) {
      try {
        handler(event);
      } on Object {
        // A handler's error must not stop the stream or the other handlers.
      }
    }
  }

  static bool _isCancellation(Object error) =>
      error is CrdtError && error.code == CrdtErrorCode.cancelled;

  Future<void> _connectLoop(int gen) async {
    while (gen == _generation) {
      Object? failure;
      try {
        await _connectOnce(gen);
      } on Object catch (error) {
        failure = error;
      }

      // A newer generation may have taken over while _connectOnce was
      // running (disconnect() immediately followed by connect()). This loop is
      // stale: it must not touch _connected or emit events, which belong to
      // the current generation now.
      if (gen != _generation) break;

      if (failure != null && _isCancellation(failure)) {
        _endForCancellation(failure);
        return;
      }
      if (failure != null) _emit(StreamError(failure));

      if (_connected) {
        _connected = false;
        _emit(const StreamDisconnected());
      }

      if (gen != _generation) break;

      // Wait before reconnecting, with jittered exponential backoff, and at
      // least as long as a throttling server asked.
      final step = _backoff.next();
      await _wait.wait(failure == null ? step : retryDelay(step, failure));
    }
  }

  /// The credentials' account was switched away: report it once and stop for
  /// good. Nothing reconnects until a caller asks with [connect].
  void _endForCancellation(Object error) {
    _clearIdleTimer();
    _shouldReconnect = false;
    _generation++;
    _emit(StreamError(error));
    if (_connected) {
      _connected = false;
      _emit(const StreamDisconnected());
    }
  }

  /// Reads the auth headers afresh. A cancellation passes through as thrown;
  /// any other exception is wrapped in an [AuthError].
  Future<Map<String, String>> _readAuth() async {
    final auth = _auth;
    if (auth == null) return const {};
    try {
      return _lowerKeys(await auth.getHeaders());
    } on Exception catch (error, stack) {
      Error.throwWithStackTrace(
        _isCancellation(error)
            ? error
            : AuthError(
                'CRDT stream auth failed while reading credentials: $error',
                cause: error,
              ),
        stack,
      );
    }
  }

  Future<void> _connectOnce(int gen) async {
    // Last line of defence: _connectLoop checks this before calling in, but a
    // stale generation must never be able to assign _abort.
    if (gen != _generation) return;

    final abort = Completer<void>();
    _abort = abort;
    _idleFired = false;
    try {
      // Read for every attempt: a cancellation thrown here ends the stream,
      // and nothing from an earlier attempt is reused.
      final authHeaders = await _readAuth();
      // A disconnect (or a newer generation) may have landed while the
      // credentials were pending. Never open a connection for it.
      if (gen != _generation) return;

      final response = await _connect(buildStreamUrl(), {
        'accept': 'text/event-stream',
        'cache-control': 'no-cache',
        ..._headers,
        ...authHeaders,
      }, abort.future);

      // A newer generation may have taken over while the request was in
      // flight. Do not touch _connected or emit on behalf of a stale one.
      if (gen != _generation) return;

      final serverTime = _observeDate(response.headers);
      final status = response.status;
      if (status < 200 || status >= 300) {
        final text = await _readErrorBody(response.body);
        throw TransportError(
          'CRDT stream returned $status: $text',
          statusCode: status,
          body: text.isEmpty ? null : text,
          serverTime: serverTime,
          headers: _lowerKeys(response.headers),
        );
      }

      _connected = true;
      _backoff.reset();
      _armIdleTimer(gen);
      _emit(const StreamConnected());

      await _read(gen, response.body, abort);
      if (gen == _generation && _idleFired) {
        throw NetworkError(
          'CRDT stream idle for ${config.idleTimeout.inMilliseconds}ms',
          code: CrdtErrorCode.syncTimeout,
        );
      }
    } finally {
      if (!abort.isCompleted) abort.complete();
      if (identical(_abort, abort)) _abort = null;
    }
  }

  /// Reads [body] until it ends, fails or [abort] completes.
  Future<void> _read(
    int gen,
    Stream<List<int>> body,
    Completer<void> abort,
  ) async {
    final done = Completer<void>();
    void finish([Object? error, StackTrace? stack]) {
      if (done.isCompleted) return;
      if (error == null) {
        done.complete();
      } else {
        done.completeError(error, stack);
      }
    }

    final parser = _SseParser(_processEvent);
    final sub = body
        .transform(const Utf8Decoder(allowMalformed: true))
        .listen(
          (chunk) {
            // A newer generation may have taken over while this read was
            // pending. Stop without touching shared state.
            if (gen != _generation) return finish();
            _armIdleTimer(gen);
            try {
              parser.add(chunk);
            } on Object catch (error, stack) {
              finish(error, stack);
            }
          },
          onError: finish,
          onDone: finish,
          cancelOnError: true,
        );
    unawaited(abort.future.then((_) => finish()));
    try {
      await done.future;
    } finally {
      _clearIdleTimerFor(gen);
      _release(sub);
    }
  }

  /// Cancels [sub] without waiting for it. A subscription that has nothing to
  /// clean up answers `cancel()` with a future created outside the current
  /// zone, and awaiting it would leave a `fake_async` zone; nothing here needs
  /// the answer anyway.
  static void _release(StreamSubscription<Object?> sub) {
    unawaited(sub.cancel().then<void>((_) {}, onError: (Object _) {}));
  }

  DateTime? _observeDate(Map<String, String> headers) {
    String? raw;
    for (final e in headers.entries) {
      if (e.key.toLowerCase() == 'date') raw = e.value;
    }
    final time = raw == null ? null : parseHttpDate(raw);
    if (time != null) {
      try {
        _onServerTime?.call(time);
      } on Object {
        // A callback's error must not fail the connection.
      }
    }
    return time;
  }

  /// The start of an error response body, for the error message.
  Future<String> _readErrorBody(Stream<List<int>> body) async {
    final bytes = <int>[];
    final done = Completer<void>();
    void finish() {
      if (!done.isCompleted) done.complete();
    }

    final sub = body.listen(
      (chunk) {
        bytes.addAll(chunk);
        if (bytes.length >= _maxErrorBody) finish();
      },
      onError: (Object _) => finish(),
      onDone: finish,
      cancelOnError: true,
    );
    try {
      await done.future.timeout(_errorBodyTimeout, onTimeout: finish);
    } finally {
      _release(sub);
    }
    final kept = bytes.length > _maxErrorBody
        ? bytes.sublist(0, _maxErrorBody)
        : bytes;
    return utf8.decode(kept, allowMalformed: true);
  }

  /// Handles one complete event. Never throws: a frame that does not decode is
  /// reported as a [StreamError] and skipped, an unknown type is ignored.
  void _processEvent(String type, String data) {
    CrdtStreamEvent event;
    try {
      switch (type) {
        case 'change':
          final change = ChangeRecord.fromJson(jsonDecode(data));
          _updateLastHlc(change.hlc);
          event = StreamChange(change);
        case 'changes':
          final decoded = jsonDecode(data);
          final changes = [
            for (final c in decoded is List ? decoded : _notAList(decoded))
              ChangeRecord.fromJson(c),
          ];
          for (final c in changes) {
            _updateLastHlc(c.hlc);
          }
          event = StreamChanges(changes);
        case 'presence':
          event = StreamPresence(PresenceEvent.fromJson(jsonDecode(data)));
        case 'error':
          // The grove extension sends `error` when it cannot start the stream
          // (crdt-js drops it silently).
          event = StreamError(TransportError(data));
        default:
          return;
      }
    } on Object catch (error) {
      // Not the frame text: it can carry data the log should not.
      final why = error is FormatException ? error.message : error.runtimeType;
      _emit(
        StreamError(
          SyncError(
            'Failed to parse SSE $type event: $why',
            code: CrdtErrorCode.validationFailed,
            phase: SyncPhase.stream,
            cause: error,
          ),
        ),
      );
      return;
    }
    _emit(event);
  }

  static Never _notAList(Object? decoded) => throw FormatException(
    'expected a JSON array, got ${decoded.runtimeType}',
  );

  void _updateLastHlc(HLC hlc) {
    final last = _lastHlc;
    if (last == null || hlc.isAfter(last)) _lastHlc = hlc;
  }
}

/// Splits a text stream into SSE events. Keeps a trailing partial line until
/// the rest of it arrives, exactly as the TS loop does.
final class _SseParser {
  _SseParser(this._onEvent);

  final void Function(String type, String data) _onEvent;
  String _buffer = '';
  String _type = '';
  final List<String> _data = [];

  void add(String chunk) {
    _buffer += chunk;
    final lines = _buffer.split('\n');
    // The last piece is an incomplete line: keep it for the next chunk.
    _buffer = lines.removeLast();
    for (var line in lines) {
      // Go parity: bufio.Scanner (ScanLines) drops the \r of a CRLF ending.
      if (line.endsWith('\r')) line = line.substring(0, line.length - 1);
      _line(line);
    }
  }

  void _line(String line) {
    // An empty line ends the event.
    if (line.isEmpty) {
      if (_data.isNotEmpty) _onEvent(_type, _data.join('\n'));
      _type = '';
      _data.clear();
      return;
    }
    // A comment line (keep-alive).
    if (line.startsWith(':')) return;
    if (line.startsWith('event:')) {
      _type = line.substring(6).trim();
    } else if (line.startsWith('data:')) {
      _data.add(line.substring(5).trim());
    }
  }
}
