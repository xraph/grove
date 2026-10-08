// Port of crdt-js src/__tests__/stream.test.ts, case for case, with an
// injected `SseConnect` fake in place of `fetch`. A fake's body is a stream
// the test feeds with encoded chunks. Reconnect waits go through an injected
// `sleep` or run under `fake_async`; nothing waits on a real clock.
import 'dart:async';
import 'dart:convert';

import 'package:fake_async/fake_async.dart';
import 'package:grove_crdt/grove_crdt.dart';
import 'package:test/test.dart';

/// What a fake `SseConnect` saw, per call.
final class _Calls {
  final urls = <Uri>[];
  final headers = <Map<String, String>>[];
  final aborted = <bool>[];

  int get count => urls.length;

  void record(Uri url, Map<String, String> h, Future<void> abort) {
    final i = urls.length;
    urls.add(url);
    headers.add(h);
    aborted.add(false);
    unawaited(abort.then((_) => aborted[i] = true));
  }
}

/// A connector that answers with [chunks] and then ends the body, like a
/// server that closes the stream.
SseConnect _chunks(
  List<String> chunks, {
  int status = 200,
  _Calls? calls,
  Map<String, String> headers = const {},
}) => (url, h, abort) async {
  calls?.record(url, h, abort);
  return SseResponse(
    status,
    Stream.fromIterable([for (final c in chunks) utf8.encode(c)]),
    headers: headers,
  );
};

/// A connector whose body never ends on its own: `new ReadableStream({
/// start() {} })`.
SseConnect _hanging({_Calls? calls}) => (url, h, abort) async {
  calls?.record(url, h, abort);
  return SseResponse(200, StreamController<List<int>>().stream);
};

/// Like the TS `createAbortAwareStream`: the body errors as soon as the
/// connection's abort future fires, as a real `fetch()` body does.
SseConnect _abortAware({_Calls? calls}) => (url, h, abort) async {
  calls?.record(url, h, abort);
  final body = StreamController<List<int>>();
  unawaited(
    abort.then((_) {
      if (!body.isClosed) {
        body.addError(StateError('aborted'));
        unawaited(body.close());
      }
    }),
  );
  return SseResponse(200, body.stream);
};

/// A sleep that never finishes, so a reconnect never fires on its own.
Future<void> _never(Duration d) => Completer<void>().future;

Map<String, Object?> _sampleChange([int ts = 100]) => {
  'table': 'users',
  'pk': '1',
  'field': 'name',
  'crdt_type': 'lww',
  'hlc': {'ts': '$ts', 'c': 0, 'node': 'server'},
  'node_id': 'server',
  'value': 'Alice',
};

String _sseEvent(String type, Object? data) =>
    'event:$type\ndata:${jsonEncode(data)}\n\n';

final Uri _base = Uri.parse('https://api.example.com');

CrdtStream _stream(
  SseConnect connect, {
  StreamConfig config = const StreamConfig(
    reconnectDelay: Duration(milliseconds: 50),
  ),
  Map<String, String> headers = const {},
  CrdtAuthProvider? auth,
  Future<void> Function(Duration)? sleep,
}) => CrdtStream(
  baseUrl: _base,
  config: config,
  headers: headers,
  auth: auth,
  connect: connect,
  sleep: sleep ?? _never,
);

List<CrdtStreamEvent> _collect(CrdtStream s) {
  final events = <CrdtStreamEvent>[];
  s.on(events.add);
  return events;
}

Iterable<T> _of<T extends CrdtStreamEvent>(List<CrdtStreamEvent> e) =>
    e.whereType<T>();

final class _FnAuth implements CrdtAuthProvider {
  _FnAuth(this.fn);
  final FutureOr<Map<String, String>> Function(int call) fn;
  int calls = 0;

  @override
  FutureOr<Map<String, String>> getHeaders() => fn(++calls);
}

CrdtError _cancelled() =>
    CrdtError('cancelled', code: CrdtErrorCode.cancelled, retryable: true);

void main() {
  group('CRDTStream', () {
    group('constructor and initial state', () {
      test('initializes as disconnected', () {
        final stream = CrdtStream(baseUrl: _base);
        expect(stream.connected, isFalse);
      });

      test('initializes lastHLC as null', () {
        final stream = CrdtStream(baseUrl: _base);
        expect(stream.lastHlc, isNull);
      });
    });

    group('on()', () {
      test('registers an event handler and returns unsubscribe', () {
        final stream = _stream(_chunks([_sseEvent('change', _sampleChange())]));
        final events = <CrdtStreamEvent>[];
        final unsub = stream.on(events.add);
        expect(unsub, isA<void Function()>());
      });

      test('supports multiple handlers', () async {
        final stream = _stream(_chunks([_sseEvent('change', _sampleChange())]));
        final events1 = <CrdtStreamEvent>[];
        final events2 = <CrdtStreamEvent>[];
        stream.on(events1.add);
        stream.on(events2.add);
        stream.connect();
        await pumpEventQueue();
        stream.disconnect();

        // Both should have received the "connected" event
        expect(events1.any((e) => e is StreamConnected), isTrue);
        expect(events2.any((e) => e is StreamConnected), isTrue);
      });
    });

    group('connect()', () {
      test('initiates SSE connection via fetch GET', () async {
        final calls = _Calls();
        final stream = _stream(_chunks([': keepalive\n\n'], calls: calls));

        stream.connect();
        await pumpEventQueue();
        stream.disconnect();

        expect(calls.count, greaterThan(0));
        // The connector makes a GET; the stream asks for event-stream.
        expect(calls.headers.first['accept'], 'text/event-stream');
        expect(calls.headers.first['cache-control'], 'no-cache');
      });

      test("emits 'connected' event on successful connection", () async {
        final stream = _stream(_chunks([': keepalive\n\n']));
        final events = _collect(stream);
        stream.connect();
        await pumpEventQueue();
        stream.disconnect();

        expect(events.any((e) => e is StreamConnected), isTrue);
      });

      test('sets connected to true', () async {
        final stream = _stream(_hanging());
        _collect(stream);
        stream.connect();
        await pumpEventQueue();

        expect(stream.connected, isTrue);
        stream.disconnect();
      });
    });

    group('disconnect()', () {
      test("emits 'disconnected' event when connected", () async {
        final stream = _stream(_hanging());
        final events = _collect(stream);
        stream.connect();
        await pumpEventQueue();
        stream.disconnect();

        expect(events.any((e) => e is StreamDisconnected), isTrue);
      });

      test('sets connected to false', () async {
        final stream = _stream(_hanging());
        stream.connect();
        await pumpEventQueue();
        stream.disconnect();
        expect(stream.connected, isFalse);
      });

      test('is safe to call when not connected', () {
        final stream = CrdtStream(baseUrl: _base);
        expect(stream.disconnect, returnsNormally);
      });
    });

    group('SSE parsing', () {
      test("parses single 'change' event", () async {
        final stream = _stream(
          _chunks([_sseEvent('change', _sampleChange(100))]),
        );
        final events = _collect(stream);
        stream.connect();
        await pumpEventQueue();
        stream.disconnect();

        final changeEvents = _of<StreamChange>(events).toList();
        expect(changeEvents, hasLength(1));
        expect(changeEvents.single.change.value!.value, 'Alice');
      });

      test("parses 'changes' event with array", () async {
        final stream = _stream(
          _chunks([
            _sseEvent('changes', [_sampleChange(100), _sampleChange(200)]),
          ]),
        );
        final events = _collect(stream);
        stream.connect();
        await pumpEventQueue();
        stream.disconnect();

        final changesEvents = _of<StreamChanges>(events).toList();
        expect(changesEvents, hasLength(1));
        expect(changesEvents.single.changes, hasLength(2));
      });

      test('ignores SSE comments (lines starting with :)', () async {
        final stream = _stream(_chunks([': keepalive\n\n']));
        final events = _collect(stream);
        stream.connect();
        await pumpEventQueue();
        stream.disconnect();

        // Should only have connected + disconnected, no change/changes/error
        final dataEvents = events.where(
          (e) => e is StreamChange || e is StreamChanges,
        );
        expect(dataEvents, isEmpty);
      });

      test('handles partial chunks correctly', () async {
        final full = _sseEvent('change', _sampleChange(300));
        // Split in the middle
        final mid = full.length ~/ 2;
        final stream = _stream(
          _chunks([full.substring(0, mid), full.substring(mid)]),
        );
        final events = _collect(stream);
        stream.connect();
        await pumpEventQueue();
        stream.disconnect();

        expect(_of<StreamChange>(events), hasLength(1));
      });

      test('emits error event on malformed JSON', () async {
        final stream = _stream(
          _chunks(['event:change\ndata:not-valid-json\n\n']),
        );
        final events = _collect(stream);
        stream.connect();
        await pumpEventQueue();
        stream.disconnect();

        expect(_of<StreamError>(events), isNotEmpty);
      });

      test('ignores unknown event types', () async {
        final stream = _stream(
          _chunks(['event:unknown\ndata:{"foo":"bar"}\n\n']),
        );
        final events = _collect(stream);
        stream.connect();
        await pumpEventQueue();
        stream.disconnect();

        final dataEvents = events.where(
          (e) => e is StreamChange || e is StreamChanges,
        );
        expect(dataEvents, isEmpty);
      });
    });

    group('lastHLC tracking', () {
      test('updates lastHLC from single change event', () async {
        final stream = _stream(
          _chunks([_sseEvent('change', _sampleChange(500))]),
        );
        stream.on((_) {});
        stream.connect();
        await pumpEventQueue();
        stream.disconnect();

        expect(stream.lastHlc, isNotNull);
        expect(stream.lastHlc!.ts, BigInt.from(500));
      });

      test('updates lastHLC to highest from changes event', () async {
        final stream = _stream(
          _chunks([
            _sseEvent('changes', [
              _sampleChange(100),
              _sampleChange(500),
              _sampleChange(300),
            ]),
          ]),
        );
        stream.on((_) {});
        stream.connect();
        await pumpEventQueue();
        stream.disconnect();

        expect(stream.lastHlc!.ts, BigInt.from(500));
      });

      test('does not regress lastHLC', () async {
        final stream = _stream(
          _chunks([
            _sseEvent('change', _sampleChange(500)),
            _sseEvent('change', _sampleChange(100)),
          ]),
        );
        stream.on((_) {});
        stream.connect();
        await pumpEventQueue();
        stream.disconnect();

        expect(stream.lastHlc!.ts, BigInt.from(500));
      });
    });

    group('buildStreamURL', () {
      Future<Uri> firstUrl(StreamConfig config) async {
        final calls = _Calls();
        final stream = _stream(
          _chunks([': keep\n\n'], calls: calls),
          config: config,
        );
        stream.connect();
        await pumpEventQueue();
        stream.disconnect();
        return calls.urls.first;
      }

      const delay = Duration(milliseconds: 50);

      test('appends /stream to baseURL', () async {
        final url = await firstUrl(const StreamConfig(reconnectDelay: delay));
        expect(url.toString(), contains('/stream'));
      });

      test('includes tables as comma-separated query param', () async {
        final url = await firstUrl(
          const StreamConfig(tables: ['users', 'posts'], reconnectDelay: delay),
        );
        expect(url.toString(), contains('tables=users%2Cposts'));
      });

      test('includes since params from initial config.since', () async {
        final url = await firstUrl(
          StreamConfig(
            since: HLC(BigInt.from(100), 5, 'n1'),
            reconnectDelay: delay,
          ),
        );
        expect(url.toString(), contains('since_ts=100'));
        expect(url.toString(), contains('since_count=5'));
        expect(url.toString(), contains('since_node=n1'));
      });

      test('omits since params when HLC is zero', () async {
        final url = await firstUrl(const StreamConfig(reconnectDelay: delay));
        expect(url.toString(), isNot(contains('since_ts')));
      });

      test('omits query string when no params needed', () async {
        final url = await firstUrl(const StreamConfig(reconnectDelay: delay));
        expect(url.toString(), 'https://api.example.com/stream');
      });
    });

    group('reconnection', () {
      test('emits error on non-ok response status', () async {
        final stream = _stream(_chunks(const [], status: 500));
        final events = _collect(stream);
        stream.connect();
        await pumpEventQueue();
        stream.disconnect();

        expect(_of<StreamError>(events), isNotEmpty);
      });

      test('handler errors do not crash the stream', () async {
        final stream = _stream(
          _chunks([_sseEvent('change', _sampleChange(100))]),
        );

        // Register handler that throws
        stream.on((_) => throw Exception('handler error'));

        // Register a second handler to verify it still gets called
        final events = _collect(stream);

        stream.connect();
        await pumpEventQueue();
        stream.disconnect();

        // Second handler should still have received events despite first one
        // throwing
        expect(events, isNotEmpty);
      });

      test('includes custom headers in fetch request', () async {
        final calls = _Calls();
        final stream = _stream(
          _chunks([': keep\n\n'], calls: calls),
          headers: {'Authorization': 'Bearer token'},
        );

        stream.connect();
        await pumpEventQueue();
        stream.disconnect();

        // Header names are lower-cased: HTTP header names are case-insensitive
        // and a later auth header must replace an earlier static one.
        expect(calls.headers.first['authorization'], 'Bearer token');
      });
    });

    group('connect re-entrancy', () {
      test('a second connect() does not start a second loop', () async {
        final calls = _Calls();
        final stream = CrdtStream(
          baseUrl: Uri.parse('http://x'),
          connect: _hanging(calls: calls),
          sleep: _never,
        );
        stream.connect();
        stream.connect();
        await pumpEventQueue();
        expect(calls.count, 1);
        stream.disconnect();
      });

      test('disconnect then connect starts a fresh loop', () async {
        final calls = _Calls();
        final stream = CrdtStream(
          baseUrl: Uri.parse('http://x'),
          connect: _hanging(calls: calls),
          sleep: _never,
        );
        stream.connect();
        await pumpEventQueue();
        stream.disconnect();
        stream.connect();
        await pumpEventQueue();
        expect(calls.count, 2);
        stream.disconnect();
      });

      test("disconnect() immediately followed by connect() does not orphan the stale loop's reconnect", () {
        fakeAsync((async) {
          final calls = _Calls();
          final stream = CrdtStream(
            baseUrl: Uri.parse('http://x'),
            config: const StreamConfig(
              reconnectDelay: Duration(milliseconds: 5000),
            ),
            connect: _abortAware(calls: calls),
          );
          final events = _collect(stream);

          // Establish the first connection.
          stream.connect();
          async.elapse(Duration.zero);
          expect(calls.count, 1);
          expect(_of<StreamConnected>(events), hasLength(1));

          // disconnect() immediately followed by connect(), in the same
          // synchronous block: loop A's aborted read has not failed yet when
          // loop B starts.
          stream.disconnect();
          stream.connect();
          async.elapse(Duration.zero);
          expect(calls.count, 2);

          // Advance well past reconnectDelay. If loop A failed to recognize
          // it was superseded, it would reconnect here, opening a third
          // connection and overwriting loop B's abort.
          async.elapse(const Duration(seconds: 10));
          expect(calls.count, 2);

          // The live connection must still be the second (newest) one: a
          // final disconnect() aborts it, proving it was never overwritten
          // by a stray reconnect from the stale first loop.
          stream.disconnect();
          async.flushMicrotasks();
          expect(calls.aborted[1], isTrue);
          expect(calls.count, 2);
        });
      });
    });
  });

  group('Dart stream behaviour', () {
    test('emits StreamError for an `event: error` frame', () async {
      final body = StreamController<List<int>>();
      final s = CrdtStream(
        baseUrl: Uri.parse('http://x/sync'),
        sleep: _never,
        connect: (url, headers, abort) async => SseResponse(200, body.stream),
      );
      final events = <CrdtStreamEvent>[];
      s.on(events.add);
      s.connect();
      await pumpEventQueue();
      body.add(
        utf8.encode(
          'event: error\ndata: crdt: metadata store not initialized\n\n',
        ),
      );
      await pumpEventQueue();
      expect(
        events.whereType<StreamError>().single.error.toString(),
        contains('metadata store not initialized'),
      );
      s.disconnect();
      await body.close();
    });

    test('adds node_id after the since parameters', () {
      final s = CrdtStream(
        baseUrl: Uri.parse('http://x/sync'),
        config: StreamConfig(
          tables: const ['a', 'b'],
          since: HLC(BigInt.from(5), 2, 'n'),
          nodeId: 'dev',
        ),
      );
      expect(
        s.buildStreamUrl().toString(),
        'http://x/sync/stream?tables=a%2Cb&since_ts=5&since_count=2&since_node=n&node_id=dev',
      );
    });

    test('a changes event split across chunks arrives once and advances lastHlc', () async {
      final body = StreamController<List<int>>();
      final s = CrdtStream(
        baseUrl: Uri.parse('http://x/sync'),
        sleep: _never,
        connect: (u, h, a) async => SseResponse(200, body.stream),
      );
      final got = <ChangeRecord>[];
      s.on((e) {
        if (e is StreamChanges) got.addAll(e.changes);
      });
      s.connect();
      await pumpEventQueue();
      const frame =
          'event: changes\ndata: [{"table":"t","pk":"1","field":"f","crdt_type":"lww",'
          '"hlc":{"ts":"1712345678901234567","c":0,"node":"s"},"node_id":"s","value":1}]\n\n';
      body.add(utf8.encode(frame.substring(0, 40)));
      body.add(utf8.encode(frame.substring(40)));
      await pumpEventQueue();
      expect(got, hasLength(1));
      expect(s.lastHlc!.ts, BigInt.parse('1712345678901234567'));
      s.disconnect();
      await body.close();
    });

    test('reports the server Date of the stream response', () async {
      final seen = <DateTime>[];
      final s = CrdtStream(
        baseUrl: Uri.parse('http://x/sync'),
        onServerTime: seen.add,
        sleep: _never,
        connect: (u, h, a) async => const SseResponse(
          200,
          Stream.empty(),
          headers: {'date': 'Sun, 04 Oct 2026 12:00:00 GMT'},
        ),
      );
      s.connect();
      await pumpEventQueue();
      expect(seen, [DateTime.utc(2026, 10, 4, 12)]);
      s.disconnect();
    });

    test('a multibyte character split across chunks decodes intact', () async {
      final change = _sampleChange()..['value'] = 'café';
      final bytes = utf8.encode(_sseEvent('change', change));
      final cut = bytes.indexOf(0xc3) + 1; // between the two bytes of e-acute
      final body = StreamController<List<int>>();
      final s = _stream((u, h, a) async => SseResponse(200, body.stream));
      final events = _collect(s);
      s.connect();
      await pumpEventQueue();
      body.add(bytes.sublist(0, cut));
      body.add(bytes.sublist(cut));
      await pumpEventQueue();
      expect(_of<StreamChange>(events).single.change.value!.value, 'café');
      s.disconnect();
      await body.close();
    });

    test('joins several data lines with a newline and accepts CRLF', () async {
      // Go parity: bufio.Scanner (ScanLines) drops the \r of a CRLF ending.
      final s = _stream(
        _chunks([
          'event: change\r\n',
          'data: {"table":"users","pk":"1","field":"name","crdt_type":"lww",\r\n',
          'data: "hlc":{"ts":"7","c":0,"node":"s"},"node_id":"s","value":1}\r\n',
          '\r\n',
        ]),
      );
      final events = _collect(s);
      s.connect();
      await pumpEventQueue();
      expect(_of<StreamChange>(events), hasLength(1));
      expect(_of<StreamError>(events), isEmpty);
      s.disconnect();
    });

    test('emits StreamPresence for a presence event', () async {
      final s = _stream(
        _chunks([
          _sseEvent('presence', {
            'type': 'join',
            'node_id': 'n2',
            'topic': 'room',
            'data': {'x': 1},
          }),
        ]),
      );
      final events = _collect(s);
      s.connect();
      await pumpEventQueue();
      final p = _of<StreamPresence>(events).single.event;
      expect((p.type, p.nodeId, p.topic), ('join', 'n2', 'room'));
      s.disconnect();
    });

    test('a non-ok response is a TransportError with status, body and '
        'headers', () async {
      final s = _stream(
        _chunks(
          ['nope'],
          status: 503,
          headers: {
            'retry-after': '2',
            'date': 'Sun, 04 Oct 2026 12:00:00 GMT',
          },
        ),
      );
      final events = _collect(s);
      s.connect();
      await pumpEventQueue();
      final e = _of<StreamError>(events).single.error as TransportError;
      expect(e.statusCode, 503);
      expect(e.body, 'nope');
      expect(e.message, contains('503'));
      expect(e.headers['retry-after'], '2');
      expect(s.connected, isFalse);
      s.disconnect();
    });

    test(
      'a reconnect waits at least the server Retry-After, capped at a minute',
      () async {
        final sleeps = <Duration>[];
        final s = _stream(
          _chunks(const [], status: 429, headers: {'retry-after': '3600'}),
          sleep: (d) {
            sleeps.add(d);
            return Completer<void>().future;
          },
        );
        s.connect();
        await pumpEventQueue();
        expect(sleeps, [maxRetryAfter]);
        s.disconnect();
      },
    );

    test('reconnects resume from lastHlc with backoff and reset it on '
        'success', () async {
      final calls = _Calls();
      final sleeps = <Duration>[];
      var n = 0;
      final s = CrdtStream(
        baseUrl: _base,
        config: const StreamConfig(
          reconnectDelay: Duration(seconds: 1),
          maxReconnectDelay: Duration(seconds: 4),
        ),
        random: () => 1.0,
        sleep: (d) async {
          sleeps.add(d);
          if (sleeps.length > 3) await Completer<void>().future;
        },
        connect: (url, h, abort) {
          calls.record(url, h, abort);
          n++;
          // Connection 1 delivers a change and ends; the rest fail with 500.
          return _chunks(
            n == 1 ? [_sseEvent('change', _sampleChange(9))] : const [],
            status: n == 1 ? 200 : 500,
          )(url, h, abort);
        },
      );
      s.connect();
      await pumpEventQueue();
      expect(calls.count, 4);
      expect(calls.urls[0].query, '');
      for (final u in calls.urls.skip(1)) {
        expect(u.query, 'since_ts=9&since_count=0&since_node=server');
      }
      // The first connection succeeded, so the schedule restarts at its first
      // step; the failures after it grow it up to the ceiling.
      expect(sleeps, const [
        Duration(seconds: 1),
        Duration(seconds: 2),
        Duration(seconds: 4),
        Duration(seconds: 4),
      ]);
      s.disconnect();
    });

    group('idle timeout', () {
      test('aborts and reconnects when nothing arrives for idleTimeout', () {
        fakeAsync((async) {
          final calls = _Calls();
          final s = CrdtStream(
            baseUrl: _base,
            config: const StreamConfig(
              idleTimeout: Duration(seconds: 45),
              reconnectDelay: Duration(seconds: 1),
              maxReconnectDelay: Duration(seconds: 1),
            ),
            random: () => 1.0,
            connect: _hanging(calls: calls),
          );
          final events = _collect(s);
          s.connect();
          async.flushMicrotasks();
          expect(calls.count, 1);
          async.elapse(const Duration(seconds: 44));
          expect(s.connected, isTrue);
          async.elapse(const Duration(seconds: 2));
          expect(calls.aborted[0], isTrue);
          // An idle recycle is quiet: no StreamError, and the events say why.
          expect(_of<StreamError>(events), isEmpty);
          expect(
            _of<StreamDisconnected>(events).single.reason,
            ConnectionReason.idle,
          );
          async.elapse(const Duration(seconds: 2));
          expect(calls.count, 2);
          expect(_of<StreamConnected>(events).map((e) => e.reason), [
            ConnectionReason.normal,
            ConnectionReason.idle,
          ]);
          s.disconnect();
          // A requested disconnect is a normal one.
          expect(
            _of<StreamDisconnected>(events).last.reason,
            ConnectionReason.normal,
          );
        });
      });

      test('a quiet stream does not grow the reconnect wait', () {
        fakeAsync((async) {
          final calls = _Calls();
          final s = CrdtStream(
            baseUrl: _base,
            config: const StreamConfig(
              idleTimeout: Duration(seconds: 45),
              reconnectDelay: Duration(seconds: 1),
              maxReconnectDelay: Duration(seconds: 30),
            ),
            random: () => 1.0,
            connect: _hanging(calls: calls),
          );
          s.connect();
          // Each cycle is 45 s idle plus a 1 s wait. Without the reset the
          // waits would be 1, 2, 4 and 8 s and the fourth connect later.
          async.elapse(const Duration(seconds: 46 * 4));
          expect(calls.count, 5);
          s.disconnect();
        });
      });

      test('a server that never answers is an error, not an idle recycle', () {
        fakeAsync((async) {
          final calls = _Calls();
          final s = CrdtStream(
            baseUrl: _base,
            config: const StreamConfig(
              idleTimeout: Duration(seconds: 45),
              reconnectDelay: Duration(seconds: 1),
            ),
            connect: (u, h, a) {
              calls.record(u, h, a);
              return Completer<SseResponse>().future;
            },
          );
          final events = _collect(s);
          s.connect();
          async.elapse(const Duration(seconds: 46));
          expect(calls.aborted[0], isTrue);
          expect(_of<StreamError>(events), hasLength(1));
          expect(_of<StreamConnected>(events), isEmpty);
          s.disconnect();
        });
      });

      test('a comment line and any received byte re-arm the timer', () {
        fakeAsync((async) {
          final body = StreamController<List<int>>();
          final s = CrdtStream(
            baseUrl: _base,
            config: const StreamConfig(idleTimeout: Duration(seconds: 45)),
            connect: (u, h, a) async => SseResponse(200, body.stream),
          );
          s.connect();
          async.flushMicrotasks();
          for (var i = 0; i < 4; i++) {
            async.elapse(const Duration(seconds: 40));
            body.add(utf8.encode(': keep-alive\n\n'));
            async.flushMicrotasks();
          }
          expect(s.connected, isTrue);
          async.elapse(const Duration(seconds: 46));
          expect(s.connected, isFalse);
          s.disconnect();
        });
      });

      test('Duration.zero disables it', () {
        fakeAsync((async) {
          final s = CrdtStream(
            baseUrl: _base,
            config: const StreamConfig(idleTimeout: Duration.zero),
            connect: _hanging(),
          );
          s.connect();
          async.flushMicrotasks();
          async.elapse(const Duration(hours: 1));
          expect(s.connected, isTrue);
          s.disconnect();
        });
      });
    });

    group('auth on every connect (pre-flight F3)', () {
      test('headers change between reconnects, each connect sees the current '
          'headers', () async {
        final calls = _Calls();
        final auth = _FnAuth((n) => {'Authorization': 'Bearer t$n'});
        var sleeps = 0;
        final s = _stream(
          _chunks([': hi\n\n'], calls: calls),
          headers: {'authorization': 'Bearer static', 'x-app': '1'},
          auth: auth,
          sleep: (d) async {
            if (++sleeps > 2) await Completer<void>().future;
          },
        );
        s.connect();
        await pumpEventQueue();
        expect(calls.count, 3);
        expect(auth.calls, 3);
        expect(
          [for (final h in calls.headers) h['authorization']],
          ['Bearer t1', 'Bearer t2', 'Bearer t3'],
        );
        // The static header stays, and the provider replaced the static
        // authorization whatever the case of its name.
        expect(calls.headers.first['x-app'], '1');
        expect(
          calls.headers.first.keys.where((k) => k == 'Authorization'),
          isEmpty,
        );
        s.disconnect();
      });

      test('a cancellation before the first connect never connects', () async {
        final calls = _Calls();
        final sleeps = <Duration>[];
        final auth = _FnAuth((n) => throw _cancelled());
        final s = _stream(
          _chunks(const [], calls: calls),
          auth: auth,
          sleep: (d) {
            sleeps.add(d);
            return Completer<void>().future;
          },
        );
        final events = _collect(s);
        s.connect();
        await pumpEventQueue();
        expect(calls.count, 0);
        expect(sleeps, isEmpty);
        expect(auth.calls, 1);
        final error = _of<StreamError>(events).single.error as CrdtError;
        expect(error.code, CrdtErrorCode.cancelled);
        expect(s.connected, isFalse);
      });

      test('a cancellation stops reconnecting for good, with no further '
          'connect calls', () {
        fakeAsync((async) {
          final calls = _Calls();
          final auth = _FnAuth(
            (n) => n == 1 ? {'authorization': 'Bearer a'} : throw _cancelled(),
          );
          final s = CrdtStream(
            baseUrl: _base,
            config: const StreamConfig(
              reconnectDelay: Duration(seconds: 1),
              maxReconnectDelay: Duration(seconds: 1),
            ),
            auth: auth,
            connect: _chunks([': bye\n\n'], calls: calls),
          );
          final events = _collect(s);
          s.connect();
          async.elapse(const Duration(minutes: 10));
          expect(calls.count, 1);
          expect(auth.calls, 2);
          final errors = _of<StreamError>(events).toList();
          expect(errors, hasLength(1));
          expect(
            (errors.single.error as CrdtError).code,
            CrdtErrorCode.cancelled,
          );
          expect(s.connected, isFalse);
          expect(async.pendingTimers, isEmpty);
        });
      });

      test('another auth error is reported and backs off like a connection '
          'failure, not a hot loop', () async {
        final calls = _Calls();
        final sleeps = <Duration>[];
        final auth = _FnAuth((n) => throw Exception('token endpoint down'));
        final s = CrdtStream(
          baseUrl: _base,
          config: const StreamConfig(reconnectDelay: Duration(seconds: 2)),
          auth: auth,
          random: () => 1.0,
          connect: _chunks(const [], calls: calls),
          sleep: (d) {
            sleeps.add(d);
            return Completer<void>().future;
          },
        );
        final events = _collect(s);
        s.connect();
        await pumpEventQueue();
        expect(calls.count, 0);
        expect(auth.calls, 1);
        expect(sleeps, [const Duration(seconds: 2)]);
        final error = _of<StreamError>(events).single.error;
        expect(error, isA<AuthError>());
        s.disconnect();
      });

      test('a disconnect while credentials are pending never opens a '
          'connection', () async {
        final calls = _Calls();
        final gate = Completer<Map<String, String>>();
        final auth = _FnAuth((n) => gate.future);
        final s = _stream(_hanging(calls: calls), auth: auth);
        s.connect();
        await pumpEventQueue();
        s.disconnect();
        gate.complete({'authorization': 'Bearer late'});
        await pumpEventQueue();
        expect(calls.count, 0);
      });
    });

    group('frames the stream does not understand', () {
      test('an unknown event is ignored and the stream keeps going', () async {
        final body = StreamController<List<int>>();
        final s = _stream((u, h, a) async => SseResponse(200, body.stream));
        final events = _collect(s);
        s.connect();
        await pumpEventQueue();
        body.add(utf8.encode('event: surprise\ndata: {"x":1}\n\n'));
        body.add(utf8.encode(_sseEvent('change', _sampleChange(3))));
        await pumpEventQueue();
        expect(_of<StreamError>(events), isEmpty);
        expect(_of<StreamChange>(events), hasLength(1));
        expect(s.connected, isTrue);
        s.disconnect();
        await body.close();
      });

      test('a payload that decodes to the wrong shape is reported, not '
          'thrown, and later frames still arrive', () async {
        final body = StreamController<List<int>>();
        final s = _stream((u, h, a) async => SseResponse(200, body.stream));
        final events = _collect(s);
        s.connect();
        await pumpEventQueue();
        body.add(utf8.encode('event: changes\ndata: [{"table":1}]\n\n'));
        body.add(utf8.encode('event: change\ndata: {"hlc":{"ts":"x"}}\n\n'));
        body.add(utf8.encode('event: presence\ndata: 7\n\n'));
        body.add(utf8.encode(_sseEvent('change', _sampleChange(3))));
        await pumpEventQueue();
        expect(_of<StreamError>(events), hasLength(3));
        expect(_of<StreamChange>(events), hasLength(1));
        expect(s.connected, isTrue);
        s.disconnect();
        await body.close();
      });

      test('a reported frame error does not include the frame text', () async {
        final s = _stream(
          _chunks(['event: change\ndata: {"secret-token-value\n\n']),
        );
        final events = _collect(s);
        s.connect();
        await pumpEventQueue();
        final e = _of<StreamError>(events).single.error;
        expect(e.toString(), isNot(contains('secret-token-value')));
        s.disconnect();
      });
    });

    test(
      'events after a handler disconnects mid-chunk are not delivered',
      () async {
        final s = _stream(
          _chunks([
            _sseEvent('change', _sampleChange(10)) +
                _sseEvent('change', _sampleChange(20)),
          ]),
        );
        final events = <CrdtStreamEvent>[];
        s.on((e) {
          events.add(e);
          if (e is StreamChange) s.disconnect();
        });
        s.connect();
        await pumpEventQueue();
        expect(_of<StreamChange>(events), hasLength(1));
        expect(s.lastHlc!.ts, BigInt.from(10));
      },
    );

    test(
      'a query already on baseUrl is kept next to the stream parameters',
      () {
        final s = CrdtStream(
          baseUrl: Uri.parse('http://x/sync?tenant=t1'),
          config: const StreamConfig(tables: ['a']),
        );
        expect(
          s.buildStreamUrl().toString(),
          'http://x/sync/stream?tenant=t1&tables=a',
        );
        expect(
          CrdtStream(baseUrl: Uri.parse('http://x/sync?tenant=t1'))
              .buildStreamUrl()
              .toString(),
          'http://x/sync/stream?tenant=t1',
        );
      },
    );

    test('a server that answers 200 and hangs up keeps backing off', () async {
      final sleeps = <Duration>[];
      final s = CrdtStream(
        baseUrl: _base,
        config: const StreamConfig(
          reconnectDelay: Duration(seconds: 1),
          maxReconnectDelay: Duration(seconds: 4),
        ),
        random: () => 1.0,
        connect: _chunks(const []),
        sleep: (d) async {
          sleeps.add(d);
          if (sleeps.length > 3) await Completer<void>().future;
        },
      );
      s.connect();
      await pumpEventQueue();
      expect(sleeps, const [
        Duration(seconds: 1),
        Duration(seconds: 2),
        Duration(seconds: 4),
        Duration(seconds: 4),
      ]);
      s.disconnect();
    });

    test('200, an error frame, close, repeated, backs off like a refused '
        'connection', () async {
      final sleeps = <Duration>[];
      final calls = _Calls();
      final s = CrdtStream(
        baseUrl: _base,
        config: const StreamConfig(
          reconnectDelay: Duration(seconds: 1),
          maxReconnectDelay: Duration(seconds: 4),
        ),
        random: () => 1.0,
        connect: _chunks([
          // What the grove extension sends when it cannot start the stream.
          ': hello\n\nevent: error\ndata: crdt: metadata store not initialized\n\n',
        ], calls: calls),
        sleep: (d) async {
          sleeps.add(d);
          if (sleeps.length > 3) await Completer<void>().future;
        },
      );
      final events = _collect(s);
      s.connect();
      await pumpEventQueue();
      expect(calls.count, 4);
      expect(sleeps, const [
        Duration(seconds: 1),
        Duration(seconds: 2),
        Duration(seconds: 4),
        Duration(seconds: 4),
      ]);
      expect(_of<StreamError>(events), hasLength(4));
      s.disconnect();
    });

    test('a connection that delivers a change, or stays open for 30 seconds, '
        'is healthy and restarts the wait', () {
      fakeAsync((async) {
        final sleeps = <Duration>[];
        final calls = _Calls();
        final bodies = <StreamController<List<int>>>[];
        var n = 0;
        final s = CrdtStream(
          baseUrl: _base,
          config: const StreamConfig(
            reconnectDelay: Duration(seconds: 1),
            maxReconnectDelay: Duration(seconds: 8),
            idleTimeout: Duration.zero,
          ),
          random: () => 1.0,
          sleep: (d) async {
            sleeps.add(d);
            if (sleeps.length > 4) await Completer<void>().future;
          },
          connect: (url, h, abort) async {
            calls.record(url, h, abort);
            n++;
            if (n <= 2) return const SseResponse(500, Stream.empty());
            final body = StreamController<List<int>>();
            bodies.add(body);
            return SseResponse(200, body.stream);
          },
        );
        s.connect();
        async.flushMicrotasks();
        // Two refusals: waits of 1 s and 2 s. The third connection only ever
        // sends comments, and stays open 35 s.
        expect(calls.count, 3);
        for (var i = 0; i < 3; i++) {
          async.elapse(const Duration(seconds: 10));
          bodies[0].add(utf8.encode(': keep-alive\n\n'));
          async.flushMicrotasks();
        }
        async.elapse(const Duration(seconds: 5));
        unawaited(bodies[0].close());
        async.flushMicrotasks();
        // The wait after it is the first step again, not 4 s.
        expect(sleeps, const [
          Duration(seconds: 1),
          Duration(seconds: 2),
          Duration(seconds: 1),
        ]);
        // The fourth connection delivers a change, closes, and the wait after
        // it is the first step again too.
        bodies[1].add(utf8.encode(_sseEvent('change', _sampleChange(5))));
        async.flushMicrotasks();
        unawaited(bodies[1].close());
        async.flushMicrotasks();
        expect(sleeps.last, const Duration(seconds: 1));
        expect(sleeps, hasLength(4));
        s.disconnect();
      });
    });

    test('the reason after an error is normal, not the idle of an earlier '
        'recycle', () {
      fakeAsync((async) {
        final calls = _Calls();
        var n = 0;
        final s = CrdtStream(
          baseUrl: _base,
          config: const StreamConfig(
            idleTimeout: Duration(seconds: 45),
            reconnectDelay: Duration(seconds: 1),
            maxReconnectDelay: Duration(seconds: 1),
          ),
          random: () => 1.0,
          connect: (url, h, abort) async {
            calls.record(url, h, abort);
            n++;
            if (n == 2) return const SseResponse(503, Stream.empty());
            return SseResponse(200, StreamController<List<int>>().stream);
          },
        );
        final events = _collect(s);
        s.connect();
        async.elapse(const Duration(seconds: 120));
        expect(calls.count, greaterThanOrEqualTo(3));
        // idle recycle, then a 503, then a connection: labelled normal.
        final connected = _of<StreamConnected>(events).toList();
        expect(connected.first.reason, ConnectionReason.normal);
        expect(connected[1].reason, ConnectionReason.normal);
        expect(_of<StreamError>(events).first.error, isA<TransportError>());
        expect(
          _of<StreamDisconnected>(events).first.reason,
          ConnectionReason.idle,
        );
        s.disconnect();
      });
    });

    test('a stale loop stops touching state after disconnect', () {
      fakeAsync((async) {
        final gate = Completer<SseResponse>();
        final calls = _Calls();
        final s = CrdtStream(
          baseUrl: _base,
          connect: (u, h, a) {
            calls.record(u, h, a);
            return gate.future;
          },
        );
        final events = _collect(s);
        s.connect();
        async.flushMicrotasks();
        s.disconnect();
        gate.complete(const SseResponse(200, Stream.empty()));
        async.elapse(const Duration(minutes: 5));
        expect(events, isEmpty);
        expect(calls.count, 1);
        expect(calls.aborted[0], isTrue);
        expect(s.connected, isFalse);
      });
    });
  });
}
