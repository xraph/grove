// Port of crdt-js src/__tests__/websocket.test.ts, case for case, against a
// fake connector. The socket is built by the test and handed out by the
// connector, so nothing touches a network. Waits run under `fake_async` or an
// injected `sleep`.
import 'dart:async';
import 'dart:convert';

import 'package:fake_async/fake_async.dart';
import 'package:grove_crdt/grove_crdt.dart';
import 'package:test/test.dart';

final Map<String, Object?> _hlc0 = {'ts': '0', 'c': 0, 'node': ''};

/// An in-memory socket. [respond] answers a client frame, like the TS
/// `FakeSocket.respond`; [serverSends] pushes an unsolicited frame; [drop]
/// ends the socket as a server hang-up does.
final class FakeWs implements WsConnection {
  final _in = StreamController<String>.broadcast();
  final sent = <Map<String, Object?>>[];
  bool closed = false;
  Map<String, Object?>? Function(Map<String, Object?> frame) respond = (_) =>
      null;

  @override
  Stream<String> get messages => _in.stream;

  @override
  void send(String text) {
    final frame = jsonDecode(text) as Map<String, Object?>;
    sent.add(frame);
    final reply = respond(frame);
    if (reply != null) scheduleMicrotask(() => serverSends(reply));
  }

  @override
  Future<void> close() async {
    closed = true;
    if (!_in.isClosed) await _in.close();
  }

  void serverSends(Map<String, Object?> frame) {
    if (!_in.isClosed) _in.add(jsonEncode(frame));
  }

  void serverSendsRaw(String text) {
    if (!_in.isClosed) _in.add(text);
  }

  /// The server hangs up.
  void drop() {
    if (!_in.isClosed) unawaited(_in.close());
  }

  Iterable<Map<String, Object?>> ofType(String type) =>
      sent.where((f) => f['type'] == type);
}

/// A connector that hands out a new [FakeWs] per call and records every call.
final class Connector {
  final sockets = <FakeWs>[];
  final urls = <Uri>[];
  final headers = <Map<String, String>>[];
  final protocols = <Iterable<String>?>[];
  final List<Object> failures = [];
  void Function(FakeWs)? onNew;

  FakeWs get latest => sockets.last;
  int get calls => urls.length;

  Future<WsConnection> call(
    Uri url,
    Map<String, String> h,
    Iterable<String>? p,
  ) async {
    urls.add(url);
    headers.add(h);
    protocols.add(p);
    if (failures.isNotEmpty) throw failures.removeAt(0);
    final ws = FakeWs();
    onNew?.call(ws);
    sockets.add(ws);
    return ws;
  }
}

final Uri _url = Uri.parse('ws://x/sync/ws');

Backoff _fast() => Backoff(
  initialDelay: const Duration(milliseconds: 1),
  maxDelay: const Duration(milliseconds: 1),
  jitter: false,
);

Future<void> _instant(Duration _) async {}

WebSocketTransport _transport(
  Connector c, {
  Duration requestTimeout = const Duration(seconds: 30),
  Duration pingInterval = Duration.zero,
  CrdtAuthProvider? auth,
  Future<void> Function(Duration)? sleep,
  Iterable<String>? protocols,
  Uri? url,
}) => WebSocketTransport(
  url: url ?? _url,
  connect: c.call,
  requestTimeout: requestTimeout,
  pingInterval: pingInterval,
  backoff: _fast,
  auth: auth,
  sleep: sleep ?? _instant,
  protocols: protocols,
);

PullRequest _pullReq() => PullRequest(tables: const ['a'], nodeId: 'n1');

PushRequest _pushReq() => const PushRequest(changes: [], nodeId: 'n1');

List<String> _types(List<CrdtStreamEvent> events) => [
  for (final e in events)
    switch (e) {
      StreamConnected() => 'connected',
      StreamDisconnected() => 'disconnected',
      StreamChange() => 'change',
      StreamChanges() => 'changes',
      StreamPresence() => 'presence',
      StreamError() => 'error',
    },
];

List<CrdtStreamEvent> _collect(CrdtSubscription sub) {
  final events = <CrdtStreamEvent>[];
  sub.on(events.add);
  return events;
}

final class _FnAuth implements CrdtAuthProvider {
  _FnAuth(this.fn);
  final FutureOr<Map<String, String>> Function(int call) fn;
  int calls = 0;

  @override
  FutureOr<Map<String, String>> getHeaders() => fn(++calls);
}

CrdtError _cancelled() =>
    CrdtError('cancelled', code: CrdtErrorCode.cancelled, retryable: true);

Map<String, Object?> _change(String pk) => {
  'table': 'docs',
  'pk': pk,
  'field': 'f',
  'crdt_type': 'lww',
  'hlc': _hlc0,
  'node_id': 'n2',
  'value': 1,
};

void main() {
  group('WebSocketTransport', () {
    test('correlates a pull request with its response by request_id', () async {
      final c = Connector();
      final t = _transport(c);
      await pumpEventQueue();
      c.latest.respond = (msg) => msg['type'] == 'pull_request'
          ? {
              'type': 'pull_response',
              'request_id': msg['request_id'],
              'payload': {'changes': <Object?>[], 'latest_hlc': _hlc0},
            }
          : null;

      final resp = await t.pull(_pullReq());
      expect(resp.changes, isEmpty);
      expect(c.latest.sent[0]['type'], 'pull_request');
      await t.close();
    });

    test(
      'rejects a request when the server answers with an error frame',
      () async {
        final c = Connector();
        final t = _transport(c);
        await pumpEventQueue();
        c.latest.respond = (msg) => {
          'type': 'error',
          'request_id': msg['request_id'],
          'payload': {'error': 'nope'},
        };
        await expectLater(
          t.push(_pushReq()),
          throwsA(
            isA<TransportError>().having(
              (e) => e.message,
              'message',
              contains('nope'),
            ),
          ),
        );
        await t.close();
      },
    );

    test('subscribe sends exactly one subscribe frame on first connect and emits inbound changes', () async {
      // No tick-wait before subscribe()/connect(): the transport connects
      // eagerly at construction, so calling connect() immediately races
      // against that in-flight connect. That race is what used to cause a
      // duplicate `subscribe` frame. Asserting a count, not `.any`, pins it.
      final c = Connector();
      final t = _transport(c);
      final sub = t.subscribe(const StreamConfig(tables: ['docs']));
      final events = _collect(sub);
      sub.connect();
      await pumpEventQueue();

      final frames = c.latest.ofType('subscribe').toList();
      expect(frames, hasLength(1));
      expect(frames.single['payload'], {
        'tables': ['docs'],
      });

      c.latest.serverSends({'type': 'change', 'payload': _change('1')});
      await pumpEventQueue();
      expect(_types(events), contains('change'));
      sub.disconnect();
      await t.close();
    });

    test('still sends exactly one subscribe frame when connect() is called after the socket is already open', () async {
      // The other half of the dedupe fix: the socket is open before
      // connect() runs, so connect() itself must send, and only once.
      final c = Connector();
      final t = _transport(c);
      await pumpEventQueue();
      expect(c.sockets, hasLength(1));

      final sub = t.subscribe(const StreamConfig(tables: ['docs']));
      sub.connect();
      await pumpEventQueue();

      expect(c.latest.ofType('subscribe'), hasLength(1));
      sub.disconnect();
      await t.close();
    });

    test(
      'reconnects after the socket drops and resubscribes exactly once',
      () async {
        final c = Connector();
        final t = _transport(c);
        final sub = t.subscribe(const StreamConfig(tables: ['docs']));
        sub.connect();
        await pumpEventQueue();

        List<Map<String, Object?>> subscribeFrames() => [
          for (final s in c.sockets) ...s.ofType('subscribe'),
        ];
        expect(subscribeFrames(), hasLength(1));
        expect(c.sockets, hasLength(1));

        // Simulate the server dropping the connection (not a client close()).
        c.latest.drop();
        await pumpEventQueue();

        expect(c.sockets, hasLength(2));
        expect(subscribeFrames(), hasLength(2));
        expect(subscribeFrames()[1]['payload'], {
          'tables': ['docs'],
        });

        sub.disconnect();
        await t.close();
      },
    );

    test('re-emits `connected` after an auto-reconnect', () async {
      // Only subscribe().connect() used to emit `connected`; the reconnect
      // path resubscribed but stayed silent, so the event log read
      // ["connected","disconnected"] forever while the socket was back up.
      final c = Connector();
      final t = _transport(c);
      final sub = t.subscribe(const StreamConfig(tables: ['docs']));
      final events = _collect(sub);
      sub.connect();
      await pumpEventQueue();
      expect(_types(events), ['connected']);

      c.latest.drop();
      await pumpEventQueue();

      expect(c.sockets, hasLength(2));
      expect(_types(events), ['connected', 'disconnected', 'connected']);
      expect(sub.connected, isTrue);

      sub.disconnect();
      await t.close();
    });

    test(
      'does not emit `connected` for a connection no subscriber asked for',
      () async {
        // The transport connects eagerly at construction. That socket has no
        // subscription behind it, so it must stay silent.
        final c = Connector();
        final t = _transport(c);
        final sub = t.subscribe(const StreamConfig(tables: ['docs']));
        final events = _collect(sub);
        await pumpEventQueue();
        expect(_types(events), isEmpty);
        await t.close();
      },
    );

    test('emits exactly one `connected` when connect() runs against an already-open socket', () async {
      // The already-open branch is the one the socket's own open will never
      // run for, so connect() owns the emit there.
      final c = Connector();
      final t = _transport(c);
      await pumpEventQueue();

      final sub = t.subscribe(const StreamConfig(tables: ['docs']));
      final events = _collect(sub);
      sub.connect();
      await pumpEventQueue();

      expect(_types(events).where((e) => e == 'connected'), hasLength(1));
      sub.disconnect();
      await t.close();
    });

    test('close() during an in-flight connect rejects the caller instead of hanging', () async {
      // The connector only answers on a later microtask, so calling close()
      // synchronously right after pull() catches the transport mid-handshake.
      final c = Connector();
      final t = _transport(c);
      final pull = t.pull(_pullReq());
      final outcome = pull.then<String>(
        (_) => 'resolved',
        onError: (_) => 'rejected',
      );
      unawaited(t.close());

      expect(
        await outcome.timeout(
          const Duration(seconds: 5),
          onTimeout: () => 'timed-out',
        ),
        'rejected',
      );
      await expectLater(
        pull,
        throwsA(
          isA<TransportError>().having(
            (e) => e.message,
            'message',
            matches(RegExp('closed', caseSensitive: false)),
          ),
        ),
      );
    });

    test('times out a request that never gets a reply and cleans up its pending entry', () {
      fakeAsync((async) {
        final c = Connector();
        final t = _transport(c, requestTimeout: const Duration(seconds: 10));
        async.flushMicrotasks();
        c.latest.respond = (_) => null; // server never answers

        Object? error;
        t.pull(_pullReq()).then<void>((_) {}, onError: (Object e) => error = e);
        async.flushMicrotasks();
        async.elapse(const Duration(seconds: 11));
        expect(error, isA<CrdtError>());
        expect(
          (error! as CrdtError).message,
          matches(RegExp('timed out', caseSensitive: false)),
        );

        // A late reply after the timeout must find nothing pending: no
        // crash, no double-settle.
        expect(() {
          c.latest.serverSends({
            'type': 'pull_response',
            'request_id': 'r1',
            'payload': {'changes': <Object?>[], 'latest_hlc': _hlc0},
          });
          async.flushMicrotasks();
        }, returnsNormally);
        // The timed-out request's timer is gone, and no other is left.
        t.close();
        async.flushMicrotasks();
      });
    });

    test(
      'updatePresence sends a presence_update frame (fire-and-forget)',
      () async {
        final c = Connector();
        final t = _transport(c);
        await pumpEventQueue();

        await t.updatePresence(
          const PresenceUpdate(nodeId: 'n1', topic: 'room', data: {'x': 1}),
        );

        expect(
          c.latest.sent,
          contains(
            equals({
              'type': 'presence_update',
              'payload': {
                'node_id': 'n1',
                'topic': 'room',
                'data': {'x': 1},
              },
            }),
          ),
        );
        await t.close();
      },
    );
  });

  group('Dart websocket behaviour', () {
    test(
      'sends a ping every pingInterval and answers a server ping with pong',
      () {
        fakeAsync((async) {
          final sockets = <FakeWs>[];
          final t = WebSocketTransport(
            url: Uri.parse('ws://x/sync/ws'),
            pingInterval: const Duration(seconds: 30),
            connect: (u, h, p) async => sockets.last,
          );
          sockets.add(FakeWs());
          async.flushMicrotasks();
          async.elapse(const Duration(seconds: 31));
          expect(
            sockets.single.sent.where((f) => f['type'] == 'ping'),
            hasLength(1),
          );
          sockets.single.serverSends({'type': 'ping', 'payload': null});
          async.flushMicrotasks();
          expect(
            sockets.single.sent.where((f) => f['type'] == 'pong'),
            hasLength(1),
          );
          t.close();
        });
      },
    );

    test('a ping frame is {"type":"ping","payload":null} and a pong from the '
        'server is ignored', () {
      fakeAsync((async) {
        final c = Connector();
        final t = _transport(c, pingInterval: const Duration(seconds: 10));
        final sub = t.subscribe(const StreamConfig());
        final events = _collect(sub);
        async.flushMicrotasks();
        async.elapse(const Duration(seconds: 25));
        expect(c.latest.ofType('ping'), hasLength(2));
        expect(c.latest.ofType('ping').first, {
          'type': 'ping',
          'payload': null,
        });
        c.latest.serverSends({'type': 'pong', 'payload': null});
        async.flushMicrotasks();
        expect(events, isEmpty);
        t.close();
        async.flushMicrotasks();
        // Closing stops the ping timer.
        expect(async.pendingTimers, isEmpty);
      });
    });

    test(
      'a correlated error frame is classifiable as a hook rejection',
      () async {
        final ws = FakeWs();
        final t = WebSocketTransport(
          url: Uri.parse('ws://x/sync/ws'),
          connect: (u, h, p) async => ws,
        );
        final push = t.push(const PushRequest(changes: [], nodeId: 'a'));
        await pumpEventQueue();
        final id = ws.sent.firstWhere(
          (f) => f['type'] == 'push_request',
        )['request_id'];
        ws.serverSends({
          'type': 'error',
          'payload': {'error': 'crdt: inbound change hook: locked'},
          'request_id': id,
        });
        await expectLater(
          push,
          throwsA(
            isA<TransportError>().having(
              (e) => classifyPushError(0, e.body),
              'rejection',
              isA<HookRejection>(),
            ),
          ),
        );
        await t.close();
      },
    );

    test('getPresence is unsupported over the socket', () {
      final t = WebSocketTransport(
        url: Uri.parse('ws://x/sync/ws'),
        connect: (u, h, p) async => FakeWs(),
      );
      expect(() => t.getPresence('topic'), throwsUnsupportedError);
      t.close();
    });

    test('never sends unsubscribe', () async {
      final ws = FakeWs();
      final t = WebSocketTransport(
        url: Uri.parse('ws://x/sync/ws'),
        connect: (u, h, p) async => ws,
      );
      final sub = t.subscribe(const StreamConfig(tables: ['t']))..connect();
      await pumpEventQueue();
      sub.disconnect();
      await pumpEventQueue();
      expect(ws.sent.where((f) => f['type'] == 'unsubscribe'), isEmpty);
      await t.close();
    });

    test(
      'request ids are r1, r2 and a push resolves from its own reply',
      () async {
        final c = Connector();
        final t = _transport(c);
        await pumpEventQueue();
        c.latest.respond = (msg) => switch (msg['type']) {
          'pull_request' => {
            'type': 'pull_response',
            'request_id': msg['request_id'],
            'payload': {'changes': <Object?>[], 'latest_hlc': _hlc0},
          },
          'push_request' => {
            'type': 'push_response',
            'request_id': msg['request_id'],
            'payload': {'merged': 3, 'latest_hlc': _hlc0},
          },
          _ => null,
        };
        await t.pull(_pullReq());
        final pushed = await t.push(_pushReq());
        expect(pushed.merged, 3);
        expect([for (final f in c.latest.sent) f['request_id']], ['r1', 'r2']);
        await t.close();
      },
    );

    test('a reply that does not decode rejects only that request', () async {
      final c = Connector();
      final t = _transport(c);
      await pumpEventQueue();
      c.latest.respond = (msg) => {
        'type': 'push_response',
        'request_id': msg['request_id'],
        'payload': {'merged': 'many'},
      };
      await expectLater(t.push(_pushReq()), throwsA(isA<TransportError>()));
      c.latest.respond = (_) => null;
      c.latest.serverSends({'type': 'change', 'payload': _change('1')});
      await t.close();
    });

    test('an uncorrelated error frame emits StreamError', () async {
      final c = Connector();
      final t = _transport(c);
      final sub = t.subscribe(const StreamConfig(tables: ['docs']));
      final events = _collect(sub);
      sub.connect();
      await pumpEventQueue();
      c.latest.serverSends({
        'type': 'error',
        'payload': {'error': 'crdt: unknown message type'},
      });
      await pumpEventQueue();
      final error = events.whereType<StreamError>().single.error;
      expect(error.toString(), contains('unknown message type'));
      sub.disconnect();
      await t.close();
    });

    test('a presence_event reaches subscribers as StreamPresence', () async {
      final c = Connector();
      final t = _transport(c);
      final sub = t.subscribe(const StreamConfig());
      final events = _collect(sub);
      sub.connect();
      await pumpEventQueue();
      c.latest.serverSends({
        'type': 'presence_event',
        'payload': {'type': 'join', 'node_id': 'n2', 'topic': 'room'},
      });
      await pumpEventQueue();
      expect(events.whereType<StreamPresence>().single.event.nodeId, 'n2');
      await t.close();
    });

    test(
      'a changes frame is handled and lastHlc follows the newest change',
      () async {
        final c = Connector();
        final t = _transport(c);
        final sub = t.subscribe(const StreamConfig(tables: ['docs']));
        final events = _collect(sub);
        sub.connect();
        await pumpEventQueue();
        Map<String, Object?> at(int ts) =>
            _change('p$ts')..['hlc'] = {'ts': '$ts', 'c': 0, 'node': 'n2'};
        c.latest.serverSends({
          'type': 'changes',
          'payload': [at(5), at(9), at(7)],
        });
        c.latest.serverSends({'type': 'change', 'payload': at(3)});
        await pumpEventQueue();
        expect(events.whereType<StreamChanges>().single.changes, hasLength(3));
        expect(sub.lastHlc!.ts, BigInt.from(9));
        await t.close();
      },
    );

    test('a subscribe-to-everything subscription sends an empty table list and '
        'reconnects', () async {
      // crdt-js reconnects only while `subscribedTables` is non-empty, so a
      // subscription to every table never came back after a drop.
      final c = Connector();
      final t = _transport(c);
      final sub = t.subscribe(const StreamConfig());
      sub.connect();
      await pumpEventQueue();
      expect(c.latest.ofType('subscribe').single['payload'], {
        'tables': <Object?>[],
      });
      c.latest.drop();
      await pumpEventQueue();
      expect(c.sockets, hasLength(2));
      expect(c.latest.ofType('subscribe'), hasLength(1));
      sub.disconnect();
      await t.close();
    });

    test('an eager connect that fails is swallowed and retried by the next '
        'call', () async {
      final c = Connector()..failures.add(NetworkError('down'));
      final t = _transport(c);
      await pumpEventQueue();
      expect(c.calls, 1);
      expect(c.sockets, isEmpty);
      final pending = t.pull(_pullReq());
      await pumpEventQueue();
      expect(c.calls, 2);
      final id = c.latest.ofType('pull_request').single['request_id'];
      c.latest.serverSends({
        'type': 'pull_response',
        'request_id': id,
        'payload': {'changes': <Object?>[], 'latest_hlc': _hlc0},
      });
      expect((await pending).changes, isEmpty);
      await t.close();
    });

    test('a failed connect while subscribed is reported and retried with '
        'backoff', () async {
      final c = Connector()
        ..failures.addAll([
          NetworkError('down'),
          NetworkError('still down'),
          NetworkError('and again'),
        ]);
      final sleeps = <Duration>[];
      final t = WebSocketTransport(
        url: _url,
        connect: c.call,
        pingInterval: Duration.zero,
        backoff: () => Backoff(
          initialDelay: const Duration(seconds: 1),
          maxDelay: const Duration(seconds: 8),
          jitter: false,
        ),
        sleep: (d) async => sleeps.add(d),
      );
      await pumpEventQueue();
      final sub = t.subscribe(const StreamConfig(tables: ['docs']));
      final events = _collect(sub);
      sub.connect();
      await pumpEventQueue();
      // The eager attempt failed, then connect()'s, then the loop's first; the
      // loop's second connected. Each wait is one step longer.
      expect(c.calls, 4);
      expect(sleeps, [const Duration(seconds: 1), const Duration(seconds: 2)]);
      expect(_types(events), ['error', 'error', 'connected']);
      expect(c.latest.ofType('subscribe'), hasLength(1));
      sub.disconnect();
      await t.close();
    });

    test('close() rejects an in-flight request, closes the socket and stops '
        'reconnecting', () async {
      final c = Connector();
      final t = _transport(c);
      final sub = t.subscribe(const StreamConfig(tables: ['docs']));
      final events = _collect(sub);
      sub.connect();
      await pumpEventQueue();
      final pull = t.pull(_pullReq());
      await pumpEventQueue();
      final settled = expectLater(pull, throwsA(isA<TransportError>()));
      await t.close();
      await settled;
      expect(c.latest.closed, isTrue);
      expect(_types(events), ['connected', 'disconnected']);
      await pumpEventQueue();
      expect(c.sockets, hasLength(1));
      await expectLater(t.pull(_pullReq()), throwsA(isA<TransportError>()));
    });

    test('headers, url and protocols reach the connector', () async {
      final c = Connector();
      final t = _transport(
        c,
        auth: StaticAuthProvider({'Authorization': 'Bearer tok'}),
        protocols: const ['grove'],
      );
      await pumpEventQueue();
      expect(c.urls.single, _url);
      expect(c.headers.single, {'Authorization': 'Bearer tok'});
      expect(c.protocols.single, ['grove']);
      await t.close();
    });

    group('frames the transport does not understand', () {
      test(
        'an unknown message type is ignored and the socket keeps working',
        () async {
          final c = Connector();
          final t = _transport(c);
          final sub = t.subscribe(const StreamConfig(tables: ['docs']));
          final events = _collect(sub);
          sub.connect();
          await pumpEventQueue();
          c.latest.serverSends({
            'type': 'telepathy',
            'payload': {'x': 1},
          });
          c.latest.serverSends({'type': 'change', 'payload': _change('1')});
          await pumpEventQueue();
          expect(_types(events), ['connected', 'change']);
          expect(sub.connected, isTrue);
          expect(c.latest.closed, isFalse);
          sub.disconnect();
          await t.close();
        },
      );

      test('a malformed frame is reported, not thrown, and later frames still '
          'arrive', () async {
        final c = Connector();
        final t = _transport(c);
        final sub = t.subscribe(const StreamConfig(tables: ['docs']));
        final events = _collect(sub);
        sub.connect();
        await pumpEventQueue();
        c.latest.serverSendsRaw('not json {');
        c.latest.serverSendsRaw('[1,2]');
        c.latest.serverSendsRaw('{"type":7}');
        c.latest.serverSends({
          'type': 'change',
          'payload': {'table': 1},
        });
        c.latest.serverSends({'type': 'changes', 'payload': 'nope'});
        c.latest.serverSends({'type': 'presence_event', 'payload': 7});
        c.latest.serverSends({'type': 'change', 'payload': _change('2')});
        await pumpEventQueue();
        expect(_types(events), [
          'connected',
          'error',
          'error',
          'error',
          'error',
          'error',
          'error',
          'change',
        ]);
        expect(sub.connected, isTrue);
        expect(c.latest.closed, isFalse);
        sub.disconnect();
        await t.close();
      });

      test('a reported frame error does not include the frame text', () async {
        final c = Connector();
        final t = _transport(c);
        final sub = t.subscribe(const StreamConfig(tables: ['docs']));
        final events = _collect(sub);
        sub.connect();
        await pumpEventQueue();
        c.latest.serverSendsRaw('{"secret-token-value');
        await pumpEventQueue();
        final e = events.whereType<StreamError>().single.error;
        expect(e.toString(), isNot(contains('secret-token-value')));
        await t.close();
      });
    });

    group('auth on every connect (pre-flight F3)', () {
      test('headers change between reconnects, each connect sees the current '
          'headers', () async {
        final c = Connector();
        final auth = _FnAuth((n) => {'Authorization': 'Bearer t$n'});
        final t = _transport(c, auth: auth);
        final sub = t.subscribe(const StreamConfig(tables: ['docs']));
        sub.connect();
        await pumpEventQueue();
        c.latest.drop();
        await pumpEventQueue();
        c.latest.drop();
        await pumpEventQueue();
        expect(c.calls, 3);
        expect(auth.calls, 3);
        expect(
          [for (final h in c.headers) h['Authorization']],
          ['Bearer t1', 'Bearer t2', 'Bearer t3'],
        );
        sub.disconnect();
        await t.close();
      });

      test('a cancellation stops reconnecting for good, with no further '
          'connect calls', () {
        fakeAsync((async) {
          final c = Connector();
          final auth = _FnAuth(
            (n) => n == 1 ? {'authorization': 'Bearer a'} : throw _cancelled(),
          );
          final t = WebSocketTransport(
            url: _url,
            connect: c.call,
            auth: auth,
            pingInterval: Duration.zero,
            backoff: _fast,
          );
          final sub = t.subscribe(const StreamConfig(tables: ['docs']));
          final events = _collect(sub);
          sub.connect();
          async.flushMicrotasks();
          expect(c.calls, 1);
          c.latest.drop();
          async.elapse(const Duration(minutes: 10));
          expect(c.calls, 1);
          expect(auth.calls, 2);
          final errors = events.whereType<StreamError>().toList();
          expect(errors, hasLength(1));
          expect(
            (errors.single.error as CrdtError).code,
            CrdtErrorCode.cancelled,
          );
          expect(sub.connected, isFalse);
          expect(async.pendingTimers, isEmpty);
          t.close();
        });
      });

      test('a cancellation passes through to pull unwrapped and nothing '
          'connects', () async {
        final c = Connector();
        final t = _transport(c, auth: _FnAuth((n) => throw _cancelled()));
        await pumpEventQueue();
        await expectLater(
          t.pull(_pullReq()),
          throwsA(
            isA<CrdtError>().having(
              (e) => e.code,
              'code',
              CrdtErrorCode.cancelled,
            ),
          ),
        );
        expect(c.calls, 0);
        await t.close();
      });

      test('another auth error is reported and backs off like a connection '
          'failure, not a hot loop', () async {
        final c = Connector();
        final sleeps = <Duration>[];
        final auth = _FnAuth((n) => throw Exception('token endpoint down'));
        final t = WebSocketTransport(
          url: _url,
          connect: c.call,
          auth: auth,
          pingInterval: Duration.zero,
          backoff: () =>
              Backoff(initialDelay: const Duration(seconds: 2), jitter: false),
          sleep: (d) {
            sleeps.add(d);
            return Completer<void>().future;
          },
        );
        final sub = t.subscribe(const StreamConfig(tables: ['docs']));
        final events = _collect(sub);
        await pumpEventQueue();
        sub.connect();
        await pumpEventQueue();
        expect(c.calls, 0);
        expect(sleeps, [const Duration(seconds: 2)]);
        expect(events.whereType<StreamError>().first.error, isA<AuthError>());
        sub.disconnect();
        await t.close();
      });

      test('a request in flight on a dropped socket fails and is not replayed '
          'on the next socket', () async {
        final c = Connector();
        var token = 'alice';
        final t = _transport(
          c,
          auth: _FnAuth((n) => {'authorization': 'Bearer $token'}),
        );
        final sub = t.subscribe(const StreamConfig(tables: ['docs']));
        sub.connect();
        await pumpEventQueue();
        final push = t.push(_pushReq());
        await pumpEventQueue();
        expect(c.latest.ofType('push_request'), hasLength(1));
        final failed = expectLater(push, throwsA(isA<NetworkError>()));

        // The account changes, and the socket dies with the push unanswered.
        token = 'bob';
        c.latest.drop();
        await pumpEventQueue();
        await failed;

        expect(c.sockets, hasLength(2));
        expect(c.headers.last['authorization'], 'Bearer bob');
        // Nothing but the subscribe frame goes out on the new socket.
        expect([for (final f in c.latest.sent) f['type']], ['subscribe']);
        sub.disconnect();
        await t.close();
      });
    });

    group('redaction', () {
      test('a failed connect names no query value', () async {
        final c = Connector()
          ..failures.add(
            NetworkError('cannot reach ws://x/sync/ws?token=sekrit&a=b'),
          );
        final t = _transport(
          c,
          url: Uri.parse('ws://x/sync/ws?token=sekrit&a=b'),
          auth: StaticAuthProvider({'Authorization': 'Bearer s3cr3t'}),
        );
        final sub = t.subscribe(const StreamConfig(tables: ['docs']));
        final events = _collect(sub);
        sub.connect();
        await pumpEventQueue();
        final error = events.whereType<StreamError>().first.error;
        final text = '$error';
        expect(text, isNot(contains('sekrit')));
        expect(text, isNot(contains('s3cr3t')));
        expect(text, contains('ws://x/sync/ws'));
        sub.disconnect();
        await t.close();
      });

      test('toString shows the url with its query values hidden', () async {
        final t = WebSocketTransport(
          url: Uri.parse('wss://user:pw@host:9/ws?token=sekrit&a=b'),
          connect: (u, h, p) async => FakeWs(),
        );
        expect(t.toString(), isNot(contains('sekrit')));
        expect(t.toString(), isNot(contains('pw')));
        expect(t.toString(), contains('wss://host:9/ws?token=***&a=***'));
        await t.close();
      });

      test('a request error names no query value either', () async {
        final c = Connector()
          ..failures.addAll([
            NetworkError('eager'),
            StateError('bad url wss://host/ws?token=sekrit'),
          ]);
        final t = _transport(c, url: Uri.parse('wss://host/ws?token=sekrit'));
        await pumpEventQueue();
        Object? caught;
        try {
          await t.pull(_pullReq());
        } on Object catch (e) {
          caught = e;
        }
        expect('$caught', isNot(contains('sekrit')));
        await t.close();
      });

      test('the web connector puts each header in the query, lower-cased', () {
        final out = webSocketUrl(Uri.parse('wss://h/ws?a=1'), {
          'Authorization': 'Bearer t',
          'X-Node': 'n 1',
        });
        expect(out.queryParameters, {
          'a': '1',
          'authorization': 'Bearer t',
          'x-node': 'n 1',
        });
        expect(out.scheme, 'wss');
        expect(out.path, '/ws');
      });
    });
  });
}
