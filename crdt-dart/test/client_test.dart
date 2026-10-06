// Port of the `CRDTClient` describe of crdt-js src/__tests__/client.test.ts
// (lines 64-477), case for case. The `CRDTError` describe is ported in
// errors_test.dart.
//
// crdt-js mocks `fetch`; here `package:http/testing.dart`'s MockClient goes in
// through `httpClient`, which the client hands to the HttpStreamTransport it
// builds from `baseUrl`.
//
// The groups after the port are new in Dart: presence re-announced after a
// stream reconnect, and dispose clearing what the old session could see.
import 'dart:async';
import 'dart:convert';

import 'package:fake_async/fake_async.dart';
import 'package:grove_crdt/grove_crdt.dart';
import 'package:http/http.dart' as http;
import 'package:http/testing.dart';
import 'package:test/test.dart';

/// What a mocked `fetch` answered in the TS file: a body and a status.
final class _Fetch {
  _Fetch(this.body, [this.status = 200]);

  final Object body;
  final int status;
  final requests = <http.Request>[];

  late final http.Client client = MockClient((r) async {
    requests.add(r);
    return http.Response(
      body is String ? body as String : jsonEncode(body),
      status,
    );
  });

  Map<String, Object?> bodyOf(int i) =>
      jsonDecode(requests[i].body) as Map<String, Object?>;
}

CrdtClient _createClient(
  _Fetch fetch, {
  Uri? baseUrl,
  Map<String, String> headers = const {},
  Transport? transport,
  StreamTransport? streamTransport,
}) => CrdtClient(
  baseUrl: baseUrl ?? Uri.parse('https://api.example.com/sync'),
  nodeId: 'test-node',
  tables: const ['users', 'posts'],
  httpClient: fetch.client,
  headers: headers,
  transport: transport,
  streamTransport: streamTransport,
);

const _pullOk = {
  'changes': <Object?>[],
  'latest_hlc': {'ts': '1', 'c': 0, 'node': 's'},
};

ChangeRecord _change({
  String table = 't',
  String node = 'test-node',
  Object? value = 'v',
  int ts = 1,
}) => ChangeRecord(
  table: table,
  pk: '1',
  field: table == 'users' ? 'name' : 'f',
  crdtType: CrdtType.lww,
  hlc: HLC(BigInt.from(ts), 0, node),
  nodeId: node,
  value: JsonValue(value),
);

/// A stream transport that records the configs it was asked to subscribe
/// with, like the TS file's inline `StreamTransport`.
final class _RecordingStreamTransport implements StreamTransport {
  StreamConfig? seen;

  @override
  Future<PullResponse> pull(PullRequest req) async => PullResponse();

  @override
  Future<PushResponse> push(PushRequest req) async => PushResponse(merged: 0);

  @override
  CrdtSubscription subscribe(StreamConfig config) {
    seen = config;
    return FakeSubscription();
  }
}

/// A subscription the test drives by hand.
final class FakeSubscription implements CrdtSubscription {
  final _handlers = <void Function(CrdtStreamEvent)>[];
  @override
  bool connected = false;

  void emit(CrdtStreamEvent e) {
    if (e is StreamConnected) connected = true;
    if (e is StreamDisconnected) connected = false;
    for (final h in _handlers.toList()) {
      h(e);
    }
  }

  int get handlerCount => _handlers.length;

  @override
  void Function() on(void Function(CrdtStreamEvent event) handler) {
    _handlers.add(handler);
    return () => _handlers.remove(handler);
  }

  @override
  void connect() => emit(const StreamConnected());

  @override
  void disconnect() => emit(const StreamDisconnected());

  @override
  HLC? get lastHlc => null;
}

/// A transport with presence and a stream, recording presence updates.
final class _PresenceStreamTransport
    implements StreamTransport, PresenceTransport {
  final updates = <PresenceUpdate>[];
  final subscription = FakeSubscription();
  List<PresenceState> snapshot = const [];
  Object? failUpdates;

  @override
  Future<PullResponse> pull(PullRequest req) async => PullResponse();

  @override
  Future<PushResponse> push(PushRequest req) async => PushResponse(merged: 0);

  @override
  CrdtSubscription subscribe(StreamConfig config) => subscription;

  @override
  Future<void> updatePresence(PresenceUpdate update) async {
    updates.add(update);
    final f = failUpdates;
    if (f != null) throw f;
  }

  @override
  Future<List<PresenceState>> getPresence(String topic) async => snapshot;
}

void main() {
  group('CRDTClient', () {
    group('constructor', () {
      test('strips trailing slashes from baseURL', () async {
        final fetch = _Fetch(_pullOk);
        final client = _createClient(
          fetch,
          baseUrl: Uri.parse('https://api.example.com/sync///'),
        );
        await client.pull();
        expect(
          fetch.requests[0].url.toString(),
          'https://api.example.com/sync/pull',
        );
      });

      test('stores configured tables', () async {
        final fetch = _Fetch(_pullOk);
        final client = _createClient(fetch);
        await client.pull();
        expect(fetch.bodyOf(0)['tables'], ['users', 'posts']);
      });

      test('creates a HybridClock with the nodeID', () {
        final client = _createClient(_Fetch(const {}));
        expect(client.nodeId, 'test-node');
        expect(client.clock.nodeId, 'test-node');
      });
    });

    group('pull()', () {
      test('sends POST to /pull with correct body', () async {
        final fetch = _Fetch(_pullOk);
        final client = _createClient(fetch);
        await client.pull();

        expect(fetch.requests, hasLength(1));
        final r = fetch.requests[0];
        expect(r.url.toString(), 'https://api.example.com/sync/pull');
        expect(r.method, 'POST');
        final body = fetch.bodyOf(0);
        expect(body['node_id'], 'test-node');
        expect(body['tables'], ['users', 'posts']);
      });

      test('includes custom headers', () async {
        final fetch = _Fetch(_pullOk);
        final client = _createClient(
          fetch,
          headers: {'Authorization': 'Bearer token'},
        );
        await client.pull();

        final headers = fetch.requests[0].headers;
        expect(headers['Authorization'], 'Bearer token');
        expect(headers['Content-Type'], startsWith('application/json'));
      });

      test('uses configured tables when none specified', () async {
        final fetch = _Fetch(_pullOk);
        final client = _createClient(fetch);
        await client.pull();
        expect(fetch.bodyOf(0)['tables'], ['users', 'posts']);
      });

      test('uses provided tables when specified', () async {
        final fetch = _Fetch(_pullOk);
        final client = _createClient(fetch);
        await client.pull(tables: ['docs']);
        expect(fetch.bodyOf(0)['tables'], ['docs']);
      });

      test('includes since HLC when provided', () async {
        final fetch = _Fetch(_pullOk);
        final client = _createClient(fetch);
        await client.pull(since: HLC(BigInt.from(100), 5, 'n'));
        // Go parity: HLC.MarshalJSON writes ts as a decimal string.
        expect(fetch.bodyOf(0)['since'], {'ts': '100', 'c': 5, 'node': 'n'});
      });

      test('omits since when not provided', () async {
        final fetch = _Fetch(_pullOk);
        final client = _createClient(fetch);
        await client.pull();
        // Go parity: crdt.PullRequest declares `Since HLC json:"since"` with
        // no omitempty, so Go always sends it and a missing one decodes as
        // the zero HLC. The Dart wire form sends the zero HLC.
        expect(fetch.bodyOf(0)['since'], {'ts': '0', 'c': 0, 'node': ''});
      });

      test('updates clock with server response HLC', () async {
        final serverHlc = HLC(BigInt.from(999000000000), 10, 'server');
        final fetch = _Fetch({
          'changes': <Object?>[],
          'latest_hlc': serverHlc.toJson(),
        });
        final client = _createClient(fetch);
        await client.pull();

        final next = client.clock.now();
        expect(next.ts >= serverHlc.ts || next.c > serverHlc.c, isTrue);
      });

      test('returns the pull response', () async {
        final change = ChangeRecord(
          table: 'users',
          pk: '1',
          field: 'name',
          crdtType: CrdtType.lww,
          hlc: HLC(BigInt.from(100), 0, 's'),
          nodeId: 's',
          value: const JsonValue('Alice'),
        );
        final fetch = _Fetch({
          'changes': [change.toJson()],
          'latest_hlc': {'ts': '100', 'c': 0, 'node': 's'},
        });
        final client = _createClient(fetch);
        final result = await client.pull();

        expect(result.changes, hasLength(1));
        expect(result.changes[0].value, const JsonValue('Alice'));
      });

      test('throws CRDTError on non-ok response', () async {
        final fetch = _Fetch('server error', 500);
        final client = _createClient(
          fetch,
          transport: HttpTransport(
            baseUrl: Uri.parse('https://api.example.com/sync'),
            client: fetch.client,
            retries: 0,
          ),
        );
        await expectLater(client.pull(), throwsA(isA<CrdtError>()));
      });

      test('includes response text in error message', () async {
        final fetch = _Fetch('bad request', 400);
        final client = _createClient(fetch);
        await expectLater(
          client.pull(),
          throwsA(
            isA<CrdtError>()
                .having((e) => e.message, 'message', contains('400'))
                .having((e) => e.statusCode, 'statusCode', 400),
          ),
        );
      });
    });

    group('push()', () {
      test('sends POST to /push with changes', () async {
        final fetch = _Fetch({
          'merged': 1,
          'latest_hlc': {'ts': '200', 'c': 0, 'node': 's'},
        });
        final client = _createClient(fetch);
        await client.push([_change(table: 'users', value: 'Alice', ts: 100)]);

        final r = fetch.requests[0];
        expect(r.url.toString(), 'https://api.example.com/sync/push');
        final body = fetch.bodyOf(0);
        expect(body['changes'], hasLength(1));
        expect(body['node_id'], 'test-node');
      });

      test('short-circuits on empty changes array', () async {
        final fetch = _Fetch(const {});
        final client = _createClient(fetch);
        final result = await client.push([]);

        expect(fetch.requests, isEmpty);
        expect(result.merged, 0);
        expect(result.latestHlc.isZero, isFalse);
      });

      test('updates clock with server response HLC', () async {
        final serverHlc = HLC(BigInt.from(999000000000), 10, 'server');
        final fetch = _Fetch({'merged': 1, 'latest_hlc': serverHlc.toJson()});
        final client = _createClient(fetch);
        await client.push([_change()]);

        final next = client.clock.now();
        expect(next.ts >= serverHlc.ts || next.c > serverHlc.c, isTrue);
      });

      test('returns the push response', () async {
        final fetch = _Fetch({
          'merged': 3,
          'latest_hlc': {'ts': '200', 'c': 0, 'node': 's'},
        });
        final client = _createClient(fetch);
        final result = await client.push([_change()]);
        expect(result.merged, 3);
      });

      test('throws CRDTError on non-ok response', () async {
        final fetch = _Fetch('error', 500);
        final client = _createClient(
          fetch,
          transport: HttpTransport(
            baseUrl: Uri.parse('https://api.example.com/sync'),
            client: fetch.client,
            retries: 0,
          ),
        );
        await expectLater(
          client.push([_change(node: 'n')]),
          throwsA(isA<CrdtError>()),
        );
      });

      test('includes custom headers', () async {
        final fetch = _Fetch({
          'merged': 1,
          'latest_hlc': {'ts': '1', 'c': 0, 'node': 's'},
        });
        final client = _createClient(fetch, headers: {'X-Custom': 'value'});
        await client.push([_change(node: 'n')]);
        expect(fetch.requests[0].headers['X-Custom'], 'value');
      });
    });

    group('stream()', () {
      test('creates a CRDTStream instance', () {
        final client = _createClient(_Fetch(const {}));
        final stream = client.stream();
        expect(stream, isA<CrdtStream>());
      });

      test('passes configured tables to stream', () {
        final client = _createClient(_Fetch(const {}));
        final stream = client.stream();
        // Dart reads the tables back from the stream's config, which TS
        // could not.
        expect((stream as CrdtStream).config.tables, ['users', 'posts']);
      });

      test('passes stream config overrides', () {
        final client = _createClient(_Fetch(const {}));
        final stream = client.stream(
          const StreamConfig(
            tables: ['override'],
            reconnectDelay: Duration(milliseconds: 1000),
          ),
        );
        expect(stream, isA<CrdtStream>());
        expect((stream as CrdtStream).config.tables, ['override']);
      });

      test('forwards the whole StreamConfig, not a hand-listed subset', () {
        final streamTransport = _RecordingStreamTransport();
        final client = _createClient(
          _Fetch(const {}),
          streamTransport: streamTransport,
        );

        final since = HLC(BigInt.from(42), 1, 'n9');
        client.stream(
          StreamConfig(
            tables: const ['docs'],
            reconnectDelay: const Duration(milliseconds: 111),
            maxReconnectDelay: const Duration(milliseconds: 2222),
            idleTimeout: const Duration(milliseconds: 3333),
            since: since,
            nodeId: 'node-x',
          ),
        );

        final seen = streamTransport.seen!;
        expect(seen.tables, ['docs']);
        expect(seen.reconnectDelay, const Duration(milliseconds: 111));
        expect(seen.maxReconnectDelay, const Duration(milliseconds: 2222));
        expect(seen.idleTimeout, const Duration(milliseconds: 3333));
        expect(seen.since, since);
        expect(seen.nodeId, 'node-x');
      });

      test(
        "still defaults tables to the client's own when the config omits them",
        () {
          final streamTransport = _RecordingStreamTransport();
          final client = _createClient(
            _Fetch(const {}),
            streamTransport: streamTransport,
          );
          client.stream(
            const StreamConfig(idleTimeout: Duration(milliseconds: 7)),
          );
          expect(streamTransport.seen!.tables, ['users', 'posts']);
          expect(
            streamTransport.seen!.idleTimeout,
            const Duration(milliseconds: 7),
          );
        },
      );
    });
  });

  // Not in the TS file.
  group('CrdtClient construction (Dart additions)', () {
    test('shares a given clock, and nodeId follows its rebase', () {
      final clock = HybridClock('dev');
      final client = CrdtClient(
        nodeId: 'dev',
        transport: _PresenceStreamTransport(),
        clock: clock,
      );
      expect(client.clock, same(clock));
      clock.rebase('dev~1');
      expect(client.nodeId, 'dev~1');
      // Presence keeps the id it was created with.
      expect(client.presence.localNodeId, 'dev');
    });

    test('refuses a clock of another node', () {
      expect(
        () => CrdtClient(
          nodeId: 'a',
          transport: _PresenceStreamTransport(),
          clock: HybridClock('b'),
        ),
        throwsArgumentError,
      );
    });
  });

  // Not in the TS file. The server drops a node's presence when its stream
  // disconnects, so the client announces its topics again on every
  // reconnect, idle recycles included.
  group('presence across stream reconnects (Dart additions)', () {
    test(
      'every reconnect re-announces each joined topic, idle ones too',
      () async {
        final t = _PresenceStreamTransport();
        final client = CrdtClient(nodeId: 'me', transport: t);
        await client.joinPresence('doc:1', {'name': 'Me'});
        await client.updatePresence('doc:2', {'name': 'Me'});
        final sub = client.stream() as FakeSubscription;
        t.updates.clear();

        sub.emit(const StreamConnected());
        await pumpEventQueue();
        expect(t.updates, isEmpty, reason: 'the first connect is no reconnect');

        sub
          ..emit(const StreamDisconnected(reason: ConnectionReason.idle))
          ..emit(const StreamConnected(reason: ConnectionReason.idle));
        await pumpEventQueue();
        expect(
          t.updates.map((u) => u.topic),
          unorderedEquals(['doc:1', 'doc:2']),
        );
        expect(t.updates.every((u) => u.nodeId == 'me'), isTrue);
        expect(t.updates.first.data, {'name': 'Me'});

        t.updates.clear();
        sub
          ..emit(const StreamDisconnected())
          ..emit(const StreamConnected());
        await pumpEventQueue();
        expect(t.updates, hasLength(2));

        await client.leavePresence('doc:2');
        t.updates.clear();
        sub
          ..emit(const StreamDisconnected())
          ..emit(const StreamConnected());
        await pumpEventQueue();
        expect(t.updates.map((u) => u.topic), ['doc:1']);
        await client.dispose();
      },
    );

    test('a failed re-announce is swallowed', () async {
      final t = _PresenceStreamTransport();
      final client = CrdtClient(nodeId: 'me', transport: t);
      await client.updatePresence('doc:1', 1);
      final sub = client.stream() as FakeSubscription;
      t.failUpdates = NetworkError('down');
      sub
        ..emit(const StreamConnected())
        ..emit(const StreamDisconnected())
        ..emit(const StreamConnected());
      await pumpEventQueue();
      expect(t.updates.last.topic, 'doc:1');
    });

    test(
      'after dispose a reconnect announces nothing and the handler is gone',
      () async {
        final t = _PresenceStreamTransport();
        final client = CrdtClient(nodeId: 'me', transport: t);
        await client.updatePresence('doc:1', 1);
        final sub = client.stream() as FakeSubscription;
        sub.emit(const StreamConnected());
        await client.dispose();
        t.updates.clear();
        expect(sub.handlerCount, 0);
        sub
          ..emit(const StreamDisconnected())
          ..emit(const StreamConnected());
        await pumpEventQueue();
        expect(t.updates, isEmpty);
      },
    );
  });

  // Not in the TS file. A join's snapshot that arrives after the topic was
  // left must not bring its peers back.
  group('presence seeding races (Dart additions)', () {
    for (final how in ['leaveAllPresence', 'dispose', 'leavePresence']) {
      test('a join racing $how seeds nothing', () async {
        final t = _HeldSnapshot();
        final client = CrdtClient(nodeId: 'dev', transport: t);
        final join = client.joinPresence('room', {'me': 1});
        await pumpEventQueue();
        expect(t.reads, 1, reason: 'the snapshot read is in flight');
        switch (how) {
          case 'leaveAllPresence':
            await client.leaveAllPresence();
          case 'dispose':
            await client.dispose();
          default:
            await client.leavePresence('room');
        }
        expect(client.presence.getPresence('room'), isEmpty);
        t.gate.complete();
        await join;
        expect(
          client.presence.getPresence('room'),
          isEmpty,
          reason: 'the seed landed after $how',
        );
      });
    }

    test('a join after a leave seeds again', () async {
      final t = _HeldSnapshot()..gate.complete();
      final client = CrdtClient(nodeId: 'dev', transport: t);
      await client.joinPresence('room', 1);
      await client.leaveAllPresence();
      await client.joinPresence('room', 1);
      expect(client.presence.getPresence('room'), hasLength(1));
    });
  });

  // Not in the TS file. An account switch disposes the client, which must
  // leave nothing of the old session visible or running.
  group('dispose and leaveAllPresence (Dart additions)', () {
    test(
      'leaveAllPresence clears the presence manager and stops heartbeats',
      () {
        fakeAsync((async) {
          final t = _PresenceStreamTransport();
          final client = CrdtClient(
            nodeId: 'me',
            transport: t,
            presence: const PresenceConfig(
              heartbeatInterval: Duration(milliseconds: 100),
            ),
          );
          t.snapshot = [
            PresenceState(
              nodeId: 'bob',
              topic: 'doc:1',
              data: const {'name': 'Bob'},
              updatedAt: DateTime.now().toUtc(),
            ),
          ];
          client.joinPresence('doc:1', {'name': 'Me'});
          async.flushMicrotasks();
          expect(client.presence.getPresence('doc:1'), hasLength(1));

          client.leaveAllPresence();
          async.flushMicrotasks();
          expect(client.presence.getPresence('doc:1'), isEmpty);
          expect(t.updates.last.data, isNull);

          t.updates.clear();
          async.elapse(const Duration(milliseconds: 500));
          expect(t.updates, isEmpty);
        });
      },
    );

    test(
      'dispose clears presence and stops heartbeats even when the leave fails',
      () {
        fakeAsync((async) {
          final t = _PresenceStreamTransport();
          final client = CrdtClient(
            nodeId: 'me',
            transport: t,
            presence: const PresenceConfig(
              heartbeatInterval: Duration(milliseconds: 100),
            ),
          );
          client.updatePresence('doc:1', 1);
          async.flushMicrotasks();
          client.applyPresenceEvent(
            const PresenceEvent(type: 'join', nodeId: 'bob', topic: 'doc:1'),
          );
          expect(client.presence.getPresence('doc:1'), hasLength(1));

          t.failUpdates = CrdtError('switched', code: CrdtErrorCode.cancelled);
          Object? error;
          var done = false;
          client.dispose().then<void>(
            (_) => done = true,
            onError: (Object e) => error = e,
          );
          async.flushMicrotasks();
          expect(done, isTrue);
          expect(error, isNull);
          expect(client.presence.getPresence('doc:1'), isEmpty);

          t.updates.clear();
          async.elapse(const Duration(seconds: 1));
          expect(t.updates, isEmpty);
        });
      },
    );

    test('an update in flight during dispose starts no heartbeat', () {
      fakeAsync((async) {
        final gate = Completer<void>();
        final t = _GatedPresence(gate.future);
        final client = CrdtClient(
          nodeId: 'me',
          transport: t,
          presence: const PresenceConfig(
            heartbeatInterval: Duration(milliseconds: 100),
          ),
        );
        client.updatePresence('doc:1', 1).ignore();
        async.flushMicrotasks();
        client.dispose();
        gate.complete();
        async.flushMicrotasks();
        final sent = t.updates.length;
        async.elapse(const Duration(seconds: 1));
        expect(t.updates.length, sent);
      });
    });

    test('dispose closes the transport it built from baseUrl', () async {
      var closed = 0;
      final inner = MockClient((r) async => http.Response('', 200));
      // Under runWithClient, `http.Client()` returns the spy, so the
      // transport the client builds owns it.
      final owned = http.runWithClient(
        () => CrdtClient(
          nodeId: 'me',
          baseUrl: Uri.parse('https://api.example.com/sync'),
        ),
        () => _CloseSpy(inner, () => closed++),
      );
      await owned.dispose();
      expect(closed, 1);

      // A caller-supplied http.Client is the caller's to close.
      final supplied = CrdtClient(
        nodeId: 'me',
        baseUrl: Uri.parse('https://api.example.com/sync'),
        httpClient: _CloseSpy(inner, () => closed++),
      );
      await supplied.dispose();
      expect(closed, 1);
    });
  });
}

/// A presence transport whose updates wait on a gate.
final class _GatedPresence implements Transport, PresenceTransport {
  _GatedPresence(this.gate);

  final Future<void> gate;
  final updates = <PresenceUpdate>[];

  @override
  Future<PullResponse> pull(PullRequest req) async => PullResponse();

  @override
  Future<PushResponse> push(PushRequest req) async => PushResponse(merged: 0);

  @override
  Future<void> updatePresence(PresenceUpdate update) async {
    updates.add(update);
    await gate;
  }

  @override
  Future<List<PresenceState>> getPresence(String topic) async => const [];
}

final class _CloseSpy extends http.BaseClient {
  _CloseSpy(this.inner, this.onClose);

  final http.Client inner;
  final void Function() onClose;

  @override
  Future<http.StreamedResponse> send(http.BaseRequest request) =>
      inner.send(request);

  @override
  void close() => onClose();
}

/// A presence transport whose snapshot read waits on a gate.
final class _HeldSnapshot implements Transport, PresenceTransport {
  final gate = Completer<void>();
  var reads = 0;

  @override
  Future<PullResponse> pull(PullRequest req) async => PullResponse();

  @override
  Future<PushResponse> push(PushRequest req) async => PushResponse(merged: 0);

  @override
  Future<void> updatePresence(PresenceUpdate update) async {}

  @override
  Future<List<PresenceState>> getPresence(String topic) async {
    reads++;
    await gate.future;
    return [
      PresenceState(
        nodeId: 'peer',
        topic: topic,
        data: const {'name': 'A peer'},
        updatedAt: DateTime.now().toUtc(),
      ),
    ];
  }
}
