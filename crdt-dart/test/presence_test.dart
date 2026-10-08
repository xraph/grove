// Port of crdt-js src/__tests__/presence.test.ts, case for case.
//
// The `PresenceManager` describe runs against `PresenceManager` with an
// injected clock. The `HttpTransport presence` describe runs against
// `HttpTransport` with `package:http/testing.dart`'s MockClient in place of the
// `fetch` mock. The `CRDTClient presence` describe runs `CrdtClient` over the
// same MockClient, with fake_async in place of vitest's fake timers.
//
// The cases after the port are new in Dart: the immutability of the cached
// answers, and `clear()` leaving nothing visible, which an account switch
// relies on.
import 'dart:convert';

import 'package:fake_async/fake_async.dart';
import 'package:grove_crdt/grove_crdt.dart';
import 'package:http/http.dart' as http;
import 'package:http/testing.dart';
import 'package:test/test.dart';

PresenceEvent _event(
  String type,
  String nodeId,
  String topic, [
  Object? data = _absent,
]) => PresenceEvent(
  type: type,
  nodeId: nodeId,
  topic: topic,
  data: identical(data, _absent) ? null : JsonValue(data),
);

const Object _absent = Object();

PresenceEvent _join(String nodeId, String topic, [Object? data = const {}]) =>
    _event('join', nodeId, topic, data);

/// What a mocked `fetch` answered in the TS file: a body and a status.
final class _Server {
  _Server(this.body, [this.status = 200]);

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
}

final class _Auth implements CrdtAuthProvider {
  _Auth(this.headers);
  final Map<String, String> headers;

  @override
  Map<String, String> getHeaders() => headers;
}

void main() {
  group('PresenceManager', () {
    late PresenceManager manager;
    late int now;

    setUp(() {
      now = 1000000;
      manager = PresenceManager('local-node', nowMs: () => now);
    });

    group('getPresence()', () {
      test('returns empty array for unknown topic', () {
        expect(manager.getPresence('unknown'), isEmpty);
      });

      test('returns peers after join event', () {
        manager.applyEvent(_join('peer-1', 'docs:1', {'name': 'Alice'}));

        final peers = manager.getPresence('docs:1');
        expect(peers, hasLength(1));
        expect(peers[0].nodeId, 'peer-1');
        expect(peers[0].data, {'name': 'Alice'});
      });

      test('excludes the local node from results', () {
        manager.applyEvent(_join('local-node', 'docs:1', {'name': 'Me'}));
        manager.applyEvent(_join('peer-1', 'docs:1', {'name': 'Alice'}));

        final peers = manager.getPresence('docs:1');
        expect(peers, hasLength(1));
        expect(peers[0].nodeId, 'peer-1');
      });

      test('returns a stable array reference across calls (useSyncExternalStore compat)', () {
        manager.applyEvent(_join('peer-1', 'docs:1'));

        final a = manager.getPresence('docs:1');
        final b = manager.getPresence('docs:1');
        expect(a, same(b));
        expect(a, equals(b));
      });

      test('returns typed presence state', () {
        manager.applyEvent(_join('peer-1', 'canvas:1', {'x': 10, 'y': 20}));

        final peers = manager.getPresence('canvas:1');
        final data = peers[0].data! as Map<String, Object?>;
        expect(data['x'], 10);
        expect(data['y'], 20);
      });
    });

    group('getPeer()', () {
      test('returns null for unknown topic', () {
        expect(manager.getPeer('unknown', 'peer-1'), isNull);
      });

      test('returns null for unknown node', () {
        manager.applyEvent(_join('peer-1', 'docs:1'));
        expect(manager.getPeer('docs:1', 'peer-2'), isNull);
      });

      test('returns the peer state', () {
        manager.applyEvent(_join('peer-1', 'docs:1', {'name': 'Alice'}));

        final peer = manager.getPeer('docs:1', 'peer-1');
        expect(peer, isNotNull);
        expect(peer!.nodeId, 'peer-1');
        expect(peer.data, {'name': 'Alice'});
      });
    });

    group('applyEvent()', () {
      test('handles join event', () {
        manager.applyEvent(_join('peer-1', 'room', {'name': 'Alice'}));

        expect(manager.getPresence('room'), hasLength(1));
      });

      test('handles update event', () {
        manager.applyEvent(_join('peer-1', 'room', {'name': 'Alice'}));
        manager.applyEvent(
          _event('update', 'peer-1', 'room', {'name': 'Alice', 'typing': true}),
        );

        final peers = manager.getPresence('room');
        expect(peers, hasLength(1));
        expect(peers[0].data, {'name': 'Alice', 'typing': true});
      });

      test('handles leave event', () {
        manager.applyEvent(_join('peer-1', 'room'));
        manager.applyEvent(_event('leave', 'peer-1', 'room'));

        expect(manager.getPresence('room'), isEmpty);
      });

      test('leave for non-existent peer is a no-op', () {
        manager.applyEvent(_event('leave', 'non-existent', 'room'));

        expect(manager.getPresence('room'), isEmpty);
      });

      test('cleans up empty topic map after last leave', () {
        manager.applyEvent(_join('peer-1', 'room'));
        manager.applyEvent(_event('leave', 'peer-1', 'room'));

        // getPresence returns empty for a cleaned-up topic.
        expect(manager.getPresence('room'), isEmpty);
      });

      test('sets updated_at on join/update', () {
        // The TS case brackets Date.now() between two reads; the clock is
        // injected here, so the stamp is exact.
        now = 5000;
        manager.applyEvent(_join('peer-1', 'room'));
        expect(
          manager.getPeer('room', 'peer-1')!.updatedAt,
          DateTime.fromMillisecondsSinceEpoch(5000, isUtc: true),
        );

        now = 7000;
        manager.applyEvent(_event('update', 'peer-1', 'room', {'a': 1}));
        expect(
          manager.getPeer('room', 'peer-1')!.updatedAt,
          DateTime.fromMillisecondsSinceEpoch(7000, isUtc: true),
        );
      });

      test('defaults data to empty object when undefined', () {
        manager.applyEvent(_event('join', 'peer-1', 'room'));

        final peer = manager.getPeer('room', 'peer-1');
        expect(peer!.data, <String, Object?>{});
      });

      test('handles multiple peers on same topic', () {
        manager.applyEvent(_join('peer-1', 'room', {'name': 'Alice'}));
        manager.applyEvent(_join('peer-2', 'room', {'name': 'Bob'}));

        final peers = manager.getPresence('room');
        expect(peers, hasLength(2));
        final names = peers
            .map((p) => (p.data! as Map<String, Object?>)['name'])
            .toList();
        expect(names, contains('Alice'));
        expect(names, contains('Bob'));
      });

      test('handles same peer on multiple topics', () {
        manager.applyEvent(_join('peer-1', 'room-a', {'context': 'a'}));
        manager.applyEvent(_join('peer-1', 'room-b', {'context': 'b'}));

        expect(manager.getPresence('room-a'), hasLength(1));
        expect(manager.getPresence('room-b'), hasLength(1));
      });
    });

    group('subscribe()', () {
      test('notifies listener on event for subscribed topic', () {
        var calls = 0;
        manager.subscribe('room', () => calls++);

        manager.applyEvent(_join('peer-1', 'room'));

        expect(calls, 1);
      });

      test('does not notify listener for other topics', () {
        var calls = 0;
        manager.subscribe('room-a', () => calls++);

        manager.applyEvent(_join('peer-1', 'room-b'));

        expect(calls, 0);
      });

      test('unsubscribes correctly', () {
        var calls = 0;
        final unsub = manager.subscribe('room', () => calls++);

        unsub();

        manager.applyEvent(_join('peer-1', 'room'));

        expect(calls, 0);
      });

      test('supports multiple listeners on same topic', () {
        var l1 = 0;
        var l2 = 0;
        manager.subscribe('room', () => l1++);
        manager.subscribe('room', () => l2++);

        manager.applyEvent(_join('peer-1', 'room'));

        expect(l1, 1);
        expect(l2, 1);
      });

      test('cleans up listener set when last listener unsubscribes', () {
        final unsub = manager.subscribe('room', () {});

        unsub();

        // Verify no error occurs with no listeners.
        manager.applyEvent(_join('peer-1', 'room'));
      });
    });

    group('subscribeAll()', () {
      test('notifies on events for any topic', () {
        var calls = 0;
        manager.subscribeAll(() => calls++);

        manager.applyEvent(_join('peer-1', 'room-a'));
        manager.applyEvent(_join('peer-2', 'room-b'));

        expect(calls, 2);
      });

      test('unsubscribes correctly', () {
        var calls = 0;
        final unsub = manager.subscribeAll(() => calls++);

        unsub();

        manager.applyEvent(_join('peer-1', 'room'));

        expect(calls, 0);
      });

      test('notifies both topic and global listeners', () {
        var topicCalls = 0;
        var globalCalls = 0;
        manager.subscribe('room', () => topicCalls++);
        manager.subscribeAll(() => globalCalls++);

        manager.applyEvent(_join('peer-1', 'room'));

        expect(topicCalls, 1);
        expect(globalCalls, 1);
      });
    });

    group('clear()', () {
      test('removes all presence state', () {
        manager.applyEvent(_join('peer-1', 'room-a'));
        manager.applyEvent(_join('peer-2', 'room-b'));

        manager.clear();

        expect(manager.getPresence('room-a'), isEmpty);
        expect(manager.getPresence('room-b'), isEmpty);
      });

      test('notifies topic listeners on clear', () {
        var calls = 0;
        manager.subscribe('room-a', () => calls++);

        manager.applyEvent(_join('peer-1', 'room-a'));
        calls = 0;

        manager.clear();

        expect(calls, 1);
      });

      test('notifies global listeners on clear', () {
        var calls = 0;
        manager.subscribeAll(() => calls++);

        manager.applyEvent(_join('peer-1', 'room-a'));
        calls = 0;

        manager.clear();

        expect(calls, 1);
      });
    });
  });

  group('HttpTransport presence', () {
    final base = Uri.parse('https://api.example.com/sync');

    group('updatePresence()', () {
      test('sends POST to /presence with correct body', () async {
        final server = _Server({});
        final transport = HttpTransport(baseUrl: base, client: server.client);

        await transport.updatePresence(
          const PresenceUpdate(
            nodeId: 'n1',
            topic: 'docs:1',
            data: {'name': 'Alice'},
          ),
        );

        final r = server.requests.single;
        expect(r.url.toString(), 'https://api.example.com/sync/presence');
        expect(r.method, 'POST');
        final body = jsonDecode(r.body) as Map<String, Object?>;
        expect(body['node_id'], 'n1');
        expect(body['topic'], 'docs:1');
        expect(body['data'], {'name': 'Alice'});
      });

      test('throws TransportError on non-ok response', () async {
        final server = _Server('error', 500);
        final transport = HttpTransport(
          baseUrl: base,
          client: server.client,
          retries: 0,
        );

        await expectLater(
          transport.updatePresence(
            const PresenceUpdate(nodeId: 'n1', topic: 'docs:1', data: {}),
          ),
          throwsA(
            isA<TransportError>().having(
              (e) => e.message,
              'message',
              contains('500'),
            ),
          ),
        );
      });

      test('includes auth headers', () async {
        final server = _Server({});
        final transport = HttpTransport(
          baseUrl: base,
          client: server.client,
          auth: _Auth({'Authorization': 'Bearer token123'}),
        );

        await transport.updatePresence(
          const PresenceUpdate(nodeId: 'n1', topic: 'docs:1', data: {}),
        );

        expect(
          server.requests.single.headers['Authorization'],
          'Bearer token123',
        );
      });
    });

    group('getPresence()', () {
      test('sends GET to /presence with topic query param', () async {
        final snapshot = {
          'topic': 'docs:1',
          'states': [
            {
              'node_id': 'peer-1',
              'topic': 'docs:1',
              'data': {'name': 'Alice'},
              'updated_at': '2026-10-04T12:00:00Z',
              'expires_at': '0001-01-01T00:00:00Z',
            },
          ],
        };
        final server = _Server(snapshot);
        final transport = HttpTransport(baseUrl: base, client: server.client);

        final result = await transport.getPresence('docs:1');

        final r = server.requests.single;
        // Differs from the TS case, which expects `docs%3A1`: Uri leaves a ':'
        // in a query as it is. Go decodes both spellings to the same topic, so
        // the case compares the decoded parameter.
        expect(r.url.path, '/sync/presence');
        expect(r.url.queryParameters, {'topic': 'docs:1'});
        expect(r.method, 'GET');
        expect(result, hasLength(1));
        expect(result[0].nodeId, 'peer-1');
      });

      test('throws TransportError on non-ok response', () async {
        final server = _Server('not found', 404);
        final transport = HttpTransport(baseUrl: base, client: server.client);

        await expectLater(
          transport.getPresence('docs:1'),
          throwsA(
            isA<TransportError>().having(
              (e) => e.message,
              'message',
              contains('404'),
            ),
          ),
        );
      });

      test('includes auth headers', () async {
        final server = _Server({'topic': 't', 'states': <Object?>[]});
        final transport = HttpTransport(
          baseUrl: base,
          client: server.client,
          auth: _Auth({'Authorization': 'Bearer xyz'}),
        );

        await transport.getPresence('t');

        expect(server.requests.single.headers['Authorization'], 'Bearer xyz');
      });

      test('encodes special characters in topic', () async {
        final server = _Server({'topic': 'a b&c', 'states': <Object?>[]});
        final transport = HttpTransport(baseUrl: base, client: server.client);

        await transport.getPresence('a b&c');

        final url = server.requests.single.url;
        // The TS case expects `topic=a%20b%26c`. Uri writes the space as `+`,
        // which Go's query parser reads as a space, so the case checks that
        // the `&` is escaped and the parameter round-trips.
        expect(url.query, contains('%26'));
        expect(url.queryParametersAll, {
          'topic': ['a b&c'],
        });
      });
    });
  });

  group('CRDTClient presence', () {
    CrdtClient createClient(_Server server, {PresenceConfig? presence}) =>
        CrdtClient(
          baseUrl: Uri.parse('https://api.example.com/sync'),
          nodeId: 'test-node',
          tables: const ['users'],
          httpClient: server.client,
          presence: presence,
        );

    List<http.Request> presenceCalls(_Server server) => [
      for (final r in server.requests)
        if (r.url.path.contains('/presence')) r,
    ];

    Map<String, Object?> body(http.Request r) =>
        jsonDecode(r.body) as Map<String, Object?>;

    group('updatePresence()', () {
      test('sends presence update via transport', () async {
        final server = _Server(const <String, Object?>{});
        final client = createClient(server, presence: const PresenceConfig());

        await client.updatePresence('docs:1', {'name': 'Alice'});

        final call = presenceCalls(server).first;
        expect(body(call)['node_id'], 'test-node');
        expect(body(call)['topic'], 'docs:1');
        expect(body(call)['data'], {'name': 'Alice'});
        await client.dispose();
      });

      test('throws when transport does not support presence', () async {
        final client = CrdtClient(
          nodeId: 'test-node',
          transport: _NoPresenceTransport(),
        );

        await expectLater(
          client.updatePresence('docs:1', {'name': 'Alice'}),
          throwsA(
            isA<UnsupportedError>().having(
              (e) => e.message,
              'message',
              contains('does not support presence'),
            ),
          ),
        );
      });

      test('starts heartbeat after update', () {
        fakeAsync((async) {
          final server = _Server(const <String, Object?>{});
          final client = createClient(
            server,
            presence: const PresenceConfig(
              heartbeatInterval: Duration(milliseconds: 100),
            ),
          );

          client.updatePresence('docs:1', {
            'cursor': {'x': 10, 'y': 20},
          });
          async.flushMicrotasks();
          server.requests.clear();

          // Advance past one heartbeat interval.
          async.elapse(const Duration(milliseconds: 105));

          expect(server.requests.length, greaterThanOrEqualTo(1));

          // Clean up to prevent leaking timers.
          client.leavePresence('docs:1');
          async.flushMicrotasks();
        });
      });

      test('heartbeat re-sends last presence data', () {
        fakeAsync((async) {
          final server = _Server(const <String, Object?>{});
          final client = createClient(
            server,
            presence: const PresenceConfig(
              heartbeatInterval: Duration(milliseconds: 100),
            ),
          );

          client.updatePresence('docs:1', {
            'cursor': {'x': 5},
          });
          async.flushMicrotasks();
          server.requests.clear();

          async.elapse(const Duration(milliseconds: 105));

          final calls = presenceCalls(server);
          expect(calls.length, greaterThanOrEqualTo(1));
          expect(body(calls[0])['data'], {
            'cursor': {'x': 5},
          });

          // Clean up.
          client.leavePresence('docs:1');
          async.flushMicrotasks();
        });
      });
    });

    group('leavePresence()', () {
      test('sends null data to server', () async {
        final server = _Server(const <String, Object?>{});
        final client = createClient(server, presence: const PresenceConfig());

        await client.updatePresence('docs:1', {'name': 'Alice'});
        server.requests.clear();

        await client.leavePresence('docs:1');

        final call = presenceCalls(server).first;
        expect(body(call).containsKey('data'), isTrue);
        expect(body(call)['data'], isNull);
      });

      test('stops heartbeat', () {
        fakeAsync((async) {
          final server = _Server(const <String, Object?>{});
          final client = createClient(
            server,
            presence: const PresenceConfig(
              heartbeatInterval: Duration(milliseconds: 100),
            ),
          );

          client.updatePresence('docs:1', {'name': 'Alice'});
          async.flushMicrotasks();
          client.leavePresence('docs:1');
          async.flushMicrotasks();
          server.requests.clear();

          // Advance past the heartbeat interval: it must not fire.
          async.elapse(const Duration(milliseconds: 200));
          async.flushTimers();

          expect(presenceCalls(server), isEmpty);
        });
      });
    });

    group('getPresence()', () {
      test('fetches presence from server via transport', () async {
        final server = _Server({
          'topic': 'docs:1',
          'states': [
            {
              'node_id': 'peer-1',
              'topic': 'docs:1',
              'data': {'name': 'Alice'},
              // Go parity: crdt.PresenceState.UpdatedAt is a time.Time, so
              // the wire carries an RFC 3339 string, not the TS number.
              'updated_at': '1970-01-01T00:00:01Z',
            },
          ],
        });
        final client = createClient(server);

        final result = await client.getPresence('docs:1');
        expect(result, hasLength(1));
        expect(result[0].nodeId, 'peer-1');
      });

      test('throws when transport does not support getPresence', () async {
        final client = CrdtClient(
          nodeId: 'test-node',
          transport: _NoPresenceTransport(),
        );

        await expectLater(
          client.getPresence('docs:1'),
          throwsA(
            isA<UnsupportedError>().having(
              (e) => e.message,
              'message',
              contains('does not support presence'),
            ),
          ),
        );
      });
    });

    group('leaveAllPresence()', () {
      test('leaves all active topics', () {
        fakeAsync((async) {
          final server = _Server(const <String, Object?>{});
          final client = createClient(
            server,
            presence: const PresenceConfig(
              heartbeatInterval: Duration(milliseconds: 100),
            ),
          );

          client.updatePresence('docs:1', {'name': 'Alice'});
          client.updatePresence('docs:2', {'name': 'Alice'});
          async.flushMicrotasks();
          server.requests.clear();

          client.leaveAllPresence();
          async.flushMicrotasks();

          // A leave went out for both topics, each with null data.
          final calls = presenceCalls(server);
          expect(calls, hasLength(2));
          for (final call in calls) {
            expect(body(call)['data'], isNull);
          }

          // The heartbeats are stopped.
          server.requests.clear();
          async.elapse(const Duration(milliseconds: 200));
          async.flushTimers();

          expect(presenceCalls(server), isEmpty);
        });
      });
    });

    group('presence field', () {
      test('exposes PresenceManager on client', () {
        final client = createClient(_Server(const <String, Object?>{}));
        expect(client.presence, isA<PresenceManager>());
      });

      test("PresenceManager uses client's nodeID", () {
        final client = createClient(_Server(const <String, Object?>{}));

        // An event for the local node is left out.
        client.presence.applyEvent(_join('test-node', 'room'));

        expect(client.presence.getPresence('room'), isEmpty);
      });

      test('PresenceManager returns remote peers', () {
        final client = createClient(_Server(const <String, Object?>{}));

        client.presence.applyEvent(
          _join('remote-peer', 'room', {'name': 'Bob'}),
        );

        final peers = client.presence.getPresence('room');
        expect(peers, hasLength(1));
        expect(peers[0].nodeId, 'remote-peer');
      });
    });
  });

  // Not in the TS file.
  group('PresenceManager (Dart additions)', () {
    late PresenceManager manager;
    late int now;

    setUp(() {
      now = 1000000;
      manager = PresenceManager('local-node', nowMs: () => now);
    });

    test('getPresence hands out unmodifiable lists, empty ones shared', () {
      manager.applyEvent(_join('peer-1', 'room'));
      expect(
        () =>
            manager.getPresence('room').add(manager.getPeer('room', 'peer-1')!),
        throwsUnsupportedError,
      );
      expect(manager.getPresence('a'), same(manager.getPresence('b')));
      expect(
        () => manager.getPresence('a').add(manager.getPeer('room', 'peer-1')!),
        throwsUnsupportedError,
      );
    });

    test(
      'a topic holding only the local node answers with the shared empty',
      () {
        manager.applyEvent(_join('local-node', 'room'));
        expect(manager.getPresence('room'), isEmpty);
        expect(manager.getPresence('room'), same(manager.getPresence('other')));
        expect(manager.getPeer('room', 'local-node'), isNotNull);
      },
    );

    test('an event on one topic leaves another topic\'s list as it was', () {
      manager.applyEvent(_join('peer-1', 'a'));
      final a = manager.getPresence('a');
      manager.applyEvent(_join('peer-2', 'b'));
      expect(manager.getPresence('a'), same(a));
    });

    test('a listener may unsubscribe itself and others while it is called', () {
      final calls = <String>[];
      late void Function() unsubSecond;
      final unsubFirst = manager.subscribe('t', () {
        calls.add('first');
        unsubSecond();
      });
      unsubSecond = manager.subscribe('t', () => calls.add('second'));
      final unsubGlobal = manager.subscribeAll(() {
        calls.add('global');
      });
      manager.applyEvent(_join('p', 't'));
      expect(calls, ['first', 'global'], reason: 'second was removed first');
      unsubFirst();
      unsubGlobal();
    });

    test('a stale unsubscribe does not remove a later subscription', () {
      // crdt-js deletes the topic's current listener set from a closure that
      // may belong to an older one. Subscribe, unsubscribe, subscribe again,
      // then call the first unsubscribe once more: the new listener stays.
      var first = 0;
      var second = 0;
      final unsubFirst = manager.subscribe('t', () => first++);
      unsubFirst();
      manager.subscribe('t', () => second++);
      unsubFirst();
      manager.applyEvent(_join('p', 't'));
      expect(first, 0);
      expect(second, 1);
    });

    test('a throwing listener does not starve the others', () {
      final calls = <String>[];
      manager.subscribe('t', () => throw StateError('boom'));
      manager.subscribe('t', () => calls.add('topic'));
      manager.subscribeAll(() => calls.add('global'));
      expect(
        () => manager.applyEvent(_join('p', 't')),
        throwsA(isA<StateError>()),
      );
      expect(calls, ['topic', 'global']);
      expect(manager.getPresence('t'), hasLength(1));
    });

    test(
      'seed replaces the topic, keeps the local node in getPeer, and notifies',
      () {
        var calls = 0;
        manager.subscribe('t', () => calls++);
        manager.applyEvent(_join('old', 't'));
        calls = 0;
        manager.seed('t', [
          PresenceState(
            nodeId: 'bob',
            topic: 't',
            updatedAt: DateTime.utc(2026, 10, 4),
          ),
          PresenceState(
            nodeId: 'local-node',
            topic: 't',
            updatedAt: DateTime.utc(2026, 10, 4),
          ),
        ]);
        expect(calls, 1);
        expect(manager.getPeer('t', 'old'), isNull);
        expect(manager.getPeer('t', 'local-node'), isNotNull);
        expect(manager.getPresence('t').map((p) => p.nodeId), ['bob']);
      },
    );

    test('seed with no states drops the topic', () {
      manager.applyEvent(_join('peer', 't'));
      manager.seed('t', const []);
      expect(manager.getPresence('t'), isEmpty);
      expect(manager.getPeer('t', 'peer'), isNull);
    });

    test('prune keeps a peer exactly at the cutoff and notifies only the '
        'topics that lost one', () {
      now = 100000;
      manager.applyEvent(_join('edge', 'a'));
      now = 130000;
      manager.applyEvent(_join('fresh', 'b'));
      final calls = <String>[];
      manager.subscribe('a', () => calls.add('a'));
      manager.subscribe('b', () => calls.add('b'));
      manager.prune(const Duration(seconds: 30));
      expect(calls, isEmpty, reason: 'the edge peer is exactly 30s old');
      now = 130001;
      manager.prune(const Duration(seconds: 30));
      expect(calls, ['a']);
      expect(manager.getPresence('a'), isEmpty);
      expect(manager.getPresence('b'), hasLength(1));
    });

    group('clear() leaves nothing visible', () {
      test('no listener reads a peer of another topic while being told', () {
        manager.applyEvent(_join('x', 'a'));
        manager.applyEvent(_join('y', 'b'));
        manager.applyEvent(_join('z', 'c'));
        // Cache an answer for every topic, as a mounted UI would have.
        for (final t in ['a', 'b', 'c']) {
          expect(manager.getPresence(t), hasLength(1));
        }

        final seen = <String>[];
        void look(String who) {
          for (final t in ['a', 'b', 'c']) {
            seen.add(
              '$who:$t:${manager.getPresence(t).length}:'
              '${manager.getPeer(t, t == 'a'
                      ? 'x'
                      : t == 'b'
                      ? 'y'
                      : 'z') == null}',
            );
          }
        }

        manager.subscribe('a', () => look('topic-a'));
        manager.subscribeAll(() => look('global'));
        manager.clear();

        expect(seen, isNotEmpty);
        for (final line in seen) {
          expect(line, endsWith(':0:true'), reason: line);
        }
      });

      test('getPresence, getPeer and a later seed see no earlier peer', () {
        manager.applyEvent(_join('x', 'a'));
        final before = manager.getPresence('a');
        manager.clear();
        expect(manager.getPresence('a'), isEmpty);
        expect(manager.getPresence('a'), isNot(same(before)));
        expect(manager.getPresence('a'), same(manager.getPresence('zzz')));
        expect(manager.getPeer('a', 'x'), isNull);

        manager.seed('a', [
          PresenceState(
            nodeId: 'q',
            topic: 'a',
            updatedAt: DateTime.utc(2026, 10, 4),
          ),
        ]);
        expect(manager.getPresence('a').map((p) => p.nodeId), ['q']);
      });

      test('prune after clear has nothing to drop or announce', () {
        manager.applyEvent(_join('x', 'a'));
        manager.clear();
        var calls = 0;
        manager.subscribeAll(() => calls++);
        now += 10000000;
        manager.prune(Duration.zero);
        expect(calls, 0);
      });

      test('listeners stay subscribed and hear the next account', () {
        manager.applyEvent(_join('x', 'a'));
        var calls = 0;
        manager.subscribe('a', () => calls++);
        manager.clear();
        expect(calls, 1);
        manager.applyEvent(_join('next-account', 'a'));
        expect(calls, 2);
        expect(manager.getPresence('a').single.nodeId, 'next-account');
      });

      test('a throwing listener still leaves the manager empty and every '
          'listener told', () {
        manager.applyEvent(_join('x', 'a'));
        manager.applyEvent(_join('y', 'b'));
        final told = <String>[];
        manager.subscribe('a', () => throw StateError('boom'));
        manager.subscribe('b', () => told.add('b'));
        manager.subscribeAll(() => told.add('global'));
        expect(manager.clear, throwsA(isA<StateError>()));
        expect(told, containsAll(['b', 'global']));
        expect(manager.getPresence('a'), isEmpty);
        expect(manager.getPresence('b'), isEmpty);
        expect(manager.getPeer('a', 'x'), isNull);
        expect(manager.getPeer('b', 'y'), isNull);
      });

      test('clearing an empty manager announces nothing', () {
        var calls = 0;
        manager.subscribeAll(() => calls++);
        manager.clear();
        expect(calls, 0);
      });
    });
  });
}

/// A transport with no presence support.
final class _NoPresenceTransport implements Transport {
  @override
  Future<PullResponse> pull(PullRequest req) async =>
      PullResponse(latestHlc: HLC(BigInt.one, 0, 's'));

  @override
  Future<PushResponse> push(PushRequest req) async =>
      PushResponse(merged: 0, latestHlc: HLC(BigInt.one, 0, 's'));
}
