// Port of crdt-js src/__tests__/custom-transport.test.ts, case for case.
//
// The TS mocks are object literals with call logs; here they are small
// classes. Where the TS file swaps `globalThis.fetch`, this file uses
// `http.runWithClient`, which swaps what `http.Client()` returns.
import 'dart:convert';

import 'package:grove_crdt/grove_crdt.dart';
import 'package:http/http.dart' as http;
import 'package:http/testing.dart';
import 'package:test/test.dart';

/// A non-HTTP transport that records its calls.
class _MockTransport implements Transport {
  final pullCalls = <PullRequest>[];
  final pushCalls = <PushRequest>[];

  @override
  Future<PullResponse> pull(PullRequest req) async {
    pullCalls.add(req);
    return PullResponse(latestHlc: HLC(BigInt.from(1000), 0, 'mock-server'));
  }

  @override
  Future<PushResponse> push(PushRequest req) async {
    pushCalls.add(req);
    return PushResponse(
      merged: req.changes.length,
      latestHlc: HLC(BigInt.from(2000), 0, 'mock-server'),
    );
  }
}

/// A subscription that announces connect and disconnect.
final class _MockSubscription implements CrdtSubscription {
  var _connected = false;
  final _handlers = <void Function(CrdtStreamEvent)>{};

  @override
  bool get connected => _connected;

  @override
  HLC? get lastHlc => null;

  @override
  void Function() on(void Function(CrdtStreamEvent event) handler) {
    _handlers.add(handler);
    return () => _handlers.remove(handler);
  }

  @override
  void connect() {
    _connected = true;
    for (final h in _handlers.toList()) {
      h(const StreamConnected());
    }
  }

  @override
  void disconnect() {
    _connected = false;
    for (final h in _handlers.toList()) {
      h(const StreamDisconnected());
    }
  }
}

final class _MockStreamTransport extends _MockTransport
    implements StreamTransport {
  final subscribeCalls = <StreamConfig>[];
  final subscription = _MockSubscription();

  @override
  CrdtSubscription subscribe(StreamConfig config) {
    subscribeCalls.add(config);
    return subscription;
  }
}

String _type(CrdtStreamEvent e) => switch (e) {
  StreamConnected() => 'connected',
  StreamDisconnected() => 'disconnected',
  StreamChange() => 'change',
  StreamChanges() => 'changes',
  StreamPresence() => 'presence',
  StreamError() => 'error',
};

void main() {
  group('CRDTClient with custom Transport', () {
    test('pull() delegates to transport.pull()', () async {
      final transport = _MockTransport();
      final client = CrdtClient(
        nodeId: 'n1',
        transport: transport,
        tables: const ['users'],
      );

      await client.pull();
      expect(transport.pullCalls, hasLength(1));
      expect(transport.pullCalls[0].tables, ['users']);
      expect(transport.pullCalls[0].nodeId, 'n1');
    });

    test('push() delegates to transport.push()', () async {
      final transport = _MockTransport();
      final client = CrdtClient(nodeId: 'n1', transport: transport);

      final changes = [
        ChangeRecord(
          table: 'users',
          pk: '1',
          field: 'name',
          crdtType: CrdtType.lww,
          hlc: HLC(BigInt.from(100), 0, 'n1'),
          nodeId: 'n1',
          value: const JsonValue('Alice'),
        ),
      ];
      final result = await client.push(changes);
      expect(transport.pushCalls, hasLength(1));
      expect(transport.pushCalls[0].changes, changes);
      expect(result.merged, 1);
    });

    test('clock still updated from transport response', () async {
      final transport = _MockTransport();
      final client = CrdtClient(nodeId: 'n1', transport: transport);

      await client.pull();
      // The transport returns latest_hlc with ts=1000.
      final next = client.clock.now();
      expect(next.ts >= BigInt.from(1000) || next.c > 0, isTrue);
    });

    test('pull() passes tables and since correctly', () async {
      final transport = _MockTransport();
      final client = CrdtClient(
        nodeId: 'n1',
        transport: transport,
        tables: const ['default'],
      );

      final since = HLC(BigInt.from(500), 0, 'n1');
      await client.pull(tables: ['override'], since: since);
      expect(transport.pullCalls[0].tables, ['override']);
      expect(transport.pullCalls[0].since, since);
    });

    test('push() short-circuits on empty changes', () async {
      final transport = _MockTransport();
      final client = CrdtClient(nodeId: 'n1', transport: transport);

      final result = await client.push([]);
      expect(transport.pushCalls, isEmpty);
      expect(result.merged, 0);
    });

    test('stream() throws when no stream transport', () {
      final client = CrdtClient(nodeId: 'n1', transport: _MockTransport());

      expect(
        client.stream,
        throwsA(
          isA<StateError>().having(
            (e) => e.message,
            'message',
            contains('No stream transport available'),
          ),
        ),
      );
    });

    test('stream() works with StreamTransport', () {
      final transport = _MockStreamTransport();
      final client = CrdtClient(
        nodeId: 'n1',
        transport: transport,
        tables: const ['users'],
      );

      final sub = client.stream();
      expect(sub.connected, isFalse);
      expect(transport.subscribeCalls, hasLength(1));
      expect(transport.subscribeCalls[0].tables, ['users']);
    });

    test('stream() passes config overrides', () {
      final transport = _MockStreamTransport();
      final client = CrdtClient(
        nodeId: 'n1',
        transport: transport,
        tables: const ['default'],
      );

      final since = HLC(BigInt.from(100), 0, 'n1');
      client.stream(
        StreamConfig(
          tables: const ['override'],
          reconnectDelay: const Duration(milliseconds: 1000),
          since: since,
        ),
      );
      expect(transport.subscribeCalls[0].tables, ['override']);
      expect(
        transport.subscribeCalls[0].reconnectDelay,
        const Duration(milliseconds: 1000),
      );
      expect(transport.subscribeCalls[0].since, since);
    });

    test('stream subscription connect/disconnect works', () {
      final transport = _MockStreamTransport();
      final client = CrdtClient(nodeId: 'n1', transport: transport);

      // An empty list in TS; the Dart default is the same empty list.
      final sub = client.stream(const StreamConfig(tables: []));
      expect(sub.connected, isFalse);

      sub.connect();
      expect(sub.connected, isTrue);

      sub.disconnect();
      expect(sub.connected, isFalse);
    });

    test('stream subscription emits events', () {
      final transport = _MockStreamTransport();
      final client = CrdtClient(nodeId: 'n1', transport: transport);

      final sub = client.stream(const StreamConfig(tables: []));
      final events = <String>[];

      sub.on((e) => events.add(_type(e)));
      sub.connect();
      sub.disconnect();

      expect(events, ['connected', 'disconnected']);
    });

    test('streamTransport config overrides transport', () {
      final transport = _MockTransport();
      final streamTransport = _MockStreamTransport();
      final client = CrdtClient(
        nodeId: 'n1',
        transport: transport,
        streamTransport: streamTransport,
        tables: const ['users'],
      );

      // stream() uses streamTransport, not transport.
      client.stream();
      expect(streamTransport.subscribeCalls, hasLength(1));
    });
  });

  group('CRDTClient constructor', () {
    test('throws when neither baseURL nor transport is provided', () {
      expect(
        () => CrdtClient(nodeId: 'n1'),
        throwsA(
          isA<ArgumentError>().having(
            (e) => e.message,
            'message',
            'CrdtClient requires either `baseUrl` or `transport`.',
          ),
        ),
      );
    });

    test('accepts baseURL without transport (backward compat)', () async {
      final mock = MockClient(
        (r) async => http.Response(
          jsonEncode({
            'changes': <Object?>[],
            'latest_hlc': {'ts': '1', 'c': 0, 'node': 's'},
          }),
          200,
        ),
      );
      final result = await http.runWithClient(() {
        final client = CrdtClient(
          baseUrl: Uri.parse('https://api.example.com/sync'),
          nodeId: 'n1',
          tables: const ['users'],
        );
        return client.pull();
      }, () => mock);
      expect(result.changes, isEmpty);
    });

    test('accepts custom transport without baseURL', () async {
      final client = CrdtClient(nodeId: 'n1', transport: _MockTransport());
      final result = await client.pull();
      expect(result.changes, isEmpty);
    });
  });
}
