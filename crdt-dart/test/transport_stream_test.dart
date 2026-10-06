// Port of the `HttpStreamTransport` describe of crdt-js
// src/__tests__/transport.test.ts (lines 270-298), case for case, plus the
// wiring the Dart port adds: the SSE endpoint path, the transport's static
// headers and auth, and an injected connector.
import 'dart:async';

import 'package:grove_crdt/grove_crdt.dart';
import 'package:http/http.dart' as http;
import 'package:http/testing.dart';
import 'package:test/test.dart';

const _pullOk = '{"changes":[],"latest_hlc":{"ts":"1","c":0,"node":"s"}}';

final class _Auth implements CrdtAuthProvider {
  int calls = 0;

  @override
  Map<String, String> getHeaders() => {'authorization': 'Bearer t${++calls}'};
}

void main() {
  group('HttpStreamTransport', () {
    test('subscribe returns a StreamSubscription (CRDTStream instance)', () {
      final client = MockClient((r) async => http.Response('{}', 200));
      final transport = HttpStreamTransport(
        baseUrl: Uri.parse('https://api.example.com/sync'),
        client: client,
      );

      final sub = transport.subscribe(const StreamConfig(tables: ['users']));
      expect(sub, isA<CrdtStream>());
      expect(sub.connected, isFalse);
      expect(sub.lastHlc, isNull);
    });

    test('inherits pull/push from HttpTransport', () async {
      final client = MockClient((r) async => http.Response(_pullOk, 200));
      final transport = HttpStreamTransport(
        baseUrl: Uri.parse('https://api.example.com/sync'),
        client: client,
      );

      final resp = await transport.pull(
        PullRequest(tables: const [], nodeId: 'n1'),
      );
      expect(resp.changes, isEmpty);
    });
  });

  group('HttpStreamTransport wiring', () {
    test('subscribe connects to the stream path with the transport headers '
        'and fresh auth', () async {
      final urls = <Uri>[];
      final headers = <Map<String, String>>[];
      final auth = _Auth();
      final transport = HttpStreamTransport(
        baseUrl: Uri.parse('https://api.example.com/sync/'),
        streamPath: '/events',
        headers: const {'x-app': 'demo'},
        auth: auth,
        client: MockClient((r) async => http.Response('{}', 200)),
        sseConnect: (url, h, abort) async {
          urls.add(url);
          headers.add(h);
          return SseResponse(200, StreamController<List<int>>().stream);
        },
      );
      final sub = transport.subscribe(
        const StreamConfig(tables: ['users'], nodeId: 'dev'),
      );
      sub.connect();
      await pumpEventQueue();
      expect(
        urls.single.toString(),
        'https://api.example.com/sync/events?tables=users&node_id=dev',
      );
      expect(headers.single['x-app'], 'demo');
      expect(headers.single['authorization'], 'Bearer t1');
      sub.disconnect();
    });

    test(
      'reports the stream Date through the transport onServerTime',
      () async {
        final seen = <DateTime>[];
        final transport = HttpStreamTransport(
          baseUrl: Uri.parse('https://api.example.com/sync'),
          client: MockClient((r) async => http.Response('{}', 200)),
          onServerTime: seen.add,
          sseConnect: (url, h, abort) async => SseResponse(
            200,
            StreamController<List<int>>().stream,
            headers: {'date': 'Sun, 04 Oct 2026 12:00:00 GMT'},
          ),
        );
        final sub = transport.subscribe(const StreamConfig())..connect();
        await pumpEventQueue();
        expect(seen, [DateTime.utc(2026, 10, 4, 12)]);
        sub.disconnect();
      },
    );
  });
}
