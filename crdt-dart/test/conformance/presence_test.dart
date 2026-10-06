@TestOn('vm')
@Tags(['conformance'])
library;

import 'dart:async';

import 'package:grove_crdt/grove_crdt.dart';
import 'package:http/http.dart' as http;
import 'package:test/test.dart';

import 'conformance_server.dart';

/// Counts the presence updates a client sends.
final class _CountingClient extends http.BaseClient {
  _CountingClient(this._inner);

  final http.Client _inner;
  int presencePosts = 0;

  @override
  Future<http.StreamedResponse> send(http.BaseRequest request) {
    if (request.method == 'POST' && request.url.path.endsWith('/presence')) {
      presencePosts++;
    }
    return _inner.send(request);
  }
}

/// Polls [read] until [done] accepts its value, or fails after [within].
Future<T> eventually<T>(
  Future<T> Function() read,
  bool Function(T) done, {
  Duration within = const Duration(seconds: 8),
}) async {
  final deadline = DateTime.now().add(within);
  while (true) {
    final value = await read();
    if (done(value)) return value;
    if (DateTime.now().isAfter(deadline)) {
      fail('gave up waiting; last value was $value');
    }
    await Future<void>.delayed(const Duration(milliseconds: 25));
  }
}

Future<Completer<void>> _awaitConnected(CrdtSubscription sub) {
  final connected = Completer<void>();
  sub.on((e) {
    if (e is StreamConnected && !connected.isCompleted) connected.complete();
  });
  return Future.value(connected);
}

void main() {
  group(
    'presence over the SSE stream',
    skip: ConformanceServer.skipReason(),
    () {
      late ConformanceServer server;
      setUp(() async => server = await ConformanceServer.start());
      tearDown(() => server.stop());

      Future<List<String>> nodesOn(HttpTransport t, String topic) async => [
        for (final s in await t.getPresence(topic)) s.nodeId,
      ];

      test(
        "a node's presence disappears when its stream disconnects",
        () async {
          final t = HttpStreamTransport(baseUrl: server.syncUrl);
          addTearDown(t.close);
          for (final node in ['dev-a', 'dev-b']) {
            await t.updatePresence(
              PresenceUpdate(
                nodeId: node,
                topic: 'notes:n1',
                data: {'at': node},
              ),
            );
          }
          final sub = t.subscribe(
            const StreamConfig(tables: ['notes'], nodeId: 'dev-a'),
          );
          final connected = await _awaitConnected(sub);
          sub.connect();
          await connected.future.timeout(const Duration(seconds: 5));
          expect(
            await nodesOn(t, 'notes:n1'),
            unorderedEquals(['dev-a', 'dev-b']),
          );
          sub.disconnect();
          // Only the node that owned the stream goes; dev-b never had one.
          await eventually(() => nodesOn(t, 'notes:n1'), (n) => n.length == 1);
          expect(await nodesOn(t, 'notes:n1'), ['dev-b']);
        },
      );

      test(
        'the client announces its presence again after the stream reconnects',
        () async {
          final counting = _CountingClient(http.Client());
          addTearDown(counting.close);
          final transport = HttpStreamTransport(
            baseUrl: server.syncUrl,
            client: counting,
          );
          final client = CrdtClient(
            nodeId: 'dev-a',
            transport: transport,
            tables: const ['notes'],
            presence: const PresenceConfig(
              heartbeatInterval: Duration(hours: 1),
            ),
          );
          addTearDown(client.dispose);
          // The server's keep-alive is 15 s, so a 700 ms idle timeout makes the
          // stream recycle itself, which is a disconnect then a connect.
          final sub = client.stream(
            const StreamConfig(
              tables: ['notes'],
              nodeId: 'dev-a',
              idleTimeout: Duration(milliseconds: 700),
              reconnectDelay: Duration(milliseconds: 50),
            ),
          );
          final connects = <ConnectionReason>[];
          final reconnected = Completer<void>();
          sub.on((e) {
            if (e is StreamConnected) {
              connects.add(e.reason);
              if (connects.length == 2 && !reconnected.isCompleted) {
                reconnected.complete();
              }
            }
          });
          final first = await _awaitConnected(sub);
          sub.connect();
          await first.future.timeout(const Duration(seconds: 5));
          await client.joinPresence('notes:n1', {'cursor': 3});
          expect(counting.presencePosts, 1);

          final observer = HttpTransport(baseUrl: server.syncUrl);
          addTearDown(observer.close);
          var sawGone = false;
          final watching = Timer.periodic(const Duration(milliseconds: 2), (
            _,
          ) async {
            if (reconnected.isCompleted) return;
            if ((await observer.getPresence('notes:n1')).isEmpty) {
              sawGone = true;
            }
          });
          await reconnected.future.timeout(const Duration(seconds: 10));
          watching.cancel();
          // The server removed the entry when the first stream closed.
          expect(sawGone, isTrue);
          // The heartbeat is an hour away, so another POST can only be the
          // re-announce, and the entry the server removed when the old stream
          // closed is back.
          await eventually(() async => counting.presencePosts, (n) => n >= 2);
          await eventually(
            () => nodesOn(observer, 'notes:n1'),
            (n) => n.contains('dev-a'),
          );
          sub.disconnect();
        },
      );
    },
  );
}
