@TestOn('vm')
@Tags(['conformance'])
library;

import 'dart:async';

import 'package:grove_crdt/grove_crdt.dart';
import 'package:test/test.dart';

import 'conformance_server.dart';

({CrdtStore store, CrdtClient client, SyncEngine engine}) replica(
  String node,
  Uri syncUrl, {
  List<String> tables = const ['notes'],
}) {
  final clock = HybridClock(node);
  final client = CrdtClient(
    nodeId: node,
    clock: clock,
    transport: HttpStreamTransport(baseUrl: syncUrl),
    tables: tables,
  );
  final store = CrdtStore(node, clock, persistDebounce: Duration.zero);
  return (
    store: store,
    client: client,
    engine: SyncEngine(client, store, tables: tables),
  );
}

Map<String, Object?> resolved(DocumentState d) => {
  for (final e in d.fields.entries) e.key: resolveFieldValue(e.value),
};

void main() {
  group('grove conformance', skip: ConformanceServer.skipReason(), () {
    late ConformanceServer server;
    setUp(() async => server = await ConformanceServer.start());
    tearDown(() => server.stop());

    test('every crdt type round-trips through the Go server', () async {
      final a = replica('dev-a', server.syncUrl);
      final b = replica('dev-b', server.syncUrl);
      a.store
        ..setField('notes', 'n1', 'title', 'Hello <b>')
        ..incrementCounter('notes', 'n1', 'views', 3)
        ..addToSet('notes', 'n1', 'tags', [
          'x',
          {'k': 1},
        ])
        ..insertIntoList('notes', 'n1', 'items', 'first')
        ..insertText('notes', 'n1', 'body', 0, 'héllo 😀')
        ..setDocumentField('notes', 'n1', 'meta', 'address.city', 'Lagos');
      await a.engine.sync();
      await b.engine.sync();
      expect(
        b.store.getDocument('notes', 'n1'),
        a.store.getDocument('notes', 'n1'),
      );
      final serverDoc = (await server.state('notes', 'n1'))!;
      final local = resolved(a.store.getDocumentState('notes', 'n1')!);
      final remote = resolved(serverDoc);
      expect(
        remote.keys,
        unorderedEquals(['title', 'views', 'tags', 'items', 'body', 'meta']),
      );
      expect(remote, local);
    });

    test('a local delete comes back as a pulled tombstone', () async {
      final a = replica('dev-a', server.syncUrl);
      final b = replica('dev-b', server.syncUrl);
      a.store.setField('notes', 'n1', 'title', 'x');
      await a.engine.sync();
      await b.engine.sync();
      a.store.deleteDocument('notes', 'n1');
      await a.engine.sync();
      await b.engine.sync();
      expect(b.store.getDocument('notes', 'n1'), isNull);
      expect((await server.state('notes', 'n1'))!.tombstone, isTrue);
    });

    test("the SSE stream delivers another device's change", () async {
      final a = replica('dev-a', server.syncUrl);
      final b = replica('dev-b', server.syncUrl);
      final got = Completer<ChangeRecord>();
      final sub = b.client.stream(const StreamConfig(tables: ['notes']));
      sub.on((e) {
        if (e is StreamChanges && !got.isCompleted) {
          got.complete(e.changes.first);
        }
      });
      sub.connect();
      a.store.setField('notes', 'n9', 'title', 'live');
      await a.engine.sync();
      final change = await got.future.timeout(const Duration(seconds: 5));
      expect(change.pk, 'n9');
      sub.disconnect();
    });

    test('the WebSocket correlates pull and push and streams from zero on subscribe', () async {
      final a = replica('dev-a', server.syncUrl);
      a.store.setField('notes', 'n1', 'title', 'before subscribe');
      await a.engine.sync();
      final ws = WebSocketTransport(
        url: server.syncUrl.replace(scheme: 'ws', path: '/sync/ws'),
      );
      final pushed = await ws.push(
        PushRequest(
          nodeId: 'dev-w',
          changes: [
            ChangeRecord(
              table: 'notes',
              pk: 'n2',
              field: 'title',
              crdtType: CrdtType.lww,
              hlc: HybridClock('dev-w').now(),
              nodeId: 'dev-w',
              value: const JsonValue('via ws'),
            ),
          ],
        ),
      );
      expect(pushed.merged, 1);
      final pulled = await ws.pull(
        PullRequest(tables: const ['notes'], nodeId: 'dev-w'),
      );
      expect(pulled.changes.map((c) => c.pk), containsAll(['n1', 'n2']));
      final streamed = <String>{};
      final done = Completer<void>();
      final sub = ws.subscribe(const StreamConfig(tables: ['notes']));
      sub.on((e) {
        if (e is StreamChange) streamed.add(e.change.pk);
        if (streamed.containsAll(['n1', 'n2']) && !done.isCompleted) {
          done.complete();
        }
      });
      sub.connect();
      await done.future.timeout(const Duration(seconds: 5));
      await ws.close();
    });

    test('a hook rejection over the WebSocket is classifiable', () async {
      await server.rejectField('locked');
      final ws = WebSocketTransport(
        url: server.syncUrl.replace(scheme: 'ws', path: '/sync/ws'),
      );
      await expectLater(
        ws.push(
          PushRequest(
            nodeId: 'dev-w',
            changes: [
              ChangeRecord(
                table: 'notes',
                pk: 'n1',
                field: 'locked',
                crdtType: CrdtType.lww,
                hlc: HybridClock('dev-w').now(),
                nodeId: 'dev-w',
                value: const JsonValue(1),
              ),
            ],
          ),
        ),
        throwsA(
          isA<TransportError>().having(
            (e) => classifyPushError(0, e.body),
            'rejection',
            isA<HookRejection>(),
          ),
        ),
      );
      await ws.close();
    });

    test('presence updates appear in the snapshot', () async {
      final t = HttpTransport(baseUrl: server.syncUrl);
      await t.updatePresence(
        const PresenceUpdate(
          nodeId: 'dev-a',
          topic: 'notes:n1',
          data: {'cursor': 3},
        ),
      );
      final states = await t.getPresence('notes:n1');
      expect(states.single.nodeId, 'dev-a');
      expect(states.single.data, {'cursor': 3});
    });

    test('rooms create, join and list participants', () async {
      final rooms = RoomClient(baseUrl: server.syncUrl);
      await rooms.joinDocumentRoom(
        'notes',
        'n1',
        'dev-a',
        const ParticipantData(name: 'Ada'),
      );
      final participants = await rooms.getParticipants(
        documentRoomId('notes', 'n1'),
      );
      expect(participants.single.nodeId, 'dev-a');
    });

    test('the foundry DTO dialect pulls and pushes, and the table is learned from changes', () async {
      HttpTransport dto() => HttpTransport(
        baseUrl: server.baseUrl,
        envelope: camelDtoEnvelope,
        pullPath: '/api/v1/datasets/ds1/sync/pull',
        pushPath: '/api/v1/datasets/ds1/sync/push',
      );
      final clock = HybridClock('dev-a');
      await dto().push(
        PushRequest(
          nodeId: 'dev-a',
          changes: [
            ChangeRecord(
              table: 'ds_notes',
              pk: 'r1',
              field: 'title',
              crdtType: CrdtType.lww,
              hlc: clock.now(),
              nodeId: 'dev-a',
              value: const JsonValue('row'),
            ),
          ],
        ),
      );
      final pulled = await dto().pull(
        PullRequest(tables: const [], nodeId: 'dev-b'),
      );
      expect(pulled.changes.single.table, 'ds_notes');
      await server.deleteDataset('ds1');
      await expectLater(
        dto().pull(PullRequest(tables: const [], nodeId: 'dev-b')),
        throwsA(
          isA<TransportError>().having((e) => e.statusCode, 'status', 404),
        ),
      );
    });
  });
}
