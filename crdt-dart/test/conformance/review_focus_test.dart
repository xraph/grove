@TestOn('vm')
@Tags(['conformance'])
library;

import 'package:grove_crdt/grove_crdt.dart';
import 'package:http/http.dart' as http;
import 'package:test/test.dart';

import 'conformance_server.dart';

// Every case here passes against grove v1.7.0 and is written to keep passing
// once fix/crdt-sync-defects merges, so a bisection works either way. The
// comments say where the two servers differ and why the case does not care.
void main() {
  group('review focus', skip: ConformanceServer.skipReason(), () {
    test(
      'a 25000-row backlog arrives complete and compaction keeps values',
      () async {
        final server = await ConformanceServer.start();
        addTearDown(server.stop);
        await server.seed('notes', 25000);
        final clock = HybridClock('dev');
        final client = CrdtClient(
          nodeId: 'dev',
          clock: clock,
          transport: HttpTransport(baseUrl: server.syncUrl),
        );
        final store = CrdtStore('dev', clock, persistDebounce: Duration.zero);
        final engine = SyncEngine(
          client,
          store,
          tables: const ['notes'],
          cursors: KeyValueReplicaStorage(MapReplicaKeyValue()),
        );
        await engine.sync();
        expect(store.getCollection('notes'), hasLength(25000));
        final before = store.getDocument('notes', 'seed-24999');
        store.compact(engine.cursor('notes')!);
        expect(store.getDocument('notes', 'seed-24999'), before);
      },
      timeout: const Timeout(Duration(minutes: 3)),
    );

    test(
      'a device three hours ahead syncs through a drift-validating server',
      () async {
        final server = await ConformanceServer.start(validate: true);
        addTearDown(server.stop);
        const ahead = Duration(hours: 3);
        int deviceNow() => DateTime.now().add(ahead).millisecondsSinceEpoch;
        final skew = ClockSkew(systemNowMs: deviceNow);
        final clock = HybridClock('dev', nowMs: deviceNow);
        final client = CrdtClient(
          nodeId: 'dev',
          clock: clock,
          transport: HttpTransport(
            baseUrl: server.syncUrl,
            onServerTime: skew.observe,
          ),
        );
        final store = CrdtStore('dev', clock, persistDebounce: Duration.zero);
        final engine = SyncEngine(
          client,
          store,
          tables: const ['notes'],
          skew: skew,
          cursors: KeyValueReplicaStorage(MapReplicaKeyValue()),
        );
        final events = <SyncEngineEvent>[];
        engine.events.listen(events.add);
        store.setField('notes', 'n1', 'title', 'written before any contact');
        await engine.sync();
        expect(store.pendingCount, 0);
        expect(store.rejectedCount, 0);
        expect(events.whereType<ClockRebased>(), hasLength(1));
        final serverField = (await server.state(
          'notes',
          'n1',
        ))!.fields['title']!;
        final serverMs = (serverField.hlc.ts ~/ BigInt.from(1000000)).toInt();
        expect(
          (serverMs - DateTime.now().millisecondsSinceEpoch).abs(),
          lessThan(const Duration(minutes: 2).inMilliseconds),
        );
      },
    );

    // grove v1.7.0 behaviour: drift validation is absolute, so a change
    // stamped days in the past is refused. fix/crdt-sync-defects accepts it
    // as it is, and this case still passes, because it only asserts the end
    // state (nothing pending, the value on the server).
    test('an edit made three days ago offline is re-stamped through drift validation', () async {
      final server = await ConformanceServer.start(validate: true);
      addTearDown(server.stop);
      var offset = -const Duration(days: 3).inMilliseconds;
      final clock = HybridClock(
        'dev',
        nowMs: () => DateTime.now().millisecondsSinceEpoch + offset,
      );
      final client = CrdtClient(
        nodeId: 'dev',
        clock: clock,
        transport: HttpTransport(baseUrl: server.syncUrl),
      );
      final store = CrdtStore('dev', clock, persistDebounce: Duration.zero);
      final engine = SyncEngine(client, store, tables: const ['notes']);
      store.setField('notes', 'n1', 'title', 'offline three days');
      offset = 0;
      await engine.sync();
      expect(store.pending, isEmpty);
      expect(
        resolveFieldValue(
          (await server.state('notes', 'n1'))!.fields['title']!,
        ),
        'offline three days',
      );
    });

    // grove v1.7.0 behaviour: a hook rejection fails the push after the
    // earlier changes of the batch merged. fix/crdt-sync-defects runs every
    // hook before merging anything. Either way the engine bisects down to
    // the one refused change, so the assertions hold under both.
    test('a server sync hook rejection marks only that change', () async {
      final server = await ConformanceServer.start();
      addTearDown(server.stop);
      await server.rejectField('locked');
      final clock = HybridClock('dev');
      final client = CrdtClient(
        nodeId: 'dev',
        clock: clock,
        transport: HttpTransport(baseUrl: server.syncUrl),
      );
      final store = CrdtStore('dev', clock, persistDebounce: Duration.zero);
      final engine = SyncEngine(client, store, tables: const ['notes']);
      final events = <SyncEngineEvent>[];
      engine.events.listen(events.add);
      store
        ..setField('notes', 'n1', 'title', 'ok')
        ..setField('notes', 'n1', 'locked', 'no')
        ..setField('notes', 'n2', 'title', 'ok too');
      final report = await engine.sync();
      expect(report.rejected, 1);
      expect(store.pending.single.change.field, 'locked');
      expect(store.pending.single.rejection!.reason, 'locked is locked');
      expect(
        events.whereType<ChangeRejected>().single.rejection,
        isA<HookRejection>(),
      );
      expect((await server.state('notes', 'n2'))!.fields['title'], isNotNull);
      expect(store.getDocument('notes', 'n1')!['locked'], 'no');
    });

    test(
      'deleting the dataset leaves pending changes intact and stops requests',
      () async {
        final server = await ConformanceServer.start();
        addTearDown(server.stop);
        var requests = 0;
        final counting = _Counting(http.Client(), () => requests++);
        final clock = HybridClock('dev');
        final client = CrdtClient(
          nodeId: 'dev',
          clock: clock,
          transport: HttpTransport(
            baseUrl: server.baseUrl,
            client: counting,
            envelope: camelDtoEnvelope,
            pullPath: '/api/v1/datasets/ds1/sync/pull',
            pushPath: '/api/v1/datasets/ds1/sync/push',
          ),
        );
        final store = CrdtStore('dev', clock, persistDebounce: Duration.zero);
        final engine = SyncEngine(client, store, tables: const ['ds_notes']);
        store.setField('ds_notes', 'r1', 'title', 'synced');
        await engine.sync();
        store.setField('ds_notes', 'r1', 'title', 'pending when deleted');
        await server.deleteDataset('ds1');
        await engine.sync();
        expect(engine.state, SyncEngineState.gone);
        expect(store.pendingCount, 1);
        final seen = requests;
        await engine.sync();
        await engine.sync();
        expect(requests, seen);
      },
    );
  });
}

final class _Counting extends http.BaseClient {
  _Counting(this._inner, this._onRequest);

  final http.Client _inner;
  final void Function() _onRequest;

  @override
  Future<http.StreamedResponse> send(http.BaseRequest request) {
    _onRequest();
    return _inner.send(request);
  }
}
