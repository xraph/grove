@TestOn('vm')
library;

import 'dart:io';

import 'package:grove_crdt/grove_crdt.dart';
import 'package:test/test.dart';

// Opt-in review probes assert the required behavior and currently fail.
// Run with GROVE_CRDT_REVIEW=1 until the production defects are fixed.
final class ReviewTransport implements Transport {
  ReviewTransport(this.changes);
  final List<ChangeRecord> changes;
  @override
  Future<PullResponse> pull(PullRequest req) async {
    final selected = changes.where((c) => c.hlc.isAfter(req.since)).toList();
    return PullResponse(changes: selected, latestHlc: changes.last.hlc);
  }

  @override
  Future<PushResponse> push(PushRequest req) async =>
      PushResponse(merged: req.changes.length);
}

void main() {
  group(
    'remaining issues review',
    skip: Platform.environment['GROVE_CRDT_REVIEW'] != '1'
        ? 'set GROVE_CRDT_REVIEW=1 to run known-failing review reproductions'
        : false,
    () {
      test('undo counter increment preserves increments received from another replica', () async {
        final local = CrdtStore(
          'local',
          HybridClock('local', nowMs: () => 1000),
        );
        final peer = CrdtStore('peer', HybridClock('peer', nowMs: () => 1001));
        addTearDown(local.dispose);
        addTearDown(peer.dispose);
        await Future.wait([local.ready, peer.ready]);
        local.incrementCounter('docs', '1', 'count', 1);
        final remote = peer.incrementCounter('docs', '1', 'count', 5)!;
        local.applyChanges([remote]);
        expect(local.getDocument('docs', '1')?['count'], 6);
        expect(local.undo(), isTrue);
        expect(local.getDocument('docs', '1')?['count'], 5);
      });
      test(
        'undo text insertion preserves concurrently received text',
        () async {
          final local = CrdtStore(
            'local',
            HybridClock('local', nowMs: () => 1000),
          );
          final peer = CrdtStore(
            'peer',
            HybridClock('peer', nowMs: () => 1001),
          );
          addTearDown(local.dispose);
          addTearDown(peer.dispose);
          await Future.wait([local.ready, peer.ready]);
          local.insertText('docs', '1', 'body', 0, 'local');
          final remote = peer.insertText('docs', '1', 'body', 0, 'peer')!;
          local.applyChanges([remote]);
          expect(local.undo(), isTrue);
          expect(local.getDocument('docs', '1')?['body'], 'peer');
        },
      );
      test(
        'a restart cannot skip pulled documents whose cursor was already saved',
        () async {
          final kv = MapReplicaKeyValue();
          final storage = KeyValueReplicaStorage(kv);
          final clock = HybridClock('me');
          final store = CrdtStore(
            'me',
            clock,
            storage: storage,
            persistDebounce: const Duration(hours: 1),
          );
          final tr = ReviewTransport([
            for (final i in [1, 2])
              ChangeRecord(
                table: 'docs',
                pk: '$i',
                field: 'title',
                crdtType: CrdtType.lww,
                nodeId: 'peer',
                hlc: HLC(BigInt.from(i * 100), 0, 'peer'),
                value: JsonValue('document $i'),
              ),
          ]);
          final client = CrdtClient(nodeId: 'me', clock: clock, transport: tr);
          final engine = SyncEngine(
            client,
            store,
            tables: ['docs'],
            cursors: storage,
          );
          addTearDown(engine.dispose);
          addTearDown(client.dispose);
          addTearDown(store.dispose);
          await engine.sync();
          // Capture exactly what reached disk at the crash boundary. A clean dispose
          // flushes the first replica later, without changing this restart snapshot.
          final restartKV = MapReplicaKeyValue()..entries.addAll(kv.entries);
          final restartStorage = KeyValueReplicaStorage(restartKV);
          final restartClock = HybridClock('me');
          final restarted = CrdtStore(
            'me',
            restartClock,
            storage: restartStorage,
          );
          final restartClient = CrdtClient(
            nodeId: 'me',
            clock: restartClock,
            transport: tr,
          );
          final restartEngine = SyncEngine(
            restartClient,
            restarted,
            tables: ['docs'],
            cursors: restartStorage,
          );
          addTearDown(restartEngine.dispose);
          addTearDown(restartClient.dispose);
          addTearDown(restarted.dispose);
          await restartEngine.sync();
          expect(restarted.getCollection('docs'), hasLength(2));
        },
      );
    },
  );
}
