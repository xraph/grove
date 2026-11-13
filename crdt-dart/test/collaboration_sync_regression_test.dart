import 'package:grove_crdt/grove_crdt.dart';
import 'package:test/test.dart';

import 'support/store_fakes.dart';

final class RegressionTransport implements Transport {
  RegressionTransport(this.changes);
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
  group('collaboration and sync regressions', () {
    test('undo and redo of a list move target its replacement node', () async {
      final store = CrdtStore('local', HybridClock('local'));
      final peer = CrdtStore('peer', HybridClock('peer'));
      addTearDown(store.dispose);
      addTearDown(peer.dispose);
      await Future.wait([store.ready, peer.ready]);
      final a = store.insertIntoList('docs', '1', 'items', 'a')!;
      final b = store.insertIntoList(
        'docs',
        '1',
        'items',
        'b',
        afterId: a.hlc,
      )!;
      var rewrite = true;
      store.use(
        FnPlugin(
          'move',
          onBeforeWrite: (event) {
            if (!rewrite) return event;
            rewrite = false;
            return WriteEvent(
              table: event.table,
              pk: event.pk,
              field: event.field,
              crdtType: event.crdtType,
              value: 'a',
              previousState: event.previousState,
              change: event.change.copyWith(
                listOp: ListOperation(
                  ListOpType.move,
                  nodeId: a.hlc,
                  parentId: b.hlc,
                  value: const JsonValue('a'),
                ),
              ),
            );
          },
        ),
      );
      store.insertIntoList('docs', '1', 'items', 'ignored');
      expect(store.getDocument('docs', '1')?['items'], ['b', 'a']);
      expect(store.undo(), isTrue);
      expect(store.getDocument('docs', '1')?['items'], ['a', 'b']);
      expect(store.redo(), isTrue);
      expect(store.getDocument('docs', '1')?['items'], ['b', 'a']);
      peer.applyChanges(store.getPendingChanges());
      expect(peer.getDocument('docs', '1')?['items'], ['b', 'a']);
    });
    test(
      'document replacement survives stale snapshots and wire round trips',
      () async {
        HLC stamp(int ts) => HLC(BigInt.from(ts), 0, 'peer');
        FieldState leaf(int ts, Object? value) => FieldState(
          type: CrdtType.lww,
          hlc: stamp(ts),
          nodeId: 'peer',
          value: JsonValue(value),
        );
        final old = DocumentCrdtState({
          'gone': leaf(100, true),
          'keep': leaf(100, 'old'),
        });
        final reset = DocumentCrdtState({'keep': leaf(200, 'new')}, stamp(200));
        for (final merged in [
          mergeDocument(old, reset),
          mergeDocument(reset, old),
        ]) {
          final decoded = DocumentCrdtState.fromJson(merged.toJson());
          expect(documentResolve(mergeDocument(decoded, old)), {'keep': 'new'});
          expect(decoded.resetHlc, stamp(200));
        }
        final store = CrdtStore('local', HybridClock('local'));
        addTearDown(store.dispose);
        await store.ready;
        ChangeRecord change(DocumentCrdtState state, int ts) => ChangeRecord(
          table: 'docs',
          pk: '1',
          field: 'meta',
          crdtType: CrdtType.document,
          hlc: stamp(ts),
          nodeId: 'peer',
          state: FieldState(
            type: CrdtType.document,
            hlc: stamp(ts),
            nodeId: 'peer',
            docState: state,
          ),
        );
        store.applyChanges([change(old, 100), change(reset, 200)]);
        store.setDocumentField('docs', '1', 'meta', 'local', true);
        store.applyChanges([change(old, 100)]);
        expect(store.getDocument('docs', '1')?['meta'], {
          'keep': 'new',
          'local': true,
        });
      },
    );
    for (final kind in ['counter', 'set', 'list', 'text', 'document']) {
      test('$kind undo and redo preserve remote edits and converge', () async {
        final local = CrdtStore('local', HybridClock('local'));
        final peer = CrdtStore('peer', HybridClock('peer'));
        addTearDown(local.dispose);
        addTearDown(peer.dispose);
        await Future.wait([local.ready, peer.ready]);
        switch (kind) {
          case 'counter':
            local.incrementCounter('docs', '1', 'f', 1);
            peer.incrementCounter('docs', '1', 'f', 5);
          case 'set':
            local.addToSet('docs', '1', 'f', ['shared']);
            peer.addToSet('docs', '1', 'f', ['shared', 'peer']);
          case 'list':
            local.insertIntoList('docs', '1', 'f', 'local');
            peer.insertIntoList('docs', '1', 'f', 'peer');
          case 'text':
            local.insertText('docs', '1', 'f', 0, 'local');
            peer.insertText('docs', '1', 'f', 0, 'peer');
          case 'document':
            local.setDocumentField('docs', '1', 'f', 'local', 1);
            peer.setDocumentField('docs', '1', 'f', 'peer', 2);
        }
        local.applyChanges(peer.getPendingChanges());
        final expected = switch (kind) {
          'counter' => 5,
          'set' => ['peer', 'shared'],
          'list' => ['peer'],
          'text' => 'peer',
          _ => {'peer': 2},
        };
        expect(local.undo(), isTrue);
        expect(local.getDocument('docs', '1')?['f'], expected);
        peer.applyChanges(local.getPendingChanges());
        expect(peer.getDocument('docs', '1')?['f'], expected);
        expect(local.redo(), isTrue);
        peer.applyChanges(local.getPendingChanges());
        expect(
          peer.getDocument('docs', '1')?['f'],
          local.getDocument('docs', '1')?['f'],
        );
        expect(local.undo(), isTrue);
        expect(local.getDocument('docs', '1')?['f'], expected);
      });
    }

    test('undo restores each formatted run and preserves peer text', () async {
      final local = CrdtStore('local', HybridClock('local'));
      final peer = CrdtStore('peer', HybridClock('peer'));
      addTearDown(local.dispose);
      addTearDown(peer.dispose);
      await Future.wait([local.ready, peer.ready]);
      local.insertText('docs', '1', 'f', 0, 'ab');
      local.formatText('docs', '1', 'f', 0, 1, {'bold': true});
      peer.applyChanges(local.getPendingChanges());
      local.formatText('docs', '1', 'f', 0, 2, {'bold': false});
      final remote = peer.insertText('docs', '1', 'f', 2, 'peer')!;
      local.applyChanges([remote]);
      expect(local.undo(), isTrue);
      expect(local.getTextDelta('docs', '1', 'f').map((d) => d.toJson()), [
        {
          'insert': 'a',
          'attributes': {'bold': true},
        },
        {'insert': 'bpeer'},
      ]);
      expect(local.redo(), isTrue);
      expect(local.getText('docs', '1', 'f'), 'abpeer');
      expect(local.undo(), isTrue);
      expect(local.getTextDelta('docs', '1', 'f').first.attributes, {
        'bold': true,
      });
    });

    test('a storage failure cannot advance either pull checkpoint', () async {
      final disk = RecordingStorage()
        ..failWrites = StateError('disk unavailable');
      final cursors = KeyValueReplicaStorage(MapReplicaKeyValue());
      final clock = HybridClock('me');
      final store = CrdtStore(
        'me',
        clock,
        storage: disk,
        persistDebounce: const Duration(hours: 1),
      );
      final transport = RegressionTransport([
        ChangeRecord(
          table: 'docs',
          pk: '1',
          field: 'title',
          crdtType: CrdtType.lww,
          nodeId: 'peer',
          hlc: HLC(BigInt.from(100), 0, 'peer'),
          value: const JsonValue('saved'),
        ),
      ]);
      final client = CrdtClient(
        nodeId: 'me',
        clock: clock,
        transport: transport,
      );
      final engine = SyncEngine(
        client,
        store,
        tables: ['docs'],
        cursors: cursors,
      );
      addTearDown(() async {
        disk.failWrites = null;
        await store.dispose();
      });
      addTearDown(client.dispose);
      addTearDown(engine.dispose);
      await expectLater(engine.sync(), throwsA(isA<ReplicaPersistFailed>()));
      expect(await cursors.readCursor('docs'), isNull);
      disk.failWrites = null;
      await engine.sync();
      expect(await cursors.readCursor('docs'), isNotNull);
      expect(disk.saved.last.doc.fields['title']?.value?.value, 'saved');
    });

    test('undo counter increment preserves increments received from another replica', () async {
      final local = CrdtStore('local', HybridClock('local', nowMs: () => 1000));
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
    test('undo text insertion preserves concurrently received text', () async {
      final local = CrdtStore('local', HybridClock('local', nowMs: () => 1000));
      final peer = CrdtStore('peer', HybridClock('peer', nowMs: () => 1001));
      addTearDown(local.dispose);
      addTearDown(peer.dispose);
      await Future.wait([local.ready, peer.ready]);
      local.insertText('docs', '1', 'body', 0, 'local');
      final remote = peer.insertText('docs', '1', 'body', 0, 'peer')!;
      local.applyChanges([remote]);
      expect(local.undo(), isTrue);
      expect(local.getDocument('docs', '1')?['body'], 'peer');
    });
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
        final tr = RegressionTransport([
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
  });
}
