// Port of crdt-js src/__tests__/storage.test.ts lines 94-290 ("CRDTStore with
// StorageAdapter"), case for case, against a recording ReplicaStorage fake.
// The group after it covers the Dart persistence rules: atomic commits,
// storage failures, corrupt documents, a closed session and dispose.
import 'dart:async';

import 'package:fake_async/fake_async.dart';
import 'package:grove_crdt/grove_crdt.dart';
import 'package:test/test.dart';

import 'support/store_fakes.dart';

HLC hlc(int ts, String node) => HLC(BigInt.from(ts), 0, node);

void main() {
  group('CRDTStore with StorageAdapter', () {
    late RecordingStorage storage;
    late HybridClock clock;

    setUp(() {
      storage = RecordingStorage();
      clock = HybridClock('test-node', nowMs: () => 1000);
    });

    test('calls loadState on init', () async {
      final store = CrdtStore('test-node', clock, storage: storage);
      await store.ready;
      expect(storage.loadStateCalls, 1);
    });

    test('calls loadPendingChanges on init', () async {
      final store = CrdtStore('test-node', clock, storage: storage);
      await store.ready;
      expect(storage.loadPendingCalls, 1);
    });

    test('ready resolves even with default MemoryStorage', () async {
      final store = CrdtStore('test-node', clock);
      await expectLater(store.ready, completes);
    });

    test('saveDocument called after setField', () async {
      final store = CrdtStore(
        'test-node',
        clock,
        storage: storage,
        persistDebounce: Duration.zero,
      );
      await store.ready;
      store.setField('users', '1', 'name', 'Alice');
      // Allow the fire-and-forget persistence to complete.
      await pumpEventQueue();
      expect(storage.saved.length, greaterThanOrEqualTo(1));
      expect(storage.saved[0].table, 'users');
      expect(storage.saved[0].pk, '1');
    });

    test('savePendingChanges called after setField', () async {
      final store = CrdtStore(
        'test-node',
        clock,
        storage: storage,
        persistDebounce: Duration.zero,
      );
      await store.ready;
      store.setField('users', '1', 'name', 'Alice');
      await pumpEventQueue();
      expect(storage.savedPending.length, greaterThanOrEqualTo(1));
      expect(storage.savedPending[0], hasLength(1));
    });

    test('savePendingChanges called after incrementCounter', () async {
      final store = CrdtStore(
        'test-node',
        clock,
        storage: storage,
        persistDebounce: Duration.zero,
      );
      await store.ready;
      store.incrementCounter('items', '1', 'views', 1);
      await pumpEventQueue();
      expect(storage.savedPending, isNotEmpty);
    });

    test('savePendingChanges called after decrementCounter', () async {
      final store = CrdtStore(
        'test-node',
        clock,
        storage: storage,
        persistDebounce: Duration.zero,
      );
      await store.ready;
      store.decrementCounter('items', '1', 'stock', 1);
      await pumpEventQueue();
      expect(storage.savedPending, isNotEmpty);
    });

    test('saveDocument called after addToSet', () async {
      final store = CrdtStore(
        'test-node',
        clock,
        storage: storage,
        persistDebounce: Duration.zero,
      );
      await store.ready;
      store.addToSet('items', '1', 'tags', ['cool']);
      await pumpEventQueue();
      expect(storage.saved, isNotEmpty);
    });

    test('saveDocument called after removeFromSet', () async {
      final store = CrdtStore(
        'test-node',
        clock,
        storage: storage,
        persistDebounce: Duration.zero,
      );
      await store.ready;
      store.addToSet('items', '1', 'tags', ['cool']);
      store.removeFromSet('items', '1', 'tags', ['cool']);
      await pumpEventQueue();
      expect(storage.saved, hasLength(2));
    });

    test('saveDocument called after deleteDocument', () async {
      final store = CrdtStore(
        'test-node',
        clock,
        storage: storage,
        persistDebounce: Duration.zero,
      );
      await store.ready;
      store.setField('users', '1', 'name', 'Alice');
      store.deleteDocument('users', '1');
      await pumpEventQueue();
      // saveDocument called for setField + deleteDocument.
      expect(storage.saved, hasLength(2));
    });

    test('saveDocument called after applyChanges', () async {
      final store = CrdtStore(
        'test-node',
        clock,
        storage: storage,
        persistDebounce: Duration.zero,
      );
      await store.ready;
      store.applyChanges([
        ChangeRecord(
          table: 'users',
          pk: '1',
          field: 'name',
          crdtType: CrdtType.lww,
          hlc: hlc(500, 'remote'),
          nodeId: 'remote',
          value: const JsonValue('Bob'),
        ),
      ]);
      await pumpEventQueue();
      expect(storage.saved, isNotEmpty);
    });

    test('clearPendingChanges persists empty array', () async {
      final store = CrdtStore(
        'test-node',
        clock,
        storage: storage,
        persistDebounce: Duration.zero,
      );
      await store.ready;
      store.setField('users', '1', 'name', 'Alice');
      store.clearPendingChanges();
      await pumpEventQueue();
      // The last savePendingChanges call should be for the empty array.
      expect(storage.savedPending.last, isEmpty);
    });

    test('hydrates persisted state on init', () async {
      storage.preloaded = {
        'users': {
          '1': DocumentState(
            table: 'users',
            pk: '1',
            fields: {
              'name': FieldState(
                type: CrdtType.lww,
                hlc: hlc(100, 'other'),
                nodeId: 'other',
                value: const JsonValue('Persisted'),
              ),
            },
          ),
        },
      };
      final store = CrdtStore('test-node', clock, storage: storage);
      await store.ready;
      final doc = store.getDocument('users', '1');
      expect(doc, isNotNull);
      expect(doc!['name'], 'Persisted');
    });

    test('hydrates persisted pending changes on init', () async {
      storage.preloadedPending = [
        PendingChange(
          ChangeRecord(
            table: 'users',
            pk: '2',
            field: 'name',
            crdtType: CrdtType.lww,
            hlc: hlc(200, 'test-node'),
            nodeId: 'test-node',
            value: const JsonValue('Pending'),
          ),
        ),
      ];
      final store = CrdtStore('test-node', clock, storage: storage);
      await store.ready;
      final pending = store.getPendingChanges();
      expect(pending, hasLength(1));
      expect(pending[0].value!.value, 'Pending');
    });

    test('storage errors do not break synchronous operations', () async {
      storage.failWrites = Exception('Storage write failed');
      storage.failPending = Exception('Storage write failed');
      final store = CrdtStore('test-node', clock, storage: storage);
      await store.ready;
      // Should not throw: persistence is fire-and-forget.
      expect(
        () => store.setField('users', '1', 'name', 'Alice'),
        returnsNormally,
      );
      expect(store.getDocument('users', '1'), isNotNull);
      // Dart: a flush reports that the write did not reach storage.
      await expectLater(
        store.flushPersistence(),
        throwsA(isA<ReplicaPersistFailed>()),
      );
      await expectLater(store.dispose(), throwsA(isA<ReplicaPersistFailed>()));
    });
  });

  group('atomic commits, storage failures, hydration and durability', () {
    test('with an atomic storage a document and its pending entry are written in one commit', () async {
      final storage = RecordingAtomicStorage();
      final store = CrdtStore(
        'n1',
        HybridClock('n1'),
        storage: storage,
        persistDebounce: Duration.zero,
      );
      await store.ready;
      final c = store.setField('t', '1', 'title', 'x')!;
      await store.flushPersistence();
      expect(storage.commits, hasLength(1));
      final commit = storage.commits.single;
      expect(commit.documents.keys, [('t', '1')]);
      expect(commit.documents[('t', '1')]!.fields['title']!.value!.value, 'x');
      expect(commit.pending!.single.change.hlc, c.hlc);
      // The plain calls are never used.
      expect(storage.saved, isEmpty);
      expect(storage.savedPending, isEmpty);
    });

    test('a debounced burst over several documents is one commit', () async {
      final storage = RecordingAtomicStorage();
      final store = CrdtStore('n1', HybridClock('n1'), storage: storage);
      await store.ready;
      store.setField('t', '1', 'a', 1);
      store.setField('t', '2', 'a', 2);
      store.deleteDocument('t', '1');
      await store.flushPersistence();
      expect(storage.commits, hasLength(1));
      expect(
        storage.commits.single.documents.keys,
        unorderedEquals([('t', '1'), ('t', '2')]),
      );
      expect(storage.commits.single.pending, hasLength(3));
    });

    test('a document-only flush commits without the pending queue', () async {
      final storage = RecordingAtomicStorage();
      final store = CrdtStore(
        'n1',
        HybridClock('n1'),
        storage: storage,
        persistDebounce: Duration.zero,
      );
      await store.ready;
      store.applyChanges([
        ChangeRecord(
          table: 't',
          pk: '1',
          field: 'f',
          crdtType: CrdtType.lww,
          hlc: hlc(5, 'srv'),
          nodeId: 'srv',
          value: const JsonValue(1),
        ),
      ]);
      await store.flushPersistence();
      expect(storage.commits.single.pending, isNull);
    });

    test('a flush over KeyValueReplicaStorage is one kv batch and reads nothing inside it', () async {
      final kv = _WatchingKv();
      final store = CrdtStore(
        'n1',
        HybridClock('n1'),
        storage: KeyValueReplicaStorage(kv),
      );
      await store.ready;
      kv.reset();
      store.setField('t', '1', 'a', 1);
      store.setField('t', '2', 'a', 1);
      await store.flushPersistence();
      expect(kv.batches, 1);
      expect(kv.puts, 0);
      expect(kv.readsInsideBatch, 0);
      expect(kv.entries.keys, containsAll(['doc/t/1', 'doc/t/2', 'pending']));
    });

    test('a failing put is reported, synchronous or not, and never escapes as an unhandled error', () async {
      for (final sync in [false, true]) {
        final errors = <Object>[];
        final storage = RecordingStorage()
          ..failWrites = StateError('put failed')
          ..failPending = StateError('put failed')
          ..throwSynchronously = sync;
        final store = CrdtStore(
          'n1',
          HybridClock('n1'),
          storage: storage,
          onStorageError: errors.add,
        );
        await store.ready;
        store.setField('t', '1', 'a', 1);
        await expectLater(
          store.flushPersistence(),
          throwsA(
            isA<ReplicaPersistFailed>().having(
              (e) => e.cause,
              'cause',
              isA<StateError>(),
            ),
          ),
          reason: 'sync: $sync',
        );
        // The queue is written first and failed, so the document was not tried.
        expect(errors, [isA<StateError>()], reason: 'sync: $sync');
        expect(storage.saved, isEmpty);
        expect(store.getDocument('t', '1')!['a'], 1);
        await expectLater(
          store.dispose(),
          throwsA(isA<ReplicaPersistFailed>()),
        );
      }
    });
    test('an atomic commit that fails once succeeds on the retry tick, with no further write', () {
      fakeAsync((async) {
        final errors = <Object>[];
        final storage = RecordingAtomicStorage()
          ..failCommit = StateError('busy');
        final store = CrdtStore(
          'n1',
          HybridClock('n1'),
          storage: storage,
          persistDebounce: Duration.zero,
          onStorageError: errors.add,
        );
        async.flushMicrotasks();
        store.setField('t', '1', 'a', 1);
        async.flushMicrotasks();
        expect(errors.single, isA<StateError>());
        expect(storage.commits, isEmpty);
        storage.failCommit = null;
        // The retry waits a tick; it is not a hot loop.
        async.elapse(const Duration(milliseconds: 10));
        expect(storage.commits, isEmpty);
        async.elapse(const Duration(milliseconds: 100));
        final commit = storage.commits.single;
        expect(commit.documents.keys, [('t', '1')]);
        expect(commit.pending!.single.change.field, 'a');
        async.elapse(const Duration(seconds: 5));
        expect(storage.commits, hasLength(1));
      });
    });

    test('a storage that stays down is retried with a growing delay, not in a loop', () {
      fakeAsync((async) {
        var attempts = 0;
        final storage = _CountingFailingAtomic(() => attempts++);
        final store = CrdtStore(
          'n1',
          HybridClock('n1'),
          storage: storage,
          persistDebounce: Duration.zero,
        );
        async.flushMicrotasks();
        store.setField('t', '1', 'a', 1);
        async.elapse(const Duration(seconds: 10));
        // 50ms, 100, 200, ... doubling: about eight tries in ten seconds.
        expect(attempts, inInclusiveRange(7, 10));
        unawaited(store.dispose().catchError((Object _) {}));
        async.flushMicrotasks();
        final after = attempts;
        async.elapse(const Duration(minutes: 5));
        expect(attempts, after); // dispose stopped the retries
      });
    });
    test('a failing load fails closed: ready errors with ReplicaUnavailable and the store refuses writes', () async {
      for (final which in ['pending', 'state']) {
        for (final sync in [false, true]) {
          final errors = <Object>[];
          final storage = RecordingStorage()..throwSynchronously = sync;
          if (which == 'pending') {
            storage.failLoadPending = StateError('closed');
          } else {
            storage.failLoad = StateError('closed');
          }
          final store = CrdtStore(
            'n1',
            HybridClock('n1'),
            storage: storage,
            onStorageError: errors.add,
          );
          await expectLater(
            store.ready,
            throwsA(
              isA<ReplicaUnavailable>().having(
                (e) => e.cause,
                'cause',
                isA<StateError>(),
              ),
            ),
            reason: '$which sync: $sync',
          );
          expect(errors.single, isA<StateError>());
          expect(() => store.setField('t', '1', 'a', 1), throwsStateError);
          expect(() => store.applyChanges([]), throwsStateError);
          await expectLater(store.flushPersistence(), throwsStateError);
          await store.dispose();
          expect(storage.saved, isEmpty);
          expect(storage.savedPending, isEmpty);
        }
      }
    });

    test('an unread pending queue is never overwritten, even by a write attempted while it loads', () async {
      final gate = Completer<void>();
      final kv = _GatedKv(gate.future);
      kv.entries['pending'] = 'stored queue bytes';
      final storage = KeyValueReplicaStorage(kv);
      final store = CrdtStore(
        'n1',
        HybridClock('n1'),
        storage: storage,
        persistDebounce: Duration.zero,
      );
      expect(
        () => store.setField('t', '1', 'a', 1),
        throwsA(isA<StateError>()),
      );
      kv.failGet = StateError('transient');
      gate.complete();
      await expectLater(store.ready, throwsA(isA<ReplicaUnavailable>()));
      expect(() => store.setField('t', '1', 'b', 2), throwsStateError);
      await store.dispose();
      expect(kv.puts, 0);
      expect(kv.batches, 0);
      expect(kv.entries['pending'], 'stored queue bytes');
    });
    test('a store nobody awaits does not raise its load failure as an unhandled error', () async {
      final storage = RecordingStorage()
        ..failLoadPending = StateError('closed');
      CrdtStore('n1', HybridClock('n1'), storage: storage);
      await pumpEventQueue();
    });

    test('a corrupt stored document is skipped on hydrate, reported with its key, and the rest load', () async {
      final kv = MapReplicaKeyValue();
      final first = CrdtStore(
        'n1',
        HybridClock('n1'),
        storage: KeyValueReplicaStorage(kv),
      );
      await first.ready;
      first.setField('t', 'good', 'a', 1);
      first.setField('t', 'bad', 'a', 1);
      await first.flushPersistence();
      kv.entries['doc/t/bad'] = '{"table":"t","pk":"bad","fields":7}';

      final errors = <Object>[];
      final second = CrdtStore(
        'n1',
        HybridClock('n1'),
        storage: KeyValueReplicaStorage(kv),
        onStorageError: errors.add,
      );
      await expectLater(second.ready, completes);
      expect(
        errors.single,
        isA<FormatException>().having(
          (e) => e.message,
          'message',
          contains('doc/t/bad'),
        ),
      );
      expect(second.getDocument('t', 'good')!['a'], 1);
      expect(second.getDocument('t', 'bad'), isNull);
      expect(second.pending, hasLength(2));
    });

    test('a corrupt stored pending queue fails closed', () async {
      final kv = MapReplicaKeyValue()..entries['pending'] = '{"not":"a list"}';
      final errors = <Object>[];
      final store = CrdtStore(
        'n1',
        HybridClock('n1'),
        storage: KeyValueReplicaStorage(kv),
        onStorageError: errors.add,
      );
      await expectLater(
        store.ready,
        throwsA(
          isA<ReplicaUnavailable>().having(
            (e) => e.cause,
            'cause',
            isA<FormatException>(),
          ),
        ),
      );
      expect(errors.single, isA<FormatException>());
      expect(() => store.setField('t', '1', 'a', 1), throwsStateError);
      expect(kv.entries['pending'], '{"not":"a list"}');
    });

    test(
      'a session closed underneath the store is reported and does not crash',
      () async {
        final kv = _ClosableKv();
        final errors = <Object>[];
        final store = CrdtStore(
          'n1',
          HybridClock('n1'),
          storage: KeyValueReplicaStorage(kv),
          onStorageError: errors.add,
        );
        await store.ready;
        kv.closed = true;
        store.setField('t', '1', 'a', 1);
        store.deleteDocument('t', '1');
        await expectLater(
          store.flushPersistence(),
          throwsA(isA<ReplicaPersistFailed>()),
        );
        expect(errors, isNotEmpty);
        expect(errors, everyElement(isA<StateError>()));
        expect(store.getDocumentState('t', '1')!.tombstone, isTrue);
        await expectLater(
          store.dispose(),
          throwsA(isA<ReplicaPersistFailed>()),
        );
      },
    );
    test(
      'dispose flushes first, then stops persisting, and writes after it throw',
      () async {
        final storage = RecordingStorage();
        final store = CrdtStore('n1', HybridClock('n1'), storage: storage);
        await store.ready;
        store.setField('t', '1', 'a', 1);
        var done = false;
        final sub = store.documentChanges.listen(
          (_) {},
          onDone: () => done = true,
        );
        await store.dispose();
        expect(storage.saved, hasLength(1));
        expect(storage.savedPending.single, hasLength(1));
        expect(done, isTrue);
        expect(() => store.setField('t', '1', 'b', 2), throwsStateError);
        expect(() => store.applyChanges([]), throwsStateError);
        expect(store.undo, throwsStateError);
        await store.flushPersistence();
        expect(storage.saved, hasLength(1));
        expect(store.getDocument('t', '1')!['a'], 1);
        await sub.cancel();
      },
    );

    test(
      'importState deletes the documents the snapshot leaves out from storage',
      () async {
        final storage = RecordingStorage();
        final store = CrdtStore(
          'n1',
          HybridClock('n1'),
          storage: storage,
          persistDebounce: Duration.zero,
        );
        await store.ready;
        store.setField('users', '1', 'name', 'Alice');
        store.setField('posts', '1', 'title', 'Hi');
        final snapshot = store.exportState()..tables.remove('posts');
        store.importState(snapshot);
        await store.flushPersistence();
        expect(storage.deleted, [('posts', '1')]);
      },
    );

    test('dropDocument removes the document from memory and storage', () async {
      final storage = RecordingStorage();
      final store = CrdtStore(
        'n1',
        HybridClock('n1'),
        storage: storage,
        persistDebounce: Duration.zero,
      );
      await store.ready;
      store.setField('t', '1', 'a', 1);
      store.dropDocument('t', '1');
      await store.flushPersistence();
      expect(store.getDocumentState('t', '1'), isNull);
      expect(store.tables, isEmpty);
      expect(storage.deleted, [('t', '1')]);
    });

    test('a write before ready throws and changes nothing', () async {
      final kv = MapReplicaKeyValue();
      final first = CrdtStore(
        'n1',
        HybridClock('n1', nowMs: () => 9000000),
        storage: KeyValueReplicaStorage(kv),
        persistDebounce: Duration.zero,
      );
      await first.ready;
      final old = first.setField('t', '1', 'title', 'old')!;
      await first.dispose();
      final second = CrdtStore(
        'n1',
        HybridClock('n1', nowMs: () => 1000),
        storage: KeyValueReplicaStorage(kv),
        persistDebounce: Duration.zero,
      );
      final before = second.clock.last;
      expect(
        () => second.setField('t', '1', 'title', 'typed before ready'),
        throwsA(
          isA<StateError>().having(
            (e) => e.message,
            'message',
            'await store.ready before writing',
          ),
        ),
      );
      expect(() => second.applyChanges([]), throwsStateError);
      expect(second.clock.last, before);
      await second.ready;
      expect(second.getDocument('t', '1')!['title'], 'old');
      final c = second.setField('t', '1', 'title', 'new')!;
      expect(second.getDocument('t', '1')!['title'], 'new');
      expect(c.hlc.isAfter(old.hlc), isTrue);
    });

    test(
      'persisted pending changes stay first, in their stored order',
      () async {
        final storage = RecordingStorage()
          ..preloadedPending = [
            for (final pk in ['9', '8'])
              PendingChange(
                ChangeRecord(
                  table: 't',
                  pk: pk,
                  field: 'f',
                  crdtType: CrdtType.lww,
                  hlc: hlc(1, 'n1'),
                  nodeId: 'n1',
                ),
              ),
          ];
        final store = CrdtStore('n1', HybridClock('n1'), storage: storage);
        await store.ready;
        store.setField('t', '1', 'a', 'fresh');
        expect(store.pending.map((p) => p.change.pk), ['9', '8', '1']);
      },
    );
    test('a failed queue write is retried by the next flush, so a pulled document does not strand the edit', () async {
      // Plain storage: the queue write fails on the local edit, then a pull
      // flushes a different document. The edit must still reach the stored
      // queue and its document.
      final kv = _FlakyKv()..failPutsTo = {'pending'};
      final errors = <Object>[];
      final s = CrdtStore(
        'a',
        HybridClock('a', nowMs: () => 1000),
        storage: _PlainOnly(KeyValueReplicaStorage(kv)),
        persistDebounce: Duration.zero,
        onStorageError: errors.add,
      );
      await s.ready;
      final edit = s.setField('t', '1', 'title', 'offline edit')!;
      await expectLater(
        s.flushPersistence(),
        throwsA(isA<ReplicaPersistFailed>()),
      );
      expect(
        kv.entries.containsKey('doc/t/1'),
        isFalse,
        reason: 'no document without its queue entry',
      );
      kv.failPutsTo = {};
      s.applyChanges([
        ChangeRecord(
          table: 't',
          pk: '2',
          field: 'x',
          crdtType: CrdtType.lww,
          hlc: hlc(5, 'srv'),
          nodeId: 'srv',
          value: const JsonValue('r'),
        ),
      ]);
      await s.flushPersistence();
      await s.dispose();
      expect(errors, isNotEmpty);

      final restarted = CrdtStore(
        'a',
        HybridClock('a', nowMs: () => 1000),
        storage: KeyValueReplicaStorage(kv),
      );
      await restarted.ready;
      expect(restarted.getDocument('t', '1')!['title'], 'offline edit');
      expect(restarted.getDocument('t', '2')!['x'], 'r');
      expect(restarted.getPendingChanges().single.hlc, edit.hlc);
    });

    test('a failed document write is retried while the stored queue is not rewritten', () async {
      final storage = RecordingStorage();
      final s = CrdtStore(
        'a',
        HybridClock('a', nowMs: () => 1000),
        storage: storage,
        persistDebounce: Duration.zero,
      );
      await s.ready;
      storage.failWrites = StateError('disk full');
      s.setField('t', '1', 'a', 1);
      await expectLater(
        s.flushPersistence(),
        throwsA(isA<ReplicaPersistFailed>()),
      );
      expect(storage.savedPending, hasLength(1));
      storage.failWrites = null;
      await s.flushPersistence();
      expect(storage.saved.single.pk, '1');
      expect(storage.savedPending, hasLength(1));
    });

    test('a failed atomic commit keeps the edit through a later pull until a commit succeeds', () async {
      final storage = RecordingAtomicStorage();
      final s = CrdtStore(
        'a',
        HybridClock('a', nowMs: () => 1000),
        storage: storage,
        persistDebounce: Duration.zero,
      );
      await s.ready;
      storage.failCommit = StateError('transient');
      s.setField('t', '1', 'title', 'edit');
      await expectLater(
        s.flushPersistence(),
        throwsA(isA<ReplicaPersistFailed>()),
      );
      storage.failCommit = null;
      s.applyChanges([
        ChangeRecord(
          table: 't',
          pk: '2',
          field: 'x',
          crdtType: CrdtType.lww,
          hlc: hlc(5, 'srv'),
          nodeId: 'srv',
          value: const JsonValue('r'),
        ),
      ]);
      await s.dispose();
      final docs = {for (final c in storage.commits) ...c.documents.keys};
      expect(docs, containsAll([('t', '1'), ('t', '2')]));
      expect(
        [
          for (final c in storage.commits)
            if (c.pending != null) c.pending!.single.change.field,
        ],
        ['title'],
      );
    });

    test('dispose after a throwing beforePersist completes with ReplicaPersistFailed after closing', () async {
      final storage = RecordingStorage();
      final errors = <Object>[];
      final s = CrdtStore(
        'a',
        HybridClock('a', nowMs: () => 1000),
        storage: storage,
        persistDebounce: const Duration(milliseconds: 50),
        onStorageError: errors.add,
      );
      await s.ready;
      s.use(_ThrowingPersist());
      s.setField('t', '1', 'title', 'x');
      var closed = false;
      final sub = s.documentChanges.listen((_) {}, onDone: () => closed = true);
      await expectLater(
        s.dispose(),
        throwsA(
          isA<ReplicaPersistFailed>().having(
            (e) => e.cause,
            'cause',
            isA<StateError>(),
          ),
        ),
      );
      expect(closed, isTrue);
      expect(storage.saved, isEmpty);
      expect(storage.savedPending, isEmpty);
      expect(errors, isNotEmpty);
      expect(() => s.setField('t', '1', 'title', 'y'), throwsStateError);
      await sub.cancel();
    });

    test('a stored queue longer than the bound is trimmed after hydration through the overflow path', () async {
      final storage = RecordingStorage()
        ..preloadedPending = [
          for (var i = 0; i < 5; i++)
            PendingChange(
              ChangeRecord(
                table: 't',
                pk: '$i',
                field: 'f',
                crdtType: CrdtType.lww,
                hlc: hlc(i + 1, 'n1'),
                nodeId: 'n1',
              ),
            ),
        ];
      final s = CrdtStore(
        'n1',
        HybridClock('n1'),
        storage: storage,
        persistDebounce: Duration.zero,
        maxPendingChanges: 3,
      );
      final dropped = <ChangeRecord>[];
      s.onPendingOverflow(dropped.addAll);
      await s.ready;
      expect(dropped.map((c) => c.pk), ['0', '1']);
      expect(s.pending.map((p) => p.change.pk), ['2', '3', '4']);
      await s.flushPersistence();
      expect(storage.savedPending.last.map((p) => p.change.pk), [
        '2',
        '3',
        '4',
      ]);
    });

    test('no storage failure ever escapes as an unhandled error', () async {
      final unhandled = <Object>[];
      await runZonedGuarded(() async {
        final storage = RecordingAtomicStorage()
          ..failCommit = StateError('commit')
          ..throwSynchronously = true;
        final s = CrdtStore(
          'a',
          HybridClock('a', nowMs: () => 1000),
          storage: storage,
          persistDebounce: const Duration(milliseconds: 1),
        );
        await s.ready;
        s.setField('t', '1', 'a', 1);
        await Future<void>.delayed(const Duration(milliseconds: 20));
        s.setField('t', '1', 'a', 2);
        await s.dispose().catchError((Object _) {});
        final bad = RecordingStorage()..failLoadPending = StateError('load');
        CrdtStore('b', HybridClock('b', nowMs: () => 1000), storage: bad);
        final bad2 = RecordingStorage()
          ..failLoad = StateError('loadsync')
          ..throwSynchronously = true;
        final s2 = CrdtStore(
          'c',
          HybridClock('c', nowMs: () => 1000),
          storage: bad2,
        );
        unawaited(s2.dispose());
        await Future<void>.delayed(const Duration(milliseconds: 20));
      }, (e, st) => unhandled.add(e));
      expect(unhandled, isEmpty);
    });

    test('table, key and field names are held as the server holds them', () {
      final store = CrdtStore('n1', HybridClock('n1'));
      store.setField('t\ud800', 'k\udc00', 'f\ud800', 'v\ud800');
      expect(store.getDocument('t�', 'k�')!['f�'], 'v�');
      expect(store.getDocument('t\ud800', 'k\udc00')!['f�'], 'v�');
      final c = store.getPendingChanges().single;
      expect(
        [c.table, c.pk, c.field, c.value!.value],
        ['t�', 'k�', 'f�', 'v�'],
      );
    });
  });
}

/// Delegates to a [MapReplicaKeyValue], which is final.
class _DelegatingKv implements ReplicaKeyValue {
  final MapReplicaKeyValue inner = MapReplicaKeyValue();

  Map<String, String> get entries => inner.entries;

  @override
  Future<String?> get(String key) => inner.get(key);

  @override
  Future<void> put(String key, String value) => inner.put(key, value);

  @override
  Future<void> delete(String key) => inner.delete(key);

  @override
  Future<Map<String, String>> scan(String prefix) => inner.scan(prefix);

  @override
  Future<void> batch(void Function(ReplicaKeyValueBatch batch) build) =>
      inner.batch(build);
}

/// A key-value store that counts writes and notices a read made while a
/// batch builder runs.
final class _WatchingKv extends _DelegatingKv {
  int batches = 0;
  int puts = 0;
  int readsInsideBatch = 0;
  bool _inBatch = false;

  void reset() {
    batches = 0;
    puts = 0;
    readsInsideBatch = 0;
  }

  @override
  Future<String?> get(String key) {
    if (_inBatch) readsInsideBatch++;
    return super.get(key);
  }

  @override
  Future<Map<String, String>> scan(String prefix) {
    if (_inBatch) readsInsideBatch++;
    return super.scan(prefix);
  }

  @override
  Future<void> put(String key, String value) {
    puts++;
    return super.put(key, value);
  }

  @override
  Future<void> batch(void Function(ReplicaKeyValueBatch batch) build) {
    batches++;
    return super.batch((b) {
      _inBatch = true;
      try {
        build(b);
      } finally {
        _inBatch = false;
      }
    });
  }
}

/// A key-value store that throws StateError once closed, as a closed
/// forge_client memory session does.
final class _ClosableKv extends _DelegatingKv {
  bool closed = false;

  void _check() {
    if (closed) throw StateError('session closed');
  }

  @override
  Future<String?> get(String key) async {
    _check();
    return super.get(key);
  }

  @override
  Future<Map<String, String>> scan(String prefix) async {
    _check();
    return super.scan(prefix);
  }

  @override
  Future<void> put(String key, String value) async {
    _check();
    return super.put(key, value);
  }

  @override
  Future<void> delete(String key) async {
    _check();
    return super.delete(key);
  }

  @override
  Future<void> batch(void Function(ReplicaKeyValueBatch batch) build) {
    _check();
    return super.batch(build);
  }
}

/// A key-value store whose reads wait for [gate], can then fail, and which
/// counts every write.
final class _GatedKv extends _DelegatingKv {
  _GatedKv(this.gate);
  final Future<void> gate;
  Object? failGet;
  int puts = 0;
  int batches = 0;

  @override
  Future<String?> get(String key) async {
    await gate;
    final f = failGet;
    if (f != null) throw f;
    return super.get(key);
  }

  @override
  Future<Map<String, String>> scan(String prefix) async {
    await gate;
    return super.scan(prefix);
  }

  @override
  Future<void> put(String key, String value) {
    puts++;
    return super.put(key, value);
  }

  @override
  Future<void> batch(void Function(ReplicaKeyValueBatch batch) build) {
    batches++;
    return super.batch(build);
  }
}

/// An atomic storage whose every commit fails, calling [onAttempt] first.
final class _CountingFailingAtomic extends RecordingAtomicStorage {
  _CountingFailingAtomic(this.onAttempt);
  final void Function() onAttempt;

  @override
  Future<void> commit({
    Map<(String, String), DocumentState?> documents = const {},
    List<PendingChange>? pending,
  }) {
    onAttempt();
    return Future<void>.error(StateError('down'));
  }
}

/// A key-value store whose puts to the keys in [failPutsTo] fail, inside a
/// batch or not.
final class _FlakyKv extends _DelegatingKv {
  Set<String> failPutsTo = {};

  @override
  Future<void> put(String key, String value) {
    if (failPutsTo.contains(key)) {
      return Future<void>.error(StateError('put $key failed'));
    }
    return super.put(key, value);
  }
}

/// Hides [AtomicReplicaStorage], so the store uses the plain calls.
final class _PlainOnly implements ReplicaStorage {
  _PlainOnly(this.inner);
  final ReplicaStorage inner;

  @override
  Future<Map<String, Map<String, DocumentState>>> loadState({
    void Function(FormatException error)? onUnreadable,
  }) => inner.loadState(onUnreadable: onUnreadable);

  @override
  Future<List<PendingChange>> loadPendingChanges() =>
      inner.loadPendingChanges();

  @override
  Future<void> saveDocument(String table, String pk, DocumentState doc) =>
      inner.saveDocument(table, pk, doc);

  @override
  Future<void> deleteDocument(String table, String pk) =>
      inner.deleteDocument(table, pk);

  @override
  Future<void> savePendingChanges(List<PendingChange> changes) =>
      inner.savePendingChanges(changes);
}

final class _ThrowingPersist extends StorePlugin {
  @override
  String get name => 'encryptor';

  @override
  DocumentState beforePersist(String table, String pk, DocumentState doc) =>
      throw StateError('no key');
}
