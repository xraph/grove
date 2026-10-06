// Port of crdt-js src/__tests__/storage.test.ts lines 94-290 ("CRDTStore with
// StorageAdapter"), case for case, against a recording ReplicaStorage fake.
// The group after it covers the Dart persistence rules: atomic commits,
// storage failures, corrupt documents, a closed session and dispose.
import 'dart:async';

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
      final store = CrdtStore('test-node', clock, storage: storage, persistDebounce: Duration.zero);
      await store.ready;
      store.setField('users', '1', 'name', 'Alice');
      // Allow the fire-and-forget persistence to complete.
      await pumpEventQueue();
      expect(storage.saved.length, greaterThanOrEqualTo(1));
      expect(storage.saved[0].table, 'users');
      expect(storage.saved[0].pk, '1');
    });

    test('savePendingChanges called after setField', () async {
      final store = CrdtStore('test-node', clock, storage: storage, persistDebounce: Duration.zero);
      await store.ready;
      store.setField('users', '1', 'name', 'Alice');
      await pumpEventQueue();
      expect(storage.savedPending.length, greaterThanOrEqualTo(1));
      expect(storage.savedPending[0], hasLength(1));
    });

    test('savePendingChanges called after incrementCounter', () async {
      final store = CrdtStore('test-node', clock, storage: storage, persistDebounce: Duration.zero);
      await store.ready;
      store.incrementCounter('items', '1', 'views', 1);
      await pumpEventQueue();
      expect(storage.savedPending, isNotEmpty);
    });

    test('savePendingChanges called after decrementCounter', () async {
      final store = CrdtStore('test-node', clock, storage: storage, persistDebounce: Duration.zero);
      await store.ready;
      store.decrementCounter('items', '1', 'stock', 1);
      await pumpEventQueue();
      expect(storage.savedPending, isNotEmpty);
    });

    test('saveDocument called after addToSet', () async {
      final store = CrdtStore('test-node', clock, storage: storage, persistDebounce: Duration.zero);
      await store.ready;
      store.addToSet('items', '1', 'tags', ['cool']);
      await pumpEventQueue();
      expect(storage.saved, isNotEmpty);
    });

    test('saveDocument called after removeFromSet', () async {
      final store = CrdtStore('test-node', clock, storage: storage, persistDebounce: Duration.zero);
      await store.ready;
      store.addToSet('items', '1', 'tags', ['cool']);
      store.removeFromSet('items', '1', 'tags', ['cool']);
      await pumpEventQueue();
      expect(storage.saved, hasLength(2));
    });

    test('saveDocument called after deleteDocument', () async {
      final store = CrdtStore('test-node', clock, storage: storage, persistDebounce: Duration.zero);
      await store.ready;
      store.setField('users', '1', 'name', 'Alice');
      store.deleteDocument('users', '1');
      await pumpEventQueue();
      // saveDocument called for setField + deleteDocument.
      expect(storage.saved, hasLength(2));
    });

    test('saveDocument called after applyChanges', () async {
      final store = CrdtStore('test-node', clock, storage: storage, persistDebounce: Duration.zero);
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
      final store = CrdtStore('test-node', clock, storage: storage, persistDebounce: Duration.zero);
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
          '1': DocumentState(table: 'users', pk: '1', fields: {
            'name': FieldState(type: CrdtType.lww, hlc: hlc(100, 'other'), nodeId: 'other', value: const JsonValue('Persisted')),
          }),
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
        PendingChange(ChangeRecord(
          table: 'users',
          pk: '2',
          field: 'name',
          crdtType: CrdtType.lww,
          hlc: hlc(200, 'test-node'),
          nodeId: 'test-node',
          value: const JsonValue('Pending'),
        )),
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
      expect(() => store.setField('users', '1', 'name', 'Alice'), returnsNormally);
      expect(store.getDocument('users', '1'), isNotNull);
      await store.flushPersistence();
    });
  });

  group('beyond the crdt-js behaviour', () {
    test('with an atomic storage a document and its pending entry are written in one commit', () async {
      final storage = RecordingAtomicStorage();
      final store = CrdtStore('n1', HybridClock('n1'), storage: storage, persistDebounce: Duration.zero);
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
      expect(storage.commits.single.documents.keys, unorderedEquals([('t', '1'), ('t', '2')]));
      expect(storage.commits.single.pending, hasLength(3));
    });

    test('a document-only flush commits without the pending queue', () async {
      final storage = RecordingAtomicStorage();
      final store = CrdtStore('n1', HybridClock('n1'), storage: storage, persistDebounce: Duration.zero);
      await store.ready;
      store.applyChanges([
        ChangeRecord(table: 't', pk: '1', field: 'f', crdtType: CrdtType.lww, hlc: hlc(5, 'srv'), nodeId: 'srv', value: const JsonValue(1)),
      ]);
      await store.flushPersistence();
      expect(storage.commits.single.pending, isNull);
    });

    test('a flush over KeyValueReplicaStorage is one kv batch and reads nothing inside it', () async {
      final kv = _WatchingKv();
      final store = CrdtStore('n1', HybridClock('n1'), storage: KeyValueReplicaStorage(kv));
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
        final store = CrdtStore('n1', HybridClock('n1'), storage: storage, onStorageError: errors.add);
        await store.ready;
        store.setField('t', '1', 'a', 1);
        await store.flushPersistence();
        expect(errors, hasLength(2), reason: 'sync: $sync');
        expect(errors, everyElement(isA<StateError>()));
        expect(store.getDocument('t', '1')!['a'], 1);
      }
    });

    test('a failing commit is reported and the next flush still writes', () async {
      final errors = <Object>[];
      final storage = RecordingAtomicStorage()..failCommit = StateError('busy');
      final store = CrdtStore('n1', HybridClock('n1'), storage: storage, persistDebounce: Duration.zero, onStorageError: errors.add);
      await store.ready;
      store.setField('t', '1', 'a', 1);
      await store.flushPersistence();
      expect(errors.single, isA<StateError>());
      storage.failCommit = null;
      store.setField('t', '1', 'b', 2);
      await store.flushPersistence();
      expect(storage.commits.single.documents[('t', '1')]!.fields.keys, unorderedEquals(['a', 'b']));
    });

    test('a failing load is reported and ready still completes with a usable store', () async {
      for (final sync in [false, true]) {
        final errors = <Object>[];
        final storage = RecordingStorage()
          ..failLoad = StateError('closed')
          ..failLoadPending = StateError('closed')
          ..throwSynchronously = sync;
        final store = CrdtStore('n1', HybridClock('n1'), storage: storage, onStorageError: errors.add);
        await expectLater(store.ready, completes);
        expect(errors, hasLength(2), reason: 'sync: $sync');
        expect(store.setField('t', '1', 'a', 1), isNotNull);
      }
    });

    test('a corrupt stored document is skipped on hydrate, reported with its key, and the rest load', () async {
      final kv = MapReplicaKeyValue();
      final first = CrdtStore('n1', HybridClock('n1'), storage: KeyValueReplicaStorage(kv));
      await first.ready;
      first.setField('t', 'good', 'a', 1);
      first.setField('t', 'bad', 'a', 1);
      await first.flushPersistence();
      kv.entries['doc/t/bad'] = '{"table":"t","pk":"bad","fields":7}';

      final errors = <Object>[];
      final second = CrdtStore('n1', HybridClock('n1'), storage: KeyValueReplicaStorage(kv), onStorageError: errors.add);
      await expectLater(second.ready, completes);
      expect(errors.single, isA<FormatException>().having((e) => e.message, 'message', contains('doc/t/bad')));
      expect(second.getDocument('t', 'good')!['a'], 1);
      expect(second.getDocument('t', 'bad'), isNull);
      expect(second.pending, hasLength(2));
    });

    test('a corrupt stored pending queue is reported and ready completes', () async {
      final kv = MapReplicaKeyValue()..entries['pending'] = '{"not":"a list"}';
      final errors = <Object>[];
      final store = CrdtStore('n1', HybridClock('n1'), storage: KeyValueReplicaStorage(kv), onStorageError: errors.add);
      await expectLater(store.ready, completes);
      expect(errors.single, isA<FormatException>());
      expect(store.pending, isEmpty);
    });

    test('a session closed underneath the store is reported and does not crash', () async {
      final kv = _ClosableKv();
      final errors = <Object>[];
      final store = CrdtStore('n1', HybridClock('n1'), storage: KeyValueReplicaStorage(kv), onStorageError: errors.add);
      await store.ready;
      kv.closed = true;
      store.setField('t', '1', 'a', 1);
      store.deleteDocument('t', '1');
      await store.flushPersistence();
      expect(errors, isNotEmpty);
      expect(errors, everyElement(isA<StateError>()));
      expect(store.getDocumentState('t', '1')!.tombstone, isTrue);
      await store.dispose();
    });

    test('dispose flushes first, then stops persisting, and writes after it throw', () async {
      final storage = RecordingStorage();
      final store = CrdtStore('n1', HybridClock('n1'), storage: storage);
      await store.ready;
      store.setField('t', '1', 'a', 1);
      var done = false;
      final sub = store.documentChanges.listen((_) {}, onDone: () => done = true);
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
    });

    test('importState deletes the documents the snapshot leaves out from storage', () async {
      final storage = RecordingStorage();
      final store = CrdtStore('n1', HybridClock('n1'), storage: storage, persistDebounce: Duration.zero);
      await store.ready;
      store.setField('users', '1', 'name', 'Alice');
      store.setField('posts', '1', 'title', 'Hi');
      final snapshot = store.exportState()..tables.remove('posts');
      store.importState(snapshot);
      await store.flushPersistence();
      expect(storage.deleted, [('posts', '1')]);
    });

    test('dropDocument removes the document from memory and storage', () async {
      final storage = RecordingStorage();
      final store = CrdtStore('n1', HybridClock('n1'), storage: storage, persistDebounce: Duration.zero);
      await store.ready;
      store.setField('t', '1', 'a', 1);
      store.dropDocument('t', '1');
      await store.flushPersistence();
      expect(store.getDocumentState('t', '1'), isNull);
      expect(store.tables, isEmpty);
      expect(storage.deleted, [('t', '1')]);
    });

    test('writes made while hydration runs survive it, and persisted pending changes go first', () async {
      final gate = Completer<void>();
      final storage = _GatedStorage(gate.future)
        ..preloaded = {
          't': {
            '1': DocumentState(table: 't', pk: '1', fields: {
              'a': FieldState(type: CrdtType.lww, hlc: hlc(1, 'old'), nodeId: 'old', value: const JsonValue('stored')),
            }),
          },
        }
        ..preloadedPending = [
          PendingChange(ChangeRecord(table: 't', pk: '9', field: 'f', crdtType: CrdtType.lww, hlc: hlc(1, 'n1'), nodeId: 'n1')),
        ];
      final store = CrdtStore('n1', HybridClock('n1'), storage: storage);
      store.setField('t', '1', 'a', 'fresh');
      gate.complete();
      await store.ready;
      expect(store.getDocument('t', '1')!['a'], 'fresh');
      expect(store.pending.map((p) => p.change.pk), ['9', '1']);
    });

    test('table, key and field names are held as the server holds them', () {
      final store = CrdtStore('n1', HybridClock('n1'));
      store.setField('t\ud800', 'k\udc00', 'f\ud800', 'v\ud800');
      expect(store.getDocument('t�', 'k�')!['f�'], 'v�');
      expect(store.getDocument('t\ud800', 'k\udc00')!['f�'], 'v�');
      final c = store.getPendingChanges().single;
      expect([c.table, c.pk, c.field, c.value!.value], ['t�', 'k�', 'f�', 'v�']);
    });
  });
}

final class _GatedStorage extends RecordingStorage {
  _GatedStorage(this.gate);
  final Future<void> gate;

  @override
  Future<Map<String, Map<String, DocumentState>>> loadState({void Function(FormatException error)? onUnreadable}) async {
    await gate;
    return super.loadState(onUnreadable: onUnreadable);
  }
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
  Future<void> batch(void Function(ReplicaKeyValueBatch batch) build) => inner.batch(build);
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
