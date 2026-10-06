// Store-level plugin tests.
//
// The first groups port crdt-js src/__tests__/plugin.test.ts lines 105-445,
// except the presence and sync hook cases (Tasks 15 and 16), keeping each
// description. crdt-js drives those cases through a bare PluginManager; those
// manager cases are already ported in plugin_manager_test.dart (Task 9), so
// here each one drives the hook through the store instead, which shows the
// store invokes it. The last groups pin the fail-closed handling of each hook
// category at the store level.
//
// beforePull, afterPull, beforePush and afterPush are invoked by the sync
// engine (Task 16), and the presence hooks by the presence client (Task 15),
// not by the store, so they are tested there.
import 'package:grove_crdt/grove_crdt.dart';
import 'package:test/test.dart';

import 'support/store_fakes.dart';

HLC hlc(int ts, String node) => HLC(BigInt.from(ts), 0, node);

ChangeRecord remote(String field, Object? value, {int ts = 500, String pk = '1'}) => ChangeRecord(
      table: 'users',
      pk: pk,
      field: field,
      crdtType: CrdtType.lww,
      hlc: hlc(ts, 'remote'),
      nodeId: 'remote',
      value: JsonValue(value),
    );

WriteEvent withValue(WriteEvent ev, Object? value) => WriteEvent(
      table: ev.table,
      pk: ev.pk,
      field: ev.field,
      crdtType: ev.crdtType,
      value: value,
      change: ev.change.copyWith(value: JsonValue(value)),
      previousState: ev.previousState,
    );

({CrdtStore store, List<(Object, String)> pluginErrors, List<Object> storageErrors}) mk({ReplicaStorage? storage}) {
  final pluginErrors = <(Object, String)>[];
  final storageErrors = <Object>[];
  final store = CrdtStore(
    'n1',
    HybridClock('n1', nowMs: () => 1000),
    storage: storage,
    persistDebounce: Duration.zero,
    onPluginError: (e, name) => pluginErrors.add((e, name)),
    onStorageError: storageErrors.add,
  );
  return (store: store, pluginErrors: pluginErrors, storageErrors: storageErrors);
}

void main() {
  group('WriteHook', () {
    test('calls beforeWrite on registered plugins', () {
      final (:store, pluginErrors: _, storageErrors: _) = mk();
      final events = <WriteEvent>[];
      store.use(FnPlugin('w', onBeforeWrite: (ev) {
        events.add(ev);
        return ev;
      }));
      final c = store.setField('users', '1', 'name', 'Alice')!;
      expect(events, hasLength(1));
      expect(events[0].table, 'users');
      expect(events[0].pk, '1');
      expect(events[0].field, 'name');
      expect(events[0].crdtType, CrdtType.lww);
      expect(events[0].value, 'Alice');
      expect(events[0].change, same(c));
      expect(events[0].previousState, isNull);
    });

    test('allows plugins to transform write events', () {
      final (:store, pluginErrors: _, storageErrors: _) = mk();
      store.use(FnPlugin('w', onBeforeWrite: (ev) => withValue(ev, 'transformed')));
      final c = store.setField('users', '1', 'name', 'Alice')!;
      expect(c.value!.value, 'transformed');
      expect(store.getDocument('users', '1')!['name'], 'transformed');
      expect(store.getPendingChanges().single.value!.value, 'transformed');
    });

    test('allows plugins to reject writes by returning null', () {
      final (:store, pluginErrors: _, storageErrors: _) = mk();
      store.use(FnPlugin('w', onBeforeWrite: (_) => null));
      expect(store.setField('users', '1', 'name', 'Alice'), isNull);
      expect(store.insertIntoList('users', '1', 'items', 'x'), isNull);
      expect(store.insertText('users', '1', 'body', 0, 'x'), isNull);
      expect(store.setDocumentField('users', '1', 'meta', 'a', 1), isNull);
      expect(store.getDocumentState('users', '1'), isNull);
      expect(store.pendingCount, 0);
      expect(store.canUndo, isFalse);
    });

    test('calls afterWrite after successful writes', () {
      final (:store, pluginErrors: _, storageErrors: _) = mk();
      final events = <WriteEvent>[];
      Object? seenInHook;
      store.use(FnPlugin('w', onAfterWrite: (ev) {
        events.add(ev);
        seenInHook = store.getDocument('users', '1')?['name'];
      }));
      store.setField('users', '1', 'name', 'Alice');
      expect(events, hasLength(1));
      expect(events[0].field, 'name');
      expect(seenInHook, 'Alice'); // applied before afterWrite runs
    });

    test('chains multiple write hooks in order', () {
      final (:store, pluginErrors: _, storageErrors: _) = mk();
      final order = <int>[];
      store.use(FnPlugin('w1', onBeforeWrite: (ev) {
        order.add(1);
        return withValue(ev, 'from-1');
      }));
      store.use(FnPlugin('w2', onBeforeWrite: (ev) {
        order.add(2);
        expect(ev.value, 'from-1');
        return withValue(ev, 'from-2');
      }));
      store.setField('users', '1', 'name', 'Alice');
      expect(order, [1, 2]);
      expect(store.getDocument('users', '1')!['name'], 'from-2');
    });
  });

  group('MergeHook', () {
    test('calls beforeMerge on incoming changes', () {
      final (:store, pluginErrors: _, storageErrors: _) = mk();
      final events = <MergeEvent>[];
      store.use(FnPlugin('m', onBeforeMerge: (ev) {
        events.add(ev);
        return ev.remote;
      }));
      final change = remote('name', 'Alice');
      store.applyChanges([change]);
      expect(events, hasLength(1));
      expect(events[0].remote, same(change));
      expect(events[0].local, isNull);
      expect(events[0].conflictDetected, isFalse);
    });

    test('allows plugins to reject changes by returning null', () {
      final (:store, pluginErrors: _, storageErrors: _) = mk();
      store.use(FnPlugin('m', onBeforeMerge: (ev) => ev.remote.field == 'secret' ? null : ev.remote));
      final affected = store.applyChanges([remote('secret', 'x'), remote('name', 'Alice', pk: '2')]);
      expect(affected, {(table: 'users', pk: '2')});
      expect(store.getDocumentState('users', '1'), isNull);
      expect(store.getDocument('users', '2')!['name'], 'Alice');
    });

    test('calls afterMerge with conflict info', () {
      final (:store, pluginErrors: _, storageErrors: _) = mk();
      store.setField('users', '1', 'name', 'local');
      final events = <MergeEvent>[];
      store.use(FnPlugin('m', onAfterMerge: events.add));
      store.applyChanges([remote('name', 'remote', ts: 999000000000000)]);
      expect(events, hasLength(1));
      expect(events[0].conflictDetected, isTrue);
      expect(events[0].result!.value!.value, 'remote');
      expect(events[0].winnerNodeId, 'remote');
    });

    test('sets conflictDetected when local state exists', () {
      final (:store, pluginErrors: _, storageErrors: _) = mk();
      store.setField('users', '1', 'name', 'old');
      final localFs = store.getDocumentState('users', '1')!.fields['name'];
      final events = <MergeEvent>[];
      store.use(FnPlugin('m', onAfterMerge: events.add));
      store.applyChanges([remote('name', 'new', ts: 999000000000000)]);
      expect(events[0].conflictDetected, isTrue);
      expect(events[0].local, same(localFs));
    });
  });

  group('ReadHook', () {
    test('transforms documents via transformDocument', () {
      final (:store, pluginErrors: _, storageErrors: _) = mk();
      store.use(FnPlugin('r', onTransformDocument: (t, p, doc) => {...doc, 'extra': true}));
      store.setField('t', '1', 'name', 'Alice');
      expect(store.getDocument('t', '1'), {'_table': 't', '_pk': '1', 'name': 'Alice', 'extra': true});
    });

    test('filters documents returning null', () {
      final (:store, pluginErrors: _, storageErrors: _) = mk();
      store.use(FnPlugin('r', onTransformDocument: (t, p, doc) => p == 'hidden' ? null : doc));
      store.setField('t', 'hidden', 'name', 'Alice');
      store.setField('t', 'shown', 'name', 'Bob');
      expect(store.getDocument('t', 'hidden'), isNull);
      expect(store.getCollection('t').map((d) => d['_pk']), ['shown']);
    });

    test('transforms collections via transformCollection', () {
      final (:store, pluginErrors: _, storageErrors: _) = mk();
      store.use(FnPlugin('r', onTransformCollection: (t, docs) => docs.sublist(0, 1)));
      for (final id in ['1', '2', '3']) {
        store.setField('t', id, 'id', int.parse(id));
      }
      final result = store.getCollection('t');
      expect(result, hasLength(1));
      expect(result[0]['id'], 1);
    });
  });

  group('StorageHook', () {
    test('transforms documents before persist', () async {
      final storage = RecordingStorage();
      final (:store, pluginErrors: _, storageErrors: _) = mk(storage: storage);
      await store.ready;
      store.use(FnPlugin('st', onBeforePersist: (t, p, doc) => doc.copyWith(tombstone: true))); // e.g., encrypt
      store.setField('t', '1', 'name', 'Alice');
      await store.flushPersistence();
      expect(storage.saved.single.doc.tombstone, isTrue);
      // Memory keeps the untransformed document.
      expect(store.getDocument('t', '1')!['name'], 'Alice');
    });

    test('transforms documents after hydrate', () async {
      final storage = RecordingStorage()
        ..preloaded = {
          't': {'1': DocumentState(table: 't', pk: '1')},
        };
      final store = CrdtStore('n1', HybridClock('n1'), storage: storage);
      store.use(FnPlugin('st', onAfterHydrate: (t, p, doc) {
        return doc.copyWith(fields: {
          ...doc.fields,
          'injected': FieldState(type: CrdtType.lww, hlc: hlc(1, 'n'), nodeId: 'n', value: const JsonValue('hydrated')),
        });
      }));
      await store.ready;
      expect(store.getDocument('t', '1')!['injected'], 'hydrated');
    });
  });

  group('fail-closed hooks at the store level', () {
    test('a throwing beforeWrite cancels the write and is reported', () {
      final (:store, :pluginErrors, storageErrors: _) = mk();
      store.use(FnPlugin('broken', onBeforeWrite: (_) => throw StateError('boom')));
      expect(store.setField('t', '1', 'f', 1), isNull);
      expect(store.addToSet('t', '1', 's', ['x']), isNull);
      expect(store.getDocumentState('t', '1'), isNull);
      expect(store.pendingCount, 0);
      expect(store.canUndo, isFalse);
      expect(pluginErrors.map((e) => e.$2), ['broken', 'broken']);
    });

    test('a throwing beforeMerge skips that change and the rest of the batch applies', () {
      final (:store, :pluginErrors, storageErrors: _) = mk();
      store.use(FnPlugin('broken', onBeforeMerge: (ev) => ev.remote.pk == 'bad' ? throw StateError('boom') : ev.remote));
      final affected = store.applyChanges([remote('a', 1, pk: 'ok1'), remote('a', 1, pk: 'bad'), remote('a', 1, pk: 'ok2')]);
      expect(affected, {(table: 'users', pk: 'ok1'), (table: 'users', pk: 'ok2')});
      expect(store.getDocumentState('users', 'bad'), isNull);
      expect(pluginErrors.single.$2, 'broken');
    });

    test('a throwing transformDocument hides the document', () {
      final (:store, :pluginErrors, storageErrors: _) = mk();
      store.setField('t', '1', 'secret', 'x');
      store.use(FnPlugin('broken', onTransformDocument: (t, p, doc) => throw StateError('boom')));
      expect(store.getDocument('t', '1'), isNull);
      expect(store.getCollection('t'), isEmpty);
      expect(pluginErrors, isNotEmpty);
    });

    test('a throwing transformCollection hides the whole collection', () {
      final (:store, :pluginErrors, storageErrors: _) = mk();
      store.setField('t', '1', 'f', 1);
      store.use(FnPlugin('broken', onTransformCollection: (t, docs) => throw StateError('boom')));
      expect(store.getCollection('t'), isEmpty);
      expect(store.getDocument('t', '1'), isNotNull);
      expect(pluginErrors.single.$2, 'broken');
    });

    test('a throwing beforePersist stops that persist: nothing reaches storage, and it is reported', () async {
      for (final atomic in [false, true]) {
        final storage = atomic ? RecordingAtomicStorage() : RecordingStorage();
        final (:store, :pluginErrors, :storageErrors) = mk(storage: storage);
        await store.ready;
        var broken = true;
        store.use(FnPlugin('encryptor', onBeforePersist: (t, p, doc) {
          if (broken) throw StateError('no key');
          return doc.copyWith(fields: const {});
        }));
        store.setField('t', '1', 'secret', 'plaintext');
        await expectLater(store.flushPersistence(), throwsA(isA<ReplicaPersistFailed>()));
        expect(storage.saved, isEmpty, reason: 'atomic: $atomic');
        expect(storage.savedPending, isEmpty);
        if (storage is RecordingAtomicStorage) expect(storage.commits, isEmpty);
        // Once for the write's own flush, once more for flushPersistence's retry.
        expect(pluginErrors.map((e) => e.$2), ['encryptor', 'encryptor']);
        expect(storageErrors, [isA<StateError>(), isA<StateError>()]);
        // The write itself stands in memory.
        expect(store.getDocument('t', '1')!['secret'], 'plaintext');

        // The flush is retried once the encryptor works again.
        broken = false;
        await store.flushPersistence();
        if (storage is RecordingAtomicStorage) {
          expect(storage.commits.single.documents[('t', '1')]!.fields, isEmpty);
          expect(storage.commits.single.pending, hasLength(1));
        } else {
          expect(storage.saved.single.doc.fields, isEmpty);
          expect(storage.savedPending.single, hasLength(1));
        }
      }
    });

    test('a throwing afterHydrate fails that document: nothing undecrypted is served, and it is reported', () async {
      final storage = RecordingStorage()
        ..preloaded = {
          't': {
            'locked': DocumentState(table: 't', pk: 'locked', fields: {
              'secret': FieldState(type: CrdtType.lww, hlc: hlc(1, 'n'), nodeId: 'n', value: const JsonValue('ciphertext')),
            }),
            'open': DocumentState(table: 't', pk: 'open', fields: {
              'a': FieldState(type: CrdtType.lww, hlc: hlc(1, 'n'), nodeId: 'n', value: const JsonValue(1)),
            }),
          },
        };
      final pluginErrors = <(Object, String)>[];
      final storageErrors = <Object>[];
      final store = CrdtStore(
        'n1',
        HybridClock('n1'),
        storage: storage,
        onPluginError: (e, n) => pluginErrors.add((e, n)),
        onStorageError: storageErrors.add,
      );
      store.use(FnPlugin('decryptor', onAfterHydrate: (t, p, doc) => p == 'locked' ? throw StateError('bad key') : doc));
      await expectLater(store.ready, completes);
      expect(store.getDocument('t', 'locked'), isNull);
      expect(store.getDocumentState('t', 'locked'), isNull);
      expect(store.getCollection('t').map((d) => d['_pk']), ['open']);
      expect(pluginErrors.single.$2, 'decryptor');
      expect(storageErrors.single, isA<StateError>());
    });

    test('a throwing notification hook is reported and the write still stands', () {
      final (:store, :pluginErrors, storageErrors: _) = mk();
      store.use(FnPlugin(
        'noisy',
        onAfterWrite: (_) => throw StateError('after write'),
        onAfterMerge: (_) => throw StateError('after merge'),
      ));
      expect(store.setField('t', '1', 'a', 1), isNotNull);
      expect(store.applyChanges([remote('b', 2)]), {(table: 'users', pk: '1')});
      expect(store.getDocument('t', '1')!['a'], 1);
      expect(store.getDocument('users', '1')!['b'], 2);
      expect(pluginErrors.map((e) => e.$2), ['noisy', 'noisy']);
    });
  });
}
