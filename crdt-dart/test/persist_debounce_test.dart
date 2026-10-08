// Port of crdt-js src/__tests__/persist-debounce.test.ts, case for case. Timers
// run under fake_async instead of real sleeps.
import 'package:fake_async/fake_async.dart';
import 'package:grove_crdt/grove_crdt.dart';
import 'package:test/test.dart';

import 'support/store_fakes.dart';

void main() {
  group('persistence debounce', () {
    test('coalesces rapid writes into one savePendingChanges call', () {
      fakeAsync((async) {
        final storage = RecordingStorage();
        final store = CrdtStore(
          'n1',
          HybridClock('n1'),
          storage: storage,
          persistDebounce: const Duration(milliseconds: 20),
        );
        async.flushMicrotasks(); // ready
        for (var i = 0; i < 50; i++) {
          store.setField('t', 'p', 'f$i', i);
        }
        expect(storage.savedPending, isEmpty);
        var flushed = false;
        store.flushPersistence().then((_) => flushed = true);
        async.flushMicrotasks();
        expect(flushed, isTrue);
        expect(storage.savedPending, hasLength(1));
        expect(storage.savedPending[0], hasLength(50));
      });
    });

    test('persistDebounceMs: 0 writes synchronously', () async {
      final storage = RecordingStorage();
      final store = CrdtStore(
        'n1',
        HybridClock('n1'),
        storage: storage,
        persistDebounce: Duration.zero,
      );
      // Changed from crdt-js: nothing persists until hydration completes, so a
      // write cannot overwrite what is still being read. After it, a write is
      // synchronous.
      await store.ready;
      store.setField('t', 'p', 'f', 1);
      expect(storage.savedPending, hasLength(1));
    });

    test('does not re-persist pending changes on a document-only drain (applyChanges)', () {
      // applyChanges only touches document state, never the pending queue: the
      // debounce drain must not re-write an unchanged queue just because a
      // document write happened to share its timer.
      fakeAsync((async) {
        final storage = RecordingStorage();
        final store = CrdtStore(
          'n1',
          HybridClock('n1'),
          storage: storage,
          persistDebounce: const Duration(milliseconds: 10),
        );
        async.flushMicrotasks(); // ready
        store.applyChanges([
          ChangeRecord(
            table: 't',
            pk: 'p',
            field: 'f',
            crdtType: CrdtType.lww,
            hlc: HLC(BigInt.one, 0, 'remote'),
            nodeId: 'remote',
            value: const JsonValue('hi'),
          ),
        ]);
        // Let the debounce timer fire and drain.
        async.elapse(const Duration(milliseconds: 30));
        expect(storage.saved, hasLength(1));
        expect(storage.savedPending, isEmpty);
      });
    });

    test('does not re-persist pending changes on a document-only flush (undo)', () {
      // Changed from crdt-js: undo here emits a compensating change (it reaches
      // the server), so it is no longer document-only and the pending queue is
      // written exactly once by the flush. The document-only flush gate this
      // case pinned is pinned by the compact case below.
      fakeAsync((async) {
        final storage = RecordingStorage();
        final store = CrdtStore(
          'n1',
          HybridClock('n1'),
          storage: storage,
          persistDebounce: const Duration(milliseconds: 10),
        );
        async.flushMicrotasks(); // ready
        store.setField('t', 'p', 'f', 1);
        store.flushPersistence();
        async.flushMicrotasks(); // clears the dirty flag from setField
        final before = storage.savedPending.length;
        store.undo();
        store.flushPersistence();
        async.flushMicrotasks();
        expect(storage.savedPending.length, before + 1);
        expect(storage.savedPending.last, hasLength(2));
      });
    });
  });

  group('flush gating, timing and order', () {
    test(
      'does not re-persist pending changes on a document-only flush (compact)',
      () {
        fakeAsync((async) {
          final storage = RecordingStorage();
          final clock = HybridClock('n1');
          final store = CrdtStore(
            'n1',
            clock,
            storage: storage,
            persistDebounce: const Duration(milliseconds: 10),
          );
          async.flushMicrotasks(); // ready
          store.insertIntoList('t', 'p', 'items', 'a');
          store.deleteFromList(
            't',
            'p',
            'items',
            store.getListNodeIds('t', 'p', 'items').single,
          );
          store.flushPersistence();
          async.flushMicrotasks();
          final before = storage.savedPending.length;
          final savedBefore = storage.saved.length;
          expect(store.compact(clock.now()), greaterThan(0));
          store.flushPersistence();
          async.flushMicrotasks();
          expect(storage.saved.length, savedBefore + 1);
          expect(storage.savedPending.length, before);
        });
      },
    );

    test('the timer drains a burst once, after the debounce window', () {
      fakeAsync((async) {
        final storage = RecordingStorage();
        final store = CrdtStore(
          'n1',
          HybridClock('n1'),
          storage: storage,
          persistDebounce: const Duration(milliseconds: 50),
        );
        async.flushMicrotasks();
        store.setField('t', 'p', 'a', 1);
        async.elapse(const Duration(milliseconds: 20));
        store.setField('t', 'p', 'b', 2);
        expect(storage.saved, isEmpty);
        async.elapse(const Duration(milliseconds: 31));
        expect(storage.saved, hasLength(1));
        expect(
          storage.saved.single.doc.fields.keys,
          unorderedEquals(['a', 'b']),
        );
        expect(storage.savedPending.single, hasLength(2));
        async.elapse(const Duration(milliseconds: 100));
        expect(storage.saved, hasLength(1));
      });
    });

    test('writes go out one flush at a time, in order', () {
      fakeAsync((async) {
        final storage = _SlowStorage();
        final store = CrdtStore(
          'n1',
          HybridClock('n1'),
          storage: storage,
          persistDebounce: Duration.zero,
        );
        async.flushMicrotasks();
        store.setField('t', 'p', 'f', 1);
        store.setField('t', 'p', 'f', 2);
        // The first flush is in flight, writing its pending queue first; the
        // second flush waits for it.
        expect(storage.calls, ['pending']);
        async.elapse(const Duration(milliseconds: 10));
        expect(storage.calls, [
          'pending',
          'doc',
        ]); // the document once the queue is stored
        async.elapse(const Duration(milliseconds: 10));
        expect(storage.calls, ['pending', 'doc', 'pending']);
        async.elapse(const Duration(milliseconds: 20));
        expect(storage.calls, ['pending', 'doc', 'pending', 'doc']);
        expect(
          [for (final d in storage.saved) d.doc.fields['f']!.value!.value],
          [1, 2],
        );
      });
    });
  });
}

final class _SlowStorage extends RecordingStorage {
  final List<String> calls = [];

  @override
  Future<void> saveDocument(String table, String pk, DocumentState doc) {
    calls.add('doc');
    return Future<void>.delayed(
      const Duration(milliseconds: 10),
      () => saved.add((table: table, pk: pk, doc: doc)),
    );
  }

  @override
  Future<void> savePendingChanges(List<PendingChange> changes) {
    calls.add('pending');
    return Future<void>.delayed(
      const Duration(milliseconds: 10),
      () => savedPending.add(List.of(changes)),
    );
  }
}
