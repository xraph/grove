// Port of crdt-js src/__tests__/offline.test.ts, case for case.
import 'package:grove_crdt/grove_crdt.dart';
import 'package:test/test.dart';

CrdtStore mkStore({int maxPendingChanges = 10000, bool throwOnOverflow = false}) => CrdtStore(
      'n1',
      HybridClock('n1'),
      persistDebounce: Duration.zero,
      maxPendingChanges: maxPendingChanges,
      throwOnOverflow: throwOnOverflow,
    );

void main() {
  group('offline queue', () {
    test('bounds the pending queue and reports the overflow', () {
      final store = mkStore(maxPendingChanges: 10);
      final dropped = <ChangeRecord>[];
      store.onPendingOverflow(dropped.addAll);

      for (var i = 0; i < 15; i++) {
        store.setField('t', 'p', 'f$i', i);
      }

      expect(store.pendingCount, 10);
      expect(dropped, hasLength(5));
      // The OLDEST are dropped: recent writes reflect what the user most
      // recently intended, and older ones are likelier superseded.
      expect(dropped[0].field, 'f0');
      expect(store.getPendingChanges()[0].field, 'f5');
    });

    test('an unbounded queue keeps everything', () {
      final store = mkStore(maxPendingChanges: 0);
      for (var i = 0; i < 50; i++) {
        store.setField('t', 'p', 'f$i', i);
      }
      expect(store.pendingCount, 50);
    });

    test('throws OfflineQueueFull when throwOnOverflow is set, leaving no trace of the rejected write', () {
      final store = mkStore(maxPendingChanges: 2, throwOnOverflow: true);
      store.setField('t', 'p', 'a', 1);
      store.setField('t', 'p', 'b', 2);
      final before = store.clock.last;
      expect(() => store.setField('t', 'p', 'c', 3), throwsA(isA<PendingQueueFullError>()));
      // No clock was consumed by the refused write.
      expect(store.clock.last, before);
      // The rejected change must not be left half-queued.
      expect(store.pendingCount, 2);
      // Nor observable anywhere else: the merge into readable state must
      // never have happened for the rejected write.
      final doc = store.getDocument('t', 'p');
      expect(doc?.containsKey('c'), isFalse);
      expect(doc?['a'], 1);
      expect(doc?['b'], 2);
      // Nor in undo history: only "a" and "b" were ever recorded, so exactly
      // two undos succeed and a third finds nothing. Changed from crdt-js: an
      // undo here queues a compensating change, so a full queue refuses it
      // (leaving the entry in place); clear the queue first.
      expect(store.undo, throwsA(isA<PendingQueueFullError>()));
      store.clearPendingChanges();
      expect(store.undo(), isTrue); // undoes "b"
      expect(store.undo(), isTrue); // undoes "a"
      expect(store.undo(), isFalse); // "c" was never recorded
    });

    test(
      'start() syncs on an interval and stop() halts it',
      () {},
      skip: 'needs SyncEngine, which is ported in Task 16; the case moves to its sync tests there.',
    );
  });

  group('eviction order and overflow handlers', () {
    test('eviction drops pushable changes before rejected ones, and never the one just queued', () {
      final store = mkStore(maxPendingChanges: 3);
      final dropped = <ChangeRecord>[];
      store.onPendingOverflow(dropped.addAll);
      final a = store.setField('t', 'p', 'a', 1)!;
      final b = store.setField('t', 'p', 'b', 1)!;
      store.setField('t', 'p', 'c', 1);
      store.markRejected(pendingKey(a), const PendingRejection(kind: 'hook', reason: 'no'));
      store.markRejected(pendingKey(b), const PendingRejection(kind: 'hook', reason: 'no'));
      store.setField('t', 'p', 'd', 1); // evicts c, the oldest pushable
      expect(dropped.map((c) => c.field), ['c']);
      store.setField('t', 'p', 'e', 1); // d is pushable and older than e
      expect(dropped.map((c) => c.field), ['c', 'd']);
      expect(store.pending.map((p) => p.change.field), ['a', 'b', 'e']);
      expect(store.rejectedCount, 2);
      expect(store.pendingCount, 1);
    });

    test('with only rejected changes left, the oldest rejected one is evicted', () {
      final store = mkStore(maxPendingChanges: 2);
      final dropped = <ChangeRecord>[];
      store.onPendingOverflow(dropped.addAll);
      for (final f in ['a', 'b']) {
        store.markRejected(pendingKey(store.setField('t', 'p', f, 1)!), const PendingRejection(kind: 'hook', reason: 'no'));
      }
      store.setField('t', 'p', 'c', 1);
      expect(dropped.map((c) => c.field), ['a']);
      expect(store.pending.map((p) => p.change.field), ['b', 'c']);
    });

    test('onPendingOverflow returns a function that unregisters the handler', () {
      final store = mkStore(maxPendingChanges: 1);
      var calls = 0;
      final off = store.onPendingOverflow((_) => calls++);
      store.setField('t', 'p', 'a', 1);
      store.setField('t', 'p', 'b', 1);
      off();
      store.setField('t', 'p', 'c', 1);
      expect(calls, 1);
    });
  });
}
