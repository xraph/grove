// Port of crdt-js src/__tests__/batch.test.ts, case for case.
import 'package:grove_crdt/grove_crdt.dart';
import 'package:test/test.dart';

import 'support/store_fakes.dart';

CrdtStore mk() => CrdtStore('n1', HybridClock('n1'));

void main() {
  group('BatchWriter parity', () {
    test('batch counters agree with direct counters (D4)', () {
      final store = mk();
      store.incrementCounter('t', 'p', 'n', 5);
      store.batch('t', 'p').incrementCounter('n', 3).commit();
      expect(store.getDocument('t', 'p')?['n'], 8);
    });

    test('batch writes run plugin hooks', () {
      final store = mk();
      final seen = <String>[];
      store.use(FnPlugin('spy', onAfterWrite: (ev) => seen.add(ev.field)));
      store.batch('t', 'p').setField('a', 1).setField('b', 2).commit();
      expect(seen, ['a', 'b']);
    });

    test('batch writes are undoable', () {
      final store = mk();
      store.batch('t', 'p').setField('a', 1).commit();
      expect(store.canUndo, isTrue);
      store.undo();
      // Go parity: the undo is a compensating LWW write of null (Go has no
      // field delete), so the field reads null rather than being absent.
      expect(store.getDocument('t', 'p')?['a'], isNull);
    });

    test('a batch notifies subscribers exactly once', () {
      final store = mk();
      var fired = 0;
      store.subscribeDocument('t', 'p', () => fired++);
      store
          .batch('t', 'p')
          .setField('a', 1)
          .setField('b', 2)
          .setField('c', 3)
          .commit();
      expect(fired, 1);
    });

    test('transact suspends notification and nests', () {
      final store = mk();
      var fired = 0;
      store.subscribeDocument('t', 'p', () => fired++);
      store.transact(() {
        store.setField('t', 'p', 'a', 1);
        store.transact(() => store.setField('t', 'p', 'b', 2));
        expect(fired, 0);
      });
      expect(fired, 1);
    });

    test('a plugin rejecting one batch write does not abort the others', () {
      final store = mk();
      store.use(
        FnPlugin(
          'gate',
          onBeforeWrite: (ev) => ev.field == 'blocked' ? null : ev,
        ),
      );
      store.batch('t', 'p').setField('ok', 1).setField('blocked', 2).commit();
      final doc = store.getDocument('t', 'p');
      expect(doc?['ok'], 1);
      expect(doc?['blocked'], isNull);
    });
  });

  group('BatchWriter queues every write type', () {
    test('every queued write type commits in one transaction', () {
      final store = mk();
      var fired = 0;
      store.subscribeDocument('t', 'p', () => fired++);
      final changes = store
          .batch('t', 'p')
          .setField('a', 1)
          .incrementCounter('n', 4)
          .decrementCounter('n')
          .addToSet('s', ['x', 'y'])
          .removeFromSet('s', ['x'])
          .insertIntoList('l', 'first')
          .setDocumentField('meta', 'author', 'alice')
          .insertText('body', 0, 'hi')
          .setText('title', 'hello')
          .commit();
      expect(changes, hasLength(9));
      expect(fired, 1);
      final doc = store.getDocument('t', 'p')!;
      expect(doc['a'], 1);
      expect(doc['n'], 3);
      expect(doc['s'], ['y']);
      expect(doc['l'], ['first']);
      expect(doc['meta'], {'author': 'alice'});
      expect(doc['body'], 'hi');
      expect(doc['title'], 'hello');
    });

    test('commit on an empty batch returns nothing and notifies nobody', () {
      final store = mk();
      var fired = 0;
      store.subscribe(() => fired++);
      expect(store.batch('t', 'p').commit(), isEmpty);
      expect(fired, 0);
    });
  });
}
