// Port of crdt-js src/__tests__/text.test.ts lines 141-179 ("store text
// integration"), case for case.
import 'package:grove_crdt/grove_crdt.dart';
import 'package:test/test.dart';

CrdtStore newStore(String nodeId) => CrdtStore(nodeId, HybridClock(nodeId));

void main() {
  group('store text integration', () {
    test('edits text fields and supports undo', () {
      final store = newStore('n1');
      store.insertText('notes', '1', 'body', 0, 'hello');
      store.insertText('notes', '1', 'body', 5, ' world');
      expect(store.getText('notes', '1', 'body'), 'hello world');

      store.deleteText('notes', '1', 'body', 5, 6);
      expect(store.getText('notes', '1', 'body'), 'hello');

      store.formatText('notes', '1', 'body', 0, 5, {'bold': true});
      expect([for (final s in store.getTextDelta('notes', '1', 'body')) s.toJson()], [
        {
          'insert': 'hello',
          'attributes': {'bold': true},
        },
      ]);

      // Undo the format, then the delete.
      expect(store.undo(), isTrue);
      expect(store.getTextDelta('notes', '1', 'body').first.attributes, isNull);
      expect(store.undo(), isTrue);
      expect(store.getText('notes', '1', 'body'), 'hello world');
    });

    test('anchors cursors through getTextRefAt/getTextIndexOf', () {
      final store = newStore('n1');
      store.insertText('notes', '1', 'body', 0, 'hello world');
      final anchor = store.getTextRefAt('notes', '1', 'body', 4)!;
      store.insertText('notes', '1', 'body', 0, '>> ');
      expect(store.getTextIndexOf('notes', '1', 'body', anchor), 7);
    });

    test('exposes list node ids in document order', () {
      final store = newStore('n1');
      store.insertIntoList('t', '1', 'items', 'first');
      final ids = store.getListNodeIds('t', '1', 'items');
      expect(ids, hasLength(1));
      store.insertIntoList('t', '1', 'items', 'second', afterId: ids[0]);
      expect(store.getListNodeIds('t', '1', 'items'), hasLength(2));
    });
  });

  group('text undo, redo, setText and edit errors', () {
    test('undo and redo of every text edit converge on a second replica', () {
      final a = newStore('a');
      final b = newStore('b');
      a.insertText('n', '1', 'body', 0, 'hello world');
      a.formatText('n', '1', 'body', 0, 5, {'bold': true});
      a.deleteText('n', '1', 'body', 5, 6);
      expect(a.undo(), isTrue); // the delete
      expect(a.undo(), isTrue); // the format
      expect(a.redo(), isTrue); // the format again
      b.applyChanges(a.getPendingChanges());
      expect(a.getText('n', '1', 'body'), 'hello world');
      expect(b.getText('n', '1', 'body'), 'hello world');
      final delta = [for (final s in b.getTextDelta('n', '1', 'body')) s.toJson()];
      expect(delta, [for (final s in a.getTextDelta('n', '1', 'body')) s.toJson()]);
      expect(delta.first, {
        'insert': 'hello',
        'attributes': {'bold': true},
      });
    });

    test('a format undo restores the earlier attribute value, not null', () {
      final store = newStore('n1');
      store.insertText('n', '1', 'body', 0, 'hi');
      store.formatText('n', '1', 'body', 0, 2, {'color': 'red'});
      store.formatText('n', '1', 'body', 0, 2, {'color': 'blue'});
      expect(store.undo(), isTrue);
      expect(store.getTextDelta('n', '1', 'body').single.attributes, {'color': 'red'});
    });

    test('setText reconciles with a delete and an insert, each pushed', () {
      final a = newStore('a');
      final b = newStore('b');
      a.setText('n', '1', 'body', 'hello world');
      final changes = a.setText('n', '1', 'body', 'hello there');
      expect(changes.map((c) => c.textOp!.op), [TextOpType.delete, TextOpType.insert]);
      b.applyChanges(a.getPendingChanges());
      expect(b.getText('n', '1', 'body'), 'hello there');
      expect(a.setText('n', '1', 'body', 'hello there'), isEmpty);
    });

    test('text edits out of range throw and change nothing', () {
      final store = newStore('n1');
      store.insertText('n', '1', 'body', 0, 'hi');
      expect(() => store.insertText('n', '1', 'body', 5, 'x'), throwsRangeError);
      expect(() => store.insertText('n', '1', 'body', -1, 'x'), throwsRangeError);
      expect(() => store.insertText('n', '1', 'body', 0, ''), throwsArgumentError);
      expect(() => store.deleteText('n', '1', 'body', 2, 1), throwsRangeError);
      expect(store.getText('n', '1', 'body'), 'hi');
      expect(store.pendingCount, 1);
    });

    test('a text edit onto a field of another type throws CrdtApplyError', () {
      final store = newStore('n1');
      store.setField('n', '1', 'body', 'plain');
      expect(() => store.insertText('n', '1', 'body', 0, 'x'), throwsA(isA<CrdtApplyError>()));
      expect(store.getDocument('n', '1')!['body'], 'plain');
      expect(store.pendingCount, 1);
    });
  });
}
