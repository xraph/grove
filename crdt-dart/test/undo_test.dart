import 'package:grove_crdt/grove_crdt.dart';
import 'package:test/test.dart';

ChangeRecord change(int ts) => ChangeRecord(
  table: 't',
  pk: '1',
  field: 'f',
  crdtType: CrdtType.lww,
  hlc: HLC(BigInt.from(ts), 0, 'a'),
  nodeId: 'a',
  value: JsonValue(ts),
);

FieldState field(int ts) => FieldState(
  type: CrdtType.lww,
  hlc: HLC(BigInt.from(ts), 0, 'a'),
  nodeId: 'a',
  value: JsonValue(ts),
);

void main() {
  test('undo returns the last recorded entry and enables redo', () {
    final u = UndoManager()..record(change(1), null);
    expect(u.canUndo, isTrue);
    final e = u.undo()!;
    expect(e.change.hlc.ts, BigInt.one);
    expect(u.canUndo, isFalse);
    expect(u.canRedo, isTrue);
    expect(u.redo()!.change.hlc.ts, BigInt.one);
  });

  test('a new record clears the redo stack', () {
    final u = UndoManager()..record(change(1), null);
    u.undo();
    u.record(change(2), null);
    expect(u.canRedo, isFalse);
  });

  test('maxHistory trims the oldest entries', () {
    final u = UndoManager(maxHistory: 2)
      ..record(change(1), null)
      ..record(change(2), null)
      ..record(change(3), null);
    expect(u.undoCount, 2);
    expect(u.undo()!.change.hlc.ts, BigInt.from(3));
    expect(u.undo()!.change.hlc.ts, BigInt.two);
    expect(u.undo(), isNull);
  });

  test('clear empties both stacks', () {
    final u = UndoManager()..record(change(1), null);
    u.undo();
    u.clear();
    expect(u.canUndo || u.canRedo, isFalse);
  });

  group('beyond the crdt-js behaviour', () {
    test('the default history is 100 entries', () {
      final u = UndoManager();
      for (var i = 0; i < 130; i++) {
        u.record(change(i), null);
      }
      expect(u.undoCount, 100);
      expect(u.maxHistory, 100);
      expect(u.undo()!.change.hlc.ts, BigInt.from(129));
    });

    test('undo and redo on empty stacks return null', () {
      final u = UndoManager();
      expect(u.undo(), isNull);
      expect(u.redo(), isNull);
      expect((u.undoCount, u.redoCount), (0, 0));
    });

    test('undo and redo move entries between the stacks, newest first', () {
      final u = UndoManager()
        ..record(change(1), null)
        ..record(change(2), null)
        ..record(change(3), null);
      expect(u.undo()!.change.hlc.ts, BigInt.from(3));
      expect(u.undo()!.change.hlc.ts, BigInt.two);
      expect((u.undoCount, u.redoCount), (1, 2));
      expect(u.redo()!.change.hlc.ts, BigInt.two);
      expect((u.undoCount, u.redoCount), (2, 1));
      expect(u.undo()!.change.hlc.ts, BigInt.two);
    });

    test('an entry keeps the change, the previous state and a timestamp', () {
      final before = DateTime.now().millisecondsSinceEpoch;
      final prev = field(1);
      final c = change(2);
      final u = UndoManager()..record(c, prev);
      final e = u.undo()!;
      expect(e.change, same(c));
      expect(e.previousState, same(prev));
      expect(e.previousDocument, isNull);
      expect(
        e.timestamp,
        inInclusiveRange(before, DateTime.now().millisecondsSinceEpoch),
      );
    });

    test('a null previous state is kept as null', () {
      final u = UndoManager()..record(change(1), null);
      expect(u.undo()!.previousState, isNull);
    });

    test('previousDocument rides along for a deleteDocument', () {
      final d = DocumentState(table: 't', pk: '1', fields: {'f': field(1)});
      final c = change(2).copyWith(tombstone: true);
      final u = UndoManager()..record(c, null, previousDocument: d);
      final e = u.undo()!;
      expect(e.previousDocument, same(d));
      expect(u.redo()!.previousDocument, same(d));
    });

    test('maxHistory 0 keeps nothing', () {
      final u = UndoManager(maxHistory: 0)..record(change(1), null);
      expect(u.canUndo, isFalse);
    });

    test('a negative maxHistory is refused', () {
      expect(() => UndoManager(maxHistory: -1), throwsRangeError);
    });

    test('popUndo pops the top entry without touching redo', () {
      final u = UndoManager()
        ..record(change(1), null)
        ..record(change(2), null);
      final e = u.popUndo()!;
      expect(e.change.hlc.ts, BigInt.two);
      expect((u.undoCount, u.redoCount), (1, 0));
      expect(u.canRedo, isFalse);
      expect(u.undo()!.change.hlc.ts, BigInt.one);
    });

    test('popUndo on an empty stack returns null and keeps redo', () {
      final u = UndoManager()..record(change(1), null);
      u.undo();
      expect(u.popUndo(), isNull);
      expect(u.redoCount, 1);
    });

    test('popUndo keeps the entry previousDocument for the caller', () {
      final d = DocumentState(table: 't', pk: '1');
      final u = UndoManager()
        ..record(
          change(1).copyWith(tombstone: true),
          null,
          previousDocument: d,
        );
      expect(u.popUndo()!.previousDocument, same(d));
      expect(u.canUndo, isFalse);
    });

    test('popUndo then pushRedo: one undo leaves exactly one redo entry', () {
      final u = UndoManager()
        ..record(change(1), null)
        ..record(change(2), null);
      final popped = u.popUndo()!;
      final compensated = UndoEntry(
        change: change(9),
        previousState: field(2),
        timestamp: popped.timestamp,
      );
      u.pushRedo(compensated);
      expect((u.undoCount, u.redoCount), (1, 1));
      expect(u.redo(), same(compensated));
      expect(u.redo(), isNull);
      expect((u.undoCount, u.redoCount), (2, 0));
    });

    test('redo never grows the undo stack past maxHistory', () {
      final u = UndoManager(maxHistory: 2)
        ..record(change(1), null)
        ..record(change(2), null);
      for (var i = 3; i <= 4; i++) {
        u.pushRedo(
          UndoEntry(change: change(i), previousState: null, timestamp: 0),
        );
      }
      u.redo();
      u.redo();
      expect(u.undoCount, 2);
    });

    test('pushRedo enables redo on its own and a later record clears it', () {
      final u = UndoManager();
      u.pushRedo(
        UndoEntry(change: change(1), previousState: null, timestamp: 0),
      );
      expect(u.canRedo, isTrue);
      u.record(change(2), null);
      expect(u.canRedo, isFalse);
    });

    test('pushRedo trims the oldest redo entries to maxHistory', () {
      final u = UndoManager(maxHistory: 2);
      for (var i = 1; i <= 3; i++) {
        u.pushRedo(
          UndoEntry(change: change(i), previousState: null, timestamp: 0),
        );
      }
      expect(u.redoCount, 2);
      expect(u.redo()!.change.hlc.ts, BigInt.from(3));
      expect(u.redo()!.change.hlc.ts, BigInt.two);
      expect(u.redo(), isNull);
    });
  });

  test(
    'pushUndo adds an entry without clearing redo, and trims to maxHistory',
    () {
      final u = UndoManager(maxHistory: 2)
        ..record(change(1), null)
        ..record(change(2), null);
      final e = u.undo()!;
      u.pushUndo(
        UndoEntry(change: change(3), previousState: null, timestamp: 0),
      );
      expect(u.canRedo, isTrue);
      expect(u.redoCount, 1);
      expect(u.undoCount, 2);
      expect(u.undo()!.change.hlc.ts, BigInt.from(3));
      expect(u.undo()!.change.hlc.ts, BigInt.one);
      expect(u.redo(), isNotNull);
      expect(e.change.hlc.ts, BigInt.two);
    },
  );
}
