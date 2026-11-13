import 'dart:convert';

import 'package:grove_crdt/grove_crdt.dart';
import 'package:test/test.dart';

HLC hlc(int ts, String node) => HLC(BigInt.from(ts), 0, node);

void main() {
  test('stable Unicode gaps follow insertions and collapse deletions', () {
    final state = newTextState();
    textInsert(state, null, 'A😀BC', 'a', hlc(1, 'a'));
    final before = textPositionAt(state, 2)!;
    final after = textPositionAt(state, 2, TextSide.after)!;
    final cursor = CursorPosition(
      selection: TextSelection(anchor: before, focus: after),
    );
    final decoded = CursorPosition.fromJson(
      jsonDecode(jsonEncode(cursor.toJson())),
    );
    expect(decoded.toJson(), cursor.toJson());
    textInsert(state, after.ref, 'XY', 'b', hlc(2, 'b'));
    expect(textValue(state), 'A😀XYBC');
    expect(textResolvePosition(state, decoded.selection!.anchor), 4);
    expect(textResolvePosition(state, after), 2);
    textDeleteOp(state, textRefAt(state, 1), 3);
    expect(textValue(state), 'ABC');
    expect(textResolvePosition(state, before), 1);
    expect(textResolvePosition(state, after), 1);
  });

  test('empty boundaries and unavailable references are explicit', () {
    final state = newTextState();
    final start = textPositionAt(state, 0, TextSide.after)!;
    final end = textPositionAt(state, 0)!;
    textInsert(state, null, 'hello', 'a', hlc(1, 'a'));
    expect(textResolvePosition(state, start), 0);
    expect(textResolvePosition(state, end), 5);
    expect(textPositionAt(state, -1), isNull);
    expect(textPositionAt(state, 6), isNull);
    expect(
      textResolvePosition(
        state,
        TextPosition(ref: TextRef(hlc(99, 'missing'), 0), side: TextSide.after),
      ),
      isNull,
    );
    expect(
      () => TextPosition.fromJson({'side': 'invalid'}),
      throwsArgumentError,
    );
  });
}
