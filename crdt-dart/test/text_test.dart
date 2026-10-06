import 'package:grove_crdt/grove_crdt.dart';
import 'package:test/test.dart';

HLC hlc(int ts, String node) => HLC(BigInt.from(ts), 0, node);

typedef Typed = ({TextOperation op, HLC hlc, String node});

/// Types chunks sequentially from one node.
List<Typed> typeChunks(TextState st, String node, int startTs, List<String> chunks) {
  final ops = <Typed>[];
  var ts = startTs;
  for (final chunk in chunks) {
    final len = textLength(st);
    final ref = len > 0 ? textRefAt(st, len - 1) : null;
    final clock = hlc(ts, node);
    ops.add((op: textInsert(st, ref, chunk, node, clock), hlc: clock, node: node));
    ts++;
  }
  return ops;
}

/// Every fragment's recorded length is the rune count of its content.
void expectRuneLengths(TextState st) {
  for (final frags in st.frags.values) {
    for (final f in frags) {
      expect(f.length, f.content.runes.length, reason: 'fragment ${f.origin} start ${f.start}');
    }
  }
}

void main() {
  group('text CRDT', () {
    test('coalesces sequential typing into one origin', () {
      final st = newTextState();
      typeChunks(st, 'a', 1, ['h', 'e', 'l', 'l', 'o']);
      expect(textValue(st), 'hello');
      expect(st.frags.keys, hasLength(1));
    });

    test('splits on middle insert', () {
      final st = newTextState();
      typeChunks(st, 'a', 1, ['hello']);
      final ref = textRefAt(st, 1)!;
      textInsert(st, ref, 'XY', 'b', hlc(100, 'b'));
      expect(textValue(st), 'heXYllo');
    });

    test('deletes ranges across origins', () {
      final st = newTextState();
      typeChunks(st, 'a', 1, ['hello']);
      textInsert(st, textRefAt(st, 4)!, ' world', 'b', hlc(100, 'b'));
      textDeleteOp(st, textRefAt(st, 4)!, 4);
      expect(textValue(st), 'hellrld');
    });

    test('formats with LWW attributes and merges delta runs', () {
      final st = newTextState();
      typeChunks(st, 'a', 1, ['hello world']);
      textFormat(st, textRefAt(st, 0)!, 5, {'bold': true}, 'a', hlc(50, 'a'));
      final delta = textDelta(st);
      expect(delta, hasLength(2));
      expect(delta[0].toJson(), {
        'insert': 'hello',
        'attributes': {'bold': true},
      });
      expect(delta[1].toJson(), {'insert': ' world'});
      // Older conflicting format loses; newer null clears.
      textFormat(st, textRefAt(st, 0)!, 5, {'bold': null}, 'b', hlc(40, 'b'));
      expect(textDelta(st)[0].attributes, {'bold': true});
      textFormat(st, textRefAt(st, 0)!, 5, {'bold': null}, 'b', hlc(60, 'b'));
      expect(textDelta(st)[0].attributes, isNull);
    });

    test('keeps relative positions across concurrent edits', () {
      final st = newTextState();
      typeChunks(st, 'a', 1, ['hello world']);
      final anchor = textRefAt(st, 4)!;
      textInsert(st, null, '>> ', 'b', hlc(100, 'b'));
      expect(textIndexOf(st, anchor), 7);
    });

    test('collapses tombstoned anchors', () {
      final st = newTextState();
      typeChunks(st, 'a', 1, ['abcdef']);
      final anchor = textRefAt(st, 2)!;
      textDeleteOp(st, textRefAt(st, 1)!, 3);
      expect(textValue(st), 'aef');
      expect(textIndexOf(st, anchor), 1);
    });

    test('converges concurrent inserts at the same point', () {
      final base = newTextState();
      final baseOps = typeChunks(base, 'a', 1, ['ab']);

      final r1 = newTextState();
      final r2 = newTextState();
      for (final t in baseOps) {
        applyTextOp(r1, t.op, t.node, t.hlc);
        applyTextOp(r2, t.op, t.node, t.hlc);
      }
      final ref = textRefAt(base, 0)!;
      final op1 = textInsert(r1, ref, 'X', 'n1', hlc(100, 'n1'));
      final op2 = textInsert(r2, ref, 'Y', 'n2', hlc(101, 'n2'));
      applyTextOp(r1, op2, 'n2', hlc(101, 'n2'));
      applyTextOp(r2, op1, 'n1', hlc(100, 'n1'));
      expect(textValue(r1), textValue(r2));
      expect(textValue(r1), 'aYXb');
    });

    test('mergeText is commutative and idempotent', () {
      final a = newTextState();
      typeChunks(a, 'a', 1, ['shared']);
      final b = newTextState();
      typeChunks(b, 'b', 100, ['other']);
      final ab = mergeText(a, b);
      final ba = mergeText(b, a);
      expect(textValue(ab), textValue(ba));
      expect(textValue(mergeText(ab, ab)), textValue(ab));
    });

    test('handles unicode by rune offsets', () {
      final st = newTextState();
      typeChunks(st, 'a', 1, ['héllo 🌍']);
      expect(textLength(st), 7);
      textInsert(st, textRefAt(st, 5)!, 'brave ', 'b', hlc(100, 'b'));
      expect(textValue(st), 'héllo brave 🌍');
    });
  });

  group('runes, not UTF-16 units', () {
    test('an astral character is one offset and one rune of fragment length', () {
      final st = newTextState();
      typeChunks(st, 'a', 1, ['😀']);
      expect(textLength(st), 1);
      expect(st.frags.values.single.single.length, 1);
      expect(textRefAt(st, 0)!.offset, 0);
      expect(textRefAt(st, 1), isNull);
    });

    test('inserts after an astral character split at the rune offset', () {
      final st = newTextState();
      typeChunks(st, 'a', 1, ['a😀b🌍c']);
      expect(textLength(st), 5);
      textInsert(st, textRefAt(st, 2)!, 'X', 'b', hlc(100, 'b'));
      expect(textValue(st), 'a😀bX🌍c');
      expect(textLength(st), 6);
      expectRuneLengths(st);
    });

    test('textRefAt and textIndexOf agree on astral characters', () {
      final st = newTextState();
      typeChunks(st, 'a', 1, ['😀a😀😀b']);
      final origin = hlc(1, 'a');
      final refs = [for (var i = 0; i < 5; i++) textRefAt(st, i)!];
      expect([for (final r in refs) r.offset], [0, 1, 2, 3, 4]);
      expect([for (final r in refs) r.origin], everyElement(origin));
      expect([for (final r in refs) textIndexOf(st, r)], [0, 1, 2, 3, 4]);
      // A cursor on the character after the emoji survives an insert before it.
      textInsert(st, null, '>>', 'b', hlc(100, 'b'));
      expect(textIndexOf(st, refs[1]), 3);
      expect(textIndexOf(st, refs[4]), 6);
    });

    test('deletes a range that spans astral characters', () {
      final st = newTextState();
      typeChunks(st, 'a', 1, ['a😀😀b🌍c']);
      final op = textDeleteOp(st, textRefAt(st, 1)!, 3);
      expect(textValue(st), 'a🌍c');
      expect(textLength(st), 3);
      expect(op.spans.single.start, 1);
      expect(op.spans.single.length, 3);
      expectRuneLengths(st);
    });

    test('an anchor on a deleted astral character collapses to its position', () {
      final st = newTextState();
      typeChunks(st, 'a', 1, ['x😀😀y']);
      final anchor = textRefAt(st, 2)!;
      textDeleteOp(st, textRefAt(st, 1)!, 2);
      expect(textValue(st), 'xy');
      expect(textIndexOf(st, anchor), 1);
      textInsert(st, anchor, 'Q', 'b', hlc(100, 'b'));
      expect(textValue(st), 'xQy');
    });

    test('formats a range that starts after an astral character', () {
      final st = newTextState();
      typeChunks(st, 'a', 1, ['😀😀hello']);
      textFormat(st, textRefAt(st, 2)!, 5, {'bold': true}, 'a', hlc(50, 'a'));
      expect([for (final d in textDelta(st)) d.toJson()], [
        {'insert': '😀😀'},
        {
          'insert': 'hello',
          'attributes': {'bold': true},
        },
      ]);
      expectRuneLengths(st);
    });

    test('mergeText splits fragments by runes', () {
      final a = newTextState();
      typeChunks(a, 'a', 1, ['😀😀ab']);
      final b = a.clone();
      textDeleteOp(a, textRefAt(a, 1)!, 2);
      textInsert(b, textRefAt(b, 0)!, '🌍', 'b', hlc(100, 'b'));
      final ab = mergeText(a, b);
      final ba = mergeText(b, a);
      expect(textValue(ab), textValue(ba));
      expect(textValue(ab), '😀🌍b');
      expectRuneLengths(ab);
    });

    test('an unpaired surrogate is one offset, as Go decodes it to one U+FFFD', () {
      final st = newTextState();
      typeChunks(st, 'a', 1, ['a\uD83Db']);
      expect(textLength(st), 3);
      expect(textRefAt(st, 2)!.offset, 2);
      textDeleteOp(st, textRefAt(st, 1)!, 1);
      expect(textValue(st), 'ab');
    });

    test('typing a high then a low surrogate never fuses them into one character', () {
      final st = newTextState();
      typeChunks(st, 'a', 1, ['x\uD83D', '\uDE00y']);
      expect(textLength(st), 4);
      expectRuneLengths(st);
      expect(textRefAt(st, 3)!.offset, 3);
      textInsert(st, textRefAt(st, 1)!, 'M', 'b', hlc(100, 'b'));
      expect(textLength(st), 5);
      expectRuneLengths(st);
      // SetString counts the same runes the offsets do.
      var t = 100;
      textSetString(st, 'x😀y', 'a', () => hlc(++t, 'a'));
      expect(textValue(st), 'x\u{1F600}y');
    });
  });

  group('textWalk', () {
    test('is iterative: a long chain of anchored origins does not overflow the stack', () {
      const n = 50000;
      final st = newTextState();
      applyTextOp(st, TextOperation(TextOpType.insert, content: 'x', origin: hlc(1, 'n0')), 'n0', hlc(1, 'n0'));
      for (var i = 1; i < n; i++) {
        final parent = hlc(i, 'n${i - 1}');
        final clock = hlc(i + 1, 'n$i');
        applyTextOp(
          st,
          TextOperation(TextOpType.insert, ref: TextRef(parent, 0), content: 'x', origin: clock),
          'n$i',
          clock,
        );
      }
      expect(textLength(st), n);
    });

    test('orders siblings newest first and places a parent continuation last', () {
      final st = newTextState();
      typeChunks(st, 'a', 1, ['ab']);
      final ref = textRefAt(st, 0)!;
      textInsert(st, ref, '1', 'x', hlc(10, 'x'));
      textInsert(st, ref, '2', 'y', hlc(20, 'y'));
      textInsert(st, ref, '3', 'z', hlc(15, 'z'));
      expect(textValue(st), 'a231b');
    });

    test('places an origin whose anchor character has not arrived after its parent head', () {
      final st = newTextState();
      applyTextOp(
        st,
        TextOperation(TextOpType.insert, ref: TextRef(hlc(1, 'a'), 3), content: 'late', origin: hlc(5, 'b')),
        'b',
        hlc(5, 'b'),
      );
      applyTextOp(
        st,
        TextOperation(TextOpType.insert, content: 'ab', origin: hlc(1, 'a')),
        'a',
        hlc(1, 'a'),
      );
      expect(textValue(st), 'ablate');
    });
  });

  group('applying ops (Go parity: TextState.Apply)', () {
    test('a duplicate insert is ignored', () {
      final st = newTextState();
      final t = typeChunks(st, 'a', 1, ['hello']).single;
      applyTextOp(st, t.op, t.node, t.hlc);
      expect(textValue(st), 'hello');
    });

    test('an insert without content is an error, as Go returns one', () {
      expect(
        () => applyTextOp(newTextState(), TextOperation(TextOpType.insert), 'a', hlc(1, 'a')),
        throwsStateError,
      );
    });

    test('a format with an equal clock does not replace an attribute', () {
      final st = newTextState();
      typeChunks(st, 'a', 1, ['hello']);
      textFormat(st, textRefAt(st, 0)!, 5, {'c': 'red'}, 'a', hlc(50, 'a'));
      textFormat(st, textRefAt(st, 0)!, 5, {'c': 'blue'}, 'b', hlc(50, 'a'));
      expect(textDelta(st).single.attributes, {'c': 'red'});
    });

    test('applyTextOpTo leaves the input untouched', () {
      final st = newTextState();
      typeChunks(st, 'a', 1, ['hello']);
      final op = TextOperation(TextOpType.insert, ref: textRefAt(st, 4), content: '!', origin: hlc(2, 'b'));
      final next = applyTextOpTo(st, op, 'b', hlc(2, 'b'));
      expect(textValue(st), 'hello');
      expect(textValue(next), 'hello!');
    });

    test('cloneTextState is deep', () {
      final st = newTextState();
      typeChunks(st, 'a', 1, ['hello']);
      final copy = cloneTextState(st);
      textDeleteOp(st, textRefAt(st, 0)!, 5);
      expect(textValue(copy), 'hello');
      expect(textValue(st), '');
    });

    test('mergeText with a missing side returns the other, or an empty state', () {
      final st = newTextState();
      typeChunks(st, 'a', 1, ['hi']);
      expect(textValue(mergeText(st, null)), 'hi');
      expect(textValue(mergeText(null, st)), 'hi');
      expect(textValue(mergeText(null, null)), '');
    });

    test('argument errors', () {
      final st = newTextState();
      typeChunks(st, 'a', 1, ['hi']);
      expect(() => textInsert(st, null, '', 'a', hlc(9, 'a')), throwsArgumentError);
      expect(() => textFormat(st, null, 1, {}, 'a', hlc(9, 'a')), throwsArgumentError);
      expect(() => resolveTextSpans(st, null, 0), throwsArgumentError);
      expect(() => resolveTextSpans(st, null, 3), throwsStateError);
      expect(() => resolveTextSpans(st, TextRef(hlc(77, 'q'), 0), 1), throwsStateError);
    });

    test('TextDeltaSegment writes attributes only when there are some', () {
      expect(const TextDeltaSegment('a').toJson(), {'insert': 'a'});
      expect(const TextDeltaSegment('a', attributes: {}).toJson(), {'insert': 'a'});
      expect(const TextDeltaSegment('a', attributes: {'b': 1}).toJson(), {
        'insert': 'a',
        'attributes': {'b': 1},
      });
    });
  });

  group('textSetString (Go SetString)', () {
    HLC n(int ts) => HLC(BigInt.from(ts), 0, 'a');

    test('emits one insert for a pure insertion', () {
      final s = newTextState();
      applyTextOp(s, textInsert(s, null, 'hello world', 'a', n(1)), 'a', n(1));
      var t = 1;
      final ops = textSetString(s, 'hello brave world', 'a', () => n(++t));
      expect(ops.map((o) => o.op), [TextOpType.insert]);
      expect(textValue(s), 'hello brave world');
    });

    test('emits a delete then an insert for a replacement', () {
      final s = newTextState();
      applyTextOp(s, textInsert(s, null, 'cat', 'a', n(1)), 'a', n(1));
      var t = 1;
      final ops = textSetString(s, 'cut', 'a', () => n(++t));
      expect(ops.map((o) => o.op), [TextOpType.delete, TextOpType.insert]);
      expect(textValue(s), 'cut');
    });

    test('counts runes, not UTF-16 units', () {
      final s = newTextState();
      applyTextOp(s, textInsert(s, null, 'a😀b', 'a', n(1)), 'a', n(1));
      var t = 1;
      textSetString(s, 'a😀😀b', 'a', () => n(++t));
      expect(textValue(s), 'a😀😀b');
      expect(textLength(s), 4);
    });

    test('is a no-op for an identical string', () {
      final s = newTextState();
      applyTextOp(s, textInsert(s, null, 'same', 'a', n(1)), 'a', n(1));
      expect(textSetString(s, 'same', 'a', () => n(2)), isEmpty);
    });

    // textInsert and textDeleteOp already apply their op, so textSetString
    // must not apply it a second time.
    test('applies each op exactly once', () {
      final s = newTextState();
      var t = 0;
      textSetString(s, 'ab', 'a', () => n(++t));
      textSetString(s, 'abab', 'a', () => n(++t));
      expect(textValue(s), 'abab');
      expect(textLength(s), 4);
    });

    test('calls nextClock once per emitted op, the delete first', () {
      final s = newTextState();
      applyTextOp(s, textInsert(s, null, 'cat', 'a', n(1)), 'a', n(1));
      final issued = <HLC>[];
      final ops = textSetString(s, 'cut', 'a', () {
        final c = n(10 + issued.length);
        issued.add(c);
        return c;
      });
      expect(issued, [n(10), n(11)]);
      // The insert op is stamped with the second clock, the delete carries none.
      expect(ops[0].origin, HLC.zero);
      expect(ops[1].origin, n(11));
    });

    test('writes to an empty text from the head', () {
      final s = newTextState();
      var t = 0;
      final ops = textSetString(s, '😀', 'a', () => n(++t));
      expect(ops.single.ref, TextRef.head);
      expect(textValue(s), '😀');
    });

    test('replaces everything with the empty string', () {
      final s = newTextState();
      applyTextOp(s, textInsert(s, null, 'a😀b', 'a', n(1)), 'a', n(1));
      var t = 1;
      final ops = textSetString(s, '', 'a', () => n(++t));
      expect(ops.map((o) => o.op), [TextOpType.delete]);
      expect(ops.single.spans.single.length, 3);
      expect(textValue(s), '');
    });

    test('keeps the common astral prefix and suffix', () {
      final s = newTextState();
      applyTextOp(s, textInsert(s, null, '😀a😀', 'a', n(1)), 'a', n(1));
      var t = 1;
      final ops = textSetString(s, '😀b😀', 'a', () => n(++t));
      expect(ops.map((o) => o.op), [TextOpType.delete, TextOpType.insert]);
      expect(ops[0].spans.single.start, 1);
      expect(ops[0].spans.single.length, 1);
      expect(ops[1].ref.offset, 0);
      expect(textValue(s), '😀b😀');
      expectRuneLengths(s);
    });
  });
}
