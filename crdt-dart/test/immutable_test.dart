import 'package:grove_crdt/grove_crdt.dart';
import 'package:test/test.dart';

HLC hlc(int n, [String node = 'n2']) => HLC(BigInt.from(n), 0, node);

ChangeRecord change(CrdtType type, HLC at, String node, {SetOperation? setOp, ListOperation? listOp, TextOperation? textOp}) =>
    ChangeRecord(
      table: 't',
      pk: 'p',
      field: 'f',
      crdtType: type,
      hlc: at,
      nodeId: node,
      setOp: setOp,
      listOp: listOp,
      textOp: textOp,
    );

void main() {
  group('mergeFieldState purity', () {
    test('does not mutate the local set state', () {
      final local = FieldState(
        type: CrdtType.set,
        hlc: hlc(1),
        nodeId: 'n1',
        setState: OrSetState(entries: {'"a"': [OrSetTag('n1', hlc(1))]}),
      );
      final before = encodeWire(local.toJson());
      final c = change(CrdtType.set, hlc(2), 'n2', setOp: const SetOperation(SetOpType.add, ['b']));
      final changeBefore = encodeWire(c.toJson());
      final merged = applyChange(local, c);
      expect(encodeWire(local.toJson()), before);
      expect(encodeWire(c.toJson()), changeBefore);
      expect(merged.setState, isNot(same(local.setState)));
      expect(merged.setState!.entries.keys.toList()..sort(), ['"a"', '"b"']);
    });

    test('does not mutate the local list state', () {
      final nodeId = hlc(1);
      final local = FieldState(
        type: CrdtType.list,
        hlc: hlc(1),
        nodeId: 'n1',
        listState: RgaListState({
          'HLC{ts:1 c:0 node:n2}': RgaNode(id: nodeId, nodeId: 'n1', parentId: HLC.zero, value: const JsonValue('x')),
        }),
      );
      final before = encodeWire(local.toJson());
      final c = change(
        CrdtType.list,
        hlc(2),
        'n2',
        listOp: ListOperation(ListOpType.insert, nodeId: hlc(2), parentId: nodeId, value: const JsonValue('y')),
      );
      final changeBefore = encodeWire(c.toJson());
      final merged = applyChange(local, c);
      expect(encodeWire(local.toJson()), before);
      expect(encodeWire(c.toJson()), changeBefore);
      expect(merged.listState, isNot(same(local.listState)));
      expect(merged.listState!.nodes, hasLength(2));
    });

    test('does not mutate the local text state', () {
      final local = FieldState(type: CrdtType.text, hlc: hlc(1), nodeId: 'n1', textState: TextState());
      final before = encodeWire(local.toJson());
      final c = change(
        CrdtType.text,
        hlc(2),
        'n2',
        textOp: TextOperation(TextOpType.insert, content: 'hi', origin: hlc(2)),
      );
      final changeBefore = encodeWire(c.toJson());
      final merged = applyChange(local, c);
      expect(encodeWire(local.toJson()), before);
      expect(encodeWire(c.toJson()), changeBefore);
      expect(merged.textState, isNot(same(local.textState)));
    });

    test('shares untouched substructure', () {
      // Go parity: the Go merge copies the maps it rebuilds, so crdt-js's
      // "removed map is shared" does not hold. What is shared is the tag
      // objects, which are immutable.
      final tag = OrSetTag('n1', hlc(1, 'n1'));
      final local = FieldState(
        type: CrdtType.set,
        hlc: hlc(1),
        nodeId: 'n1',
        setState: OrSetState(entries: {'"a"': [tag]}, removed: {'n1:HLC{ts:1 c:0 node:n1}': true}),
      );
      final merged = applyChange(local, change(CrdtType.set, hlc(2), 'n2', setOp: const SetOperation(SetOpType.add, ['b'])));
      expect(merged.setState!.entries['"a"']!.single, same(tag));
      expect(merged.setState!.removed, local.setState!.removed);
    });
  });

  group('purity beyond the crdt-js cases', () {
    test('applying a text edit to a non-empty text state leaves the previous state intact', () {
      var fs = applyChange(
        null,
        change(CrdtType.text, hlc(1, 'a'), 'a', textOp: TextOperation(TextOpType.insert, content: 'hello', origin: hlc(1, 'a'))),
      );
      final before = encodeWire(fs.toJson());
      final span = TextSpan(hlc(1, 'a'), 1, 2);
      final deleted = applyChange(
        fs,
        change(CrdtType.text, hlc(2, 'a'), 'a', textOp: TextOperation(TextOpType.delete, spans: [span])),
      );
      expect(encodeWire(fs.toJson()), before);
      expect(textValue(fs.textState!), 'hello');
      expect(textValue(deleted.textState!), 'hlo');
      final formatted = applyChange(
        deleted,
        change(
          CrdtType.text,
          hlc(3, 'a'),
          'a',
          textOp: TextOperation(TextOpType.format, spans: [TextSpan(hlc(1, 'a'), 0, 1)], attrs: const {'bold': JsonValue(true)}),
        ),
      );
      expect(textDelta(deleted.textState!).first.attributes, isNull);
      expect(textDelta(formatted.textState!).first.attributes, {'bold': true});
      fs = formatted;
      expect(textValue(fs.textState!), 'hlo');
    });

    test('mergeField leaves both text states intact', () {
      final a = newTextState();
      applyTextOp(a, TextOperation(TextOpType.insert, content: 'abc', origin: hlc(1, 'a')), 'a', hlc(1, 'a'));
      final b = a.clone();
      applyTextOp(b, TextOperation(TextOpType.delete, spans: [TextSpan(hlc(1, 'a'), 0, 2)]), 'b', hlc(2, 'b'));
      final fa = textFieldState(a, hlc(1, 'a'), 'a');
      final fb = textFieldState(b, hlc(2, 'b'), 'b');
      final ja = encodeWire(fa.toJson());
      final jb = encodeWire(fb.toJson());
      final merged = mergeField(fa, fb);
      expect(encodeWire(fa.toJson()), ja);
      expect(encodeWire(fb.toJson()), jb);
      expect(merged.value!.value, 'c');
    });

    test('document path writes and deletes leave the previous document intact', () {
      final first = applyChange(
        null,
        ChangeRecord(
          table: 't',
          pk: 'p',
          field: 'f',
          crdtType: CrdtType.document,
          hlc: hlc(1, 'a'),
          nodeId: 'a',
          value: const JsonValue({'path': 'a.b', 'value': 1}),
        ),
      );
      final before = encodeWire(first.toJson());
      final second = applyChange(
        first,
        ChangeRecord(
          table: 't',
          pk: 'p',
          field: 'f',
          crdtType: CrdtType.document,
          hlc: hlc(2, 'a'),
          nodeId: 'a',
          value: const JsonValue({'path': 'a.b', 'value': null}),
          tombstone: true,
        ),
      );
      expect(encodeWire(first.toJson()), before);
      expect(documentResolve(first.docState!), {
        'a': {'b': 1},
      });
      expect(documentResolve(second.docState!), isEmpty);
    });

    test('documentResolve does not change a field value or the state it reads', () {
      final leaf = <String, Object?>{'k': 1};
      final state = DocumentCrdtState({
        'c': FieldState(type: CrdtType.lww, hlc: hlc(1, 'a'), nodeId: 'a', value: JsonValue(leaf)),
        'c.d': FieldState(type: CrdtType.lww, hlc: hlc(1, 'a'), nodeId: 'a', value: const JsonValue(2)),
      });
      final before = encodeWire(state.toJson());
      final resolved = documentResolve(state);
      expect(encodeWire(state.toJson()), before);
      expect(leaf, {'k': 1});
      // The result is the caller's to change.
      (resolved['c']! as Map<String, Object?>)['extra'] = true;
      expect(documentResolve(state), {
        'c': {'k': 1, 'd': 2},
      });
    });

    test('merge functions leave their inputs intact', () {
      final tag = OrSetTag('a', hlc(1, 'a'));
      final s1 = OrSetState(entries: {'"x"': [tag]}, removed: {'k': true});
      final s2 = OrSetState(entries: {'"x"': [OrSetTag('b', hlc(2, 'b'))]}, removed: {'j': true});
      final b1 = encodeWire(s1.toJson());
      final b2 = encodeWire(s2.toJson());
      mergeSet(s1, s2);
      expect(encodeWire(s1.toJson()), b1);
      expect(encodeWire(s2.toJson()), b2);

      final l1 = RgaListState({
        hlcString(hlc(1, 'a')): RgaNode(id: hlc(1, 'a'), nodeId: 'a', parentId: HLC.zero, value: const JsonValue(1)),
      });
      final l2 = RgaListState({
        hlcString(hlc(1, 'a')): RgaNode(id: hlc(1, 'a'), nodeId: 'a', parentId: HLC.zero, value: const JsonValue(1), tombstone: true),
      });
      final lb1 = encodeWire(l1.toJson());
      final lb2 = encodeWire(l2.toJson());
      final ml = mergeList(l1, l2);
      expect(encodeWire(l1.toJson()), lb1);
      expect(encodeWire(l2.toJson()), lb2);
      expect(ml.nodes.values.single.tombstone, isTrue);

      const c1 = PnCounterState(inc: {'a': 1});
      const c2 = PnCounterState(inc: {'a': 2, 'b': 1});
      final mc = mergeCounter(c1, c2);
      expect(c1.inc, {'a': 1});
      expect(c2.inc, {'a': 2, 'b': 1});
      expect(mc.inc, {'a': 2, 'b': 1});
    });
  });
}
