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

// Changes every map and list in [v], so a result that shares anything with
// a state shows up as a change in that state.
void scribble(Object? v) {
  if (v is Map<String, Object?>) {
    for (final value in v.values.toList()) {
      scribble(value);
    }
    v['scribbled'] = true;
  } else if (v is List<Object?>) {
    for (final value in v.toList()) {
      scribble(value);
    }
    v.add('scribbled');
  }
}

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
      // The result is the caller's to change, and the state it read is not.
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

  group('resolved values are the caller\'s', () {
    test('a leaf object with no nested path is copied, so changing the result leaves the state alone', () {
      final leaf = <String, Object?>{
        'k': 1,
        'inner': <Object?>[1, <String, Object?>{'z': 2}],
      };
      final state = DocumentCrdtState({
        'n': FieldState(type: CrdtType.lww, hlc: hlc(1, 'a'), nodeId: 'a', value: JsonValue(leaf)),
      });
      final before = encodeWire(state.toJson());
      final resolved = documentResolve(state);
      scribble(resolved);
      expect(encodeWire(state.toJson()), before);
      expect(leaf.containsKey('scribbled'), isFalse);
      expect(documentResolve(state), {
        'n': {
          'k': 1,
          'inner': [
            1,
            {'z': 2},
          ],
        },
      });
    });

    test('resolveFieldValue copies lww, list, set and fallback values', () {
      final nested = <String, Object?>{
        'a': <Object?>[1, 2],
      };
      final fields = <FieldState>[
        FieldState(type: CrdtType.lww, hlc: hlc(1, 'a'), nodeId: 'a', value: JsonValue(nested)),
        FieldState(type: CrdtType.text, hlc: hlc(1, 'a'), nodeId: 'a', value: JsonValue(nested)),
        FieldState(type: CrdtType.document, hlc: hlc(1, 'a'), nodeId: 'a', value: JsonValue(nested)),
        FieldState(type: CrdtType.none, hlc: hlc(1, 'a'), nodeId: 'a', value: JsonValue(nested)),
        FieldState(
          type: CrdtType.list,
          hlc: hlc(1, 'a'),
          nodeId: 'a',
          listState: RgaListState({
            hlcString(hlc(1, 'a')): RgaNode(id: hlc(1, 'a'), nodeId: 'a', parentId: HLC.zero, value: JsonValue(nested)),
          }),
        ),
        FieldState(
          type: CrdtType.set,
          hlc: hlc(1, 'a'),
          nodeId: 'a',
          setState: OrSetState(entries: {'{"a":[1]}': [OrSetTag('a', hlc(1, 'a'))]}),
        ),
      ];
      for (final fs in fields) {
        final before = encodeWire(fs.toJson());
        scribble(resolveFieldValue(fs));
        expect(encodeWire(fs.toJson()), before, reason: fs.type.wire);
      }
      expect(nested, {
        'a': [1, 2],
      });
    });
  });

  group('every export leaves its inputs intact', () {
    HLC n(int ts, String node) => HLC(BigInt.from(ts), 0, node);

    TextState text(String node, int ts, String content) {
      final t = newTextState();
      applyTextOp(t, TextOperation(TextOpType.insert, content: content, origin: n(ts, node)), node, n(ts, node));
      applyTextOp(t, TextOperation(TextOpType.delete, spans: [TextSpan(n(ts, node), 0, 1)]), node, n(ts + 1, node));
      applyTextOp(
        t,
        TextOperation(TextOpType.format, spans: [TextSpan(n(ts, node), 1, 1)], attrs: const {'bold': JsonValue(true)}),
        node,
        n(ts + 2, node),
      );
      return t;
    }

    FieldState leaf(Object? v, HLC at) => FieldState(type: CrdtType.lww, hlc: at, nodeId: at.node, value: JsonValue(v));

    // A document holding every kind of field, with a text field and a document
    // nested in it. Every call builds fresh objects.
    DocumentCrdtState doc(String node, int ts) => DocumentCrdtState({
          'title': leaf({'k': node, 'list': [1, 2]}, n(ts, node)),
          'plain': leaf([1, {'z': node}], n(ts, node)),
          'views': FieldState(
            type: CrdtType.counter,
            hlc: n(ts, node),
            nodeId: node,
            counterState: PnCounterState(inc: {node: ts}, dec: {node: 1}),
          ),
          'tags': setFieldState(
            OrSetState(entries: {'"$node"': [OrSetTag(node, n(ts, node))], '{"o":[1]}': [OrSetTag(node, n(ts, node))]}),
            n(ts, node),
            node,
          ),
          'items': listFieldState(
            RgaListState({
              hlcString(n(ts, node)): RgaNode(
                id: n(ts, node),
                nodeId: node,
                parentId: HLC.zero,
                value: JsonValue({'v': node}),
              ),
            }),
            n(ts, node),
            node,
          ),
          'body': textFieldState(text(node, ts, 'hello'), n(ts, node), node),
          'inner': documentFieldState(
            DocumentCrdtState({
              'deep.text': textFieldState(text(node, ts, 'abc'), n(ts, node), node),
              'deep.n': leaf({'x': 1}, n(ts, node)),
              'deep': leaf(5, n(ts - 1, node)),
            }),
            n(ts, node),
            node,
          ),
          'mixed': node == 'a' ? leaf(1, n(ts, node)) : FieldState(type: CrdtType.counter, hlc: n(ts, node), nodeId: node),
        });

    DocumentState record(String node, int ts, {bool tombstone = false}) => DocumentState(
          table: 't',
          pk: '1',
          fields: {
            'doc': FieldState(type: CrdtType.document, hlc: n(ts, node), nodeId: node, docState: doc(node, ts)),
            'body': textFieldState(text(node, ts, 'text'), n(ts, node), node),
          },
          tombstone: tombstone,
          tombstoneHlc: tombstone ? n(ts + 5, node) : null,
        );

    // One set of inputs per case, built fresh so a case cannot see another's.
    final inputs = <String, Object? Function()>{
      'counterA': () => const PnCounterState(inc: {'a': 3}, dec: {'a': 1}),
      'counterB': () => const PnCounterState(inc: {'a': 5, 'b': 2}),
      'setA': () => OrSetState(
            entries: {'"x"': [OrSetTag('a', n(1, 'a'))], '{"o":[1]}': [OrSetTag('a', n(1, 'a'))]},
            removed: {'k': true},
          ),
      'setB': () => OrSetState(
            entries: {'"x"': [OrSetTag('b', n(2, 'b'))]},
            removed: {removedKey('"x"', OrSetTag('a', n(1, 'a'))): true},
          ),
      'listA': () => RgaListState({
            hlcString(n(1, 'a')): RgaNode(id: n(1, 'a'), nodeId: 'a', parentId: HLC.zero, value: const JsonValue({'v': 1})),
            hlcString(n(2, 'a')): RgaNode(id: n(2, 'a'), nodeId: 'a', parentId: n(1, 'a'), value: const JsonValue([1])),
          }),
      'listB': () => RgaListState({
            hlcString(n(1, 'a')): RgaNode(
              id: n(1, 'a'),
              nodeId: 'a',
              parentId: HLC.zero,
              value: const JsonValue({'v': 1}),
              tombstone: true,
            ),
          }),
      'docA': () => doc('a', 10),
      'docB': () => doc('b', 20),
      'textA': () => text('a', 1, 'hello'),
      'fieldDocA': () => FieldState(type: CrdtType.document, hlc: n(10, 'a'), nodeId: 'a', docState: doc('a', 10)),
      'fieldDocB': () => FieldState(type: CrdtType.document, hlc: n(20, 'b'), nodeId: 'b', docState: doc('b', 20)),
      'fieldTextA': () => textFieldState(text('a', 1, 'hello'), n(3, 'a'), 'a'),
      'fieldTextB': () => textFieldState(text('b', 5, 'world'), n(7, 'b'), 'b'),
      'fieldSetA': () => setFieldState(
            OrSetState(entries: {'"x"': [OrSetTag('a', n(1, 'a'))]}),
            n(1, 'a'),
            'a',
          ),
      'fieldSetB': () => setFieldState(
            OrSetState(entries: {'"y"': [OrSetTag('b', n(2, 'b'))]}),
            n(2, 'b'),
            'b',
          ),
      'fieldListA': () => listFieldState(
            RgaListState({
              hlcString(n(1, 'a')): RgaNode(id: n(1, 'a'), nodeId: 'a', parentId: HLC.zero, value: const JsonValue({'v': 1})),
            }),
            n(1, 'a'),
            'a',
          ),
      'fieldListB': () => listFieldState(
            RgaListState({
              hlcString(n(2, 'b')): RgaNode(id: n(2, 'b'), nodeId: 'b', parentId: HLC.zero, value: const JsonValue([2])),
            }),
            n(2, 'b'),
            'b',
          ),
      'fieldCounterA': () => FieldState(
            type: CrdtType.counter,
            hlc: n(1, 'a'),
            nodeId: 'a',
            counterState: const PnCounterState(inc: {'a': 1}),
          ),
      'fieldCounterB': () => FieldState(
            type: CrdtType.counter,
            hlc: n(2, 'b'),
            nodeId: 'b',
            counterState: const PnCounterState(inc: {'b': 1}),
          ),
      'fieldLwwA': () => leaf({'k': [1]}, n(1, 'a')),
      'fieldLwwB': () => leaf({'k': [2]}, n(2, 'b')),
      'stateA': () => record('a', 10),
      'stateB': () => record('b', 20, tombstone: true),
      'docChange': () => ChangeRecord(
            table: 't',
            pk: '1',
            field: 'doc',
            crdtType: CrdtType.document,
            hlc: n(30, 'c'),
            nodeId: 'c',
            value: const JsonValue({'path': 'deep.n', 'value': {'y': [1]}}),
          ),
      'docDelete': () => ChangeRecord(
            table: 't',
            pk: '1',
            field: 'doc',
            crdtType: CrdtType.document,
            hlc: n(40, 'c'),
            nodeId: 'c',
            value: const JsonValue({'path': 'deep'}),
            tombstone: true,
          ),
      'textChange': () => ChangeRecord(
            table: 't',
            pk: '1',
            field: 'body',
            crdtType: CrdtType.text,
            hlc: n(50, 'c'),
            nodeId: 'c',
            textOp: TextOperation(TextOpType.insert, content: 'more', origin: n(50, 'c')),
          ),
      'setChange': () => ChangeRecord(
            table: 't',
            pk: '1',
            field: 'tags',
            crdtType: CrdtType.set,
            hlc: n(60, 'c'),
            nodeId: 'c',
            setOp: const SetOperation(SetOpType.remove, ['x', {'o': [1]}]),
          ),
      'listChange': () => ChangeRecord(
            table: 't',
            pk: '1',
            field: 'items',
            crdtType: CrdtType.list,
            hlc: n(70, 'c'),
            nodeId: 'c',
            listOp: ListOperation(ListOpType.move, nodeId: n(1, 'a'), parentId: n(2, 'a'), value: const JsonValue({'v': 1})),
          ),
      'counterChange': () => ChangeRecord(
            table: 't',
            pk: '1',
            field: 'views',
            crdtType: CrdtType.counter,
            hlc: n(80, 'c'),
            nodeId: 'c',
            counterDelta: const CounterDelta(4, 1),
          ),
      'lwwChange': () => ChangeRecord(
            table: 't',
            pk: '1',
            field: 'title',
            crdtType: CrdtType.lww,
            hlc: n(90, 'c'),
            nodeId: 'c',
            value: const JsonValue({'k': [9]}),
          ),
      'carrierChange': () => ChangeRecord(
            table: 't',
            pk: '1',
            field: 'doc',
            crdtType: CrdtType.document,
            hlc: n(100, 'c'),
            nodeId: 'c',
            state: FieldState(type: CrdtType.document, hlc: n(100, 'c'), nodeId: 'c', docState: doc('c', 100)),
          ),
    };

    // Each case names the inputs it reads, then calls through them.
    final cases = <String, ({List<String> uses, Object? Function(Map<String, Object?> i) call})>{
      'mergeCounter': (uses: ['counterA', 'counterB'], call: (i) => mergeCounter(i['counterA']! as PnCounterState, i['counterB']! as PnCounterState)),
      'counterValue': (uses: ['counterA'], call: (i) => counterValue(i['counterA']! as PnCounterState)),
      'tagKey': (uses: [], call: (i) => tagKey(OrSetTag('a', n(1, 'a')))),
      'removedKey': (uses: [], call: (i) => removedKey('"x"', OrSetTag('a', n(1, 'a')))),
      'tagRemoved': (uses: ['setB'], call: (i) => tagRemoved(i['setB']! as OrSetState, '"x"', OrSetTag('a', n(1, 'a')))),
      'mergeSet': (uses: ['setA', 'setB'], call: (i) => mergeSet(i['setA']! as OrSetState, i['setB']! as OrSetState)),
      'setElementKeys': (uses: ['setA'], call: (i) => setElementKeys(i['setA']! as OrSetState)),
      'setElements': (uses: ['setA'], call: (i) => setElements(i['setA']! as OrSetState)),
      'keysForElement': (uses: ['setA'], call: (i) => keysForElement(i['setA']! as OrSetState, {'o': [1]})),
      'mergeList': (uses: ['listA', 'listB'], call: (i) => mergeList(i['listA']! as RgaListState, i['listB']! as RgaListState)),
      'listElements': (uses: ['listA'], call: (i) => listElements(i['listA']! as RgaListState)),
      'listNodeIds': (uses: ['listA'], call: (i) => listNodeIds(i['listA']! as RgaListState)),
      'mergeDocument': (uses: ['docA', 'docB'], call: (i) => mergeDocument(i['docA']! as DocumentCrdtState, i['docB']! as DocumentCrdtState)),
      'documentResolve': (uses: ['docA'], call: (i) => documentResolve(i['docA']! as DocumentCrdtState)),
      'resolveFieldValue (document)': (uses: ['fieldDocA'], call: (i) => resolveFieldValue(i['fieldDocA']! as FieldState)),
      'resolveFieldValue (text)': (uses: ['fieldTextA'], call: (i) => resolveFieldValue(i['fieldTextA']! as FieldState)),
      'resolveFieldValue (lww)': (uses: ['fieldLwwA'], call: (i) => resolveFieldValue(i['fieldLwwA']! as FieldState)),
      'resolveFieldValue (list)': (uses: ['fieldListA'], call: (i) => resolveFieldValue(i['fieldListA']! as FieldState)),
      'resolveFieldValue (set)': (uses: ['fieldSetA'], call: (i) => resolveFieldValue(i['fieldSetA']! as FieldState)),
      'setFieldState': (uses: ['setA'], call: (i) => setFieldState(i['setA']! as OrSetState, n(1, 'a'), 'a')),
      'listFieldState': (uses: ['listA'], call: (i) => listFieldState(i['listA']! as RgaListState, n(1, 'a'), 'a')),
      'documentFieldState': (uses: ['docA'], call: (i) => documentFieldState(i['docA']! as DocumentCrdtState, n(1, 'a'), 'a')),
      'textFieldState': (uses: ['textA'], call: (i) => textFieldState(i['textA']! as TextState, n(1, 'a'), 'a')),
      'mergeField (document)': (uses: ['fieldDocA', 'fieldDocB'], call: (i) => mergeField(i['fieldDocA']! as FieldState, i['fieldDocB']! as FieldState)),
      'mergeField (text)': (uses: ['fieldTextA', 'fieldTextB'], call: (i) => mergeField(i['fieldTextA']! as FieldState, i['fieldTextB']! as FieldState)),
      'mergeField (text, null side)': (uses: ['fieldTextA'], call: (i) => mergeField(null, i['fieldTextA']! as FieldState)),
      'mergeField (set)': (uses: ['fieldSetA', 'fieldSetB'], call: (i) => mergeField(i['fieldSetA']! as FieldState, i['fieldSetB']! as FieldState)),
      'mergeField (list)': (uses: ['fieldListA', 'fieldListB'], call: (i) => mergeField(i['fieldListA']! as FieldState, i['fieldListB']! as FieldState)),
      'mergeField (counter)': (uses: ['fieldCounterA', 'fieldCounterB'], call: (i) => mergeField(i['fieldCounterA']! as FieldState, i['fieldCounterB']! as FieldState)),
      'mergeField (lww)': (uses: ['fieldLwwA', 'fieldLwwB'], call: (i) => mergeField(i['fieldLwwA']! as FieldState, i['fieldLwwB']! as FieldState)),
      'mergeState': (uses: ['stateA', 'stateB'], call: (i) => mergeState(i['stateA']! as DocumentState, i['stateB']! as DocumentState)),
      'mergeState (reversed)': (uses: ['stateA', 'stateB'], call: (i) => mergeState(i['stateB']! as DocumentState, i['stateA']! as DocumentState)),
      'applyChange (document write)': (uses: ['fieldDocA', 'docChange'], call: (i) => applyChange(i['fieldDocA']! as FieldState, i['docChange']! as ChangeRecord)),
      'applyChange (document delete)': (uses: ['fieldDocA', 'docDelete'], call: (i) => applyChange(i['fieldDocA']! as FieldState, i['docDelete']! as ChangeRecord)),
      'applyChange (document carrier)': (uses: ['fieldDocA', 'carrierChange'], call: (i) => applyChange(i['fieldDocA']! as FieldState, i['carrierChange']! as ChangeRecord)),
      'applyChange (text)': (uses: ['fieldTextA', 'textChange'], call: (i) => applyChange(i['fieldTextA']! as FieldState, i['textChange']! as ChangeRecord)),
      'applyChange (set)': (uses: ['fieldSetA', 'setChange'], call: (i) => applyChange(i['fieldSetA']! as FieldState, i['setChange']! as ChangeRecord)),
      'applyChange (list)': (uses: ['fieldListA', 'listChange'], call: (i) => applyChange(i['fieldListA']! as FieldState, i['listChange']! as ChangeRecord)),
      'applyChange (counter)': (uses: ['fieldCounterA', 'counterChange'], call: (i) => applyChange(i['fieldCounterA']! as FieldState, i['counterChange']! as ChangeRecord)),
      'applyChange (lww)': (uses: ['fieldLwwA', 'lwwChange'], call: (i) => applyChange(i['fieldLwwA']! as FieldState, i['lwwChange']! as ChangeRecord)),
      'applyChange (null local)': (uses: ['carrierChange'], call: (i) => applyChange(null, i['carrierChange']! as ChangeRecord)),
      'elementKey': (uses: [], call: (i) => elementKey({'o': [1]})),
    };

    String snapshot(Object? v) => switch (v) {
          final TextState t => encodeWire(t.toJson()),
          final PnCounterState c => encodeWire(c.toJson()),
          final OrSetState c => encodeWire(c.toJson()),
          final RgaListState c => encodeWire(c.toJson()),
          final DocumentCrdtState c => encodeWire(c.toJson()),
          final FieldState c => encodeWire(c.toJson()),
          final DocumentState c => encodeWire(c.toJson()),
          final ChangeRecord c => encodeWire(c.toJson()),
          _ => '$v',
        };

    for (final entry in cases.entries) {
      test(entry.key, () {
        final used = {for (final name in entry.value.uses) name: inputs[name]!()};
        final before = {for (final e in used.entries) e.key: snapshot(e.value)};
        final result = entry.value.call(used);
        for (final e in used.entries) {
          expect(snapshot(e.value), before[e.key], reason: '${entry.key} changed ${e.key}');
        }
        // A plain map or list result is the caller's: changing it changes no input.
        if (result is Map<String, Object?> || (result is List<Object?> && result is! List<HLC>)) {
          scribble(result);
          for (final e in used.entries) {
            expect(snapshot(e.value), before[e.key], reason: '${entry.key} result aliases ${e.key}');
          }
        }
      });
    }

    test('the table covers every function merge.dart and apply.dart export', () {
      final names = cases.keys.map((k) => k.split(' ').first).toSet();
      for (final fn in [
        'mergeCounter', 'counterValue', 'tagKey', 'removedKey', 'tagRemoved', 'mergeSet', 'setElementKeys', 'setElements',
        'keysForElement', 'mergeList', 'listElements', 'listNodeIds', 'mergeDocument', 'documentResolve', 'resolveFieldValue',
        'setFieldState', 'listFieldState', 'documentFieldState', 'textFieldState', 'mergeField', 'mergeState', 'applyChange',
        'elementKey', //
      ]) {
        expect(names, contains(fn));
      }
    });
  });
}
