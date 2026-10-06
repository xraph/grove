import 'package:grove_crdt/grove_crdt.dart';
import 'package:test/test.dart';

HLC h(int ts, String node, [int c = 0]) => HLC(BigInt.from(ts), c, node);

FieldState lwwField(Object? value, HLC hlc) =>
    FieldState(type: CrdtType.lww, hlc: hlc, nodeId: hlc.node, value: JsonValue(value));

ChangeRecord change(
  CrdtType type,
  HLC hlc, {
  String field = 'f',
  JsonValue? value,
  CounterDelta? counterDelta,
  SetOperation? setOp,
  ListOperation? listOp,
  TextOperation? textOp,
  FieldState? state,
  bool tombstone = false,
}) =>
    ChangeRecord(
      table: 't',
      pk: '1',
      field: field,
      crdtType: type,
      hlc: hlc,
      nodeId: hlc.node,
      value: value,
      counterDelta: counterDelta,
      setOp: setOp,
      listOp: listOp,
      textOp: textOp,
      state: state,
      tombstone: tombstone,
    );

void main() {
  // The mergeLWW cases run through mergeField: the Go register merge is
  // MergeLWW inside MergeField, and Dart has no separate register type.
  group('mergeLWW', () {
    test('returns remote when remote HLC is higher', () {
      final merged = mergeField(lwwField('old', h(100, 'a')), lwwField('new', h(200, 'b')));
      expect(merged.value!.value, 'new');
    });

    test('returns local when local HLC is higher', () {
      final merged = mergeField(lwwField('keep', h(300, 'a')), lwwField('discard', h(200, 'b')));
      expect(merged.value!.value, 'keep');
    });

    test('breaks tie by node ID (higher node wins)', () {
      final merged = mergeField(lwwField('from-a', h(100, 'a')), lwwField('from-b', h(100, 'b')));
      // "b" > "a" => remote.hlc is after local.hlc => remote wins
      expect(merged.value!.value, 'from-b');
    });

    test('returns remote when local is null', () {
      final remote = lwwField('val', h(1, 'a'));
      expect(mergeField(null, remote), same(remote));
    });

    test('returns local when remote is null', () {
      final local = lwwField('val', h(1, 'a'));
      expect(mergeField(local, null), same(local));
    });

    test('preserves the winning value', () {
      final obj = <String, Object?>{'nested': true};
      final merged = mergeField(lwwField(obj, h(200, 'a')), lwwField('other', h(100, 'b')));
      expect(merged.value!.value, same(obj));
    });
  });

  group('newPNCounterState', () {
    test('creates empty state with empty inc and dec maps', () {
      expect(const PnCounterState().toJson(), {'inc': <String, int>{}, 'dec': <String, int>{}});
    });
  });

  group('mergeCounter', () {
    test('takes max per node for increments', () {
      const local = PnCounterState(inc: {'node-1': 10}, dec: {'node-1': 2});
      const remote = PnCounterState(inc: {'node-1': 8, 'node-2': 5});
      final merged = mergeCounter(local, remote);
      // node-1 inc: max(10, 8) = 10, node-2 inc: 5, node-1 dec: 2
      expect(counterValue(merged), 13);
    });

    test('takes max per node for decrements', () {
      const local = PnCounterState(dec: {'node-1': 3});
      const remote = PnCounterState(dec: {'node-1': 5});
      expect(mergeCounter(local, remote).dec['node-1'], 5);
    });

    test('is idempotent', () {
      const a = PnCounterState(inc: {'node-1': 10});
      final merged1 = mergeCounter(a, a);
      final merged2 = mergeCounter(merged1, a);
      expect(counterValue(merged1), counterValue(merged2));
    });

    test('is commutative', () {
      const a = PnCounterState(inc: {'node-1': 10});
      const b = PnCounterState(inc: {'node-2': 5});
      expect(counterValue(mergeCounter(a, b)), counterValue(mergeCounter(b, a)));
    });

    test(
      'returns remote when local is null',
      () {},
      skip: 'mergeCounter takes non-null states; the null sides are mergeField (see "mergeField null sides")',
    );

    test(
      'returns local when remote is null',
      () {},
      skip: 'mergeCounter takes non-null states; the null sides are mergeField (see "mergeField null sides")',
    );

    test('handles disjoint node sets', () {
      const local = PnCounterState(inc: {'node-1': 10});
      const remote = PnCounterState(inc: {'node-2': 7});
      final merged = mergeCounter(local, remote);
      expect(merged.inc['node-1'], 10);
      expect(merged.inc['node-2'], 7);
    });
  });

  group('counterValue', () {
    test('returns sum(inc) - sum(dec)', () {
      const state = PnCounterState(inc: {'node-1': 10, 'node-2': 5}, dec: {'node-1': 3});
      expect(counterValue(state), 12);
    });

    test('returns 0 for empty state', () {
      expect(counterValue(const PnCounterState()), 0);
    });

    test('returns negative value when dec > inc', () {
      const state = PnCounterState(inc: {'node-1': 2}, dec: {'node-1': 5});
      expect(counterValue(state), -3);
    });
  });

  group('tagKey', () {
    test('produces deterministic Go-compatible key', () {
      final tag = OrSetTag('n1', h(100, 'n1'));
      expect(tagKey(tag), 'n1:HLC{ts:100 c:0 node:n1}');
    });
  });

  group('newORSetState', () {
    test('creates empty state with empty entries and removed maps', () {
      expect(const OrSetState().toJson(), {'entries': <String, Object?>{}, 'removed': <String, bool>{}});
    });
  });

  group('mergeSet', () {
    final hlcA = h(1, 'node-1');
    final hlcB = h(2, 'node-2');

    test('unions entries from both sides', () {
      final local = OrSetState(entries: {'"a"': [OrSetTag('node-1', hlcA)]});
      final remote = OrSetState(entries: {'"b"': [OrSetTag('node-2', hlcB)]});
      final merged = mergeSet(local, remote);
      expect(merged.entries.keys, contains('"a"'));
      expect(merged.entries.keys, contains('"b"'));
    });

    test('unions removed flags from both sides', () {
      const local = OrSetState(removed: {'key1': true});
      const remote = OrSetState(removed: {'key2': true});
      final merged = mergeSet(local, remote);
      expect(merged.removed['key1'], isTrue);
      expect(merged.removed['key2'], isTrue);
    });

    test('deduplicates tags per element', () {
      final state = OrSetState(entries: {'"a"': [OrSetTag('node-1', hlcA)]});
      final merged = mergeSet(state, state);
      // Same tag from both sides should be deduplicated to 1
      expect(merged.entries['"a"'], hasLength(1));
    });

    test(
      'returns remote when local is null',
      () {},
      skip: 'mergeSet takes non-null states; the null sides are mergeField (see "mergeField null sides")',
    );

    test(
      'returns local when remote is null',
      () {},
      skip: 'mergeSet takes non-null states; the null sides are mergeField (see "mergeField null sides")',
    );

    test('is commutative', () {
      final a = OrSetState(entries: {'"x"': [OrSetTag('node-1', hlcA)]});
      final b = OrSetState(entries: {'"y"': [OrSetTag('node-2', hlcB)]});
      expect(setElements(mergeSet(a, b)), setElements(mergeSet(b, a)));
    });

    test('is idempotent', () {
      final a = OrSetState(entries: {'"x"': [OrSetTag('node-1', hlcA)]});
      final merged1 = mergeSet(a, a);
      final merged2 = mergeSet(merged1, a);
      expect(setElements(merged1), setElements(merged2));
    });
  });

  group('setElements', () {
    test('returns elements with at least one non-removed tag', () {
      final state = OrSetState(entries: {
        '"hello"': [OrSetTag('node-1', h(1, 'node-1'))],
        '"world"': [OrSetTag('node-1', h(2, 'node-1'))],
      });
      expect(setElements(state), ['hello', 'world']);
    });

    test('excludes elements whose tags are all removed', () {
      final tag = OrSetTag('node-1', h(1, 'node-1'));
      final state = OrSetState(entries: {'"hello"': [tag]}, removed: {tagKey(tag): true});
      expect(setElements(state), isEmpty);
    });

    test('handles concurrent add-remove (add wins with new tag)', () {
      final tag1 = OrSetTag('node-1', h(1, 'node-1'));
      final tag2 = OrSetTag('node-1', h(2, 'node-1'));
      // tag1 is removed but tag2 is not => element is present
      final state = OrSetState(entries: {'"x"': [tag1, tag2]}, removed: {tagKey(tag1): true});
      expect(setElements(state), ['x']);
    });

    test('returns elements sorted by key', () {
      final state = OrSetState(entries: {
        '"b"': [OrSetTag('n', h(1, 'n'))],
        '"a"': [OrSetTag('n', h(2, 'n'))],
      });
      expect(setElements(state), ['a', 'b']);
    });

    test('JSON.parse values that are valid JSON strings', () {
      final state = OrSetState(entries: {'42': [OrSetTag('n', h(1, 'n'))]});
      // "42" is valid JSON => parsed to number 42
      expect(setElements(state), [42]);
    });

    test('returns raw key when JSON.parse fails', () {
      final state = OrSetState(entries: {'not-json{': [OrSetTag('n', h(1, 'n'))]});
      expect(setElements(state), ['not-json{']);
    });

    test('returns empty array for empty state', () {
      expect(setElements(const OrSetState()), isEmpty);
    });
  });

  group('mergeFieldState', () {
    group('lww type', () {
      test('merges LWW when remote HLC is higher', () {
        final local = lwwField('local', h(100, 'a'));
        final result = applyChange(local, change(CrdtType.lww, h(200, 'b'), value: const JsonValue('remote')));
        expect(result.value!.value, 'remote');
      });

      test('keeps local when local HLC is higher', () {
        final local = lwwField('local', h(300, 'a'));
        final result = applyChange(local, change(CrdtType.lww, h(200, 'b'), value: const JsonValue('remote')));
        expect(result.value!.value, 'local');
      });

      test('creates new field state when local is null', () {
        final result = applyChange(null, change(CrdtType.lww, h(100, 'a'), value: const JsonValue('new')));
        expect(result.type, CrdtType.lww);
        expect(result.value!.value, 'new');
      });
    });

    group('counter type', () {
      test('applies counter delta to existing state', () {
        final local = FieldState(
          type: CrdtType.counter,
          hlc: h(100, 'a'),
          nodeId: 'a',
          counterState: const PnCounterState(inc: {'a': 5}),
        );
        final result = applyChange(local, change(CrdtType.counter, h(200, 'b'), counterDelta: const CounterDelta(3, 0)));
        expect(result.counterState, isNotNull);
        expect(counterValue(result.counterState!), 8); // 5 + 3
      });

      test('creates new counter state when local is null', () {
        final result = applyChange(null, change(CrdtType.counter, h(100, 'a'), counterDelta: const CounterDelta(7, 0)));
        expect(result.type, CrdtType.counter);
        expect(counterValue(result.counterState!), 7);
      });

      test('handles change without counter_delta', () {
        // Go parity: ApplyChange returns "crdt: counter change missing
        // counter_delta" where crdt-js folded it as an empty counter.
        expect(
          () => applyChange(null, change(CrdtType.counter, h(100, 'a'))),
          throwsA(isA<CrdtApplyError>().having((e) => e.message, 'message', 'crdt: counter change missing counter_delta')),
        );
      });

      test('treats deltas as cumulative per-node snapshots (max-merge, idempotent)', () {
        // The wire delta is the sending node's cumulative totals, matching
        // Go's ApplyChange, so redelivery cannot double-count.
        final change1 = change(CrdtType.counter, h(100, 'a'), counterDelta: const CounterDelta(3, 0));
        final state1 = applyChange(null, change1);

        final change2 = change(CrdtType.counter, h(200, 'a'), counterDelta: const CounterDelta(7, 0)); // cumulative: 3 then +4 more
        final state2 = applyChange(state1, change2);
        expect(counterValue(state2.counterState!), 7);

        // Redelivering an older snapshot changes nothing.
        final replayed = applyChange(state2, change1);
        expect(counterValue(replayed.counterState!), 7);
      });
    });

    group('set type', () {
      test('applies add operation', () {
        final result = applyChange(
          null,
          change(CrdtType.set, h(100, 'a'), setOp: const SetOperation(SetOpType.add, ['x', 'y'])),
        );
        expect(result.type, CrdtType.set);
        final elems = setElements(result.setState!);
        expect(elems, contains('x'));
        expect(elems, contains('y'));
      });

      test('applies remove operation', () {
        final afterAdd = applyChange(
          null,
          change(CrdtType.set, h(100, 'a'), setOp: const SetOperation(SetOpType.add, ['x'])),
        );
        expect(setElements(afterAdd.setState!), contains('x'));

        final afterRemove = applyChange(
          afterAdd,
          change(CrdtType.set, h(200, 'a'), setOp: const SetOperation(SetOpType.remove, ['x'])),
        );
        expect(setElements(afterRemove.setState!), isNot(contains('x')));
      });

      test('creates new set state when local is null', () {
        final result = applyChange(
          null,
          change(CrdtType.set, h(100, 'a'), setOp: const SetOperation(SetOpType.add, ['a'])),
        );
        expect(result.setState, isNotNull);
      });

      test('handles change without set_op', () {
        // Go parity: ApplyChange returns "crdt: set change missing set_op"
        // where crdt-js folded it as an empty set.
        expect(
          () => applyChange(null, change(CrdtType.set, h(100, 'a'))),
          throwsA(isA<CrdtApplyError>().having((e) => e.message, 'message', 'crdt: set change missing set_op')),
        );
      });
    });

    group('default (unknown type) fallback', () {
      test('treats unknown crdt_type as LWW', () {
        // Go parity: ApplyChange returns "crdt: apply unknown type" for a type
        // it does not know, where crdt-js folded it as LWW. Dart refuses the
        // type already when it decodes the wire string.
        expect(() => CrdtType.fromWire('mystery'), throwsFormatException);
        // The empty type Go emits on pulled tombstone rows is CrdtType.none,
        // and applying it is the Go error.
        expect(
          () => applyChange(null, change(CrdtType.none, h(100, 'a'), value: const JsonValue('fallback'))),
          throwsA(isA<CrdtApplyError>().having((e) => e.message, 'message', 'crdt: apply unknown type: ')),
        );
      });
    });
  });

  group('List CRDT merge', () {
    RgaNode makeNode(int ts, String node, {int parentTs = 0, Object? value, bool tombstone = false}) => RgaNode(
          id: h(ts, node),
          nodeId: node,
          parentId: parentTs == 0 ? HLC.zero : h(parentTs, node),
          value: JsonValue(value ?? 'val-$ts'),
          tombstone: tombstone,
        );

    Map<String, RgaNode> keyed(List<RgaNode> nodes) => {for (final n in nodes) hlcString(n.id): n};

    test('merges two empty lists', () {
      final merged = mergeList(const RgaListState(), const RgaListState());
      expect(merged.nodes, isEmpty);
      expect(listElements(merged), isEmpty);
    });

    test('merges disjoint lists', () {
      final local = RgaListState(keyed([makeNode(1, 'a', value: 'x')]));
      final remote = RgaListState(keyed([makeNode(2, 'b', value: 'y')]));
      final merged = mergeList(local, remote);
      expect(merged.nodes, hasLength(2));

      final elems = listElements(merged);
      expect(elems, contains('x'));
      expect(elems, contains('y'));
    });

    test('preserves tombstones from both sides', () {
      final local = RgaListState(keyed([makeNode(1, 'a', value: 'x')]));
      final remote = RgaListState(keyed([makeNode(2, 'b', value: 'y', tombstone: true)]));
      final merged = mergeList(local, remote);

      expect(merged.nodes, hasLength(2));
      // Only non-tombstoned nodes appear in elements.
      expect(listElements(merged), ['x']);
    });

    test('listElements returns visible elements in order', () {
      // Build a 3-element list: A -> B -> C (chained via parent_id).
      final state = RgaListState(keyed([
        makeNode(1, 'a', value: 'A'),
        makeNode(2, 'a', parentTs: 1, value: 'B'),
        makeNode(3, 'a', parentTs: 2, value: 'C'),
      ]));

      expect(listElements(state), ['A', 'B', 'C']);
    });

    test('listNodeIds returns HLC IDs', () {
      final state = RgaListState(keyed([
        makeNode(1, 'a', value: 'A'),
        makeNode(2, 'a', parentTs: 1, value: 'B'),
      ]));

      final ids = listNodeIds(state);
      expect(ids, hasLength(2));
      expect(ids[0].ts, BigInt.one);
      expect(ids[1].ts, BigInt.two);
    });
  });

  group('Document CRDT merge', () {
    FieldState doc(HLC hlc, DocumentCrdtState s) =>
        FieldState(type: CrdtType.document, hlc: hlc, nodeId: hlc.node, docState: s);

    test('merges two empty documents', () {
      final merged = mergeDocument(const DocumentCrdtState(), const DocumentCrdtState());
      expect(merged.fields, isEmpty);
      expect(documentResolve(merged), isEmpty);
    });

    test('merges disjoint paths', () {
      final local = DocumentCrdtState({'name': lwwField('Alice', h(1, 'a'))});
      final remote = DocumentCrdtState({'email': lwwField('alice@example.com', h(2, 'b'))});

      final resolved = documentResolve(mergeDocument(local, remote));
      expect(resolved['name'], 'Alice');
      expect(resolved['email'], 'alice@example.com');
    });

    test('LWW resolution for shared paths', () {
      final local = DocumentCrdtState({'name': lwwField('Alice', h(100, 'a'))});
      final remote = DocumentCrdtState({'name': lwwField('Bob', h(200, 'b'))});

      final resolved = documentResolve(mergeDocument(local, remote));
      // Remote has higher HLC, so Bob wins.
      expect(resolved['name'], 'Bob');
    });

    test('documentResolve builds nested structure', () {
      final state = DocumentCrdtState({
        'title': lwwField('My Doc', h(1, 'a')),
        'count': FieldState(
          type: CrdtType.counter,
          hlc: h(2, 'a'),
          nodeId: 'a',
          counterState: const PnCounterState(inc: {'a': 5}),
        ),
      });

      final resolved = documentResolve(state);
      expect(resolved['title'], 'My Doc');
      expect(resolved['count'], 5);
    });

    test('documentResolve handles deep paths', () {
      // Simulate a nested document within a document field.
      final innerDoc = DocumentCrdtState({
        'street': lwwField('123 Main St', h(1, 'a')),
        'city': lwwField('Springfield', h(2, 'a')),
      });
      final state = DocumentCrdtState({'address': doc(h(3, 'a'), innerDoc)});

      final resolved = documentResolve(state);
      expect(resolved['address'], {'street': '123 Main St', 'city': 'Springfield'});
    });
  });

  group('Go parity extras', () {
    HLC n(int ts, String node) => HLC(BigInt.from(ts), 0, node);

    test('a redelivered older counter change keeps the newer field clock', () {
      final first = applyChange(
        null,
        ChangeRecord(
          table: 't',
          pk: '1',
          field: 'v',
          crdtType: CrdtType.counter,
          hlc: n(5, 'a'),
          nodeId: 'a',
          counterDelta: const CounterDelta(2, 0),
        ),
      );
      final again = applyChange(
        first,
        ChangeRecord(
          table: 't',
          pk: '1',
          field: 'v',
          crdtType: CrdtType.counter,
          hlc: n(1, 'a'),
          nodeId: 'a',
          counterDelta: const CounterDelta(1, 0),
        ),
      );
      expect(again.hlc, n(5, 'a'));
      expect(counterValue(again.counterState!), 2);
    });

    test('a list insert with a zero node id uses the change clock', () {
      final fs = applyChange(
        null,
        ChangeRecord(
          table: 't',
          pk: '1',
          field: 'l',
          crdtType: CrdtType.list,
          hlc: n(7, 'a'),
          nodeId: 'a',
          listOp: ListOperation(ListOpType.insert, value: const JsonValue('x')),
        ),
      );
      expect(fs.listState!.nodes.keys, [hlcString(n(7, 'a'))]);
    });

    test('keysForElement finds a non-canonical key for the same value', () {
      final s = OrSetState(entries: {'"a<b"': [OrSetTag('js', n(1, 'js'))]});
      expect(keysForElement(s, 'a<b'), ['"a<b"']);
    });

    test('a remove naming a RawJson key removes that exact key', () {
      final local = applyChange(
        null,
        ChangeRecord(
          table: 't',
          pk: '1',
          field: 's',
          crdtType: CrdtType.set,
          hlc: n(2, 'a'),
          nodeId: 'a',
          state: setFieldState(OrSetState(entries: {'"a<b"': [OrSetTag('js', n(1, 'js'))]}), n(1, 'js'), 'js'),
        ),
      );
      final removed = applyChange(
        local,
        ChangeRecord(
          table: 't',
          pk: '1',
          field: 's',
          crdtType: CrdtType.set,
          hlc: n(3, 'a'),
          nodeId: 'a',
          setOp: SetOperation(SetOpType.remove, const [RawJson('"a<b"')], tags: [OrSetTag('js', n(1, 'js'))]),
        ),
      );
      expect(setElements(removed.setState!), isEmpty);
    });

    test('documentResolve nests dotted paths and the nested path wins', () {
      final d = DocumentCrdtState({
        'a': FieldState(type: CrdtType.lww, hlc: n(1, 'a'), nodeId: 'a', value: const JsonValue(1)),
        'a.b': FieldState(type: CrdtType.lww, hlc: n(1, 'a'), nodeId: 'a', value: const JsonValue(2)),
      });
      expect(documentResolve(d), {
        'a': {'b': 2},
      });
    });
  });

  group('mergeField null sides', () {
    test('a counter field merges with a null side as the other side itself', () {
      final c = FieldState(
        type: CrdtType.counter,
        hlc: h(1, 'a'),
        nodeId: 'a',
        counterState: const PnCounterState(inc: {'a': 5}),
      );
      expect(mergeField(null, c), same(c));
      expect(mergeField(c, null), same(c));
    });

    test('a set field merges with a null side as the other side itself', () {
      final s = setFieldState(OrSetState(entries: {'"a"': [OrSetTag('n', h(1, 'n'))]}), h(1, 'n'), 'n');
      expect(mergeField(null, s), same(s));
      expect(mergeField(s, null), same(s));
    });

    test('both sides null is an ArgumentError', () {
      expect(() => mergeField(null, null), throwsArgumentError);
    });
  });

  group('mergeField', () {
    test('different types throw a CrdtMergeError with Go text', () {
      expect(
        () => mergeField(lwwField('x', h(1, 'a')), FieldState(type: CrdtType.counter, hlc: h(2, 'b'), nodeId: 'b')),
        throwsA(isA<CrdtMergeError>().having((e) => e.message, 'message', 'crdt: cannot merge different types: lww vs counter')),
      );
    });

    test('two untyped fields throw a CrdtMergeError', () {
      final none = FieldState(type: CrdtType.none, hlc: h(1, 'a'), nodeId: 'a');
      expect(
        () => mergeField(none, none),
        throwsA(isA<CrdtMergeError>().having((e) => e.message, 'message', 'crdt: unknown type: ')),
      );
    });

    test('a counter merge keeps the newer clock and node and drops the value', () {
      final a = FieldState(
        type: CrdtType.counter,
        hlc: h(5, 'a'),
        nodeId: 'a',
        counterState: const PnCounterState(inc: {'a': 1}),
      );
      final b = FieldState(
        type: CrdtType.counter,
        hlc: h(2, 'b'),
        nodeId: 'b',
        counterState: const PnCounterState(inc: {'b': 1}),
      );
      for (final merged in [mergeField(a, b), mergeField(b, a)]) {
        expect(merged.hlc, h(5, 'a'));
        expect(merged.nodeId, 'a');
        expect(merged.value, isNull);
        expect(counterValue(merged.counterState!), 2);
      }
    });

    test('a set merge materialises the live elements into value', () {
      final a = setFieldState(OrSetState(entries: {'"a"': [OrSetTag('n', h(1, 'n'))]}), h(1, 'n'), 'n');
      final b = setFieldState(OrSetState(entries: {'"b"': [OrSetTag('m', h(2, 'm'))]}), h(2, 'm'), 'm');
      final merged = mergeField(a, b);
      expect(merged.value!.value, ['a', 'b']);
      expect(merged.hlc, h(2, 'm'));
    });

    test('an empty set or list merge writes [] as value where Go writes null', () {
      final s = mergeField(
        setFieldState(const OrSetState(), h(1, 'a'), 'a'),
        setFieldState(const OrSetState(), h(2, 'a'), 'a'),
      );
      expect(s.value!.value, isEmpty);
      final l = mergeField(
        listFieldState(const RgaListState(), h(1, 'a'), 'a'),
        listFieldState(const RgaListState(), h(2, 'a'), 'a'),
      );
      expect(l.value!.value, isEmpty);
    });

    test('a text merge materialises the visible text and does not alias an input', () {
      final a = newTextState();
      applyTextOp(a, TextOperation(TextOpType.insert, content: 'ab', origin: h(1, 'a')), 'a', h(1, 'a'));
      final fa = textFieldState(a, h(1, 'a'), 'a');
      final fb = FieldState(type: CrdtType.text, hlc: h(2, 'b'), nodeId: 'b');
      final merged = mergeField(fa, fb);
      expect(merged.value!.value, 'ab');
      expect(merged.textState, isNot(same(a)));
      expect(merged.hlc, h(2, 'b'));
    });
  });

  group('mergeDocument', () {
    test('a type mismatch at a path resolves by the higher clock, either way round', () {
      final counter = FieldState(
        type: CrdtType.counter,
        hlc: h(9, 'b'),
        nodeId: 'b',
        counterState: const PnCounterState(inc: {'b': 4}),
      );
      final a = DocumentCrdtState({'x': lwwField(1, h(1, 'a'))});
      final b = DocumentCrdtState({'x': counter});
      expect(documentResolve(mergeDocument(a, b)), {'x': 4});
      expect(documentResolve(mergeDocument(b, a)), {'x': 4});

      final olderCounter = FieldState(
        type: CrdtType.counter,
        hlc: h(0, 'b'),
        nodeId: 'b',
        counterState: const PnCounterState(inc: {'b': 4}),
      );
      expect(documentResolve(mergeDocument(a, DocumentCrdtState({'x': olderCounter}))), {'x': 1});
    });

    test('Go parity: same-type counters and sets under one path merge, not pick by clock', () {
      // crdt-js picked the higher clock here; Go MergeDocument calls MergeField.
      FieldState counter(String node, int ts, int inc) => FieldState(
            type: CrdtType.counter,
            hlc: h(ts, node),
            nodeId: node,
            counterState: PnCounterState(inc: {node: inc}),
          );
      final merged = mergeDocument(
        DocumentCrdtState({'n': counter('a', 1, 3)}),
        DocumentCrdtState({'n': counter('b', 2, 4)}),
      );
      expect(documentResolve(merged), {'n': 7});

      final set = mergeDocument(
        DocumentCrdtState({'s': setFieldState(OrSetState(entries: {'"a"': [OrSetTag('a', h(1, 'a'))]}), h(1, 'a'), 'a')}),
        DocumentCrdtState({'s': setFieldState(OrSetState(entries: {'"b"': [OrSetTag('b', h(2, 'b'))]}), h(2, 'b'), 'b')}),
      );
      expect(documentResolve(set), {
        's': ['a', 'b'],
      });
    });
  });

  group('documentResolve', () {
    test('resolves a nested document, set, list and text from their states', () {
      final text = newTextState();
      applyTextOp(text, TextOperation(TextOpType.insert, content: 'hi', origin: h(1, 'a')), 'a', h(1, 'a'));
      final d = DocumentCrdtState({
        'doc': FieldState(
          type: CrdtType.document,
          hlc: h(1, 'a'),
          nodeId: 'a',
          docState: DocumentCrdtState({'k': lwwField('v', h(1, 'a'))}),
        ),
        'set': FieldState(type: CrdtType.set, hlc: h(1, 'a'), nodeId: 'a'),
        'list': FieldState(type: CrdtType.list, hlc: h(1, 'a'), nodeId: 'a'),
        'counter': FieldState(type: CrdtType.counter, hlc: h(1, 'a'), nodeId: 'a'),
        'text': FieldState(type: CrdtType.text, hlc: h(1, 'a'), nodeId: 'a', textState: text),
      });
      expect(documentResolve(d), {
        'doc': {'k': 'v'},
        'set': isEmpty,
        'list': isEmpty,
        'counter': 0,
        'text': 'hi',
      });
    });

    test('a leaf object at a prefix takes the nested paths in a copy and is left untouched', () {
      final leaf = <String, Object?>{'k': 1};
      final d = DocumentCrdtState({
        'c': lwwField(leaf, h(1, 'a')),
        'c.d': lwwField(2, h(1, 'a')),
      });
      expect(documentResolve(d), {
        'c': {'k': 1, 'd': 2},
      });
      expect(leaf, {'k': 1});
    });

    test('a deeper path replaces a scalar leaf at every prefix', () {
      final d = DocumentCrdtState({
        'a': lwwField(1, h(1, 'a')),
        'a.b': lwwField(2, h(1, 'a')),
        'a.b.c': lwwField(3, h(1, 'a')),
      });
      expect(documentResolve(d), {
        'a': {
          'b': {'c': 3},
        },
      });
    });
  });

  group('list walk', () {
    RgaNode node(int ts, String n, {HLC? parent, bool tombstone = false, Object? value}) => RgaNode(
          id: h(ts, n),
          nodeId: n,
          parentId: parent ?? HLC.zero,
          value: JsonValue(value ?? ts),
          tombstone: tombstone,
        );

    test('concurrent siblings list the newer insert first and children follow their parent', () {
      final s = RgaListState({
        hlcString(h(1, 'a')): node(1, 'a'),
        hlcString(h(2, 'b')): node(2, 'b'),
        hlcString(h(3, 'a')): node(3, 'a', parent: h(1, 'a')),
      });
      expect(listElements(s), [2, 1, 3]);
      expect(listNodeIds(s), [h(2, 'b'), h(1, 'a'), h(3, 'a')]);
    });

    test('a tombstoned node still anchors its children', () {
      final s = RgaListState({
        hlcString(h(1, 'a')): node(1, 'a', tombstone: true),
        hlcString(h(2, 'a')): node(2, 'a', parent: h(1, 'a')),
      });
      expect(listElements(s), [2]);
    });

    test('a node whose parent is unknown is not listed', () {
      final s = RgaListState({hlcString(h(2, 'a')): node(2, 'a', parent: h(1, 'a'))});
      expect(listElements(s), isEmpty);
    });

    test('a tombstone from either side wins, whichever side is local', () {
      final live = RgaListState({hlcString(h(1, 'a')): node(1, 'a')});
      final dead = RgaListState({hlcString(h(1, 'a')): node(1, 'a', tombstone: true)});
      expect(listElements(mergeList(live, dead)), isEmpty);
      expect(listElements(mergeList(dead, live)), isEmpty);
    });

    test('two nodes sharing an id under different keys cannot loop the walk', () {
      final s = RgaListState({
        'k1': RgaNode(id: h(1, 'a'), nodeId: 'a', parentId: HLC.zero, value: const JsonValue('x')),
        'k2': RgaNode(id: h(1, 'a'), nodeId: 'a', parentId: h(1, 'a'), value: const JsonValue('y')),
      });
      expect(listElements(s), hasLength(2));
    });

    test('a very long parent chain does not overflow the stack', () {
      final nodes = <String, RgaNode>{};
      for (var i = 1; i <= 20000; i++) {
        nodes[hlcString(h(i, 'a'))] = node(i, 'a', parent: i == 1 ? null : h(i - 1, 'a'));
      }
      expect(listElements(RgaListState(nodes)), hasLength(20000));
    });
  });

  group('set keys', () {
    test('elements sort in Go byte order, so an astral key follows U+FFFD', () {
      final tag = OrSetTag('n', h(1, 'n'));
      final s = OrSetState(entries: {
        '"\u{1F600}"': [tag],
        '"�"': [tag],
        '"a"': [tag],
      });
      expect(setElements(s), ['a', '�', '\u{1F600}']);
    });

    test('keysForElement matches numbers by value and returns every spelling', () {
      final tag = OrSetTag('n', h(1, 'n'));
      final s = OrSetState(entries: {
        '1': [tag],
        '1.0': [tag],
        '"1"': [tag],
      });
      expect(keysForElement(s, 1), unorderedEquals(['1', '1.0']));
      expect(keysForElement(s, '1'), ['"1"']);
      expect(keysForElement(s, 2), isEmpty);
    });

    test('keysForElement compares objects and arrays deeply', () {
      final tag = OrSetTag('n', h(1, 'n'));
      final s = OrSetState(entries: {
        '{"b":[1,2],"a":null}': [tag],
      });
      expect(keysForElement(s, {'a': null, 'b': [1, 2]}), ['{"b":[1,2],"a":null}']);
    });

    test('elementKey writes a RawJson verbatim and anything else as Go JSON', () {
      expect(elementKey(const RawJson('"a<b"')), '"a<b"');
      expect(elementKey('a<b'), r'"a\u003cb"');
      expect(elementKey({'k': 1}), '{"k":1}');
    });

    test('a legacy tag-only removal key still hides the element', () {
      final tag = OrSetTag('n', h(1, 'n'));
      final s = OrSetState(entries: {'"x"': [tag]}, removed: {tagKey(tag): true});
      expect(tagRemoved(s, '"x"', tag), isTrue);
      expect(setElementKeys(s), isEmpty);
    });

    test('an element scoped removal hides only that element', () {
      final tag = OrSetTag('n', h(1, 'n'));
      final s = OrSetState(
        entries: {'"x"': [tag], '"y"': [tag]},
        removed: {removedKey('"x"', tag): true},
      );
      expect(setElementKeys(s), ['"y"']);
    });
  });

  group('applyChange', () {
    test('a legacy remove (no tags) removes only the tags older than the remove', () {
      final added = applyChange(null, change(CrdtType.set, h(1, 'a'), setOp: const SetOperation(SetOpType.add, ['x'])));
      final newerAdd = applyChange(added, change(CrdtType.set, h(9, 'b'), setOp: const SetOperation(SetOpType.add, ['x'])));
      final removed = applyChange(
        newerAdd,
        change(CrdtType.set, h(5, 'c'), setOp: const SetOperation(SetOpType.remove, ['x'])),
      );
      // The add at 9 is newer than the remove at 5, so it survives (add wins).
      expect(setElements(removed.setState!), ['x']);
    });

    test('a text change whose op cannot apply throws a CrdtApplyError wrapping the cause', () {
      Object? thrown;
      try {
        applyChange(null, change(CrdtType.text, h(1, 'a'), textOp: TextOperation(TextOpType.insert)));
      } on CrdtApplyError catch (e) {
        thrown = e;
      }
      expect(thrown, isA<CrdtApplyError>().having((e) => e.message, 'message', 'crdt: text insert without content'));
      expect((thrown! as CrdtApplyError).cause, isA<StateError>());
    });

    test('a text change leaves value unset and keeps the newer authorship stamp', () {
      final first = applyChange(
        null,
        change(CrdtType.text, h(5, 'a'), textOp: TextOperation(TextOpType.insert, content: 'x', origin: h(5, 'a'))),
      );
      expect(first.value, isNull);
      final older = applyChange(
        first,
        change(CrdtType.text, h(2, 'b'), textOp: TextOperation(TextOpType.insert, content: 'y', origin: h(2, 'b'))),
      );
      expect(older.hlc, h(5, 'a'));
      expect(older.nodeId, 'a');
      expect(textValue(older.textState!), anyOf('xy', 'yx'));
    });

    test('a document path write with a missing value key stores an absent value', () {
      final fs = applyChange(
        null,
        change(CrdtType.document, h(1, 'a'), value: const JsonValue({'path': 'p'})),
      );
      expect(fs.docState!.fields['p']!.value, isNull);
      expect(documentResolve(fs.docState!), {'p': null});
    });

    test('a document change takes its payload keys case-insensitively, later keys winning', () {
      final fs = applyChange(
        null,
        change(CrdtType.document, h(1, 'a'), value: const JsonValue({'Path': 'p', 'VALUE': 1, 'value': 2})),
      );
      expect(documentResolve(fs.docState!), {'p': 2});
    });

    test('a document path delete older than the stored path changes nothing', () {
      final written = applyChange(
        null,
        change(CrdtType.document, h(5, 'a'), value: const JsonValue({'path': 'p', 'value': 1})),
      );
      final stale = applyChange(
        written,
        change(CrdtType.document, h(2, 'b'), value: const JsonValue({'path': 'p'}), tombstone: true),
      );
      expect(documentResolve(stale.docState!), {'p': 1});
      expect(stale.hlc, h(5, 'a'));
    });

    test('a state carrier of another type than the change throws with Go text', () {
      final carrier = FieldState(type: CrdtType.counter, hlc: h(1, 'a'), nodeId: 'a');
      expect(
        () => applyChange(null, change(CrdtType.lww, h(1, 'a'), state: carrier)),
        throwsA(isA<CrdtApplyError>().having((e) => e.message, 'message', 'crdt: change type lww carries counter state')),
      );
    });

    test('a change of another type than the field throws before anything else', () {
      expect(
        () => applyChange(lwwField('v', h(1, 'a')), change(CrdtType.counter, h(2, 'a'))),
        throwsA(isA<CrdtApplyError>().having((e) => e.message, 'message', 'crdt: cannot apply counter change onto lww field')),
      );
    });

    test('a list move tombstones the old node and reinserts under the new parent', () {
      var fs = applyChange(
        null,
        change(CrdtType.list, h(1, 'a'), listOp: ListOperation(ListOpType.insert, value: const JsonValue('a'))),
      );
      fs = applyChange(
        fs,
        change(
          CrdtType.list,
          h(2, 'a'),
          listOp: ListOperation(ListOpType.insert, parentId: h(1, 'a'), value: const JsonValue('b')),
        ),
      );
      fs = applyChange(
        fs,
        change(
          CrdtType.list,
          h(3, 'a'),
          listOp: ListOperation(ListOpType.move, nodeId: h(1, 'a'), parentId: h(2, 'a'), value: const JsonValue('a')),
        ),
      );
      expect(listElements(fs.listState!), ['b', 'a']);
      expect(fs.value!.value, ['b', 'a']);
    });

    test('a list delete for an unseen node keeps the tombstone so a late insert stays deleted', () {
      var fs = applyChange(
        null,
        change(CrdtType.list, h(9, 'c'), listOp: ListOperation(ListOpType.delete, nodeId: h(2, 'a'))),
      );
      fs = applyChange(
        fs,
        change(
          CrdtType.list,
          h(2, 'a'),
          listOp: ListOperation(ListOpType.insert, nodeId: h(2, 'a'), value: const JsonValue('late')),
        ),
      );
      expect(listElements(fs.listState!), isEmpty);
      expect(fs.listState!.nodes, hasLength(1));
    });
  });

  group('mergeState', () {
    DocumentState state(Map<String, FieldState> fields, {bool tombstone = false, HLC? at}) =>
        DocumentState(table: 't', pk: '1', fields: fields, tombstone: tombstone, tombstoneHlc: at);

    test('a null side yields the other side itself', () {
      final s = state({});
      expect(mergeState(null, s), same(s));
      expect(mergeState(s, null), same(s));
      expect(() => mergeState(null, null), throwsArgumentError);
    });

    test('a local tombstone newer than every remote field wins', () {
      final merged = mergeState(
        state({}, tombstone: true, at: h(8, 'a')),
        state({'title': lwwField('x', h(6, 'b'))}),
      );
      expect(merged.tombstone, isTrue);
      expect(merged.tombstoneHlc, h(8, 'a'));
    });

    test('a type mismatch names the field', () {
      expect(
        () => mergeState(
          state({'x': lwwField('a', h(1, 'a'))}),
          state({'x': FieldState(type: CrdtType.counter, hlc: h(2, 'b'), nodeId: 'b')}),
        ),
        throwsA(isA<CrdtMergeError>().having((e) => e.message, 'message', 'field x: crdt: cannot merge different types: lww vs counter')),
      );
    });
  });
}
