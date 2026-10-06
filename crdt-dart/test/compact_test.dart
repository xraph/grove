import 'dart:convert';

import 'package:grove_crdt/grove_crdt.dart';
import 'package:test/test.dart';

import 'support/fixture.dart';
import 'support/json_equivalent.dart';

HLC h(int n) => HLC(BigInt.from(n), 0, 'n1');
String key(int n) => hlcString(h(n));

RgaNode node(int n, HLC parent, Object? value, {bool tombstone = false}) =>
    RgaNode(id: h(n), nodeId: 'n1', parentId: parent, value: JsonValue(value), tombstone: tombstone);

TextFragment frag(int origin, int start, String content, int length, {bool tombstone = false}) =>
    TextFragment(origin: h(origin), start: start, content: content, length: length, tombstone: tombstone);

void main() {
  group('compaction', () {
    test('drops tombstoned leaf nodes older than the horizon', () {
      final state = RgaListState({
        key(1): node(1, HLC.zero, 'a'),
        key(2): node(2, h(1), 'b', tombstone: true),
      });
      final r = compactListState(state, h(10));
      expect(r.dropped, 1);
      expect(r.state.nodes.keys.toList(), [key(1)]);
    });

    test('keeps a tombstoned node that still anchors a child', () {
      final state = RgaListState({
        key(1): node(1, HLC.zero, 'a', tombstone: true),
        key(2): node(2, h(1), 'b'),
      });
      expect(compactListState(state, h(10)).dropped, 0);
    });

    test('cascades: dropping a leaf exposes its parent', () {
      final state = RgaListState({
        key(1): node(1, HLC.zero, 'a', tombstone: true),
        key(2): node(2, h(1), 'b', tombstone: true),
      });
      final r = compactListState(state, h(10));
      expect(r.dropped, 2);
      expect(r.state.nodes, isEmpty);
    });

    test('does not drop anything newer than the horizon', () {
      final state = RgaListState({key(20): node(20, HLC.zero, 'a', tombstone: true)});
      expect(compactListState(state, h(10)).dropped, 0);
    });

    test('drops removed OR-Set tags and prunes tagless entries', () {
      final tag = OrSetTag('n1', h(1));
      final state = OrSetState(
        entries: {'"a"': [tag]},
        removed: {'"a"|n1:${hlcString(h(1))}': true},
      );
      final r = compactSetState(state, h(10));
      expect(r.dropped, 1);
      expect(r.state.entries['"a"'], isNull);
      expect(r.state.removed, isEmpty);
    });

    test('skeletonizes tombstoned text but preserves addresses', () {
      final state = TextState({
        key(1): [
          frag(1, 0, 'gone', 4, tombstone: true),
          frag(1, 4, 'kept', 4),
        ],
      });
      final r = compactTextState(state, h(10));
      expect(r.dropped, 1);
      final frags = r.state.frags[key(1)]!;
      expect(frags[0].content, '');
      expect(frags[0].length, 4); // address preserved
      expect(frags[1].content, 'kept');
    });

    test(
      'store.compact is a no-op for a zero horizon',
      () {},
      skip: 'needs CrdtStore, which is ported in Task 10. The store-independent '
          'zero-horizon cases are below.',
    );

    test('compactDocument compacts list and set fields, leaves other field types untouched', () {
      final title = FieldState(type: CrdtType.lww, hlc: h(1), nodeId: 'n1', value: const JsonValue('hello'));
      final doc = DocumentState(table: 't', pk: 'p', fields: {
        'items': FieldState(
          type: CrdtType.list,
          hlc: h(1),
          nodeId: 'n1',
          listState: RgaListState({
            key(1): node(1, HLC.zero, 'a'),
            key(2): node(2, h(1), 'b', tombstone: true),
          }),
        ),
        'tags': FieldState(
          type: CrdtType.set,
          hlc: h(1),
          nodeId: 'n1',
          setState: OrSetState(
            entries: {'"a"': [OrSetTag('n1', h(1))]},
            removed: {'"a"|n1:${key(1)}': true},
          ),
        ),
        // A field type compaction must leave untouched entirely.
        'title': title,
      });

      final r = compactDocument(doc, h(10));
      expect(r.dropped, 2); // 1 dropped list leaf + 1 dropped set tag
      expect(r.doc.fields['items']!.listState!.nodes.keys.toList(), [key(1)]);
      expect(r.doc.fields['tags']!.setState!.entries['"a"'], isNull);
      // Untouched field keeps its exact reference, not just an equal copy.
      expect(r.doc.fields['title'], same(title));
    });

    test("does not recurse into nested docState (Go's State.Compact has no TypeDocument case)", () {
      final nested = FieldState(
        type: CrdtType.document,
        hlc: h(1),
        nodeId: 'n1',
        docState: DocumentCrdtState({
          'inner': FieldState(
            type: CrdtType.list,
            hlc: h(1),
            nodeId: 'n1',
            listState: RgaListState({
              key(1): node(1, HLC.zero, 'x'),
              key(2): node(2, h(1), 'y', tombstone: true),
            }),
          ),
        }),
      );
      final doc = DocumentState(table: 't', pk: 'p', fields: {'nested': nested});

      final r = compactDocument(doc, h(10));
      expect(r.dropped, 0);
      // Not recursed: the droppable tombstone inside the nested docState
      // survives untouched, and the field keeps its exact reference.
      expect(r.doc.fields['nested'], same(nested));
      expect(r.doc.fields['nested']!.docState!.fields['inner']!.listState!.nodes.keys.toList(), [key(1), key(2)]);
    });

    test('returns the identical object at every level when nothing is dropped', () {
      final listState = RgaListState({key(1): node(1, HLC.zero, 'a')});
      expect(compactListState(listState, h(10)).state, same(listState));

      final setState = OrSetState(entries: {'"a"': [OrSetTag('n1', h(1))]});
      expect(compactSetState(setState, h(10)).state, same(setState));

      final textState = TextState({
        key(1): [frag(1, 0, 'kept', 4)],
      });
      expect(compactTextState(textState, h(10)).state, same(textState));

      final doc = DocumentState(table: 't', pk: 'p', fields: {
        'items': FieldState(type: CrdtType.list, hlc: h(1), nodeId: 'n1', listState: listState),
        'tags': FieldState(type: CrdtType.set, hlc: h(1), nodeId: 'n1', setState: setState),
      });
      expect(compactDocument(doc, h(10)).doc, same(doc));
    });
  });

  group('compaction beyond the crdt-js cases', () {
    test('a zero horizon returns the identical input at every level', () {
      final listState = RgaListState({key(1): node(1, HLC.zero, 'a', tombstone: true)});
      expect(compactListState(listState, HLC.zero).state, same(listState));
      final setState = OrSetState(
        entries: {'"a"': [OrSetTag('n1', h(1))]},
        removed: {'"a"|n1:${key(1)}': true},
      );
      expect(compactSetState(setState, HLC.zero).state, same(setState));
      final textState = TextState({
        key(1): [frag(1, 0, 'gone', 4, tombstone: true)],
      });
      expect(compactTextState(textState, HLC.zero).state, same(textState));
      final doc = DocumentState(table: 't', pk: 'p', fields: {
        'items': FieldState(type: CrdtType.list, hlc: h(1), nodeId: 'n1', listState: listState),
      });
      expect(compactDocument(doc, HLC.zero).doc, same(doc));
    });

    test('keeps the legacy tag-only marker and the sibling that relies on it', () {
      // Go parity: ORSetState.Compact deletes only the element-scoped marker.
      final tag = OrSetTag('n1', h(1));
      final legacy = '${tag.node}:${key(1)}';
      final state = OrSetState(
        entries: {'"x"': [tag], '"y"': [tag]},
        removed: {legacy: true},
      );
      final r = compactSetState(state, h(10));
      expect(r.dropped, 2);
      expect(r.state.entries, isEmpty);
      expect(r.state.removed, {legacy: true});
    });

    test('a removed tag that is not older than the horizon stays', () {
      final tag = OrSetTag('n1', h(10));
      final state = OrSetState(
        entries: {'"a"': [tag]},
        removed: {'"a"|n1:${key(10)}': true},
      );
      expect(compactSetState(state, h(10)).dropped, 0);
    });

    test('prunes an entry with no tags without counting it', () {
      // Go parity: ORSetState.Compact deletes it and does not count it.
      final state = OrSetState(entries: {
        '"gone"': const [],
        '"kept"': [OrSetTag('n1', h(1))],
      });
      final r = compactSetState(state, h(10));
      expect(r.dropped, 0);
      expect(r.state.entries.keys.toList(), ['"kept"']);
    });

    test('a tombstone at exactly the horizon stays, and the horizon breaks ties by counter and node', () {
      final at = RgaListState({key(10): node(10, HLC.zero, 'a', tombstone: true)});
      expect(compactListState(at, h(10)).dropped, 0);
      HLC id(int c, String node) => HLC(BigInt.from(10), c, node);
      RgaNode tomb(HLC i) => RgaNode(id: i, nodeId: i.node, parentId: HLC.zero, tombstone: true);
      final tied = RgaListState({
        hlcString(id(2, 'n1')): tomb(id(2, 'n1')),
        hlcString(id(3, 'n1')): tomb(id(3, 'n1')),
        hlcString(id(4, 'n1')): tomb(id(4, 'n1')),
      });
      final r = compactListState(tied, id(3, 'n1'));
      expect(r.dropped, 1);
      expect(r.state.nodes.keys, containsAll([hlcString(id(3, 'n1')), hlcString(id(4, 'n1'))]));
    });

    test('clears the attrs of a skeleton and keeps them on live text', () {
      final attrs = {'bold': AttrState(const JsonValue(true), h(5), 'n1')};
      final state = TextState({
        key(1): [
          TextFragment(origin: h(1), start: 0, content: '😀gone', length: 5, tombstone: true, attrs: attrs),
          TextFragment(origin: h(1), start: 5, content: 'live', length: 4, attrs: attrs),
        ],
      });
      final r = compactTextState(state, h(10));
      final frags = r.state.frags[key(1)]!;
      expect(frags[0].attrs, isEmpty);
      expect(frags[0].length, 5);
      expect(frags[1].attrs.keys, ['bold']);
    });

    test('coalesces contiguous skeletons and counts each merge', () {
      final state = TextState({
        key(1): [
          frag(1, 0, 'ab', 2, tombstone: true),
          frag(1, 2, '😀😀', 2, tombstone: true),
          frag(1, 4, 'cd', 2, tombstone: true),
          frag(1, 6, 'live', 4),
        ],
      });
      final r = compactTextState(state, h(10));
      expect(r.dropped, 5); // three freed, two merged away
      final frags = r.state.frags[key(1)]!;
      expect(frags, hasLength(2));
      expect((frags[0].start, frags[0].length, frags[0].content), (0, 6, ''));
      expect(frags[1].content, 'live');
    });

    test('does not coalesce skeletons across a gap or a live fragment', () {
      final gap = TextState({
        key(1): [frag(1, 0, 'ab', 2, tombstone: true), frag(1, 5, 'cd', 2, tombstone: true)],
      });
      expect(compactTextState(gap, h(10)).state.frags[key(1)], hasLength(2));
      final live = TextState({
        key(1): [
          frag(1, 0, 'ab', 2, tombstone: true),
          frag(1, 2, 'xy', 2),
          frag(1, 4, 'cd', 2, tombstone: true),
        ],
      });
      expect(compactTextState(live, h(10)).state.frags[key(1)], hasLength(3));
    });

    test('coalesces skeletons that were already empty, which counts as a drop', () {
      final state = TextState({
        key(1): [
          TextFragment(origin: h(1), start: 0, content: '', length: 3, tombstone: true),
          TextFragment(origin: h(1), start: 3, content: '', length: 2, tombstone: true),
        ],
      });
      final r = compactTextState(state, h(10));
      expect(r.dropped, 1);
      expect(r.state.frags[key(1)]!.single.length, 5);
    });

    test('compaction after convergence leaves every visible value unchanged', () {
      HLC n(int ts, String node) => HLC(BigInt.from(ts), 0, node);
      var list = const RgaListState();
      var fs = applyChange(
          null,
          ChangeRecord(
              table: 't',
              pk: '1',
              field: 'l',
              crdtType: CrdtType.list,
              hlc: n(1, 'a'),
              nodeId: 'a',
              listOp: ListOperation(ListOpType.insert, value: const JsonValue('x'))));
      fs = applyChange(
          fs,
          ChangeRecord(
              table: 't',
              pk: '1',
              field: 'l',
              crdtType: CrdtType.list,
              hlc: n(2, 'a'),
              nodeId: 'a',
              listOp: ListOperation(ListOpType.insert,
                  nodeId: n(2, 'a'), parentId: n(1, 'a'), value: const JsonValue('y'))));
      fs = applyChange(
          fs,
          ChangeRecord(
              table: 't',
              pk: '1',
              field: 'l',
              crdtType: CrdtType.list,
              hlc: n(3, 'a'),
              nodeId: 'a',
              listOp: ListOperation(ListOpType.delete, nodeId: n(2, 'a'))));
      list = fs.listState!;
      final before = listElements(list);
      final result = compactListState(list, n(10, 'z'));
      expect(result.dropped, 1);
      expect(listElements(result.state), before);
    });
  });

  group('compaction purity', () {
    String wire(FieldState f) => encodeWire(f.toJson());

    test('never mutates a list, set, text or document it compacts', () {
      final list = RgaListState({
        key(1): node(1, HLC.zero, 'a'),
        key(2): node(2, h(1), 'b', tombstone: true),
      });
      final set = OrSetState(
        entries: {'"a"': [OrSetTag('n1', h(1))], '"b"': [OrSetTag('n1', h(2))]},
        removed: {'"a"|n1:${key(1)}': true},
      );
      final text = TextState({
        key(1): [
          TextFragment(
            origin: h(1),
            start: 0,
            content: 'gone',
            length: 4,
            tombstone: true,
            attrs: {'bold': AttrState(const JsonValue(true), h(2), 'n1')},
          ),
          frag(1, 4, 'kept', 4),
        ],
      });
      final doc = DocumentState(table: 't', pk: 'p', fields: {
        'l': FieldState(type: CrdtType.list, hlc: h(1), nodeId: 'n1', listState: list),
        's': FieldState(type: CrdtType.set, hlc: h(1), nodeId: 'n1', setState: set),
        'x': FieldState(type: CrdtType.text, hlc: h(1), nodeId: 'n1', textState: text),
      });
      final listBefore = encodeWire(list.toJson());
      final setBefore = encodeWire(set.toJson());
      final textBefore = encodeWire(text.toJson());
      final docBefore = encodeWire(doc.toJson());

      final rl = compactListState(list, h(10));
      final rs = compactSetState(set, h(10));
      final rt = compactTextState(text, h(10));
      final rd = compactDocument(doc, h(10));
      expect((rl.dropped, rs.dropped, rt.dropped, rd.dropped), (1, 1, 1, 3));

      expect(encodeWire(list.toJson()), listBefore);
      expect(encodeWire(set.toJson()), setBefore);
      expect(encodeWire(text.toJson()), textBefore);
      expect(encodeWire(doc.toJson()), docBefore);

      // The results hold no fragment or collection the inputs hold, so a later
      // in-place edit of a result cannot reach back into its input.
      expect(rl.state, isNot(same(list)));
      expect(rs.state.entries, isNot(same(set.entries)));
      expect(rs.state.removed, isNot(same(set.removed)));
      final inFrags = text.frags[key(1)]!;
      for (final f in rt.state.frags[key(1)]!) {
        expect(inFrags.any((g) => identical(f, g)), isFalse);
      }
      rt.state.frags[key(1)]!.last.content = 'changed';
      expect(encodeWire(text.toJson()), textBefore);
      expect(wire(rd.doc.fields['x']!), isNot(wire(doc.fields['x']!)));
    });
  });

  group('compaction matches Go', () {
    final cases = (jsonDecode(readFixture('test/fixtures/compact_golden.json')) as List<Object?>)
        .cast<Map<String, Object?>>();

    test('has fixtures', () {
      expect(cases.length, greaterThanOrEqualTo(30));
      expect(cases.map((c) => c['kind']).toSet(), {'list', 'set', 'text', 'document'});
    });

    for (final c in cases) {
      test('${c['kind']}: ${c['name']}', () {
        final before = HLC.fromJson(c['before']);
        final wantDropped = c['dropped']! as int;
        final input = c['input'];
        final inputWire = encodeWire(input);
        final Object? got;
        final int dropped;
        final Object? same0;
        switch (c['kind']) {
          case 'list':
            final s = RgaListState.fromJson(input);
            final r = compactListState(s, before);
            got = r.state.toJson();
            dropped = r.dropped;
            same0 = identical(r.state, s);
            expect(encodeWire(s.toJson()), encodeWire(RgaListState.fromJson(input).toJson()), reason: 'input mutated');
          case 'set':
            final s = OrSetState.fromJson(input);
            final r = compactSetState(s, before);
            got = r.state.toJson();
            dropped = r.dropped;
            same0 = identical(r.state, s);
            expect(encodeWire(s.toJson()), encodeWire(OrSetState.fromJson(input).toJson()), reason: 'input mutated');
          case 'text':
            final s = TextState.fromJson(input);
            final r = compactTextState(s, before);
            got = r.state.toJson();
            dropped = r.dropped;
            same0 = identical(r.state, s);
            expect(encodeWire(s.toJson()), encodeWire(TextState.fromJson(input).toJson()), reason: 'input mutated');
          case 'document':
            final s = DocumentState.fromJson(input);
            final r = compactDocument(s, before);
            got = r.doc.toJson();
            dropped = r.dropped;
            same0 = identical(r.doc, s);
            expect(encodeWire(s.toJson()), encodeWire(DocumentState.fromJson(input).toJson()), reason: 'input mutated');
          default:
            fail('unknown kind ${c['kind']}');
        }
        final gotJson = jsonDecode(encodeWire(got));
        expect(
          jsonEquivalent(c['output'], gotJson),
          isTrue,
          reason: 'go:   ${jsonEncode(c['output'])}\ndart: ${jsonEncode(gotJson)}',
        );
        expect(dropped, wantDropped);
        // Nothing dropped and nothing else changed: the input comes back as is.
        final unchanged = jsonEquivalent(jsonDecode(inputWire), jsonDecode(encodeWire(got)));
        if (wantDropped == 0 && unchanged) expect(same0, isTrue);
      });
    }
  });
}
