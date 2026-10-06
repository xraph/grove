// Port of crdt-js src/__tests__/list-scale.test.ts, case for case.
import 'package:grove_crdt/grove_crdt.dart';
import 'package:test/test.dart';

/// A linear chain of [n] nodes: the shape sequential appends produce.
RgaListState chain(int n) {
  final nodes = <String, RgaNode>{};
  var parent = HLC.zero;
  for (var i = 0; i < n; i++) {
    final id = HLC(BigInt.from(i + 1), 0, 'n1');
    nodes['HLC{ts:${i + 1} c:0 node:n1}'] = RgaNode(id: id, nodeId: 'n1', parentId: parent, value: JsonValue(i));
    parent = id;
  }
  return RgaListState(nodes);
}

void main() {
  group('list traversal scale', () {
    test('resolves a 50,000-element list without overflowing the stack (D9)', () {
      final elements = listElements(chain(50000));
      expect(elements, hasLength(50000));
      expect(elements[0], 0);
      expect(elements[49999], 49999);
    });

    test('listNodeIds matches listElements order at scale', () {
      final ids = listNodeIds(chain(20000));
      expect(ids, hasLength(20000));
      expect(ids[0].ts, BigInt.one);
      expect(ids[19999].ts, BigInt.from(20000));
    });

    test('preserves sibling ordering (newest first) and skips tombstones', () {
      final a = HLC(BigInt.one, 0, 'n1');
      final b = HLC(BigInt.two, 0, 'n1');
      final state = RgaListState({
        'HLC{ts:1 c:0 node:n1}': RgaNode(id: a, nodeId: 'n1', parentId: HLC.zero, value: const JsonValue('a')),
        'HLC{ts:2 c:0 node:n1}': RgaNode(id: b, nodeId: 'n1', parentId: HLC.zero, value: const JsonValue('b')),
        'HLC{ts:3 c:0 node:n1}': RgaNode(
          id: HLC(BigInt.from(3), 0, 'n1'),
          nodeId: 'n1',
          parentId: a,
          value: const JsonValue('gone'),
          tombstone: true,
        ),
      });
      // Siblings sort HLC-descending (RGA insert-right), so b precedes a.
      expect(listElements(state), ['b', 'a']);
    });
  });
}
