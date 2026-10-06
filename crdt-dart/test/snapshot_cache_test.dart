// Port of crdt-js src/__tests__/snapshot-cache.test.ts, case for case. The
// `useSyncExternalStore` wording becomes "identical object on repeat reads".
import 'package:grove_crdt/grove_crdt.dart';
import 'package:test/test.dart';

import 'support/store_fakes.dart';

CrdtStore mk() => CrdtStore('n1', HybridClock('n1'));

void main() {
  group('snapshot stability', () {
    test('getDocument returns the same reference between writes', () {
      final store = mk();
      store.setField('t', 'p', 'f', 1);
      expect(store.getDocument('t', 'p'), same(store.getDocument('t', 'p')));
    });

    test('getDocument returns a new reference after a write', () {
      final store = mk();
      store.setField('t', 'p', 'f', 1);
      final first = store.getDocument('t', 'p');
      store.setField('t', 'p', 'f', 2);
      expect(store.getDocument('t', 'p'), isNot(same(first)));
    });

    test('getCollection returns the same reference between writes', () {
      final store = mk();
      store.setField('t', 'p', 'f', 1);
      expect(store.getCollection('t'), same(store.getCollection('t')));
    });

    test('getCollection invalidates when any document in the table changes', () {
      final store = mk();
      store.setField('t', 'p1', 'f', 1);
      final first = store.getCollection('t');
      store.setField('t', 'p2', 'f', 2);
      expect(store.getCollection('t'), isNot(same(first)));
      expect(store.getCollection('t'), hasLength(2));
    });

    test('getListNodeIds returns the same reference between writes', () {
      final store = mk();
      store.insertIntoList('t', 'p', 'items', 'a');
      expect(store.getListNodeIds('t', 'p', 'items'), same(store.getListNodeIds('t', 'p', 'items')));
    });

    test('registering a plugin invalidates cached snapshots', () {
      final store = mk();
      store.setField('t', 'p', 'f', 1);
      final before = store.getDocument('t', 'p');
      store.use(FnPlugin('tagger', onTransformDocument: (t, p, doc) => {...doc, 'tagged': true}));
      final after = store.getDocument('t', 'p');
      expect(after, isNot(same(before)));
      expect(after?['tagged'], isTrue);
    });
  });

  group('beyond the crdt-js behaviour', () {
    test('a returned document is frozen, so changing it never changes the store', () {
      final store = mk();
      store.setField('t', 'p', 'meta', {
        'tags': ['a'],
        'inner': {'x': 1},
      });
      final doc = store.getDocument('t', 'p')!;
      expect(() => doc['meta'] = 'x', throwsUnsupportedError);
      final meta = doc['meta']! as Map<String, Object?>;
      expect(() => (meta['tags']! as List<Object?>).add('b'), throwsUnsupportedError);
      expect(() => (meta['inner']! as Map<String, Object?>)['x'] = 2, throwsUnsupportedError);
      expect(store.getDocument('t', 'p')!['meta'], {
        'tags': ['a'],
        'inner': {'x': 1},
      });
      expect(store.exportTable('t')['p']!.fields['meta']!.value!.value, {
        'tags': ['a'],
        'inner': {'x': 1},
      });
    });

    test('a returned collection and its documents are frozen', () {
      final store = mk();
      store.setField('t', 'p', 'f', [1, 2]);
      final col = store.getCollection('t');
      expect(() => col.add(const {}), throwsUnsupportedError);
      expect(() => col.first['f'] = 0, throwsUnsupportedError);
      expect(() => (col.first['f']! as List<Object?>).clear(), throwsUnsupportedError);
      expect(store.getDocument('t', 'p')!['f'], [1, 2]);
    });

    test('a map the app wrote and then changes does not change the store', () {
      final store = mk();
      final value = <String, Object?>{
        'list': [1],
      };
      store.setField('t', 'p', 'f', value);
      (value['list']! as List<Object?>).add(2);
      value['extra'] = true;
      expect(store.getDocument('t', 'p')!['f'], {
        'list': [1],
      });
    });

    test('a plugin that keeps the map it returned cannot change later reads', () {
      final store = mk();
      Map<String, Object?>? kept;
      store.use(FnPlugin('keeper', onTransformDocument: (t, p, doc) => kept = {...doc}));
      store.setField('t', 'p', 'f', 1);
      final first = store.getDocument('t', 'p');
      kept!['f'] = 99;
      expect(store.getDocument('t', 'p'), same(first));
      expect(first!['f'], 1);
    });
  });
}
