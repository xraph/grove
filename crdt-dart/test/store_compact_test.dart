// Port of crdt-js src/__tests__/compact.test.ts lines 181-356 (the four
// CRDTStore.compact cases), case for case. The zero-horizon store case is in
// compact_test.dart, beside the rest of that file's port.
import 'package:grove_crdt/grove_crdt.dart';
import 'package:test/test.dart';

import 'support/store_fakes.dart';

HLC h(int n) => HLC(BigInt.from(n), 0, 'n1');
String key(int n) => 'HLC{ts:$n c:0 node:n1}';

StateSnapshot snapshot(Map<String, Map<String, DocumentState>> tables) => StateSnapshot(
      version: 1,
      nodeId: 'n1',
      timestamp: DateTime.now().millisecondsSinceEpoch,
      pending: [],
      tables: tables,
    );

void main() {
  test('CRDTStore.compact sweeps every document across every table', () {
    final store = CrdtStore('n1', HybridClock('n1'), persistDebounce: Duration.zero);
    store.importState(snapshot({
      't1': {
        'p1': DocumentState(table: 't1', pk: 'p1', fields: {
          'items': FieldState(
            type: CrdtType.list,
            hlc: h(1),
            nodeId: 'n1',
            listState: RgaListState({
              key(1): RgaNode(id: h(1), nodeId: 'n1', parentId: HLC.zero, value: const JsonValue('a')),
              key(2): RgaNode(id: h(2), nodeId: 'n1', parentId: h(1), value: const JsonValue('b'), tombstone: true),
            }),
          ),
        }),
      },
      't2': {
        'p2': DocumentState(table: 't2', pk: 'p2', fields: {
          'tags': FieldState(
            type: CrdtType.set,
            hlc: h(1),
            nodeId: 'n1',
            setState: OrSetState(
              entries: {
                '"a"': [OrSetTag('n1', h(1))],
                '"b"': [OrSetTag('n1', h(3))],
              },
              removed: {'"a"|n1:${key(1)}': true},
            ),
          ),
        }),
      },
    }));

    // Visible reads are unaffected by compaction: the dropped state was
    // already dead weight, never part of a resolved value.
    expect(store.getDocument('t1', 'p1')?['items'], ['a']);
    expect(store.getDocument('t2', 'p2')?['tags'], ['b']);

    final dropped = store.compact(h(10));
    expect(dropped, 2); // 1 list node in t1 + 1 set tag in t2

    expect(store.getDocument('t1', 'p1')?['items'], ['a']);
    expect(store.getDocument('t2', 'p2')?['tags'], ['b']);

    // The mid-iteration setDocument path actually rewrote the raw state.
    final exported = store.exportState();
    expect(exported.tables['t1']!['p1']!.fields['items']!.listState!.nodes.keys.toList(), [key(1)]);
    expect(exported.tables['t2']!['p2']!.fields['tags']!.setState!.entries['"a"'], isNull);
    expect(exported.tables['t2']!['p2']!.fields['tags']!.setState!.entries['"b"'], isNotNull);
  });

  test('CRDTStore.compact is a no-op that preserves document identity when nothing drops', () {
    final store = CrdtStore('n1', HybridClock('n1'), persistDebounce: Duration.zero);
    store.importState(snapshot({
      't': {
        'p': DocumentState(table: 't', pk: 'p', fields: {
          'items': FieldState(
            type: CrdtType.list,
            hlc: h(1),
            nodeId: 'n1',
            listState: RgaListState({
              key(1): RgaNode(id: h(1), nodeId: 'n1', parentId: HLC.zero, value: const JsonValue('a')),
            }),
          ),
        }),
      },
    }));

    final before = store.getDocument('t', 'p');
    final beforeCollection = store.getCollection('t');
    expect(store.compact(h(10)), 0);
    // Not just equal: the exact same cached reference, proving setDocument was
    // never called for this document (which is what keeps the identity-keyed
    // snapshot cache from invalidating on a no-op sweep).
    expect(store.getDocument('t', 'p'), same(before));
    // getCollection's cache is keyed on the table version, which setDocument
    // bumps on every call: this pins that compact() actually skips setDocument
    // for untouched documents, not merely that compactDocument itself returns
    // an identical reference.
    expect(store.getCollection('t'), same(beforeCollection));
  });

  test('compact persists the compacted document and notifies its subscribers', () async {
    // The integration seam: compact() used to call setDocument() and nothing
    // else, so storage kept the uncompacted document and a reload restored
    // every tombstone that had just been dropped. persistDebounce: zero makes
    // storage writes synchronous.
    final storage = RecordingStorage();
    final clock = HybridClock('n1');
    final store = CrdtStore('n1', clock, storage: storage, persistDebounce: Duration.zero);
    await store.ready;

    store.insertIntoList('t', 'p', 'items', 'a');
    final nodeId = store.getListNodeIds('t', 'p', 'items').single;
    store.deleteFromList('t', 'p', 'items', nodeId);

    // Horizon strictly after the tombstone, so the node is compactable.
    final horizon = clock.now();

    await store.flushPersistence();
    storage.saved.clear();
    var notified = 0;
    final unsubscribe = store.subscribeDocument('t', 'p', () => notified++);

    final dropped = store.compact(horizon);

    expect(dropped, greaterThan(0));
    expect(storage.saved, hasLength(1));
    expect(storage.saved[0].table, 't');
    expect(storage.saved[0].pk, 'p');
    expect(storage.saved[0].doc.fields['items']!.listState!.nodes, isEmpty);
    expect(notified, 1);

    unsubscribe();
  });

  test('a whole-store compaction still coalesces into one flush per document', () async {
    final storage = RecordingStorage();
    final clock = HybridClock('n1');
    final store = CrdtStore('n1', clock, storage: storage, persistDebounce: Duration.zero);
    await store.ready;

    for (final pk in ['p1', 'p2']) {
      store.insertIntoList('t', pk, 'items', 'a');
      store.insertIntoList('t', pk, 'items', 'b');
      for (final id in store.getListNodeIds('t', pk, 'items')) {
        store.deleteFromList('t', pk, 'items', id);
      }
    }
    final horizon = clock.now();

    await store.flushPersistence();
    storage.saved.clear();
    store.compact(horizon);
    await store.flushPersistence();

    // Two documents, four dropped nodes, but one write each: the transact()
    // wrapper is what collapses them.
    expect(storage.saved, hasLength(2));
  });
}
