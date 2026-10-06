// Port of crdt-js src/__tests__/store.test.ts, case for case.
import 'package:grove_crdt/grove_crdt.dart';
import 'package:test/test.dart';

({CrdtStore store, HybridClock clock}) createTestStore([String nodeId = 'test-node']) {
  var time = 1000;
  final clock = HybridClock(nodeId, nowMs: () => time++);
  return (store: CrdtStore(nodeId, clock), clock: clock);
}

HLC hlc(int ts, int c, String node) => HLC(BigInt.from(ts), c, node);

ChangeRecord lww(String table, String pk, String field, HLC at, Object? value) =>
    ChangeRecord(table: table, pk: pk, field: field, crdtType: CrdtType.lww, hlc: at, nodeId: at.node, value: JsonValue(value));

/// Counts calls, like vitest's `vi.fn()`.
final class Spy {
  int calls = 0;
  void call() => calls++;
}

final class _Plugin extends StorePlugin {
  _Plugin(
    this.name, {
    this.onInit,
    this.onDestroy,
    this.onBeforeWrite,
    this.onBeforeMerge,
    this.onTransformDocument,
    this.onTransformCollection,
  });

  @override
  final String name;
  final void Function()? onInit;
  final void Function()? onDestroy;
  final WriteEvent? Function(WriteEvent e)? onBeforeWrite;
  final ChangeRecord? Function(MergeEvent e)? onBeforeMerge;
  final Map<String, Object?>? Function(Map<String, Object?> doc)? onTransformDocument;
  final List<Map<String, Object?>> Function(List<Map<String, Object?>> docs)? onTransformCollection;

  @override
  void init() => onInit?.call();

  @override
  void destroy() => onDestroy?.call();

  @override
  WriteEvent? beforeWrite(WriteEvent e) => onBeforeWrite == null ? e : onBeforeWrite!(e);

  @override
  ChangeRecord? beforeMerge(MergeEvent e) => onBeforeMerge == null ? e.remote : onBeforeMerge!(e);

  @override
  Map<String, Object?>? transformDocument(String table, String pk, Map<String, Object?> doc) =>
      onTransformDocument == null ? doc : onTransformDocument!(doc);

  @override
  List<Map<String, Object?>> transformCollection(String table, List<Map<String, Object?>> docs) =>
      onTransformCollection == null ? docs : onTransformCollection!(docs);
}

void main() {
  group('CRDTStore', () {
    group('getDocument', () {
      test('returns null for non-existent document', () {
        final (:store, clock: _) = createTestStore();
        expect(store.getDocument('users', '1'), isNull);
      });

      test('returns resolved document after setField', () {
        final (:store, clock: _) = createTestStore();
        store.setField('users', '1', 'name', 'Alice');
        final doc = store.getDocument('users', '1');
        expect(doc, isNotNull);
        expect(doc!['name'], 'Alice');
      });

      test('returns null for tombstoned document', () {
        final (:store, clock: _) = createTestStore();
        store.setField('users', '1', 'name', 'Alice');
        store.deleteDocument('users', '1');
        expect(store.getDocument('users', '1'), isNull);
      });

      test('includes _table and _pk metadata fields', () {
        final (:store, clock: _) = createTestStore();
        store.setField('users', '1', 'name', 'Alice');
        final doc = store.getDocument('users', '1');
        expect(doc!['_table'], 'users');
        expect(doc['_pk'], '1');
      });
    });

    group('getCollection', () {
      test('returns empty array for non-existent table', () {
        final (:store, clock: _) = createTestStore();
        expect(store.getCollection('missing'), isEmpty);
      });

      test('returns all non-tombstoned documents in table', () {
        final (:store, clock: _) = createTestStore();
        store.setField('users', '1', 'name', 'Alice');
        store.setField('users', '2', 'name', 'Bob');
        store.setField('users', '3', 'name', 'Charlie');
        store.deleteDocument('users', '3');
        expect(store.getCollection('users'), hasLength(2));
      });

      test('excludes tombstoned documents', () {
        final (:store, clock: _) = createTestStore();
        store.setField('users', '1', 'name', 'Alice');
        store.deleteDocument('users', '1');
        final col = store.getCollection('users');
        expect(col.every((d) => d['_pk'] != '1'), isTrue);
      });
    });

    group('setField', () {
      test('creates a new document if it does not exist', () {
        final (:store, clock: _) = createTestStore();
        store.setField('users', '1', 'name', 'Alice');
        expect(store.getDocument('users', '1'), isNotNull);
      });

      test('updates an existing field', () {
        final (:store, clock: _) = createTestStore();
        store.setField('users', '1', 'name', 'Alice');
        store.setField('users', '1', 'name', 'Bob');
        expect(store.getDocument('users', '1')!['name'], 'Bob');
      });

      test('returns a ChangeRecord with correct properties', () {
        final (:store, clock: _) = createTestStore();
        final change = store.setField('users', '1', 'name', 'Alice')!;
        expect(change.table, 'users');
        expect(change.pk, '1');
        expect(change.field, 'name');
        expect(change.crdtType, CrdtType.lww);
        expect(change.nodeId, 'test-node');
        expect(change.value!.value, 'Alice');
        expect(change.hlc.ts > BigInt.zero, isTrue);
      });

      test('adds the change to pending', () {
        final (:store, clock: _) = createTestStore();
        expect(store.pendingCount, 0);
        store.setField('users', '1', 'name', 'Alice');
        expect(store.pendingCount, 1);
      });

      test('uses the clock to generate monotonically increasing HLCs', () {
        final (:store, clock: _) = createTestStore();
        final c1 = store.setField('users', '1', 'name', 'a')!;
        final c2 = store.setField('users', '1', 'name', 'b')!;
        expect(c2.hlc.ts >= c1.hlc.ts, isTrue);
        if (c2.hlc.ts == c1.hlc.ts) expect(c2.hlc.c, greaterThan(c1.hlc.c));
      });
    });

    group('incrementCounter', () {
      test('creates counter field with default delta of 1', () {
        final (:store, clock: _) = createTestStore();
        store.incrementCounter('users', '1', 'views');
        expect(store.getDocument('users', '1')!['views'], 1);
      });

      test('accepts custom delta', () {
        final (:store, clock: _) = createTestStore();
        store.incrementCounter('users', '1', 'views', 5);
        expect(store.getDocument('users', '1')!['views'], 5);
      });

      test('accumulates multiple increments', () {
        final (:store, clock: _) = createTestStore();
        store.incrementCounter('users', '1', 'views');
        store.incrementCounter('users', '1', 'views');
        store.incrementCounter('users', '1', 'views');
        expect(store.getDocument('users', '1')!['views'], 3);
      });

      test('returns ChangeRecord with counter_delta', () {
        final (:store, clock: _) = createTestStore();
        final change = store.incrementCounter('users', '1', 'views', 5)!;
        expect(change.crdtType, CrdtType.counter);
        expect(change.counterDelta!.toJson(), {'inc': 5, 'dec': 0});
      });
    });

    group('decrementCounter', () {
      test('creates counter field with negative effect', () {
        final (:store, clock: _) = createTestStore();
        store.decrementCounter('users', '1', 'views');
        expect(store.getDocument('users', '1')!['views'], -1);
      });

      test('accepts custom delta', () {
        final (:store, clock: _) = createTestStore();
        store.decrementCounter('users', '1', 'views', 3);
        expect(store.getDocument('users', '1')!['views'], -3);
      });

      test('works with mixed increment/decrement', () {
        final (:store, clock: _) = createTestStore();
        store.incrementCounter('users', '1', 'views', 5);
        store.decrementCounter('users', '1', 'views', 2);
        expect(store.getDocument('users', '1')!['views'], 3);
      });

      test('returns ChangeRecord with counter_delta', () {
        final (:store, clock: _) = createTestStore();
        final change = store.decrementCounter('users', '1', 'views', 2)!;
        expect(change.counterDelta!.toJson(), {'inc': 0, 'dec': 2});
      });
    });

    group('addToSet', () {
      test('adds elements to a set field', () {
        final (:store, clock: _) = createTestStore();
        store.addToSet('users', '1', 'tags', ['a', 'b']);
        final tags = store.getDocument('users', '1')!['tags']! as List<Object?>;
        expect(tags, contains('a'));
        expect(tags, contains('b'));
      });

      test('returns ChangeRecord with set_op add', () {
        final (:store, clock: _) = createTestStore();
        final change = store.addToSet('users', '1', 'tags', ['x'])!;
        expect(change.crdtType, CrdtType.set);
        expect(change.setOp!.toJson(), {
          'op': 'add',
          'elements': ['x'],
        });
      });

      test('handles adding to existing set', () {
        final (:store, clock: _) = createTestStore();
        store.addToSet('users', '1', 'tags', ['a']);
        store.addToSet('users', '1', 'tags', ['b']);
        final tags = store.getDocument('users', '1')!['tags']! as List<Object?>;
        expect(tags, contains('a'));
        expect(tags, contains('b'));
      });
    });

    group('removeFromSet', () {
      test('removes elements from a set field', () {
        final (:store, clock: _) = createTestStore();
        // Add elements in separate operations so they get unique HLC tags
        store.addToSet('users', '1', 'tags', ['a']);
        store.addToSet('users', '1', 'tags', ['b']);
        store.removeFromSet('users', '1', 'tags', ['a']);
        final tags = store.getDocument('users', '1')!['tags']! as List<Object?>;
        expect(tags, isNot(contains('a')));
        expect(tags, contains('b'));
      });

      test('returns ChangeRecord with set_op remove', () {
        final (:store, clock: _) = createTestStore();
        final change = store.removeFromSet('users', '1', 'tags', ['x'])!;
        expect(change.setOp!.toJson(), {
          'op': 'remove',
          'elements': ['x'],
        });
      });

      test('is a no-op when element was never added', () {
        final (:store, clock: _) = createTestStore();
        store.addToSet('users', '1', 'tags', ['a']);
        store.removeFromSet('users', '1', 'tags', ['z']);
        expect(store.getDocument('users', '1')!['tags'], contains('a'));
      });
    });

    group('deleteDocument', () {
      test('tombstones the document', () {
        final (:store, clock: _) = createTestStore();
        store.setField('users', '1', 'name', 'Alice');
        store.deleteDocument('users', '1');
        expect(store.getDocument('users', '1'), isNull);
      });

      test('returns ChangeRecord with tombstone flag', () {
        final (:store, clock: _) = createTestStore();
        expect(store.deleteDocument('users', '1').tombstone, isTrue);
      });

      test('adds the change to pending', () {
        final (:store, clock: _) = createTestStore();
        final before = store.pendingCount;
        store.deleteDocument('users', '1');
        expect(store.pendingCount, before + 1);
      });

      test('uses empty field name', () {
        final (:store, clock: _) = createTestStore();
        expect(store.deleteDocument('users', '1').field, '');
      });
    });

    group('applyChanges', () {
      test('applies a batch of remote changes', () {
        final (:store, clock: _) = createTestStore();
        store.applyChanges([
          lww('users', '1', 'name', hlc(500000000000, 0, 'remote'), 'Alice'),
          lww('users', '2', 'name', hlc(500000000000, 1, 'remote'), 'Bob'),
        ]);
        expect(store.getDocument('users', '1')!['name'], 'Alice');
        expect(store.getDocument('users', '2')!['name'], 'Bob');
      });

      test('merges LWW fields correctly (remote wins)', () {
        final (:store, clock: _) = createTestStore();
        store.setField('users', '1', 'name', 'Local');
        store.applyChanges([lww('users', '1', 'name', hlc(999000000000, 0, 'remote'), 'Remote')]);
        expect(store.getDocument('users', '1')!['name'], 'Remote');
      });

      test('merges counter fields correctly', () {
        final (:store, clock: _) = createTestStore();
        store.incrementCounter('users', '1', 'views', 3);
        store.applyChanges([
          ChangeRecord(
            table: 'users',
            pk: '1',
            field: 'views',
            crdtType: CrdtType.counter,
            hlc: hlc(999000000000, 0, 'remote'),
            nodeId: 'remote',
            counterDelta: const CounterDelta(5, 0),
          ),
        ]);
        expect(store.getDocument('users', '1')!['views'], 8); // 3 + 5
      });

      test('merges set fields correctly', () {
        final (:store, clock: _) = createTestStore();
        store.addToSet('users', '1', 'tags', ['local']);
        store.applyChanges([
          ChangeRecord(
            table: 'users',
            pk: '1',
            field: 'tags',
            crdtType: CrdtType.set,
            hlc: hlc(999000000000, 0, 'remote'),
            nodeId: 'remote',
            setOp: const SetOperation(SetOpType.add, ['remote']),
          ),
        ]);
        final tags = store.getDocument('users', '1')!['tags']! as List<Object?>;
        expect(tags, contains('local'));
        expect(tags, contains('remote'));
      });

      test('handles tombstone changes', () {
        final (:store, clock: _) = createTestStore();
        store.setField('users', '1', 'name', 'Alice');
        store.applyChanges([
          ChangeRecord(
            table: 'users',
            pk: '1',
            field: '',
            crdtType: CrdtType.lww,
            hlc: hlc(999000000000, 0, 'remote'),
            nodeId: 'remote',
            tombstone: true,
          ),
        ]);
        expect(store.getDocument('users', '1'), isNull);
      });

      test('batch-notifies listeners once per affected document', () {
        final (:store, clock: _) = createTestStore();
        final listener = Spy();
        store.subscribeDocument('users', '1', listener.call);
        store.applyChanges([
          lww('users', '1', 'name', hlc(500000000000, 0, 'r'), 'A'),
          lww('users', '1', 'email', hlc(500000000000, 1, 'r'), 'a@b.c'),
        ]);
        // Both changes are to the same doc, so batch-notify fires once
        expect(listener.calls, 1);
      });
    });

    group('getPendingChanges / clearPendingChanges / pendingCount', () {
      test('returns empty array initially', () {
        final (:store, clock: _) = createTestStore();
        expect(store.getPendingChanges(), isEmpty);
      });

      test('accumulates changes from local mutations', () {
        final (:store, clock: _) = createTestStore();
        store.setField('users', '1', 'a', 1);
        store.setField('users', '1', 'b', 2);
        store.incrementCounter('users', '1', 'c');
        expect(store.pendingCount, 3);
        expect(store.getPendingChanges(), hasLength(3));
      });

      test('returns a copy (not reference) of pending', () {
        final (:store, clock: _) = createTestStore();
        store.setField('users', '1', 'a', 1);
        final p1 = store.getPendingChanges();
        final p2 = store.getPendingChanges();
        expect(p1, isNot(same(p2)));
        expect(p1, p2);
      });

      test('clearPendingChanges resets to empty', () {
        final (:store, clock: _) = createTestStore();
        store.setField('users', '1', 'a', 1);
        store.clearPendingChanges();
        expect(store.pendingCount, 0);
        expect(store.getPendingChanges(), isEmpty);
      });
    });

    group('subscribe (global)', () {
      test('notifies listener on any state change', () {
        final (:store, clock: _) = createTestStore();
        final listener = Spy();
        store.subscribe(listener.call);
        store.setField('users', '1', 'name', 'Alice');
        expect(listener.calls, greaterThan(0));
      });

      test('returns an unsubscribe function', () {
        final (:store, clock: _) = createTestStore();
        final listener = Spy();
        final unsub = store.subscribe(listener.call);
        unsub();
        store.setField('users', '1', 'name', 'Alice');
        expect(listener.calls, 0);
      });

      test('supports multiple listeners', () {
        final (:store, clock: _) = createTestStore();
        final listener1 = Spy();
        final listener2 = Spy();
        store.subscribe(listener1.call);
        store.subscribe(listener2.call);
        store.setField('users', '1', 'name', 'Alice');
        expect(listener1.calls, greaterThan(0));
        expect(listener2.calls, greaterThan(0));
      });
    });

    group('subscribeDocument', () {
      test('notifies listener only for matching table and pk', () {
        final (:store, clock: _) = createTestStore();
        final listener = Spy();
        store.subscribeDocument('users', '1', listener.call);
        store.setField('users', '1', 'name', 'Alice');
        expect(listener.calls, 1);
        store.setField('users', '2', 'name', 'Bob');
        expect(listener.calls, 1); // not called again
      });

      test('returns an unsubscribe function', () {
        final (:store, clock: _) = createTestStore();
        final listener = Spy();
        final unsub = store.subscribeDocument('users', '1', listener.call);
        unsub();
        store.setField('users', '1', 'name', 'Alice');
        expect(listener.calls, 0);
      });

      test('cleans up internal map when last listener unsubscribes', () {
        final (:store, clock: _) = createTestStore();
        final listener = Spy();
        final unsub = store.subscribeDocument('users', '1', listener.call);
        unsub();
        // After unsub, internal docListeners map entry should be removed.
        // We verify indirectly: subscribing again and mutating should still work.
        final listener2 = Spy();
        store.subscribeDocument('users', '1', listener2.call);
        store.setField('users', '1', 'name', 'Alice');
        expect(listener2.calls, 1);
      });
    });

    group('subscribeCollection', () {
      test('notifies listener for any change in the table', () {
        final (:store, clock: _) = createTestStore();
        final listener = Spy();
        store.subscribeCollection('users', listener.call);
        store.setField('users', '1', 'name', 'Alice');
        store.setField('users', '2', 'name', 'Bob');
        expect(listener.calls, 2);
      });

      test('does not notify for changes in other tables', () {
        final (:store, clock: _) = createTestStore();
        final listener = Spy();
        store.subscribeCollection('users', listener.call);
        store.setField('posts', '1', 'title', 'Hi');
        expect(listener.calls, 0);
      });

      test('returns an unsubscribe function', () {
        final (:store, clock: _) = createTestStore();
        final listener = Spy();
        final unsub = store.subscribeCollection('users', listener.call);
        unsub();
        store.setField('users', '1', 'name', 'Alice');
        expect(listener.calls, 0);
      });

      test('cleans up internal map when last listener unsubscribes', () {
        final (:store, clock: _) = createTestStore();
        final listener = Spy();
        final unsub = store.subscribeCollection('users', listener.call);
        unsub();
        final listener2 = Spy();
        store.subscribeCollection('users', listener2.call);
        store.setField('users', '1', 'name', 'Alice');
        expect(listener2.calls, 1);
      });
    });

    group('resolveDocument (via getDocument)', () {
      test('resolves LWW fields to their value', () {
        final (:store, clock: _) = createTestStore();
        store.setField('t', '1', 'name', 'Alice');
        expect(store.getDocument('t', '1')!['name'], 'Alice');
      });

      test('resolves counter fields to their numeric value', () {
        final (:store, clock: _) = createTestStore();
        store.incrementCounter('t', '1', 'views', 7);
        expect(store.getDocument('t', '1')!['views'], 7);
      });

      test('resolves set fields to their element array', () {
        final (:store, clock: _) = createTestStore();
        store.addToSet('t', '1', 'tags', ['a', 'b']);
        final tags = store.getDocument('t', '1')!['tags'];
        expect(tags, isA<List<Object?>>());
        expect(tags, contains('a'));
        expect(tags, contains('b'));
      });

      test('falls through to value for unknown field types', () {
        // Go parity: crdt-js stores a change of an unknown type and resolves its
        // value. Go `ApplyChange` rejects it ("crdt: apply unknown type"), so the
        // store reports it through onError and stores nothing. Dart has no
        // unknown type; the empty type Go emits on pulled tombstone rows is the
        // closest, and a non-tombstone change of that type is the case here.
        final errors = <Object>[];
        final store = CrdtStore(
          'test-node',
          HybridClock('test-node', nowMs: () => 1000),
          onError: (e, c) => errors.add(e),
        );
        store.applyChanges([
          ChangeRecord(
            table: 't',
            pk: '1',
            field: 'custom',
            crdtType: CrdtType.none,
            hlc: hlc(999000000000, 0, 'r'),
            nodeId: 'r',
            value: const JsonValue('custom-val'),
          ),
        ]);
        expect(errors.single, isA<CrdtApplyError>());
        expect(store.getDocument('t', '1'), isNull);
      });
    });

    group('CRDTStore - Plugins', () {
      test('registers plugins via use()', () {
        final (:store, clock: _) = createTestStore();
        final init = Spy();
        store.use(_Plugin('test-plugin', onInit: init.call));
        expect(init.calls, 1);
      });

      test('removes plugins via removePlugin()', () {
        final (:store, clock: _) = createTestStore();
        final destroy = Spy();
        store.use(_Plugin('test-plugin', onDestroy: destroy.call));
        store.removePlugin('test-plugin');
        expect(destroy.calls, 1);
      });

      test('gets plugins via getPlugin()', () {
        final (:store, clock: _) = createTestStore();
        final plugin = _Plugin('my-plugin');
        store.use(plugin);
        expect(store.getPlugin<StorePlugin>('my-plugin'), same(plugin));
        expect(store.getPlugin<StorePlugin>('non-existent'), isNull);
      });

      test('runs WriteHook.beforeWrite on setField', () {
        final (:store, clock: _) = createTestStore();
        final calls = <WriteEvent>[];
        store.use(_Plugin('w', onBeforeWrite: (e) {
          calls.add(e);
          return e;
        }));
        store.setField('users', '1', 'name', 'Alice');
        expect(calls, hasLength(1));
        expect(calls[0].table, 'users');
        expect(calls[0].field, 'name');
      });

      test('rejects writes when plugin returns null', () {
        final (:store, clock: _) = createTestStore();
        store.use(_Plugin('blocker', onBeforeWrite: (_) => null));
        final result = store.setField('users', '1', 'name', 'Alice');
        expect(result, isNull);
        expect(store.getDocument('users', '1'), isNull);
        expect(store.pendingCount, 0);
      });

      test('runs MergeHook.beforeMerge on applyChanges', () {
        final (:store, clock: _) = createTestStore();
        var calls = 0;
        store.use(_Plugin('m', onBeforeMerge: (e) {
          calls++;
          return e.remote;
        }));
        store.applyChanges([lww('users', '1', 'name', hlc(500000000000, 0, 'remote'), 'Alice')]);
        expect(calls, 1);
      });

      test('runs ReadHook.transformDocument on getDocument', () {
        final (:store, clock: _) = createTestStore();
        store.use(_Plugin('read', onTransformDocument: (doc) => {...doc, 'computed': true}));
        store.setField('users', '1', 'name', 'Alice');
        final doc = store.getDocument('users', '1');
        expect(doc!['computed'], isTrue);
        expect(doc['name'], 'Alice');
      });

      test('runs ReadHook.transformCollection on getCollection', () {
        final (:store, clock: _) = createTestStore();
        store.use(_Plugin('read', onTransformCollection: (docs) => docs.sublist(0, 1))); // Only return first doc
        store.setField('users', '1', 'name', 'Alice');
        store.setField('users', '2', 'name', 'Bob');
        expect(store.getCollection('users'), hasLength(1));
      });
    });

    group('CRDTStore - Batch Writes', () {
      test('creates a batch writer via batch()', () {
        final (:store, clock: _) = createTestStore();
        final batch = store.batch('users', '1');
        expect(batch, isA<BatchWriter>());
      });

      test('commits multiple field changes atomically', () {
        final (:store, clock: _) = createTestStore();
        final changes = store.batch('users', '1').setField('name', 'Alice').setField('email', 'alice@example.com').commit();
        expect(changes, hasLength(2));
        final doc = store.getDocument('users', '1')!;
        expect(doc['name'], 'Alice');
        expect(doc['email'], 'alice@example.com');
      });

      test('batch setField + incrementCounter together', () {
        final (:store, clock: _) = createTestStore();
        store.batch('users', '1').setField('name', 'Alice').incrementCounter('views', 5).commit();
        final doc = store.getDocument('users', '1')!;
        expect(doc['name'], 'Alice');
        expect(doc['views'], 5);
      });

      test('batch changes appear in pending', () {
        final (:store, clock: _) = createTestStore();
        expect(store.pendingCount, 0);
        store.batch('users', '1').setField('name', 'Alice').setField('email', 'alice@example.com').commit();
        expect(store.pendingCount, 2);
      });

      test('batch notifies listeners once', () {
        final (:store, clock: _) = createTestStore();
        final listener = Spy();
        store.subscribeDocument('users', '1', listener.call);
        store.batch('users', '1').setField('name', 'Alice').setField('email', 'alice@example.com').commit();
        // BatchWriter.commit() runs the whole batch inside one transact(),
        // so notifyListeners fires once for the batch, not once per field.
        expect(listener.calls, 1);
      });
    });

    group('CRDTStore - State Export/Import', () {
      test('exports state as snapshot', () {
        final (:store, clock: _) = createTestStore();
        store.setField('users', '1', 'name', 'Alice');
        final snapshot = store.exportState();
        expect(snapshot.version, 1);
        expect(snapshot.nodeId, 'test-node');
        expect(snapshot.timestamp, greaterThan(0));
        expect(snapshot.tables['users'], isNotNull);
        expect(snapshot.tables['users']!['1'], isNotNull);
      });

      test('imports state from snapshot', () {
        final (store: store1, clock: _) = createTestStore('node-1');
        store1.setField('users', '1', 'name', 'Alice');
        final snapshot = store1.exportState();
        final (store: store2, clock: _) = createTestStore('node-2');
        store2.importState(snapshot);
        expect(store2.getDocument('users', '1')!['name'], 'Alice');
      });

      test('import replaces existing state', () {
        final (:store, clock: _) = createTestStore();
        store.setField('users', '1', 'name', 'Alice');
        store.setField('posts', '1', 'title', 'Hello');
        // Import a snapshot with only users table.
        final snapshot = store.exportState();
        // Remove posts from snapshot.
        snapshot.tables.remove('posts');
        store.importState(snapshot);
        expect(store.getDocument('users', '1'), isNotNull);
        expect(store.getCollection('posts'), isEmpty);
      });

      test('export includes pending changes', () {
        final (:store, clock: _) = createTestStore();
        store.setField('users', '1', 'name', 'Alice');
        store.setField('users', '1', 'email', 'alice@example.com');
        final snapshot = store.exportState();
        expect(snapshot.pending, hasLength(2));
        expect(snapshot.pending[0].change.field, 'name');
        expect(snapshot.pending[1].change.field, 'email');
      });

      test('exportTable returns single table', () {
        final (:store, clock: _) = createTestStore();
        store.setField('users', '1', 'name', 'Alice');
        store.setField('posts', '1', 'title', 'Hello');
        final usersTable = store.exportTable('users');
        expect(usersTable.keys.toList(), ['1']);
        expect(usersTable['1']!.table, 'users');
        expect(store.exportTable('nonexistent'), isEmpty);
      });
    });
  });
}
