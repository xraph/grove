// Port of crdt-js src/__tests__/cow-store.test.ts, case for case.
import 'package:grove_crdt/grove_crdt.dart';
import 'package:test/test.dart';

import 'support/store_fakes.dart';

({CrdtStore store, HybridClock clock}) mk() {
  final clock = HybridClock('n1');
  return (store: CrdtStore('n1', clock), clock: clock);
}

void main() {
  group('copy-on-write store', () {
    test('notifies subscribers when the pk contains a colon (D2)', () {
      final (:store, :clock) = mk();
      var fired = 0;
      store.subscribeDocument('docs', 'doc:1', () => fired++);
      store.applyChanges([
        ChangeRecord(
          table: 'docs',
          pk: 'doc:1',
          field: 'title',
          crdtType: CrdtType.lww,
          hlc: clock.now(),
          nodeId: 'n2',
          value: const JsonValue('hi'),
        ),
      ]);
      expect(fired, 1);
      expect(store.getDocument('docs', 'doc:1'), containsPair('title', 'hi'));
    });

    test('does not confuse table:pk key collisions', () {
      final (:store, clock: _) = mk();
      var a = 0, b = 0;
      store.subscribeDocument('a:b', 'c', () => a++);
      store.subscribeDocument('a', 'b:c', () => b++);
      store.setField('a:b', 'c', 'f', 1);
      expect(a, 1);
      expect(b, 0);
    });

    // Not a COW identity check: exportTable deep-clones, so this holds even
    // against a store that mutates in place. The beforePersist test below is the
    // one that observes real document identity.
    test('exportTable hands back an independent snapshot per call', () {
      final (:store, clock: _) = mk();
      store.setField('t', 'p', 'f', 1);
      final first = store.exportTable('t')['p']!;
      store.setField('t', 'p', 'f', 2);
      final second = store.exportTable('t')['p']!;
      expect(first, isNot(same(second)));
      expect(first.fields['f']!.value!.value, 1);
    });

    test('installs a new DocumentState object rather than mutating in place', () async {
      // persistDebounce: zero: this test asserts one beforePersist call per
      // setField; the default debounce coalesces same-document writes into a
      // single call, which is what the debounce is for but not what this
      // identity check is testing.
      final store = CrdtStore('n1', HybridClock('n1'), persistDebounce: Duration.zero);
      // Dart: nothing persists until hydration completes.
      await store.ready;
      final seen = <DocumentState>[];
      store.use(FnPlugin('identity-watcher', onBeforePersist: (t, p, doc) {
        seen.add(doc);
        return doc;
      }));

      store.setField('t', 'p', 'f', 1);
      store.setField('t', 'p', 'f', 2);

      expect(seen, hasLength(2));
      // Different objects, and the first snapshot still reads as it did.
      expect(seen[0], isNot(same(seen[1])));
      expect(seen[0].fields, isNot(same(seen[1].fields)));
      expect(seen[0].fields['f']!.value!.value, 1);
      expect(seen[1].fields['f']!.value!.value, 2);
    });

    test('invalidates the version of every table an imported snapshot omits', () {
      final (:store, clock: _) = mk();
      store.setField('users', 'u1', 'name', 'Alice');
      store.setField('posts', 'p1', 'title', 'Hello');

      // Warm the collection cache before the import. importState clears the
      // state directly rather than going through setDocument, so a table the
      // snapshot omits is wiped without its version moving. If importState's
      // version bump were removed, getCollection's cache would keep serving
      // this stale, pre-import resolution.
      final usersBefore = store.getCollection('users');
      expect(usersBefore, hasLength(1));

      store.importState(StateSnapshot(
        version: 1,
        nodeId: 'n1',
        timestamp: DateTime.now().millisecondsSinceEpoch,
        tables: {
          'posts': {'p1': DocumentState(table: 'posts', pk: 'p1')},
        },
        pending: [],
      ));

      expect(store.getCollection('users'), isNot(same(usersBefore)));
      expect(store.getCollection('users'), isEmpty);
      expect(store.getDocument('users', 'u1'), isNull);
      expect(store.getCollection('posts'), hasLength(1));
    });

    test('undo restores a text field that did not exist before', () {
      final (:store, clock: _) = mk();
      store.insertText('notes', 'n1', 'body', 0, 'hello');
      expect(store.getText('notes', 'n1', 'body'), 'hello');
      expect(store.undo(), isTrue);
      expect(store.getText('notes', 'n1', 'body'), '');
      // Go parity: crdt-js removes the field entirely. Undo here emits a
      // compensating text delete (Go `TextState.SetString` toward ""), and Go
      // has no field delete, so the field stays behind as an empty text state
      // whose characters are tombstoned. The visible value is what is
      // restored.
      final body = store.exportTable('notes')['n1']!.fields['body']!;
      expect(body.type, CrdtType.text);
      expect(textValue(body.textState!), '');
      expect(store.getDocument('notes', 'n1')!['body'], '');
    });

    test('gives afterMerge the post-merge field state as the result', () {
      final (:store, :clock) = mk();
      store.setField('users', 'u1', 'name', 'local');
      final localState = store.exportTable('users')['u1']!.fields['name']!;

      final seen = <MergeEvent>[];
      store.use(FnPlugin('watcher', onAfterMerge: seen.add));

      store.applyChanges([
        ChangeRecord(
          table: 'users',
          pk: 'u1',
          field: 'name',
          crdtType: CrdtType.lww,
          hlc: clock.now(),
          nodeId: 'n2',
          value: const JsonValue('remote'),
        ),
      ]);

      expect(seen, hasLength(1));
      final event = seen[0];
      expect(event.conflictDetected, isTrue);
      expect(event.local!.value!.value, 'local');
      expect(event.result, isNotNull);
      expect(event.result!.value!.value, 'remote');
      expect(event.winnerNodeId, 'n2');
      expect(event.result, isNot(same(event.local)));
      expect(localState.value!.value, 'local');
    });

    // D8 was "every local write deep-clones the whole field state for undo",
    // which made writes quadratic. The honest signature of the fix is
    // structural, not temporal: undo's previousState must be the SAME OBJECT
    // that was in the document, not a copy of it.
    test('undo captures the previous field state by reference, not by clone (D8)', () async {
      final store = CrdtStore('n1', HybridClock('n1'), persistDebounce: Duration.zero);
      // Dart: nothing persists until hydration completes.
      await store.ready;
      FieldState? liveAfterFirstWrite;
      FieldState? previousSeenBySecondWrite;
      store.use(FnPlugin(
        'identity-probe',
        onBeforePersist: (t, p, doc) {
          liveAfterFirstWrite ??= doc.fields['f'];
          return doc;
        },
        onBeforeWrite: (ev) {
          if (ev.previousState != null) previousSeenBySecondWrite = ev.previousState;
          return ev;
        },
      ));

      store.setField('t', 'p', 'f', 1);
      store.setField('t', 'p', 'f', 2);

      // A deep clone would be equal but not identical. That is the whole point.
      expect(previousSeenBySecondWrite, isNotNull);
      expect(previousSeenBySecondWrite, same(liveAfterFirstWrite));
    });

    test('3000 sequential list appends resolve correctly', () {
      final (:store, clock: _) = mk();
      HLC? after;
      for (var i = 0; i < 3000; i++) {
        final c = store.insertIntoList('t', 'p', 'items', i, afterId: after)!;
        after = c.listOp!.nodeId;
      }
      final items = store.getDocument('t', 'p')?['items'] as List<Object?>?;
      expect(items, hasLength(3000));
      expect(items?[0], 0);
      expect(items?[2999], 2999);
    }, timeout: const Timeout(Duration(seconds: 30)));

    group('text resolution', () {
      test('getDocument exposes text fields (D3)', () {
        final (:store, clock: _) = mk();
        store.insertText('notes', 'n1', 'body', 0, 'hello');
        expect(store.getDocument('notes', 'n1')?['body'], 'hello');
      });

      test('nested documents expose text fields', () {
        final (:store, clock: _) = mk();
        store.insertText('notes', 'n1', 'body', 0, 'hi');
        store.setDocumentField('notes', 'n1', 'meta', 'author', 'alice');
        final doc = store.getDocument('notes', 'n1');
        expect(doc?['body'], 'hi');
        expect(doc?['meta'], {'author': 'alice'});
      });
    });
  });
}
