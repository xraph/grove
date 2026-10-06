import 'package:grove_crdt/grove_crdt.dart';
import 'package:test/test.dart';

import 'support/store_fakes.dart';

HLC n(int ts, [String node = 'srv']) => HLC(BigInt.from(ts), 0, node);

CrdtStore newStore({String node = 'a', ReplicaStorage? storage, int Function()? now}) =>
    CrdtStore(node, HybridClock(node, nowMs: now ?? () => 1000), storage: storage, persistDebounce: Duration.zero);

void main() {
  test('a pulled tombstone deletes the record and later field writes do not resurrect it', () {
    final s = newStore();
    s.setField('t', '1', 'title', 'x');
    s.applyChanges([
      ChangeRecord(table: 't', pk: '1', field: '_tombstone', crdtType: CrdtType.none, hlc: n(5), nodeId: 'srv', tombstone: true),
    ]);
    expect(s.getDocument('t', '1'), isNull);
    s.applyChanges([
      ChangeRecord(table: 't', pk: '1', field: 'title', crdtType: CrdtType.lww, hlc: n(9), nodeId: 'srv', value: const JsonValue('y')),
    ]);
    expect(s.getDocument('t', '1'), isNull);
  });

  test('tombstone clock is the max regardless of arrival order', () {
    final s = newStore();
    s.applyChanges([
      ChangeRecord(table: 't', pk: '1', field: '_tombstone', crdtType: CrdtType.none, hlc: n(9), nodeId: 'srv', tombstone: true),
      ChangeRecord(table: 't', pk: '1', field: '_tombstone', crdtType: CrdtType.none, hlc: n(4), nodeId: 'srv', tombstone: true),
    ]);
    expect(s.getDocumentState('t', '1')!.tombstoneHlc, n(9));
  });

  test('a local delete pushes field "" with crdt_type lww', () {
    final c = newStore().deleteDocument('t', '1');
    expect(c.field, '');
    expect(c.crdtType, CrdtType.lww);
    expect(c.tombstone, isTrue);
  });

  test('a change that fails to apply is reported and the batch continues', () {
    final errors = <Object>[];
    final s = CrdtStore('a', HybridClock('a', nowMs: () => 1000), persistDebounce: Duration.zero, onError: (e, c) => errors.add(e));
    s.setField('t', '1', 'title', 'x');
    final affected = s.applyChanges([
      ChangeRecord(table: 't', pk: '1', field: 'title', crdtType: CrdtType.counter, hlc: n(5), nodeId: 'srv', counterDelta: const CounterDelta(1, 0)),
      ChangeRecord(table: 't', pk: '2', field: 'title', crdtType: CrdtType.lww, hlc: n(5), nodeId: 'srv', value: const JsonValue('ok')),
    ]);
    expect(errors.single, isA<CrdtApplyError>());
    expect(affected, contains((table: 't', pk: '2')));
    expect(s.getDocument('t', '2')!['title'], 'ok');
  });

  test('hydration advances the clock past every persisted HLC', () async {
    final kv = MapReplicaKeyValue();
    final first = newStore(storage: KeyValueReplicaStorage(kv), now: () => 5000);
    await first.ready;
    final written = first.setField('t', '1', 'title', 'x')!;
    await first.flushPersistence();
    final second = newStore(storage: KeyValueReplicaStorage(kv), now: () => 1000);
    await second.ready;
    expect(second.setField('t', '1', 'title', 'y')!.hlc.isAfter(written.hlc), isTrue);
  });

  test('rejected changes are excluded from push until retried, and the mark persists', () async {
    final kv = MapReplicaKeyValue();
    final s = newStore(storage: KeyValueReplicaStorage(kv));
    await s.ready;
    final c = s.setField('t', '1', 'title', 'x')!;
    s.markRejected(pendingKey(c), const PendingRejection(kind: 'hook', reason: 'locked'));
    expect(s.getPendingChanges(), isEmpty);
    expect(s.rejectedCount, 1);
    await s.flushPersistence();
    final reloaded = newStore(storage: KeyValueReplicaStorage(kv));
    await reloaded.ready;
    expect(reloaded.pending.single.rejection!.reason, 'locked');
    reloaded.retryRejected(pendingKey(c));
    expect(reloaded.getPendingChanges(), hasLength(1));
  });

  test('restampPending moves an LWW change and its field to a fresh clock', () {
    var now = 99999999;
    final s = newStore(now: () => now);
    final c = s.setField('t', '1', 'title', 'x')!;
    now = 1000;
    final fresh = s.restampPending(pendingKey(c))!;
    expect(fresh.hlc.isAfter(c.hlc), isTrue);
    expect(s.getDocumentState('t', '1')!.fields['title']!.hlc, fresh.hlc);
    expect(s.getPendingChanges().single.hlc, fresh.hlc);
  });

  test('restampPending re-tags a set add', () {
    final s = newStore();
    final c = s.addToSet('t', '1', 'tags', ['x'])!;
    final fresh = s.restampPending(pendingKey(c))!;
    final tags = s.getDocumentState('t', '1')!.fields['tags']!.setState!.entries['"x"']!;
    expect(tags.single.hlc, fresh.hlc);
  });

  test('restampPending refuses list, text and counter changes', () {
    final s = newStore();
    expect(s.restampPending(pendingKey(s.insertIntoList('t', '1', 'items', 'v')!)), isNull);
    expect(s.restampPending(pendingKey(s.insertText('t', '1', 'body', 0, 'hi')!)), isNull);
    expect(s.restampPending(pendingKey(s.incrementCounter('t', '1', 'views')!)), isNull);
  });

  test('a rebased clock gives new changes and re-stamped changes the new node id', () {
    var now = 99999999;
    final clock = HybridClock('dev', nowMs: () => now);
    final s = CrdtStore('dev', clock, persistDebounce: Duration.zero);
    final c = s.setField('t', '1', 'title', 'x')!;
    now = 1000;
    clock.rebase('dev~1');
    final fresh = s.restampPending(pendingKey(c))!;
    expect(fresh.nodeId, 'dev~1');
    expect(fresh.hlc.node, 'dev~1');
    expect(s.setField('t', '1', 'other', 'y')!.nodeId, 'dev~1');
  });

  group('reconcileField', () {
    test('lww writes only when the value differs', () {
      final s = newStore();
      expect(s.reconcileField('t', '1', 'f', CrdtType.lww, 'a'), hasLength(1));
      expect(s.reconcileField('t', '1', 'f', CrdtType.lww, 'a'), isEmpty);
    });
    test('counter emits the delta toward the target', () {
      final s = newStore();
      s.incrementCounter('t', '1', 'v', 5);
      final out = s.reconcileField('t', '1', 'v', CrdtType.counter, 2);
      expect(out.single.counterDelta!.dec, 3);
      expect(s.getDocument('t', '1')!['v'], 2);
    });
    test('set adds and removes the difference', () {
      final s = newStore();
      s.addToSet('t', '1', 's', ['a', 'b']);
      s.reconcileField('t', '1', 's', CrdtType.set, ['b', 'c']);
      expect(s.getDocument('t', '1')!['s'], ['b', 'c']);
    });
    test('list splices the middle', () {
      final s = newStore();
      s.reconcileField('t', '1', 'l', CrdtType.list, ['a', 'b', 'c']);
      s.reconcileField('t', '1', 'l', CrdtType.list, ['a', 'x', 'c']);
      expect(s.getDocument('t', '1')!['l'], ['a', 'x', 'c']);
    });
    test('text reconciles with a prefix and suffix diff', () {
      final s = newStore();
      s.reconcileField('t', '1', 'body', CrdtType.text, 'hello world');
      s.reconcileField('t', '1', 'body', CrdtType.text, 'hello brave world');
      expect(s.getText('t', '1', 'body'), 'hello brave world');
    });
    test('document writes changed leaves and deletes missing paths', () {
      final s = newStore();
      s.reconcileField('t', '1', 'meta', CrdtType.document, {'a': {'b': 1, 'c': 2}});
      s.reconcileField('t', '1', 'meta', CrdtType.document, {'a': {'b': 3}});
      expect(s.getDocument('t', '1')!['meta'], {'a': {'b': 3}});
    });
  });

  test('undo emits a compensating change that a second replica converges on', () {
    final a = newStore(node: 'a');
    final b = newStore(node: 'b');
    final first = a.setField('t', '1', 'title', 'v1')!;
    final second = a.setField('t', '1', 'title', 'v2')!;
    expect(a.undo(), isTrue);
    final compensation = a.getPendingChanges().last;
    expect(compensation.hlc.isAfter(second.hlc), isTrue);
    b.applyChanges([first, second, compensation]);
    expect(b.getDocument('t', '1')!['title'], 'v1');
    expect(a.getDocument('t', '1')!['title'], 'v1');
  });

  test('undoing an unpushed delete restores the document and drops the tombstone', () {
    final s = newStore();
    s.setField('t', '1', 'title', 'x');
    s.deleteDocument('t', '1');
    expect(s.undo(), isTrue);
    expect(s.getDocument('t', '1')!['title'], 'x');
    expect(s.getPendingChanges().where((c) => c.tombstone), isEmpty);
  });

  test('undoing a pushed delete is refused', () {
    final s = newStore();
    s.setField('t', '1', 'title', 'x');
    final del = s.deleteDocument('t', '1');
    s.clearPendingChanges([del]);
    expect(s.undo(), isFalse);
    expect(s.getDocument('t', '1'), isNull);
  });

  test('removeFromSet removes a key written non-canonically by another engine', () {
    final s = newStore();
    s.applyChanges([
      ChangeRecord(table: 't', pk: '1', field: 's', crdtType: CrdtType.set, hlc: n(2), nodeId: 'js',
          state: setFieldState(OrSetState(entries: {'"a<b"': [OrSetTag('js', n(1, 'js'))]}), n(1, 'js'), 'js')),
    ]);
    final c = s.removeFromSet('t', '1', 's', ['a<b'])!;
    expect(encodeWire(c.toJson()), contains(r'"elements":["a<b"]'));
    expect(s.getDocument('t', '1')!['s'], isEmpty);
  });

  test('documentChanges emits one event per changed document per flush', () async {
    final s = newStore();
    final events = <DocKey>[];
    final sub = s.documentChanges.listen(events.add);
    s.transact(() {
      s.setField('t', '1', 'a', 1);
      s.setField('t', '1', 'b', 2);
      s.setField('t', '2', 'a', 1);
    });
    await Future<void>.delayed(Duration.zero);
    expect(events, unorderedEquals([(table: 't', pk: '1'), (table: 't', pk: '2')]));
    await sub.cancel();
  });

  group('beyond the brief', () {
    test('a batch of good, bad, good applies both good changes and reports once', () {
      final errors = <(Object, ChangeRecord)>[];
      final s = CrdtStore('a', HybridClock('a', nowMs: () => 1000), persistDebounce: Duration.zero, onError: (e, c) => errors.add((e, c)));
      s.setField('t', 'x', 'title', 'lww');
      final bad = ChangeRecord(table: 't', pk: 'x', field: 'title', crdtType: CrdtType.counter, hlc: n(5), nodeId: 'srv', counterDelta: const CounterDelta(1, 0));
      final affected = s.applyChanges([
        ChangeRecord(table: 't', pk: '1', field: 'f', crdtType: CrdtType.lww, hlc: n(5), nodeId: 'srv', value: const JsonValue('one')),
        bad,
        ChangeRecord(table: 't', pk: '2', field: 'f', crdtType: CrdtType.lww, hlc: n(6), nodeId: 'srv', value: const JsonValue('two')),
      ]);
      expect(errors, hasLength(1));
      expect(errors.single.$1, isA<CrdtApplyError>());
      expect(errors.single.$2, same(bad));
      expect(affected, {(table: 't', pk: '1'), (table: 't', pk: '2')});
      expect(s.getDocument('t', '1')!['f'], 'one');
      expect(s.getDocument('t', '2')!['f'], 'two');
      expect(s.getDocument('t', 'x')!['title'], 'lww');
    });

    test('a remote text insert without content is reported and skipped', () {
      final errors = <Object>[];
      final s = CrdtStore('a', HybridClock('a', nowMs: () => 1000), onError: (e, c) => errors.add(e));
      s.applyChanges([
        ChangeRecord(table: 't', pk: '1', field: 'body', crdtType: CrdtType.text, hlc: n(5), nodeId: 'srv', textOp: TextOperation(TextOpType.insert)),
        ChangeRecord(table: 't', pk: '1', field: 'ok', crdtType: CrdtType.lww, hlc: n(6), nodeId: 'srv', value: const JsonValue(1)),
      ]);
      expect(errors.single, isA<CrdtApplyError>().having((e) => e.cause, 'cause', isA<StateError>()));
      expect(s.getDocument('t', '1'), {'_table': 't', '_pk': '1', 'ok': 1});
    });

    test('an onError handler that throws does not abort the batch', () {
      final s = CrdtStore('a', HybridClock('a', nowMs: () => 1000), onError: (e, c) => throw StateError('handler'));
      final affected = s.applyChanges([
        ChangeRecord(table: 't', pk: '1', field: 'f', crdtType: CrdtType.none, hlc: n(5), nodeId: 'srv', value: const JsonValue(1)),
        ChangeRecord(table: 't', pk: '2', field: 'f', crdtType: CrdtType.lww, hlc: n(6), nodeId: 'srv', value: const JsonValue(2)),
      ]);
      expect(affected, {(table: 't', pk: '2')});
    });

    test('a store needs the clock of its own node', () {
      expect(() => CrdtStore('a', HybridClock('b')), throwsArgumentError);
    });

    test('a local write onto a field of another type throws and leaves no trace', () {
      final s = newStore();
      s.incrementCounter('t', '1', 'n');
      expect(() => s.setField('t', '1', 'n', 'x'), throwsA(isA<CrdtApplyError>()));
      expect(s.getDocument('t', '1')!['n'], 1);
      expect(s.pendingCount, 1);
    });

    test('a value that is not JSON is refused before anything changes', () {
      final s = newStore();
      expect(() => s.setField('t', '1', 'f', DateTime(2026)), throwsArgumentError);
      expect(() => s.addToSet('t', '1', 's', [double.nan]), throwsArgumentError);
      expect(s.getDocumentState('t', '1'), isNull);
      expect(s.pendingCount, 0);
    });

    test('strings the app writes are held as the server holds them', () {
      final s = newStore();
      s.setField('t', '1', 'f', {'k\ud800': ['v\udc00']});
      s.addToSet('t', '1', 's', ['a\ud800']);
      s.insertIntoList('t', '1', 'l', 'x\ud800');
      s.setDocumentField('t', '1', 'd', 'p\ud800', 'y\ud800');
      s.insertText('t', '1', 'body', 0, 'z\ud800');
      expect(s.getDocument('t', '1'), {
        '_table': 't',
        '_pk': '1',
        'f': {'k�': ['v�']},
        's': ['a�'],
        'l': ['x�'],
        'd': {'p�': 'y�'},
        'body': 'z�',
      });
      expect(s.getDocumentState('t', '1')!.fields['s']!.setState!.entries.keys, ['"a�"']);
    });

    test('one undo leaves exactly one redo entry', () {
      final s = newStore();
      s.setField('t', '1', 'f', 1);
      s.setField('t', '1', 'f', 2);
      expect(s.undo(), isTrue);
      expect(s.redo(), isTrue);
      expect(s.canRedo, isFalse);
      expect(s.getDocument('t', '1')!['f'], 2);
      expect(s.undo(), isTrue);
      expect(s.undo(), isTrue);
      expect(s.redo(), isTrue);
      expect(s.redo(), isTrue);
      expect(s.canRedo, isFalse);
    });

    test('a refused undo of a pushed delete leaves nothing to redo', () {
      final s = newStore();
      s.setField('t', '1', 'title', 'x');
      final del = s.deleteDocument('t', '1');
      s.clearPendingChanges([del]);
      expect(s.undo(), isFalse);
      expect(s.canRedo, isFalse);
      // The entry was discarded, so the next undo reaches the setField.
      expect(s.canUndo, isTrue);
    });

    test('a redone delete can be undone again while its new tombstone is pending', () {
      final s = newStore();
      s.setField('t', '1', 'title', 'x');
      s.deleteDocument('t', '1');
      expect(s.undo(), isTrue);
      expect(s.getDocument('t', '1')!['title'], 'x');
      expect(s.redo(), isTrue);
      expect(s.getDocument('t', '1'), isNull);
      expect(s.getPendingChanges().where((c) => c.tombstone), hasLength(1));
      expect(s.undo(), isTrue);
      expect(s.getDocument('t', '1')!['title'], 'x');
      expect(s.getPendingChanges().where((c) => c.tombstone), isEmpty);
    });

    test('undoing an unpushed delete of a document that never existed removes it', () {
      final s = newStore();
      s.deleteDocument('t', '1');
      expect(s.undo(), isTrue);
      expect(s.getDocumentState('t', '1'), isNull);
      expect(s.pending, isEmpty);
    });

    test('undoing a delete that a later remote tombstone superseded is refused', () {
      final s = newStore();
      s.setField('t', '1', 'title', 'x');
      s.deleteDocument('t', '1');
      s.applyChanges([
        ChangeRecord(table: 't', pk: '1', field: '_tombstone', crdtType: CrdtType.none, hlc: n(999999999999999), nodeId: 'srv', tombstone: true),
      ]);
      expect(s.undo(), isFalse);
      expect(s.getDocument('t', '1'), isNull);
    });

    group('undo and redo converge on a second replica', () {
      void converge(String what, void Function(CrdtStore s) write, Object? before, Object? after) {
        test(what, () {
          final a = newStore(node: 'a');
          final b = newStore(node: 'b');
          write(a);
          expect(a.getDocument('t', '1')?['f'], after);
          expect(a.undo(), isTrue);
          b.applyChanges(a.getPendingChanges());
          expect(a.getDocument('t', '1')?['f'], before);
          expect(b.getDocument('t', '1')?['f'], before);
          expect(a.redo(), isTrue);
          b.applyChanges(a.getPendingChanges());
          expect(a.getDocument('t', '1')?['f'], after);
          expect(b.getDocument('t', '1')?['f'], after);
          expect(a.undo(), isTrue);
          b.applyChanges(a.getPendingChanges());
          expect(b.getDocument('t', '1')?['f'], before);
        });
      }

      converge('lww', (s) {
        s.setField('t', '1', 'f', 'v1');
        s.setField('t', '1', 'f', 'v2');
      }, 'v1', 'v2');
      converge('counter', (s) {
        s.incrementCounter('t', '1', 'f', 5);
        s.decrementCounter('t', '1', 'f', 2);
      }, 5, 3);
      converge('set add', (s) {
        s.addToSet('t', '1', 'f', ['a']);
        s.addToSet('t', '1', 'f', ['b']);
      }, ['a'], ['a', 'b']);
      converge('set remove', (s) {
        s.addToSet('t', '1', 'f', ['a', 'b']);
        s.removeFromSet('t', '1', 'f', ['a']);
      }, ['a', 'b'], ['b']);
      converge('list insert', (s) {
        s.insertIntoList('t', '1', 'f', 'x');
        s.insertIntoList('t', '1', 'f', 'y', afterId: s.getListNodeIds('t', '1', 'f').single);
      }, ['x'], ['x', 'y']);
      converge('list delete', (s) {
        s.insertIntoList('t', '1', 'f', 'x');
        s.deleteFromList('t', '1', 'f', s.getListNodeIds('t', '1', 'f').single);
      }, ['x'], <Object?>[]);
      converge('document path', (s) {
        s.setDocumentField('t', '1', 'f', 'a', 1);
        s.setDocumentField('t', '1', 'f', 'b', 2);
      }, {'a': 1}, {'a': 1, 'b': 2});
      converge('document path delete', (s) {
        s.setDocumentField('t', '1', 'f', 'a', 1);
        s.deleteDocumentField('t', '1', 'f', 'a');
      }, {'a': 1}, <String, Object?>{});
      converge('text', (s) {
        s.insertText('t', '1', 'f', 0, 'hello');
        s.insertText('t', '1', 'f', 5, ' world');
      }, 'hello', 'hello world');
    });

    test('markRejected, retryRejected and discardPending manage the queue and notify', () {
      final s = newStore();
      var notified = 0;
      s.subscribe(() => notified++);
      final a = s.setField('t', '1', 'a', 1)!;
      final b = s.setField('t', '1', 'b', 1)!;
      notified = 0;
      s.markRejected(pendingKey(a), const PendingRejection(kind: 'validation', reason: 'bad'));
      expect(notified, 1);
      expect(s.getPendingChanges(), [same(b)]);
      expect(s.pendingCount, 1);
      expect(s.rejectedCount, 1);
      s.retryRejected(pendingKey(b)); // not rejected: no-op
      expect(notified, 1);
      final removed = s.discardPending(pendingKey(a))!;
      expect(removed.rejection!.kind, 'validation');
      expect(s.pending.map((p) => p.change), [same(b)]);
      expect(s.discardPending('nope'), isNull);
      // The local effect stays until the caller drops the field.
      expect(s.getDocument('t', '1')!['a'], 1);
      s.dropField('t', '1', 'a');
      expect(s.getDocument('t', '1')!.containsKey('a'), isFalse);
    });

    test('clearPendingChanges with the pushed records keeps writes made during the push', () {
      final s = newStore();
      s.setField('t', '1', 'a', 1);
      final pushed = s.getPendingChanges();
      s.setField('t', '1', 'b', 2);
      s.clearPendingChanges(pushed);
      expect(s.getPendingChanges().single.field, 'b');
    });

    test('a re-stamped change is not cleared by the push of its old record', () {
      var now = 99999999;
      final s = newStore(now: () => now);
      final c = s.setField('t', '1', 'a', 1)!;
      now = 1000;
      s.restampPending(pendingKey(c));
      s.clearPendingChanges([c]);
      expect(s.pendingCount, 1);
    });

    test('restampPending moves a record tombstone and a document path to a fresh clock', () {
      var now = 99999999;
      final s = newStore(now: () => now);
      final path = s.setDocumentField('t', '1', 'meta', 'a', 1)!;
      final del = s.deleteDocument('t', '2');
      now = 1000;
      final freshPath = s.restampPending(pendingKey(path))!;
      final freshDel = s.restampPending(pendingKey(del))!;
      expect(s.getDocumentState('t', '1')!.fields['meta']!.docState!.fields['a']!.hlc, freshPath.hlc);
      expect(s.getDocumentState('t', '2')!.tombstoneHlc, freshDel.hlc);
      expect(s.restampPending('unknown'), isNull);
    });

    test('maxHlc covers documents, tombstones and pending changes', () {
      final s = newStore();
      expect(s.maxHlc, HLC.zero);
      s.applyChanges([
        ChangeRecord(table: 't', pk: '1', field: '_tombstone', crdtType: CrdtType.none, hlc: n(7000000000000), nodeId: 'srv', tombstone: true),
      ]);
      expect(s.maxHlc, n(7000000000000));
      final c = s.setField('t', '2', 'f', 1)!;
      expect(s.maxHlc, c.hlc.isAfter(n(7000000000000)) ? c.hlc : n(7000000000000));
    });

    test('a snapshot round-trips through JSON, and accepts crdt-js bare pending records', () {
      final s = newStore();
      final c = s.setField('t', '1', 'f', 'x')!;
      s.markRejected(pendingKey(c), const PendingRejection(kind: 'hook', reason: 'no'));
      final json = s.exportState().toJson();
      expect(json.keys, ['version', 'nodeId', 'timestamp', 'tables', 'pending']);
      final back = StateSnapshot.fromJson(json);
      expect(back.tables['t']!['1']!.fields['f']!.value!.value, 'x');
      expect(back.pending.single.rejection!.reason, 'no');
      final js = StateSnapshot.fromJson({
        'version': 1,
        'nodeId': 'js',
        'timestamp': 5,
        'tables': <String, Object?>{},
        'pending': [c.toJson()],
      });
      expect(js.pending.single.change.hlc, c.hlc);
      expect(js.pending.single.rejection, isNull);
    });

    test('exportState and importState copy, so neither side shares state with a snapshot', () {
      final s = newStore();
      s.insertText('t', '1', 'body', 0, 'hi');
      final snap = s.exportState();
      snap.tables['t']!['1']!.fields['body']!.textState!.frags.clear();
      expect(s.getText('t', '1', 'body'), 'hi');
      final other = newStore(node: 'b');
      other.importState(snap..tables['t']!['1'] = s.exportTable('t')['1']!);
      snap.tables['t']!['1']!.fields['body']!.textState!.frags.clear();
      expect(other.getText('t', '1', 'body'), 'hi');
    });

    test('a pre-parity list key is rebuilt from the node id on hydrate', () async {
      final kv = MapReplicaKeyValue();
      final id = HLC(BigInt.from(5), 0, 'a');
      final doc = DocumentState(table: 't', pk: '1', fields: {
        'l': FieldState(
          type: CrdtType.list,
          hlc: id,
          nodeId: 'a',
          listState: RgaListState({'5:0:a': RgaNode(id: id, nodeId: 'a', parentId: HLC.zero, value: const JsonValue('x'))}),
        ),
      });
      await KeyValueReplicaStorage(kv).saveDocument('t', '1', doc);
      final s = newStore(storage: KeyValueReplicaStorage(kv));
      await s.ready;
      expect(s.getDocumentState('t', '1')!.fields['l']!.listState!.nodes.keys, [hlcString(id)]);
      expect(s.getDocument('t', '1')!['l'], ['x']);
    });

    test('hydration notifies global listeners and emits each hydrated document', () async {
      final kv = MapReplicaKeyValue();
      final first = newStore(storage: KeyValueReplicaStorage(kv));
      await first.ready;
      first.setField('t', '1', 'f', 1);
      first.setField('t', '2', 'f', 1);
      await first.flushPersistence();
      final second = newStore(storage: KeyValueReplicaStorage(kv));
      var notified = 0;
      second.subscribe(() => notified++);
      final events = <DocKey>[];
      final sub = second.documentChanges.listen(events.add);
      await second.ready;
      await Future<void>.delayed(Duration.zero);
      expect(notified, 1);
      expect(events, unorderedEquals([(table: 't', pk: '1'), (table: 't', pk: '2')]));
      await sub.cancel();
    });

    test('a listener that unsubscribes while being notified does not break the others', () {
      final s = newStore();
      var second = 0;
      late void Function() off;
      off = s.subscribe(() => off());
      s.subscribe(() => second++);
      s.setField('t', '1', 'f', 1);
      s.setField('t', '1', 'f', 2);
      expect(second, 2);
    });
  });

  group('fix round 0', () {
    test('hydration seeds the clock past a persisted max an hour ahead of the device clock', () async {
      final kv = MapReplicaKeyValue();
      const hourMs = 3600 * 1000;
      final first = newStore(storage: KeyValueReplicaStorage(kv), now: () => 10 * hourMs);
      await first.ready;
      final written = first.setField('t', '1', 'title', 'x')!;
      await first.flushPersistence();
      // The device clock now reads an hour behind, far past the drift bound.
      final second = newStore(storage: KeyValueReplicaStorage(kv), now: () => 9 * hourMs);
      await second.ready;
      final next = second.setField('t', '1', 'title', 'y')!;
      expect(next.hlc.isAfter(written.hlc), isTrue);
      expect(second.getDocument('t', '1')!['title'], 'y');
    });

    test('hydration seeds the clock past the max pending HLC too', () async {
      final kv = MapReplicaKeyValue();
      const hourMs = 3600 * 1000;
      final first = newStore(storage: KeyValueReplicaStorage(kv), now: () => 10 * hourMs);
      await first.ready;
      final c = first.setField('t', '1', 'title', 'x')!;
      await first.flushPersistence();
      kv.entries.removeWhere((k, _) => k.startsWith('doc/'));
      final second = newStore(storage: KeyValueReplicaStorage(kv), now: () => 9 * hourMs);
      await second.ready;
      expect(second.setField('t', '2', 'f', 1)!.hlc.isAfter(c.hlc), isTrue);
    });

    test('a pending remove built from a non-canonical key sends the same bytes after a restart', () async {
      final kv = MapReplicaKeyValue();
      final first = newStore(storage: KeyValueReplicaStorage(kv));
      await first.ready;
      first.applyChanges([
        ChangeRecord(table: 't', pk: '1', field: 's', crdtType: CrdtType.set, hlc: n(2), nodeId: 'js',
            state: setFieldState(OrSetState(entries: {'"a<b"': [OrSetTag('js', n(1, 'js'))]}), n(1, 'js'), 'js')),
      ]);
      final c = first.removeFromSet('t', '1', 's', ['a<b', 'plain'])!;
      final bytes = encodeWire(c.toJson());
      expect(bytes, contains(r'"elements":["a<b","plain"]'));
      await first.flushPersistence();

      final second = newStore(storage: KeyValueReplicaStorage(kv));
      await second.ready;
      final restored = second.getPendingChanges().single;
      expect(encodeWire(restored.toJson()), bytes);
      expect(restored.setOp!.elements.first, isA<RawJson>());
      expect(restored.setOp!.elements.last, 'plain');
    });

    test('quarantine: a key whose afterHydrate failed is never written, and local writes to it throw', () async {
      for (final atomic in [false, true]) {
        final storage = atomic ? RecordingAtomicStorage() : RecordingStorage();
        storage.preloaded = {
          't': {
            'locked': DocumentState(table: 't', pk: 'locked', fields: {
              'secret': FieldState(type: CrdtType.lww, hlc: n(1), nodeId: 'srv', value: const JsonValue('ciphertext')),
            }),
            'open': DocumentState(table: 't', pk: 'open'),
          },
        };
        final storageErrors = <Object>[];
        final s = CrdtStore('a', HybridClock('a', nowMs: () => 1000),
            storage: storage, persistDebounce: Duration.zero, onStorageError: storageErrors.add);
        s.use(_HydrateFails('locked'));
        await s.ready;
        expect(storageErrors, hasLength(1)); // reported once
        expect(s.getDocument('t', 'locked'), isNull);

        // A remote change is applied in memory and served, but not persisted.
        s.applyChanges([
          ChangeRecord(table: 't', pk: 'locked', field: 'title', crdtType: CrdtType.lww, hlc: n(5), nodeId: 'srv', value: const JsonValue('hi')),
        ]);
        expect(s.getDocument('t', 'locked')!['title'], 'hi');
        expect(storageErrors, hasLength(2));
        expect(storageErrors.last, isA<StateError>());

        // Every local write to it throws, and nothing is queued.
        expect(() => s.setField('t', 'locked', 'title', 'x'), throwsStateError);
        expect(() => s.deleteDocument('t', 'locked'), throwsStateError);
        expect(() => s.insertText('t', 'locked', 'body', 0, 'x'), throwsStateError);
        expect(() => s.reconcileField('t', 'locked', 'n', CrdtType.counter, 3), throwsStateError);
        expect(s.pending, isEmpty);

        // Neither a drop nor a flush ever touches its stored bytes.
        s.setField('t', 'open', 'f', 1);
        s.dropDocument('t', 'locked');
        await s.flushPersistence();
        final written = storage is RecordingAtomicStorage
            ? [for (final c in storage.commits) ...c.documents.keys]
            : [for (final d in storage.saved) (d.table, d.pk), ...storage.deleted];
        expect(written, isNotEmpty, reason: 'atomic: $atomic');
        expect(written.where((k) => k.$2 == 'locked'), isEmpty, reason: 'atomic: $atomic');
        await s.dispose();
      }
    });
  });
}

final class _HydrateFails extends StorePlugin {
  _HydrateFails(this.pk);
  final String pk;

  @override
  String get name => 'decryptor';

  @override
  DocumentState afterHydrate(String table, String pk, DocumentState doc) =>
      pk == this.pk ? throw StateError('bad key') : doc;
}
