import 'dart:convert';

import 'package:grove_crdt/grove_crdt.dart';
import 'package:test/test.dart';

HLC n(int ts) => HLC(BigInt.from(ts), 0, 'a');

DocumentState doc(String pk, {String table = 'notes/x'}) => DocumentState(table: table, pk: pk, fields: {
      'title': FieldState(type: CrdtType.lww, hlc: n(1), nodeId: 'a', value: const JsonValue('t')),
    });

ChangeRecord lwwChange(int ts, {String table = 't', String pk = '1', String field = 'f'}) =>
    ChangeRecord(table: table, pk: pk, field: field, crdtType: CrdtType.lww, hlc: n(ts), nodeId: 'a', value: JsonValue(ts));

/// A store whose every operation fails, for error propagation.
final class _FailingKv implements ReplicaKeyValue {
  @override
  Future<String?> get(String key) => Future.error(StateError('get failed'));

  @override
  Future<void> put(String key, String value) => Future.error(StateError('put failed'));

  @override
  Future<void> delete(String key) => Future.error(StateError('delete failed'));

  @override
  Future<Map<String, String>> scan(String prefix) => Future.error(StateError('scan failed'));

  @override
  Future<void> batch(void Function(ReplicaKeyValueBatch batch) build) => Future.error(StateError('batch failed'));
}

void main() {
  group('MemoryStorage', () {
    test('loadState returns empty Map', () async {
      const storage = MemoryReplicaStorage();
      final state = await storage.loadState();
      expect(state, isA<Map<String, Map<String, DocumentState>>>());
      expect(state, isEmpty);
    });

    test('saveDocument is a no-op (does not throw)', () async {
      const storage = MemoryReplicaStorage();
      final d = DocumentState(table: 't', pk: '1');
      await expectLater(storage.saveDocument('t', '1', d), completion(isNull));
    });

    test('deleteDocument is a no-op (does not throw)', () async {
      const storage = MemoryReplicaStorage();
      await expectLater(storage.deleteDocument('t', '1'), completion(isNull));
    });

    test('loadPendingChanges returns empty array', () async {
      const storage = MemoryReplicaStorage();
      final changes = await storage.loadPendingChanges();
      expect(changes, isEmpty);
    });

    test('savePendingChanges is a no-op (does not throw)', () async {
      const storage = MemoryReplicaStorage();
      await expectLater(storage.savePendingChanges([]), completion(isNull));
    });
  });

  group('KeyValueReplicaStorage', () {
    test('round-trips documents, including slashes in table and pk', () async {
      final kv = MapReplicaKeyValue();
      final s = KeyValueReplicaStorage(kv, prefix: 'd/ds1/');
      await s.saveDocument('notes/x', 'a/b', doc('a/b'));
      final loaded = await KeyValueReplicaStorage(kv, prefix: 'd/ds1/').loadState();
      expect(loaded['notes/x']!['a/b']!.fields['title']!.value, const JsonValue('t'));
    });

    test('deleteDocument removes only that document', () async {
      final s = KeyValueReplicaStorage(MapReplicaKeyValue());
      await s.saveDocument('notes/x', '1', doc('1'));
      await s.saveDocument('notes/x', '2', doc('2'));
      await s.deleteDocument('notes/x', '1');
      expect((await s.loadState())['notes/x']!.keys, ['2']);
    });

    test('persists pending changes with their rejection marks', () async {
      final kv = MapReplicaKeyValue();
      final change = ChangeRecord(
          table: 't', pk: '1', field: 'f', crdtType: CrdtType.lww, hlc: n(3), nodeId: 'a', value: const JsonValue(1));
      await KeyValueReplicaStorage(kv).savePendingChanges([
        PendingChange(change, rejection: const PendingRejection(kind: 'hook', reason: 'no')),
      ]);
      final back = await KeyValueReplicaStorage(kv).loadPendingChanges();
      expect(back.single.key, pendingKey(change));
      expect(back.single.rejection!.reason, 'no');
    });

    test('isolates prefixes', () async {
      final kv = MapReplicaKeyValue();
      await KeyValueReplicaStorage(kv, prefix: 'd/a/').saveDocument('t', '1', doc('1'));
      expect(await KeyValueReplicaStorage(kv, prefix: 'd/b/').loadState(), isEmpty);
    });

    test('stores per-table cursors and meta', () async {
      final kv = MapReplicaKeyValue();
      final s = KeyValueReplicaStorage(kv);
      await s.writeCursor('notes', HLC(BigInt.parse('1712345678901234567'), 2, 'srv'));
      await s.writeMeta('node', 'dev-1');
      final again = KeyValueReplicaStorage(kv);
      expect((await again.readCursor('notes'))!.ts, BigInt.parse('1712345678901234567'));
      expect(await again.readCursor('other'), isNull);
      expect(await again.readMeta('node'), 'dev-1');
    });

    test('clearAll drops everything under the prefix', () async {
      final kv = MapReplicaKeyValue();
      final s = KeyValueReplicaStorage(kv, prefix: 'd/a/');
      await s.saveDocument('t', '1', doc('1'));
      await s.writeCursor('t', n(1));
      await KeyValueReplicaStorage(kv, prefix: 'd/b/').writeMeta('k', 'v');
      await s.clearAll();
      expect(await s.loadState(), isEmpty);
      expect(await s.readCursor('t'), isNull);
      expect(await KeyValueReplicaStorage(kv, prefix: 'd/b/').readMeta('k'), 'v');
    });
  });

  group('KeyValueReplicaStorage beyond the crdt-js cases', () {
    test('the document key is percent-encoded, so a slash in a table or pk cannot collide', () async {
      final kv = MapReplicaKeyValue();
      final s = KeyValueReplicaStorage(kv);
      // ('a/b', 'c') and ('a', 'b/c') join to the same string without encoding.
      await s.saveDocument('a/b', 'c', doc('c', table: 'a/b'));
      await s.saveDocument('a', 'b/c', doc('b/c', table: 'a'));
      final loaded = await s.loadState();
      expect(loaded['a/b']!.keys, ['c']);
      expect(loaded['a']!.keys, ['b/c']);
      expect(kv.entries.keys.where((k) => k.startsWith('doc/')), hasLength(2));
    });

    test('an unpaired surrogate in a table or pk keys as U+FFFD, as the server holds it', () async {
      final kv = MapReplicaKeyValue();
      final s = KeyValueReplicaStorage(kv);
      await s.saveDocument('t\uD800', 'a\uD800', doc('a\uD800', table: 't\uD800'));
      await s.saveDocument('t\uFFFD', 'a\uDC00', doc('a\uDC00', table: 't\uFFFD'));
      // All three names are one name to the server, so there is one document.
      expect(kv.entries.keys, ['doc/t%EF%BF%BD/a%EF%BF%BD']);
      final loaded = await s.loadState();
      expect(loaded.keys, ['t\uFFFD']);
      expect(loaded['t\uFFFD']!.keys, ['a\uFFFD']);
      await s.deleteDocument('t\uD800', 'a\uD800');
      expect(kv.entries, isEmpty);
      await s.writeCursor('t\uD800', n(1));
      expect(await s.readCursor('t\uFFFD'), n(1));
    });

    test('keeps empty and non-ASCII table and pk apart', () async {
      final s = KeyValueReplicaStorage(MapReplicaKeyValue());
      await s.saveDocument('', '', doc('', table: ''));
      await s.saveDocument('té', '\u{1F600}', doc('\u{1F600}', table: 'té'));
      final loaded = await s.loadState();
      expect(loaded['']!['']!.pk, '');
      expect(loaded['té']!['\u{1F600}']!.pk, '\u{1F600}');
    });

    test('a saved document replaces the earlier one', () async {
      final s = KeyValueReplicaStorage(MapReplicaKeyValue());
      await s.saveDocument('t', '1', doc('1', table: 't'));
      await s.saveDocument(
          't',
          '1',
          DocumentState(table: 't', pk: '1', tombstone: true, tombstoneHlc: n(9)));
      final back = (await s.loadState())['t']!['1']!;
      expect(back.tombstone, isTrue);
      expect(back.tombstoneHlc, n(9));
      expect(back.fields, isEmpty);
    });

    test('stores a document as wire JSON', () async {
      final kv = MapReplicaKeyValue();
      await KeyValueReplicaStorage(kv).saveDocument('t', '1', doc('1', table: 't'));
      final raw = kv.entries['doc/t/1']!;
      expect(jsonDecode(raw), jsonDecode(encodeWire(doc('1', table: 't').toJson())));
    });

    test('an empty pending queue loads as empty, and a saved empty queue replaces a full one', () async {
      final kv = MapReplicaKeyValue();
      final s = KeyValueReplicaStorage(kv);
      expect(await s.loadPendingChanges(), isEmpty);
      await s.savePendingChanges([PendingChange(lwwChange(1)), PendingChange(lwwChange(2))]);
      expect(await s.loadPendingChanges(), hasLength(2));
      await s.savePendingChanges([]);
      expect(await s.loadPendingChanges(), isEmpty);
    });

    test('the pending queue keeps its order, restamps and rejection marks', () async {
      final s = KeyValueReplicaStorage(MapReplicaKeyValue());
      await s.savePendingChanges([
        PendingChange(lwwChange(3), restamps: 2),
        PendingChange(lwwChange(1), rejection: const PendingRejection(kind: 'drift', reason: 'ahead')),
        PendingChange(lwwChange(2)),
      ]);
      final back = await s.loadPendingChanges();
      expect(back.map((p) => p.change.hlc.ts.toInt()), [3, 1, 2]);
      expect(back.map((p) => p.restamps), [2, 0, 0]);
      expect(back.map((p) => p.isRejected), [false, true, false]);
      expect(back[1].rejection, const PendingRejection(kind: 'drift', reason: 'ahead'));
    });

    test('prefixes isolate pending changes, cursors and meta too', () async {
      final kv = MapReplicaKeyValue();
      final a = KeyValueReplicaStorage(kv, prefix: 'd/a/');
      final b = KeyValueReplicaStorage(kv, prefix: 'd/b/');
      await a.savePendingChanges([PendingChange(lwwChange(1))]);
      await a.writeCursor('t', n(1));
      await a.writeMeta('k', 'v');
      expect(await b.loadPendingChanges(), isEmpty);
      expect(await b.readCursor('t'), isNull);
      expect(await b.readMeta('k'), isNull);
    });

    test('clearAll also drops the pending queue and meta', () async {
      final kv = MapReplicaKeyValue();
      final s = KeyValueReplicaStorage(kv, prefix: 'd/a/');
      await s.savePendingChanges([PendingChange(lwwChange(1))]);
      await s.writeMeta('k', 'v');
      await s.clearAll();
      expect(kv.entries, isEmpty);
      await s.clearAll(); // nothing left: still fine
    });

    test('a cursor round-trips the full int64 range as a decimal string', () async {
      final s = KeyValueReplicaStorage(MapReplicaKeyValue());
      final cursor = HLC(BigInt.parse('9223372036854775807'), 4294967295, 'srv');
      await s.writeCursor('t', cursor);
      expect(await s.readCursor('t'), cursor);
    });

    test('a slash in a table name keeps cursors apart', () async {
      final s = KeyValueReplicaStorage(MapReplicaKeyValue());
      await s.writeCursor('a/b', n(1));
      await s.writeCursor('a', n(2));
      expect(await s.readCursor('a/b'), n(1));
      expect(await s.readCursor('a'), n(2));
    });

    test('loadState rejects a malformed stored document and names its key', () async {
      final kv = MapReplicaKeyValue();
      kv.entries['doc/t/1'] = '{"table": 5}';
      await expectLater(
        KeyValueReplicaStorage(kv).loadState(),
        throwsA(isA<FormatException>().having((e) => e.message, 'message', contains('doc/t/1'))),
      );
      kv.entries
        ..clear()
        ..['doc/t'] = '{}';
      await expectLater(KeyValueReplicaStorage(kv).loadState(), throwsFormatException);
    });

    test('a bad percent-escape in a stored key is a FormatException that names the key', () async {
      for (final bad in ['doc/%/1', 'doc/%ZZ/1', 'doc/t/%C3']) {
        final kv = MapReplicaKeyValue()..entries[bad] = '{}';
        await expectLater(
          KeyValueReplicaStorage(kv).loadState(),
          throwsA(isA<FormatException>().having((e) => e.message, 'message', contains(bad))),
          reason: bad,
        );
      }
    });

    test('loadPendingChanges rejects a stored queue that is not an array', () async {
      final kv = MapReplicaKeyValue()..entries['pending'] = '{}';
      await expectLater(KeyValueReplicaStorage(kv).loadPendingChanges(), throwsFormatException);
    });

    test('a storage failure is an error on the future, never a synchronous throw', () async {
      final s = KeyValueReplicaStorage(_FailingKv());
      final futures = <Future<Object?>>[
        s.loadState(),
        s.saveDocument('t', '1', doc('1', table: 't')),
        s.deleteDocument('t', '1'),
        s.loadPendingChanges(),
        s.savePendingChanges([PendingChange(lwwChange(1))]),
        s.readCursor('t'),
        s.writeCursor('t', n(1)),
        s.readMeta('k'),
        s.writeMeta('k', 'v'),
        s.clearAll(),
        s.commit(documents: {('t', '1'): doc('1', table: 't')}),
      ];
      for (final f in futures) {
        await expectLater(f, throwsA(isA<StateError>()));
      }
    });

    test('an encoding failure is an error on the future too', () async {
      final s = KeyValueReplicaStorage(MapReplicaKeyValue());
      final bad = DocumentState(table: 't', pk: '1', fields: {
        'f': FieldState(type: CrdtType.lww, hlc: n(1), nodeId: 'a', value: const JsonValue(Object())),
      });
      await expectLater(s.saveDocument('t', '1', bad), throwsA(anything));
    });
  });

  group('AtomicReplicaStorage', () {
    test('commit writes documents, deletes, and the pending queue together', () async {
      final kv = MapReplicaKeyValue();
      final s = KeyValueReplicaStorage(kv);
      await s.saveDocument('t', 'old', doc('old', table: 't'));
      await s.commit(
        documents: {('t', 'new'): doc('new', table: 't'), ('t', 'old'): null},
        pending: [PendingChange(lwwChange(5))],
      );
      expect((await s.loadState())['t']!.keys, ['new']);
      expect((await s.loadPendingChanges()).single.change.hlc, n(5));
    });

    test('commit leaves the pending queue alone when none is given', () async {
      final s = KeyValueReplicaStorage(MapReplicaKeyValue());
      await s.savePendingChanges([PendingChange(lwwChange(1))]);
      await s.commit(documents: {('t', '1'): doc('1', table: 't')});
      expect(await s.loadPendingChanges(), hasLength(1));
    });

    test('commit with nothing to write does not touch the store', () async {
      final s = KeyValueReplicaStorage(_FailingKv());
      await s.commit();
    });

    test('commit is one batch, so a failing batch writes nothing', () async {
      final kv = _CountingKv(MapReplicaKeyValue(), failBatch: true);
      final s = KeyValueReplicaStorage(kv);
      await expectLater(
        s.commit(documents: {('t', '1'): doc('1', table: 't')}, pending: [PendingChange(lwwChange(1))]),
        throwsA(isA<StateError>()),
      );
      expect(kv.batches, 1);
      expect(kv.puts, 0);
      expect(kv.inner.entries, isEmpty);
    });

    test('commit issues exactly one batch and no separate put', () async {
      final kv = _CountingKv(MapReplicaKeyValue());
      final s = KeyValueReplicaStorage(kv);
      await s.commit(
        documents: {('t', '1'): doc('1', table: 't'), ('t', '2'): doc('2', table: 't')},
        pending: [PendingChange(lwwChange(1))],
      );
      expect(kv.batches, 1);
      expect((kv.puts, kv.deletes, kv.reads), (0, 0, 0));
      expect(kv.inner.entries.keys, containsAll(['doc/t/1', 'doc/t/2', 'pending']));
    });

    test('KeyValueReplicaStorage is atomic, MemoryReplicaStorage is not', () {
      expect(KeyValueReplicaStorage(MapReplicaKeyValue()), isA<AtomicReplicaStorage>());
      expect(const MemoryReplicaStorage(), isNot(isA<AtomicReplicaStorage>()));
    });
  });

  group('MapReplicaKeyValue', () {
    test('scan matches the prefix literally and returns UTF-16 order', () async {
      final kv = MapReplicaKeyValue();
      for (final k in ['b/2', 'a_1', 'a%1', 'a/10', 'a/2', 'ab']) {
        await kv.put(k, k);
      }
      expect((await kv.scan('a')).keys.toList(), ['a%1', 'a/10', 'a/2', 'a_1', 'ab']);
      expect((await kv.scan('a_')).keys.toList(), ['a_1']);
      expect((await kv.scan('a%')).keys.toList(), ['a%1']);
      expect(await kv.scan('zz'), isEmpty);
    });

    test('a batch applies every write, in order', () async {
      final kv = MapReplicaKeyValue();
      await kv.put('gone', '1');
      await kv.batch((b) {
        b.put('k', '1');
        b.put('k', '2');
        b.delete('gone');
      });
      expect(kv.entries, {'k': '2'});
    });

    test('a batch whose builder throws applies nothing', () async {
      final kv = MapReplicaKeyValue();
      await expectLater(
        kv.batch((b) {
          b.put('k', '1');
          throw StateError('boom');
        }),
        throwsStateError,
      );
      expect(kv.entries, isEmpty);
    });

    test('a write after the builder returned throws a StateError', () async {
      final kv = MapReplicaKeyValue();
      late ReplicaKeyValueBatch kept;
      await kv.batch((b) => kept = b);
      expect(() => kept.put('k', 'v'), throwsStateError);
      expect(() => kept.delete('k'), throwsStateError);
      expect(kv.entries, isEmpty);
    });
  });

  group('pendingKey', () {
    test('joins table, pk, field and the HLC string with U+0000', () {
      final c = lwwChange(3, table: 'tbl', pk: 'p', field: 'fld');
      expect(pendingKey(c), 'tbl\u0000p\u0000fld\u0000HLC{ts:3 c:0 node:a}');
    });

    test('differs when any part differs', () {
      final base = pendingKey(lwwChange(1));
      expect(pendingKey(lwwChange(2)), isNot(base));
      expect(pendingKey(lwwChange(1, table: 'u')), isNot(base));
      expect(pendingKey(lwwChange(1, pk: '2')), isNot(base));
      expect(pendingKey(lwwChange(1, field: 'g')), isNot(base));
    });
  });

  group('PendingChange', () {
    test('round-trips through JSON, omitting the default restamps and a missing rejection', () {
      final p = PendingChange(lwwChange(1));
      final json = p.toJson();
      expect(json.keys, ['change']);
      final back = PendingChange.fromJson(jsonDecode(encodeWire(json)));
      expect(back.key, p.key);
      expect(back.isRejected, isFalse);
      expect(back.restamps, 0);
    });

    test('round-trips a rejection and restamps', () {
      final p = PendingChange(lwwChange(1), rejection: const PendingRejection(kind: 'server', reason: 'x'), restamps: 3);
      final back = PendingChange.fromJson(jsonDecode(encodeWire(p.toJson())));
      expect(back.rejection, const PendingRejection(kind: 'server', reason: 'x'));
      expect(back.restamps, 3);
    });

    test('copyWith replaces, keeps, and clears the rejection', () {
      final p = PendingChange(lwwChange(1));
      final marked = p.copyWith(rejection: const PendingRejection(kind: 'hook', reason: 'no'));
      expect(marked.isRejected, isTrue);
      expect(marked.copyWith(restamps: 1).rejection, isNotNull);
      expect(marked.copyWith(clearRejection: true).isRejected, isFalse);
      expect(marked.copyWith(change: lwwChange(2)).key, pendingKey(lwwChange(2)));
      expect(p.isRejected, isFalse);
    });

    test('fromJson rejects the wrong JSON types', () {
      expect(() => PendingChange.fromJson('x'), throwsFormatException);
      expect(() => PendingChange.fromJson(<String, Object?>{}), throwsFormatException);
      expect(() => PendingChange.fromJson({'change': lwwChange(1).toJson(), 'restamps': 'two'}), throwsFormatException);
      expect(() => PendingChange.fromJson({'change': lwwChange(1).toJson(), 'rejection': 'x'}), throwsFormatException);
      expect(() => PendingChange.fromJson({'change': lwwChange(1).toJson(), 'rejection': {'kind': 'hook'}}),
          throwsFormatException);
      expect(() => PendingChange.fromJson({'change': lwwChange(1).toJson(), 'rejection': {'kind': 1, 'reason': 'x'}}),
          throwsFormatException);
    });
  });
}

/// Counts what reaches the backing store.
final class _CountingKv implements ReplicaKeyValue {
  _CountingKv(this.inner, {this.failBatch = false});

  final MapReplicaKeyValue inner;
  final bool failBatch;
  int batches = 0;
  int puts = 0;
  int reads = 0; // get and scan
  int deletes = 0;

  @override
  Future<String?> get(String key) {
    reads++;
    return inner.get(key);
  }

  @override
  Future<void> put(String key, String value) {
    puts++;
    return inner.put(key, value);
  }

  @override
  Future<void> delete(String key) {
    deletes++;
    return inner.delete(key);
  }

  @override
  Future<Map<String, String>> scan(String prefix) {
    reads++;
    return inner.scan(prefix);
  }

  @override
  Future<void> batch(void Function(ReplicaKeyValueBatch batch) build) {
    batches++;
    if (failBatch) return Future.error(StateError('batch failed'));
    return inner.batch(build);
  }
}
