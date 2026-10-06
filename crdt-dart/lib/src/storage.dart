import 'dart:collection';
import 'dart:convert';

import 'hlc.dart';
import 'pending.dart';
import 'types.dart';

/// Persistence for a replica. Port of crdt-js `StorageAdapter`, with pending
/// changes carrying their rejection marks.
abstract interface class ReplicaStorage {
  /// Every persisted document, by table then primary key.
  Future<Map<String, Map<String, DocumentState>>> loadState();

  /// Persists one document.
  Future<void> saveDocument(String table, String pk, DocumentState doc);

  /// Removes one document.
  Future<void> deleteDocument(String table, String pk);

  /// The persisted pending queue, oldest first.
  Future<List<PendingChange>> loadPendingChanges();

  /// Replaces the persisted pending queue.
  Future<void> savePendingChanges(List<PendingChange> changes);
}

/// A [ReplicaStorage] that can persist documents and the pending queue in one
/// atomic write.
///
/// A store that flushes a document and its pending entry separately can be
/// interrupted between the two, leaving a local edit in the document that no
/// pending entry will ever push. One [commit] per flush closes that window.
abstract interface class AtomicReplicaStorage implements ReplicaStorage {
  /// Persists [documents] and, when it is not null, replaces the pending queue
  /// with [pending], all together or not at all.
  ///
  /// Keys are `(table, pk)`. A null document deletes it.
  Future<void> commit({
    Map<(String, String), DocumentState?> documents = const {},
    List<PendingChange>? pending,
  });
}

/// Per-table pull cursors and small replica metadata.
abstract interface class SyncCursorStore {
  /// The cursor for [table], or null before the first pull.
  Future<HLC?> readCursor(String table);

  /// Persists the cursor for [table].
  Future<void> writeCursor(String table, HLC cursor);

  /// A metadata value.
  Future<String?> readMeta(String key);

  /// Persists a metadata value.
  Future<void> writeMeta(String key, String value);

  /// Removes everything this store holds.
  Future<void> clearAll();
}

/// The crdt-js `MemoryStorage`: persists nothing.
final class MemoryReplicaStorage implements ReplicaStorage {
  /// Creates the no-op storage.
  const MemoryReplicaStorage();

  @override
  Future<Map<String, Map<String, DocumentState>>> loadState() async => {};

  @override
  Future<void> saveDocument(String table, String pk, DocumentState doc) async {}

  @override
  Future<void> deleteDocument(String table, String pk) async {}

  @override
  Future<List<PendingChange>> loadPendingChanges() async => [];

  @override
  Future<void> savePendingChanges(List<PendingChange> changes) async {}
}

/// String key-value storage. Signature-identical to forge_client's
/// `KeyValueStore`, so an adapter is a direct delegation.
abstract interface class ReplicaKeyValue {
  /// Reads a value.
  Future<String?> get(String key);

  /// Writes a value.
  Future<void> put(String key, String value);

  /// Deletes a value.
  Future<void> delete(String key);

  /// Every entry whose key starts with [prefix], read as a literal string, in
  /// ascending UTF-16 code unit order.
  Future<Map<String, String>> scan(String prefix);

  /// Applies every write [build] records, all together, or none if it throws.
  ///
  /// [build] runs synchronously and must not await: the batch closes when it
  /// returns, and a write recorded after that throws [StateError]. A batch
  /// cannot read, so a read-modify-write is not possible inside it.
  Future<void> batch(void Function(ReplicaKeyValueBatch batch) build);
}

/// Writes collected by [ReplicaKeyValue.batch]. Usable only while the builder
/// runs.
abstract interface class ReplicaKeyValueBatch {
  /// Queues a write.
  void put(String key, String value);

  /// Queues a delete.
  void delete(String key);
}

/// In-memory [ReplicaKeyValue], for tests and restart simulation.
///
/// It keeps the contract of the real store: [scan] is in UTF-16 order, a batch
/// applies all of its writes or none, and a write after the builder returned
/// throws a [StateError].
final class MapReplicaKeyValue implements ReplicaKeyValue {
  /// The backing map, exposed for tests.
  final Map<String, String> entries = {};

  @override
  Future<String?> get(String key) async => entries[key];

  @override
  Future<void> put(String key, String value) async => entries[key] = value;

  @override
  Future<void> delete(String key) async => entries.remove(key);

  @override
  Future<Map<String, String>> scan(String prefix) async {
    final keys = [
      for (final k in entries.keys)
        if (k.startsWith(prefix)) k,
    ]..sort();
    return LinkedHashMap.fromIterable(keys, value: (k) => entries[k]!);
  }

  @override
  Future<void> batch(void Function(ReplicaKeyValueBatch batch) build) async {
    final b = _MapBatch();
    try {
      build(b);
    } finally {
      b.closed = true;
    }
    for (final op in b.ops) {
      op(entries);
    }
  }
}

final class _MapBatch implements ReplicaKeyValueBatch {
  final List<void Function(Map<String, String>)> ops = [];
  bool closed = false;

  void _check() {
    if (closed) throw StateError('the batch is closed: build runs synchronously and must not await');
  }

  @override
  void put(String key, String value) {
    _check();
    ops.add((m) => m[key] = value);
  }

  @override
  void delete(String key) {
    _check();
    ops.add((m) => m.remove(key));
  }
}

/// [ReplicaStorage] and [SyncCursorStore] over a [ReplicaKeyValue].
///
/// Keys, all under [prefix]:
///
/// - `doc/<table>/<pk>`: the document as wire JSON. `<table>` and `<pk>` are
///   percent-encoded, so a `/` inside either cannot collide.
/// - `pending`: a JSON array of [PendingChange.toJson].
/// - `cursor/<table>`: the pull cursor as wire JSON.
/// - `meta/<key>`: the raw value.
///
/// Every method is `async`, so an encoding failure and a storage failure alike
/// surface as an error on the returned future, never as a synchronous throw.
///
/// [clearAll] removes every key that starts with [prefix], so two replicas
/// must not use prefixes where one starts with the other.
final class KeyValueReplicaStorage implements AtomicReplicaStorage, SyncCursorStore {
  /// Stores everything under [prefix] in [kv].
  KeyValueReplicaStorage(this.kv, {this.prefix = ''});

  /// The backing store.
  final ReplicaKeyValue kv;

  /// Key prefix isolating this replica.
  final String prefix;

  String get _docPrefix => '${prefix}doc/';

  String get _pendingKey => '${prefix}pending';

  String _docKey(String table, String pk) => '$_docPrefix${Uri.encodeComponent(table)}/${Uri.encodeComponent(pk)}';

  String _cursorKey(String table) => '${prefix}cursor/${Uri.encodeComponent(table)}';

  String _encodePending(List<PendingChange> changes) => encodeWire([for (final c in changes) c.toJson()]);

  @override
  Future<Map<String, Map<String, DocumentState>>> loadState() async {
    final out = <String, Map<String, DocumentState>>{};
    for (final e in (await kv.scan(_docPrefix)).entries) {
      final parts = e.key.substring(_docPrefix.length).split('/');
      if (parts.length != 2) throw FormatException('crdt: malformed document key "${e.key}"');
      final String table;
      final String pk;
      final DocumentState doc;
      try {
        table = Uri.decodeComponent(parts[0]);
        pk = Uri.decodeComponent(parts[1]);
        doc = DocumentState.fromJson(jsonDecode(e.value));
      } on FormatException catch (err) {
        throw FormatException('crdt: stored document "${e.key}" is unreadable: ${err.message}');
      }
      (out[table] ??= {})[pk] = doc;
    }
    return out;
  }

  @override
  Future<void> saveDocument(String table, String pk, DocumentState doc) async =>
      kv.put(_docKey(table, pk), encodeWire(doc.toJson()));

  @override
  Future<void> deleteDocument(String table, String pk) async => kv.delete(_docKey(table, pk));

  @override
  Future<List<PendingChange>> loadPendingChanges() async {
    final raw = await kv.get(_pendingKey);
    if (raw == null) return [];
    final decoded = jsonDecode(raw);
    if (decoded is! List<Object?>) throw const FormatException('crdt: stored pending queue is not a JSON array');
    return [for (final j in decoded) PendingChange.fromJson(j)];
  }

  @override
  Future<void> savePendingChanges(List<PendingChange> changes) async => kv.put(_pendingKey, _encodePending(changes));

  @override
  Future<void> commit({
    Map<(String, String), DocumentState?> documents = const {},
    List<PendingChange>? pending,
  }) async {
    // Encode everything before the batch: its builder is synchronous, closes
    // when it returns, and cannot read.
    final writes = <String, String?>{
      for (final e in documents.entries)
        _docKey(e.key.$1, e.key.$2): e.value == null ? null : encodeWire(e.value!.toJson()),
      if (pending != null) _pendingKey: _encodePending(pending),
    };
    if (writes.isEmpty) return;
    await kv.batch((b) {
      for (final w in writes.entries) {
        final value = w.value;
        if (value == null) {
          b.delete(w.key);
        } else {
          b.put(w.key, value);
        }
      }
    });
  }

  @override
  Future<HLC?> readCursor(String table) async {
    final raw = await kv.get(_cursorKey(table));
    return raw == null ? null : HLC.fromJson(jsonDecode(raw));
  }

  @override
  Future<void> writeCursor(String table, HLC cursor) async => kv.put(_cursorKey(table), encodeWire(cursor.toJson()));

  @override
  Future<String?> readMeta(String key) async => kv.get('${prefix}meta/$key');

  @override
  Future<void> writeMeta(String key, String value) async => kv.put('${prefix}meta/$key', value);

  @override
  Future<void> clearAll() async {
    final keys = (await kv.scan(prefix)).keys.toList();
    if (keys.isEmpty) return;
    await kv.batch((b) {
      for (final k in keys) {
        b.delete(k);
      }
    });
  }
}
