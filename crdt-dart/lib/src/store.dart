/// The in-memory replica store: documents, the pending queue, undo,
/// subscriptions and persistence.
///
/// A port of `crdt-js/src/store.ts`. Remote and local changes fold through
/// [applyChange], the port of Go `ApplyChange`, so this replica holds exactly
/// what the Go server holds for the same changes. The Go-parity and offline
/// additions are documented on [CrdtStore].
library;

import 'dart:async';
import 'dart:collection';
import 'dart:convert';

import 'apply.dart';
import 'compact.dart';
import 'go_json.dart';
import 'hlc.dart';
import 'merge.dart';
import 'pending.dart';
import 'plugin.dart';
import 'storage.dart';
import 'text.dart';
import 'types.dart';
import 'undo.dart';
import 'wire_helpers.dart';

/// Identifies one document.
typedef DocKey = ({String table, String pk});

/// Thrown by a local write when the pending queue is full and the store was
/// created with `throwOnOverflow: true`. Port of the crdt-js `CRDTError` with
/// code `OfflineQueueFull`.
final class PendingQueueFullError implements Exception {
  /// Creates the error for a queue bounded at [limit] changes.
  const PendingQueueFullError(this.limit);

  /// The queue bound.
  final int limit;

  /// The crdt-js message text.
  String get message => 'crdt: pending queue full ($limit changes)';

  @override
  String toString() => message;
}

/// [CrdtStore.ready] completes with this when the persisted replica could
/// not be read: the pending queue, or the document set as a whole.
///
/// The store then fails closed. It refuses writes and flushes (they throw a
/// [StateError]) and never writes to storage, so the stored queue, which holds
/// the user's unpushed offline edits, is never overwritten by a store that
/// does not know what it held. Retry by constructing a new store.
final class ReplicaUnavailable implements Exception {
  /// Wraps the error the load failed with.
  const ReplicaUnavailable(this.cause);

  /// The error the load failed with.
  final Object cause;

  @override
  String toString() => 'crdt: the persisted replica could not be read: $cause';
}

/// [CrdtStore.flushPersistence] and [CrdtStore.dispose] complete with this
/// when a write failed: something the store holds did not reach storage.
///
/// What failed stays queued and is retried on the next flush, so after a
/// [CrdtStore.flushPersistence] the store keeps trying. After a
/// [CrdtStore.dispose] it does not: that data did not reach storage.
final class ReplicaPersistFailed implements Exception {
  /// Wraps the error the write failed with.
  const ReplicaPersistFailed(this.cause);

  /// The error the last failed write failed with.
  final Object cause;

  @override
  String toString() => 'crdt: a replica write failed: $cause';
}

/// A serializable snapshot of a store. Port of crdt-js `StateSnapshot`, with
/// the pending queue carrying rejection marks.
final class StateSnapshot {
  /// Creates a snapshot.
  StateSnapshot({
    required this.version,
    required this.nodeId,
    required this.timestamp,
    required this.tables,
    required this.pending,
  });

  /// The snapshot format version (1).
  final int version;

  /// The node that exported it.
  final String nodeId;

  /// When it was exported, in milliseconds since the Unix epoch.
  final int timestamp;

  /// Documents by table, then primary key.
  final Map<String, Map<String, DocumentState>> tables;

  /// The pending queue, oldest first.
  final List<PendingChange> pending;

  /// JSON form, with the crdt-js client-side key names.
  Map<String, Object?> toJson() => {
        'version': version,
        'nodeId': nodeId,
        'timestamp': timestamp,
        'tables': {
          for (final t in tables.entries) t.key: {for (final d in t.value.entries) d.key: d.value.toJson()},
        },
        'pending': [for (final p in pending) p.toJson()],
      };

  /// Decodes the JSON form. A pending entry may be a [PendingChange] or, as
  /// crdt-js exports it, a bare change record.
  static StateSnapshot fromJson(Object? j) {
    final m = wireObj(j);
    return StateSnapshot(
      version: wireInt(m, 'version'),
      nodeId: wireStr(m, 'nodeId'),
      timestamp: wireInt(m, 'timestamp'),
      tables: wireMap(m['tables'], (t) => wireMap(t, DocumentState.fromJson)),
      pending: wireList(m['pending'], (p) {
        final pm = wireObj(p);
        return pm.containsKey('change') ? PendingChange.fromJson(pm) : PendingChange(ChangeRecord.fromJson(pm));
      }),
    );
  }
}

final class _Cached<T> {
  const _Cached(this.value);
  final T value;
}

final class _FrozenMap extends UnmodifiableMapView<String, Object?> {
  _FrozenMap(super.map);
}

final class _FrozenList extends UnmodifiableListView<Object?> {
  _FrozenList(super.source);
}

/// A deep copy of [v] whose maps and lists refuse changes.
Object? _freeze(Object? v) => switch (v) {
      _FrozenMap() || _FrozenList() => v,
      final Map<String, Object?> m => _FrozenMap({for (final e in m.entries) e.key: _freeze(e.value)}),
      final List<Object?> l => _FrozenList([for (final e in l) _freeze(e)]),
      _ => v,
    };

/// A deep copy of a `toJson` tree, so a decode of it shares nothing mutable
/// with the original.
Object? _copyJson(Object? v) => switch (v) {
      final Map<String, Object?> m => <String, Object?>{for (final e in m.entries) e.key: _copyJson(e.value)},
      final List<Object?> l => <Object?>[for (final e in l) _copyJson(e)],
      _ => v,
    };

DocumentState _copyDoc(DocumentState d) => DocumentState.fromJson(_copyJson(d.toJson()));

PendingChange _copyPending(PendingChange p) => PendingChange.fromJson(_copyJson(p.toJson()));

/// Rebuilds RGA node keys from each node's id, migrating states persisted with
/// the pre-parity key format. Port of crdt-js `normalizeHLCKeys`, without the
/// mutation: a new document comes back when a key changes.
DocumentState _normalizeHlcKeys(DocumentState doc) {
  Map<String, FieldState>? fields;
  for (final e in doc.fields.entries) {
    final nodes = e.value.listState?.nodes;
    if (nodes == null || nodes.entries.every((n) => n.key == hlcString(n.value.id))) continue;
    final fixed = {for (final n in nodes.values) hlcString(n.id): n};
    (fields ??= {...doc.fields})[e.key] = e.value.copyWith(listState: RgaListState(fixed));
  }
  return fields == null ? doc : doc.copyWith(fields: fields);
}

const List<HLC> _emptyHlcs = <HLC>[];
const List<TextDeltaSegment> _emptyDelta = <TextDeltaSegment>[];

/// The in-memory replica. Port of crdt-js `CRDTStore`.
///
/// Holds documents by table and primary key, applies local writes and remote
/// changes, queues local changes for push, and notifies subscribers. Every
/// write replaces the document object (copy-on-write), which keeps the
/// identity-keyed read caches valid.
///
/// Where this differs from crdt-js:
///
/// - Changes fold through [applyChange] (Go `ApplyChange`), local and remote
///   alike. A remote change that throws ([CrdtApplyError], [CrdtMergeError],
///   [StateError], [FormatException]) is reported through `onError` and
///   skipped; the rest of the batch applies.
/// - Record tombstones are sticky, as in Go server storage, and the tombstone
///   clock is the later of the stored and incoming clocks, so arrival order
///   does not matter. A pulled tombstone arrives as `field: "_tombstone"`,
///   `crdtType: CrdtType.none`.
/// - The pending queue holds [PendingChange]s with rejection marks.
///   [getPendingChanges] and [pendingCount] leave rejected changes out.
/// - Undo emits compensating changes, so an undo reaches the server and every
///   other replica. See [undo].
/// - Hydration advances the clock past every persisted HLC before [ready]
///   completes, and writes wait for [ready], so a restarted device never
///   mints a clock below one it already issued.
/// - Every change carries `clock.nodeId`, so a clock rebase switches the node
///   id of every later change.
/// - Table, primary key and field names, and every string the app writes, are
///   held as Go holds them: an unpaired surrogate becomes U+FFFD.
/// - [getDocument] and [getCollection] return frozen deep copies. A read is
///   cached until the document changes, so a repeat read gives the identical
///   object, and changing it throws instead of changing what the next read
///   returns.
///
/// A document whose `afterHydrate` hook throws (a decryptor without its key)
/// is quarantined for the life of this store: it is not served from storage,
/// a local write to it throws a [StateError], and its stored bytes are never
/// overwritten or deleted, by local writes or by remote changes. A remote
/// change for it is still applied in memory and served, but not persisted,
/// and each one is reported through `onStorageError`. The stored copy
/// survives until the plugin is fixed or the app resets the replica.
///
/// Persistence is debounced over `persistDebounce`. [Duration.zero] writes
/// synchronously when no earlier write is still in flight. Writes go out one
/// flush at a time, in order. With an [AtomicReplicaStorage] each flush is one
/// [AtomicReplicaStorage.commit] holding the documents and the pending queue
/// together. A storage failure, synchronous or asynchronous, is reported
/// through `onStorageError` and never becomes an unhandled error; the
/// in-memory replica keeps working.
final class CrdtStore {
  /// Creates a store for [nodeId] stamping changes with [clock].
  ///
  /// [nodeId] must equal `clock.nodeId`. [storage] defaults to
  /// [MemoryReplicaStorage]. [maxPendingChanges] bounds the pending queue (0
  /// disables the bound); when it is reached the oldest changes are dropped
  /// and reported through [onPendingOverflow], or, with [throwOnOverflow], the
  /// write throws [PendingQueueFullError]. [undoHistory] bounds the undo
  /// stack.
  ///
  /// [onError] hears a remote change that could not be applied. A storage
  /// failure (including a document that cannot be decoded on hydrate, a
  /// `beforePersist` or `afterHydrate` hook that threw, and a storage session
  /// closed underneath the store) goes to [onStorageError] instead, because it
  /// has no change to name. [onPluginError] hears every error a plugin throws.
  /// All three default to ignoring the error.
  CrdtStore(
    String nodeId,
    this.clock, {
    ReplicaStorage? storage,
    this._persistDebounce = const Duration(milliseconds: 50),
    this._maxPendingChanges = 10000,
    this._throwOnOverflow = false,
    int undoHistory = 100,
    this._onError,
    this._onStorageError,
    PluginErrorHandler? onPluginError,
  })  : _storage = storage ?? const MemoryReplicaStorage(),
        _undo = UndoManager(maxHistory: undoHistory),
        _plugins = PluginManager(onPluginError: onPluginError) {
    if (nodeId != clock.nodeId) {
      throw ArgumentError.value(nodeId, 'nodeId', 'must equal the clock node id "${clock.nodeId}"');
    }
    if (_storage is MemoryReplicaStorage) {
      // Nothing to load: hydration is complete at once.
      _hydrated = true;
      ready = Future<void>.value();
      return;
    }
    ready = _hydrate();
    // A store nobody awaits must not raise an unhandled error; a caller that
    // awaits ready still receives it.
    ready.ignore();
  }

  /// The clock that stamps every local change.
  final HybridClock clock;

  final ReplicaStorage _storage;
  final Duration _persistDebounce;
  final int _maxPendingChanges;
  final bool _throwOnOverflow;
  final UndoManager _undo;
  final void Function(Object error, ChangeRecord change)? _onError;
  final void Function(Object error)? _onStorageError;
  final PluginManager _plugins;

  /// Completes when persisted state has been hydrated. Await it before the
  /// first write: a write before then throws a [StateError], because until
  /// the persisted history is loaded the store can neither seed its clock
  /// past it nor merge with it. Reads work at once and see an empty replica.
  /// With [MemoryReplicaStorage] (the default) there is nothing to load, and
  /// this is complete at once.
  ///
  /// A document that cannot be decoded, or whose `afterHydrate` throws, is
  /// reported and skipped (see the quarantine on [CrdtStore]). A listener
  /// that throws during hydration is reported. When the pending queue, or the
  /// document set as a whole, cannot be read, this completes with a
  /// [ReplicaUnavailable] and the store fails closed.
  late final Future<void> ready;

  /// The node id stamped on new changes: `clock.nodeId`.
  String get nodeId => clock.nodeId;

  /// table -> pk -> document.
  final Map<String, Map<String, DocumentState>> _state = {};

  /// Local changes waiting to be pushed, oldest first.
  final List<PendingChange> _pending = [];

  final Set<void Function(List<ChangeRecord> dropped)> _overflowHandlers = {};
  final Set<void Function()> _globalListeners = {};

  /// table -> pk -> listeners. Nested, never a delimited key: a pk may
  /// contain any character.
  final Map<String, Map<String, Set<void Function()>>> _docListeners = {};
  final Map<String, Set<void Function()>> _tableListeners = {};
  final StreamController<DocKey> _events = StreamController<DocKey>.broadcast();

  /// Bumped on every document replacement; keys the collection cache.
  final Map<String, int> _tableVersions = {};

  // Read caches keyed on document identity. Safe because every write replaces
  // the document object.
  Expando<_Cached<Map<String, Object?>?>> _docCache = Expando('docCache');
  Expando<Map<String, List<HLC>>> _listIdCache = Expando('listIdCache');
  Expando<Map<String, List<TextDeltaSegment>>> _textDeltaCache = Expando('textDeltaCache');
  final Map<String, ({int version, List<Map<String, Object?>> items})> _collectionCache = {};

  int _txDepth = 0;
  Map<String, Set<String>> _txTouched = {};
  bool _txGlobal = false;
  int _suppressUndo = 0;

  // Persistence queue: documents touched since the last flush, and whether the
  // pending queue changed. Read at flush time.
  Map<String, Set<String>> _docQueue = {};
  bool _pendingDirty = false;
  Timer? _persistTimer;
  Timer? _retryTimer;
  late Duration _retryDelay = _baseRetryDelay;
  int _failures = 0;
  Object? _lastFailure;
  Future<void>? _writeTail;

  bool _disposed = false;
  bool _persistStopped = false;
  bool _hydrated = false;
  Object? _unavailable;

  /// Documents whose `afterHydrate` threw. Their stored bytes are never
  /// overwritten or deleted by this store.
  final Set<(String, String)> _quarantined = {};

  // --- Plugins ---

  /// Registers [plugin]. Cached reads are dropped, because a read hook may
  /// change what they return.
  void use(StorePlugin plugin) {
    _plugins.use(plugin);
    _invalidateSnapshots();
  }

  /// Removes the plugin called [name].
  void removePlugin(String name) {
    _plugins.remove(name);
    _invalidateSnapshots();
  }

  /// The plugin called [name], or null when there is none or it is not a [T].
  T? getPlugin<T extends StorePlugin>(String name) => _plugins.get<T>(name);

  /// The hook chain, for the sync engine (`beforePush`, `beforePull` and their
  /// `after` hooks run there, not here).
  PluginManager get pluginManager => _plugins;

  // --- Read ---

  /// The resolved document, or null when it does not exist, is tombstoned, or
  /// a `transformDocument` hook hid it. Includes `_table` and `_pk`.
  ///
  /// The map is a frozen deep copy, cached until the document changes.
  Map<String, Object?>? getDocument(String table, String pk) {
    final t = goString(table);
    final p = goString(pk);
    final doc = _getDoc(t, p);
    if (doc == null || doc.tombstone) return null;
    final hit = _docCache[doc];
    if (hit != null) return hit.value;
    final transformed = _plugins.dispatchTransformDocument(t, p, _resolveDocument(doc));
    final frozen = transformed == null ? null : _freeze(transformed)! as Map<String, Object?>;
    _docCache[doc] = _Cached(frozen);
    return frozen;
  }

  /// Every visible document of [table], after the read hooks. The list and
  /// its maps are frozen, and cached until a document of the table changes.
  List<Map<String, Object?>> getCollection(String table) {
    final t = goString(table);
    final version = _tableVersions[t] ?? 0;
    final hit = _collectionCache[t];
    if (hit != null && hit.version == version) return hit.items;
    final result = <Map<String, Object?>>[];
    for (final e in (_state[t] ?? const <String, DocumentState>{}).entries) {
      if (e.value.tombstone) continue;
      final doc = getDocument(t, e.key);
      if (doc != null) result.add(doc);
    }
    final items = List<Map<String, Object?>>.unmodifiable([
      for (final d in _plugins.dispatchTransformCollection(t, result)) _freeze(d)! as Map<String, Object?>,
    ]);
    _collectionCache[t] = (version: version, items: items);
    return items;
  }

  /// The raw state of a document, tombstoned or not. Do not mutate it: a
  /// [TextState] inside is mutable.
  DocumentState? getDocumentState(String table, String pk) => _getDoc(goString(table), goString(pk));

  /// Every table that holds a document.
  Iterable<String> get tables => List.unmodifiable(_state.keys);

  /// The ordered RGA node ids of a list field's visible elements, parallel to
  /// the resolved list. Cached until the document changes.
  List<HLC> getListNodeIds(String table, String pk, String field) {
    final doc = _getDoc(goString(table), goString(pk));
    if (doc == null) return _emptyHlcs;
    final f = goString(field);
    final byField = _listIdCache[doc] ??= {};
    final hit = byField[f];
    if (hit != null) return hit;
    final ls = doc.fields[f]?.listState;
    final ids = ls == null ? _emptyHlcs : List<HLC>.unmodifiable(listNodeIds(ls));
    byField[f] = ids;
    return ids;
  }

  /// The visible text of a text field, or `''`.
  String getText(String table, String pk, String field) {
    final ts = _getDoc(goString(table), goString(pk))?.fields[goString(field)]?.textState;
    return ts == null ? '' : textValue(ts);
  }

  /// The Quill-style attribute runs of a text field. Cached until the document
  /// changes.
  List<TextDeltaSegment> getTextDelta(String table, String pk, String field) {
    final doc = _getDoc(goString(table), goString(pk));
    if (doc == null) return _emptyDelta;
    final f = goString(field);
    final byField = _textDeltaCache[doc] ??= {};
    final hit = byField[f];
    if (hit != null) return hit;
    final ts = doc.fields[f]?.textState;
    final delta = ts == null ? _emptyDelta : List<TextDeltaSegment>.unmodifiable(textDelta(ts));
    byField[f] = delta;
    return delta;
  }

  /// The stable address of the character at a visible [index], which
  /// survives concurrent edits (cursor anchoring).
  TextRef? getTextRefAt(String table, String pk, String field, int index) {
    final ts = _getDoc(goString(table), goString(pk))?.fields[goString(field)]?.textState;
    return ts == null ? null : textRefAt(ts, index);
  }

  /// The current visible index of a stable text address.
  int? getTextIndexOf(String table, String pk, String field, TextRef ref) {
    final ts = _getDoc(goString(table), goString(pk))?.fields[goString(field)]?.textState;
    return ts == null ? null : textIndexOf(ts, ref);
  }

  // --- Local writes ---

  /// Writes an LWW field. Returns the change, or null when a plugin
  /// cancelled it. Throws a [CrdtApplyError] when the field holds another
  /// type, and an [ArgumentError] when [value] is not JSON.
  ChangeRecord? setField(String table, String pk, String field, Object? value) {
    _checkWritable();
    _assertPendingCapacity();
    final v = goJsonCopy(value);
    final hlc = clock.now();
    return _commitLocal(
      ChangeRecord(
        table: goString(table),
        pk: goString(pk),
        field: goString(field),
        crdtType: CrdtType.lww,
        hlc: hlc,
        nodeId: hlc.node,
        value: JsonValue(v),
      ),
      v,
    );
  }

  /// Increments a counter by [delta]. The change carries this node's
  /// cumulative totals, which merge max-per-node, so redelivery is
  /// idempotent, as in Go.
  ChangeRecord? incrementCounter(String table, String pk, String field, [int delta = 1]) =>
      _counter(table, pk, field, delta, 0, delta);

  /// Decrements a counter by [delta].
  ChangeRecord? decrementCounter(String table, String pk, String field, [int delta = 1]) =>
      _counter(table, pk, field, 0, delta, -delta);

  ChangeRecord? _counter(String table, String pk, String field, int inc, int dec, int eventValue) {
    _checkWritable();
    _assertPendingCapacity();
    final t = goString(table);
    final p = goString(pk);
    final f = goString(field);
    final hlc = clock.now();
    final cs = _getDoc(t, p)?.fields[f]?.counterState;
    final totals = CounterDelta((cs?.inc[hlc.node] ?? 0) + inc, (cs?.dec[hlc.node] ?? 0) + dec);
    return _commitLocal(
      ChangeRecord(table: t, pk: p, field: f, crdtType: CrdtType.counter, hlc: hlc, nodeId: hlc.node, counterDelta: totals),
      eventValue,
    );
  }

  /// Adds [elements] to a set field.
  ChangeRecord? addToSet(String table, String pk, String field, List<Object?> elements) {
    _checkWritable();
    _assertPendingCapacity();
    final els = [for (final e in elements) goJsonCopy(e)];
    final hlc = clock.now();
    return _commitLocal(
      ChangeRecord(
        table: goString(table),
        pk: goString(pk),
        field: goString(field),
        crdtType: CrdtType.set,
        hlc: hlc,
        nodeId: hlc.node,
        setOp: SetOperation(SetOpType.add, els),
      ),
      els,
    );
  }

  /// Removes [elements] from a set field, naming the observed tags so the
  /// remove is exact everywhere.
  ///
  /// Each element is sent as every stored key whose decoded value equals it
  /// ([keysForElement]), verbatim as a [RawJson], so a key another engine
  /// wrote non-canonically is removed on the server too. The op carries the
  /// tags observed under those keys. An element with no stored key is sent as
  /// its value. When no element has a key the op has no tags: a legacy remove,
  /// which Go applies to tags older than the change.
  ChangeRecord? removeFromSet(String table, String pk, String field, List<Object?> elements) {
    _checkWritable();
    _assertPendingCapacity();
    final t = goString(table);
    final p = goString(pk);
    final f = goString(field);
    final els = [for (final e in elements) goJsonCopy(e)];
    final hlc = clock.now();
    final setState = _getDoc(t, p)?.fields[f]?.setState;
    final wire = <Object?>[];
    final tags = <OrSetTag>[];
    final seenTags = <String>{};
    for (final el in els) {
      final keys = setState == null ? const <String>[] : keysForElement(setState, el);
      var named = false;
      for (final k in keys) {
        if (!_isJson(k)) continue; // a non-JSON key cannot travel on the wire
        named = true;
        wire.add(RawJson(k));
        for (final tag in setState!.entries[k]!) {
          if (seenTags.add(tagKey(tag))) tags.add(tag);
        }
      }
      if (!named) wire.add(el);
    }
    return _commitLocal(
      ChangeRecord(
        table: t,
        pk: p,
        field: f,
        crdtType: CrdtType.set,
        hlc: hlc,
        nodeId: hlc.node,
        setOp: SetOperation(SetOpType.remove, wire, tags: tags),
      ),
      els,
    );
  }

  static bool _isJson(String s) {
    try {
      jsonDecode(s);
      return true;
    } on FormatException {
      return false;
    }
  }

  /// Inserts [value] into a list field after [afterId], or at the head when
  /// it is null.
  ChangeRecord? insertIntoList(String table, String pk, String field, Object? value, {HLC? afterId}) {
    _checkWritable();
    _assertPendingCapacity();
    final v = goJsonCopy(value);
    final hlc = clock.now();
    return _commitLocal(
      ChangeRecord(
        table: goString(table),
        pk: goString(pk),
        field: goString(field),
        crdtType: CrdtType.list,
        hlc: hlc,
        nodeId: hlc.node,
        listOp: ListOperation(ListOpType.insert, nodeId: hlc, parentId: afterId ?? HLC.zero, value: JsonValue(v)),
      ),
      v,
    );
  }

  /// Deletes the list node [nodeId].
  ChangeRecord? deleteFromList(String table, String pk, String field, HLC nodeId) {
    _checkWritable();
    _assertPendingCapacity();
    final hlc = clock.now();
    return _commitLocal(
      ChangeRecord(
        table: goString(table),
        pk: goString(pk),
        field: goString(field),
        crdtType: CrdtType.list,
        hlc: hlc,
        nodeId: hlc.node,
        listOp: ListOperation(ListOpType.delete, nodeId: nodeId),
      ),
      nodeId,
    );
  }

  TextState _textStateOf(String t, String p, String f) {
    final fs = _getDoc(t, p)?.fields[f];
    return fs?.type == CrdtType.text && fs?.textState != null ? fs!.textState! : newTextState();
  }

  ChangeRecord? _commitText(String t, String p, String f, HLC hlc, TextOperation op, TextState next, Object? value) =>
      _commitLocal(
        ChangeRecord(table: t, pk: p, field: f, crdtType: CrdtType.text, hlc: hlc, nodeId: hlc.node, textOp: op),
        value,
        prebuilt: (existing) {
          final keep = existing != null && existing.hlc.isAfter(hlc);
          return FieldState(
            type: CrdtType.text,
            hlc: keep ? existing.hlc : hlc,
            nodeId: keep ? existing.nodeId : hlc.node,
            textState: next,
          );
        },
      );

  /// Inserts [content] at visible character [index]. Sequential typing
  /// coalesces into one origin. Throws an [ArgumentError] for empty content
  /// and a [RangeError] when there is no character before [index].
  ChangeRecord? insertText(String table, String pk, String field, int index, String content) {
    _checkWritable();
    _assertPendingCapacity();
    if (content.isEmpty) throw ArgumentError.value(content, 'content', 'crdt: empty text insert');
    if (index < 0) throw RangeError.value(index, 'index', 'crdt: negative text index');
    final t = goString(table);
    final p = goString(pk);
    final f = goString(field);
    final hlc = clock.now();
    final current = _textStateOf(t, p, f);
    TextRef? ref;
    if (index > 0) {
      ref = textRefAt(current, index - 1) ?? (throw RangeError('crdt: no text character at index ${index - 1}'));
    }
    // The builders apply the op as they build it, so build against a clone and
    // keep the clone as the new state.
    final next = current.clone();
    final op = textInsert(next, ref, content, hlc.node, hlc);
    return _commitText(t, p, f, hlc, op, next, content);
  }

  /// Deletes [length] visible characters from [index].
  ChangeRecord? deleteText(String table, String pk, String field, int index, int length) {
    _checkWritable();
    _assertPendingCapacity();
    final t = goString(table);
    final p = goString(pk);
    final f = goString(field);
    final hlc = clock.now();
    final current = _textStateOf(t, p, f);
    final ref = textRefAt(current, index) ?? (throw RangeError('crdt: no text character at index $index'));
    final next = current.clone();
    final op = textDeleteOp(next, ref, length);
    return _commitText(t, p, f, hlc, op, next, length);
  }

  /// Sets formatting [attrs] on [length] visible characters from [index]. A
  /// null value clears the attribute.
  ChangeRecord? formatText(String table, String pk, String field, int index, int length, Map<String, Object?> attrs) {
    _checkWritable();
    _assertPendingCapacity();
    final t = goString(table);
    final p = goString(pk);
    final f = goString(field);
    final a = goJsonCopy(attrs)! as Map<String, Object?>;
    final hlc = clock.now();
    final current = _textStateOf(t, p, f);
    final ref = textRefAt(current, index) ?? (throw RangeError('crdt: no text character at index $index'));
    final next = current.clone();
    final op = textFormat(next, ref, length, a, hlc.node, hlc);
    return _commitText(t, p, f, hlc, op, next, a);
  }

  /// Replaces a text field's visible text with [value] using a common prefix
  /// and suffix diff (Go `TextState.SetString`), and returns the changes: a
  /// delete, an insert, both, or none. Each is recorded for undo on its own.
  List<ChangeRecord> setText(String table, String pk, String field, String value) {
    _checkWritable();
    _assertPendingCapacity();
    final t = goString(table);
    final p = goString(pk);
    final f = goString(field);
    _assertPendingCapacity(_textOpsFor(t, p, f, value));
    return transact(() {
      final working = _textStateOf(t, p, f).clone();
      final clocks = <HLC>[];
      final ops = textSetString(working, value, nodeId, () {
        final h = clock.now();
        clocks.add(h);
        return h;
      });
      final out = <ChangeRecord>[];
      for (var i = 0; i < ops.length; i++) {
        final h = clocks[i];
        final c = _commitLocal(
          ChangeRecord(table: t, pk: p, field: f, crdtType: CrdtType.text, hlc: h, nodeId: h.node, textOp: ops[i]),
          value,
        );
        if (c != null) out.add(c);
      }
      return out;
    });
  }

  /// Writes [value] at the dotted [path] inside a nested document field.
  ChangeRecord? setDocumentField(String table, String pk, String field, String path, Object? value) {
    _checkWritable();
    _assertPendingCapacity();
    final v = goJsonCopy(value);
    final hlc = clock.now();
    return _commitLocal(
      ChangeRecord(
        table: goString(table),
        pk: goString(pk),
        field: goString(field),
        crdtType: CrdtType.document,
        hlc: hlc,
        nodeId: hlc.node,
        value: JsonValue({'path': goString(path), 'value': v}),
      ),
      v,
    );
  }

  /// Deletes the dotted [path], and the paths under it, inside a nested
  /// document field.
  ChangeRecord? deleteDocumentField(String table, String pk, String field, String path) {
    _checkWritable();
    _assertPendingCapacity();
    final hlc = clock.now();
    return _commitLocal(
      ChangeRecord(
        table: goString(table),
        pk: goString(pk),
        field: goString(field),
        crdtType: CrdtType.document,
        hlc: hlc,
        nodeId: hlc.node,
        tombstone: true,
        value: JsonValue({'path': goString(path)}),
      ),
      null,
    );
  }

  /// Tombstones a document and returns the change: `field: ""`,
  /// `crdtType: CrdtType.lww`, `tombstone: true`, which Go validation
  /// accepts. Plugins are not consulted, as in crdt-js.
  ChangeRecord deleteDocument(String table, String pk) {
    _checkWritable();
    _assertPendingCapacity();
    final t = goString(table);
    final p = goString(pk);
    _checkNotQuarantined(t, p);
    final hlc = clock.now();
    final change = ChangeRecord(
      table: t,
      pk: p,
      field: '',
      crdtType: CrdtType.lww,
      hlc: hlc,
      nodeId: hlc.node,
      tombstone: true,
    );
    final previous = _getDoc(t, p);
    _applyChangeInternal(change);
    _recordUndo(change, null, previousDocument: previous);
    _enqueuePending(change);
    _persistWrite(t, p);
    _notifyListeners(t, p);
    return change;
  }

  /// Runs a local change through `beforeWrite`, applies it, records it for
  /// undo, queues it, persists and notifies, then runs `afterWrite`.
  ChangeRecord? _commitLocal(
    ChangeRecord change,
    Object? value, {
    FieldState Function(FieldState? existing)? prebuilt,
  }) {
    _checkNotQuarantined(change.table, change.pk);
    final previous = _captureFieldState(change.table, change.pk, change.field);
    final allowed = _plugins.dispatchBeforeWrite(WriteEvent(
      table: change.table,
      pk: change.pk,
      field: change.field,
      crdtType: change.crdtType,
      value: value,
      change: change,
      previousState: previous,
    ));
    if (allowed == null) return null;
    final c = _normalizeKeys(allowed.change);
    final sameKey = c.table == change.table && c.pk == change.pk && c.field == change.field;
    _applyChangeInternal(c, prebuilt: identical(allowed.change, change) ? prebuilt : null);
    _recordUndo(c, sameKey ? previous : _captureFieldState(c.table, c.pk, c.field));
    _enqueuePending(c);
    _persistWrite(c.table, c.pk);
    _notifyListeners(c.table, c.pk);
    _plugins.dispatchAfterWrite(allowed);
    return c;
  }

  void _recordUndo(ChangeRecord c, FieldState? previous, {DocumentState? previousDocument}) {
    if (_suppressUndo == 0) _undo.record(c, previous, previousDocument: previousDocument);
  }

  // --- Reconciliation and recovery (Dart additions) ---

  /// Reconciles [field] toward [value] with the smallest set of local ops for
  /// its type, and returns the changes it made. Used by undo and by
  /// forge_client_grove to turn a REST-shaped write into CRDT ops.
  List<ChangeRecord> reconcileField(String table, String pk, String field, CrdtType type, Object? value) {
    _checkWritable();
    final t = goString(table);
    final p = goString(pk);
    final f = goString(field);
    final want0 = goJsonCopy(value);
    final current = getDocumentState(t, p)?.fields[f];
    // Plan every step first, so the queue bound is checked for all of them
    // before anything changes.
    final steps = <List<ChangeRecord> Function()>[];
    var needed = 0;
    void step(ChangeRecord? Function() op) {
      needed++;
      steps.add(() {
        final c = op();
        return c == null ? const [] : [c];
      });
    }

    switch (type) {
      case CrdtType.lww || CrdtType.none:
        if (current == null || !jsonDeepEquals(resolveFieldValue(current), want0)) step(() => setField(t, p, f, want0));
      case CrdtType.counter:
        final target = (want0 as num?)?.toInt() ?? 0;
        final now = current?.counterState == null ? 0 : counterValue(current!.counterState!);
        if (target > now) step(() => incrementCounter(t, p, f, target - now));
        if (target < now) step(() => decrementCounter(t, p, f, now - target));
      case CrdtType.set:
        final want = (want0 as List<Object?>?) ?? const [];
        final have = current?.setState == null ? const <Object?>[] : setElements(current!.setState!);
        final adds = [
          for (final w in want)
            if (!have.any((h) => jsonDeepEquals(h, w))) w,
        ];
        final removes = [
          for (final h in have)
            if (!want.any((w) => jsonDeepEquals(h, w))) h,
        ];
        if (removes.isNotEmpty) step(() => removeFromSet(t, p, f, removes));
        if (adds.isNotEmpty) step(() => addToSet(t, p, f, adds));
      case CrdtType.list:
        final want = (want0 as List<Object?>?) ?? const [];
        final ids = current?.listState == null ? const <HLC>[] : listNodeIds(current!.listState!);
        final have = current?.listState == null ? const <Object?>[] : listElements(current!.listState!);
        var prefix = 0;
        while (prefix < have.length && prefix < want.length && jsonDeepEquals(have[prefix], want[prefix])) {
          prefix++;
        }
        var suffix = 0;
        while (suffix < have.length - prefix &&
            suffix < want.length - prefix &&
            jsonDeepEquals(have[have.length - 1 - suffix], want[want.length - 1 - suffix])) {
          suffix++;
        }
        for (var i = prefix; i < have.length - suffix; i++) {
          final id = ids[i];
          step(() => deleteFromList(t, p, f, id));
        }
        HLC? after = prefix == 0 ? null : ids[prefix - 1];
        for (var i = prefix; i < want.length - suffix; i++) {
          final v = want[i];
          step(() {
            final c = insertIntoList(t, p, f, v, afterId: after);
            if (c != null) after = c.listOp!.nodeId.isZero ? c.hlc : c.listOp!.nodeId;
            return c;
          });
        }
      case CrdtType.text:
        final text = (want0 as String?) ?? '';
        needed += _textOpsFor(t, p, f, text);
        steps.add(() => setText(t, p, f, text));
      case CrdtType.document:
        final want = _flattenPaths((want0 as Map<String, Object?>?) ?? const {});
        final have = current?.docState == null
            ? const <String, Object?>{}
            : {
                for (final e in current!.docState!.fields.entries) e.key: resolveFieldValue(e.value),
              };
        for (final e in want.entries) {
          if (!have.containsKey(e.key) || !jsonDeepEquals(have[e.key], e.value)) {
            step(() => setDocumentField(t, p, f, e.key, e.value));
          }
        }
        for (final path in have.keys) {
          if (!want.containsKey(path)) step(() => deleteDocumentField(t, p, f, path));
        }
    }
    _assertPendingCapacity(needed);
    return transact(() => [for (final s in steps) ...s()]);
  }

  /// How many changes [setText] would queue to reach [value].
  int _textOpsFor(String t, String p, String f, String value) =>
      textSetString(_textStateOf(t, p, f).clone(), value, nodeId, () => HLC.zero).length;

  static Map<String, Object?> _flattenPaths(Map<String, Object?> m, [String prefix = '']) {
    final out = <String, Object?>{};
    for (final e in m.entries) {
      final path = prefix.isEmpty ? e.key : '$prefix.${e.key}';
      final v = e.value;
      if (v is Map<String, Object?> && v.isNotEmpty) {
        out.addAll(_flattenPaths(v, path));
      } else {
        out[path] = v;
      }
    }
    return out;
  }

  /// Gives a pending change a fresh clock (and the clock's current node id)
  /// after a clock correction. Returns the new change, or null when the
  /// change cannot be re-stamped or the key is unknown. List and text
  /// changes carry HLC-based identities that other ops may already anchor
  /// to; counter changes carry per-node cumulative totals that a node id
  /// change would split. Those surface as rejected instead.
  ChangeRecord? restampPending(String key) {
    _checkWritable();
    final index = _pending.indexWhere((p) => p.key == key);
    if (index < 0) return null;
    final entry = _pending[index];
    final old = entry.change;
    if (old.crdtType == CrdtType.list || old.crdtType == CrdtType.text || old.crdtType == CrdtType.counter) return null;
    final hlc = clock.now();
    final fresh = old.copyWith(hlc: hlc, nodeId: hlc.node);
    final doc = _docOrEmpty(old.table, old.pk);
    DocumentState next = doc;
    if (_isRecordDelete(old)) {
      if (doc.tombstone && doc.tombstoneHlc == old.hlc) next = doc.copyWith(tombstoneHlc: hlc);
    } else {
      final fs = doc.fields[old.field];
      if (fs != null) next = _withField(doc, old.field, _restampField(fs, old, hlc));
    }
    if (!identical(next, doc)) _setDocument(old.table, old.pk, next);
    _pending[index] = entry.copyWith(change: fresh, clearRejection: true, restamps: entry.restamps + 1);
    if (old.crdtType == CrdtType.set && old.setOp?.op == SetOpType.add) {
      // A later pending remove that observed the add names its old tag. Point
      // it at the new one, for every element it covers, or the server would
      // keep the element the user removed.
      final oldTag = tagKey(OrSetTag(old.nodeId, old.hlc));
      final newTag = OrSetTag(hlc.node, hlc);
      for (var i = index + 1; i < _pending.length; i++) {
        final c = _pending[i].change;
        final op = c.setOp;
        if (c.table != old.table || c.pk != old.pk || c.field != old.field) continue;
        if (op == null || op.op != SetOpType.remove || !op.tags.any((t) => tagKey(t) == oldTag)) continue;
        _pending[i] = _pending[i].copyWith(
          change: c.copyWith(
            setOp: SetOperation(
              op.op,
              op.elements,
              tags: [for (final t in op.tags) tagKey(t) == oldTag ? newTag : t],
            ),
          ),
        );
      }
    }
    _persistWrite(old.table, old.pk);
    _notifyListeners(old.table, old.pk);
    return fresh;
  }

  FieldState _restampField(FieldState fs, ChangeRecord old, HLC hlc) {
    final stampedHere = fs.hlc == old.hlc;
    final fieldHlc = stampedHere ? hlc : fs.hlc;
    final fieldNode = stampedHere ? hlc.node : fs.nodeId;
    switch (old.crdtType) {
      case CrdtType.set when old.setOp?.op == SetOpType.add:
        final oldTag = tagKey(OrSetTag(old.nodeId, old.hlc));
        final newTag = OrSetTag(hlc.node, hlc);
        final entries = {
          for (final e in fs.setState!.entries.entries) e.key: [for (final t in e.value) tagKey(t) == oldTag ? newTag : t],
        };
        // A local remove of the add already marked the old tag removed; move
        // the mark to the new tag, as the rewritten pending remove will on the
        // server.
        final newKey = tagKey(newTag);
        final removed = <String, bool>{
          for (final e in fs.setState!.removed.entries)
            if (e.key == oldTag)
              newKey: e.value
            else if (e.key.endsWith('|$oldTag'))
              '${e.key.substring(0, e.key.length - oldTag.length)}$newKey': e.value
            else
              e.key: e.value,
        };
        return setFieldState(OrSetState(entries: entries, removed: removed), fieldHlc, fieldNode);
      case CrdtType.document:
        final path = (old.value!.value! as Map<String, Object?>)['path']! as String;
        final inner = fs.docState!.fields[path];
        final fields = {...fs.docState!.fields};
        if (inner != null && inner.hlc == old.hlc) fields[path] = inner.copyWith(hlc: hlc, nodeId: hlc.node);
        return documentFieldState(DocumentCrdtState(fields), fieldHlc, fieldNode);
      default:
        return stampedHere ? fs.copyWith(hlc: hlc, nodeId: hlc.node) : fs;
    }
  }

  /// Marks a pending change as refused by the server.
  void markRejected(String key, PendingRejection rejection) {
    _checkWritable();
    final i = _pending.indexWhere((p) => p.key == key);
    if (i < 0) return;
    _pending[i] = _pending[i].copyWith(rejection: rejection);
    _persistPending();
    _notifyGlobalSoon();
  }

  /// Makes a rejected change pushable again.
  void retryRejected(String key) {
    _checkWritable();
    final i = _pending.indexWhere((p) => p.key == key);
    if (i < 0 || !_pending[i].isRejected) return;
    _pending[i] = _pending[i].copyWith(clearRejection: true);
    _persistPending();
    _notifyGlobalSoon();
  }

  /// Removes a pending change without pushing it and returns it. The local
  /// state still contains its effect: the caller drops the field
  /// ([dropField]) and re-pulls it, then this store re-applies any later
  /// pending changes on that field (see `SyncEngine.discardRejected`).
  PendingChange? discardPending(String key) {
    _checkWritable();
    final i = _pending.indexWhere((p) => p.key == key);
    if (i < 0) return null;
    final removed = _pending.removeAt(i);
    _persistPending();
    _notifyGlobalSoon();
    return removed;
  }

  /// Forgets one field's local state.
  void dropField(String table, String pk, String field) {
    _checkWritable();
    final t = goString(table);
    final p = goString(pk);
    final f = goString(field);
    final doc = _getDoc(t, p);
    if (doc == null || !doc.fields.containsKey(f)) return;
    _setDocument(
      t,
      p,
      doc.copyWith(fields: {
        for (final e in doc.fields.entries)
          if (e.key != f) e.key: e.value,
      }),
    );
    _persistDocument(t, p);
    _notifyListeners(t, p);
  }

  /// Removes a document from memory and storage.
  void dropDocument(String table, String pk) {
    _checkWritable();
    final t = goString(table);
    final p = goString(pk);
    if (!_removeDocument(t, p)) return;
    _persistDocument(t, p);
    _notifyListeners(t, p);
  }

  /// The highest HLC in the replica, over documents and pending changes.
  HLC get maxHlc {
    var max = HLC.zero;
    for (final docs in _state.values) {
      for (final d in docs.values) {
        if (d.tombstoneHlc.isAfter(max)) max = d.tombstoneHlc;
        for (final f in d.fields.values) {
          if (f.hlc.isAfter(max)) max = f.hlc;
        }
      }
    }
    for (final p in _pending) {
      if (p.change.hlc.isAfter(max)) max = p.change.hlc;
    }
    return max;
  }

  // --- Undo / redo ---

  /// Whether [undo] has an entry.
  bool get canUndo => _undo.canUndo;

  /// Whether [redo] has an entry.
  bool get canRedo => _undo.canRedo;

  /// Undoes the last local write. Returns false when there is nothing to undo
  /// or the undo is refused.
  ///
  /// Undo emits compensating changes, so it reaches the server and every
  /// other replica: the field is reconciled toward its value before the write
  /// ([reconcileField]; null when the field did not exist), and those changes
  /// are queued for push but not recorded for undo. A `formatText` is undone
  /// by a format over the same characters (the original spans) with each
  /// attribute set back to its value at the first character, or null.
  ///
  /// Because undo reconciles the whole field toward its earlier value, it also
  /// overrides concurrent edits to that field made since, as crdt-js undo
  /// does when it restores the earlier field state.
  ///
  /// Undoing a `deleteDocument` whose tombstone is still pending drops it from
  /// the queue and restores the document's earlier tombstone state. Once the
  /// tombstone was pushed (or a later tombstone superseded it) undo returns
  /// false and discards the entry, because server tombstones are sticky.
  ///
  /// Throws [PendingQueueFullError] (leaving the entry in place) when the
  /// queue is full and the store throws on overflow.
  bool undo() {
    _checkWritable();
    if (!_undo.canUndo) return false;
    _assertPendingCapacity();
    final entry = _undo.popUndo()!;
    final c = entry.change;
    if (_isRecordDelete(c)) return _undoDelete(entry);
    _suppressUndo++;
    try {
      transact(() => _compensate(entry));
    } on Object {
      _undo.pushUndo(entry);
      rethrow;
    } finally {
      _suppressUndo--;
    }
    _undo.pushRedo(entry);
    return true;
  }

  void _compensate(UndoEntry entry) {
    final c = entry.change;
    final prev = entry.previousState;
    final op = c.textOp;
    if (c.crdtType == CrdtType.text && op != null && op.op == TextOpType.format) {
      if (op.spans.isEmpty) return;
      final first = op.spans.first;
      final ts = prev?.textState;
      _formatSpans(c.table, c.pk, c.field, op.spans, {
        for (final name in op.attrs.keys) name: ts == null ? null : _attrAt(ts, first, name),
      });
      return;
    }
    reconcileField(c.table, c.pk, c.field, c.crdtType, prev == null ? null : resolveFieldValue(prev));
  }

  /// The value of attribute [name] at the first character of [span] in [s].
  static Object? _attrAt(TextState s, TextSpan span, String name) {
    for (final f in s.frags[textOriginKey(span.origin)] ?? const <TextFragment>[]) {
      if (span.start >= f.start && span.start < f.start + f.length) return goJsonCopy(f.attrs[name]?.value.value);
    }
    return null;
  }

  ChangeRecord? _formatSpans(String t, String p, String f, List<TextSpan> spans, Map<String, Object?> attrs) {
    _assertPendingCapacity();
    final hlc = clock.now();
    final op = TextOperation(
      TextOpType.format,
      spans: spans,
      attrs: {for (final e in attrs.entries) e.key: JsonValue(e.value)},
    );
    return _commitLocal(
      ChangeRecord(table: t, pk: p, field: f, crdtType: CrdtType.text, hlc: hlc, nodeId: hlc.node, textOp: op),
      attrs,
    );
  }

  bool _undoDelete(UndoEntry entry) {
    final c = entry.change;
    final i = _pending.indexWhere((p) => p.key == pendingKey(c));
    final doc = _getDoc(c.table, c.pk);
    if (i < 0 || doc == null || !doc.tombstone || doc.tombstoneHlc != c.hlc) return false;
    _pending.removeAt(i);
    _markPending();
    final prev = entry.previousDocument;
    if (prev == null && doc.fields.isEmpty) {
      _removeDocument(c.table, c.pk);
    } else {
      _setDocument(
        c.table,
        c.pk,
        doc.copyWith(tombstone: prev?.tombstone ?? false, tombstoneHlc: prev?.tombstoneHlc ?? HLC.zero),
      );
    }
    _persistWrite(c.table, c.pk);
    _notifyListeners(c.table, c.pk);
    _undo.pushRedo(entry);
    return true;
  }

  /// Redoes the last undone write: the original intent is applied again with
  /// new clocks through the same mutators. Returns false when there is
  /// nothing to redo.
  bool redo() {
    _checkWritable();
    if (!_undo.canRedo) return false;
    _assertPendingCapacity();
    final entry = _undo.redo()!;
    // redo() put the entry back on the undo stack; it is replaced below by one
    // holding the replayed change.
    _undo.popUndo();
    final c = entry.change;
    final isDelete = _isRecordDelete(c);
    final before = isDelete ? _getDoc(c.table, c.pk) : null;
    ChangeRecord? replayed;
    _suppressUndo++;
    try {
      replayed = transact(() => _replay(entry));
    } on Object {
      _undo.pushRedo(entry);
      rethrow;
    } finally {
      _suppressUndo--;
    }
    _undo.pushUndo(UndoEntry(
      change: replayed ?? c,
      previousState: entry.previousState,
      previousDocument: isDelete ? before : entry.previousDocument,
      timestamp: DateTime.now().millisecondsSinceEpoch,
    ));
    return true;
  }

  ChangeRecord? _replay(UndoEntry entry) {
    final c = entry.change;
    final prev = entry.previousState;
    if (_isRecordDelete(c)) return deleteDocument(c.table, c.pk);
    switch (c.crdtType) {
      case CrdtType.lww || CrdtType.none:
        return setField(c.table, c.pk, c.field, c.value?.value);
      case CrdtType.counter:
        final d = c.counterDelta!;
        final base = prev?.counterState;
        final inc = d.inc - (base?.inc[c.nodeId] ?? 0);
        final dec = d.dec - (base?.dec[c.nodeId] ?? 0);
        _assertPendingCapacity((inc > 0 ? 1 : 0) + (dec > 0 ? 1 : 0));
        ChangeRecord? last;
        if (inc > 0) last = incrementCounter(c.table, c.pk, c.field, inc);
        if (dec > 0) last = decrementCounter(c.table, c.pk, c.field, dec);
        return last;
      case CrdtType.set:
        final op = c.setOp!;
        final elements = [for (final e in op.elements) e is RawJson ? jsonDecode(e.json) : e];
        return op.op == SetOpType.add
            ? addToSet(c.table, c.pk, c.field, elements)
            : removeFromSet(c.table, c.pk, c.field, elements);
      case CrdtType.document:
        final payload = c.value?.value;
        final path = payload is Map<String, Object?> ? payload['path'] : null;
        if (path is! String) return null;
        return c.tombstone
            ? deleteDocumentField(c.table, c.pk, c.field, path)
            : setDocumentField(c.table, c.pk, c.field, path, (payload! as Map<String, Object?>)['value']);
      case CrdtType.text when c.textOp?.op == TextOpType.format:
        final op = c.textOp!;
        return _formatSpans(c.table, c.pk, c.field, op.spans, {
          for (final e in op.attrs.entries) e.key: e.value.value,
        });
      case CrdtType.list || CrdtType.text:
        // Node and origin ids change on every insert, so a list or text write
        // is replayed as the value it produced.
        final out = reconcileField(c.table, c.pk, c.field, c.crdtType, resolveFieldValue(applyChange(prev, c)));
        return out.isEmpty ? null : out.last;
    }
  }

  // --- Sync ---

  /// Applies remote changes from a pull or a stream and returns the documents
  /// that changed.
  ///
  /// Each change folds through [applyChange], exactly as Go folds it. A change
  /// that throws ([CrdtApplyError], [CrdtMergeError], [StateError],
  /// [FormatException]) is reported through `onError` with that change and
  /// skipped; the rest of the batch still applies. A `beforeMerge` hook that
  /// returns null, or throws, skips its change.
  Set<DocKey> applyChanges(List<ChangeRecord> changes) {
    _checkWritable();
    final affected = <DocKey>{};
    for (final raw in changes) {
      try {
        final change = _normalizeKeys(raw);
        final local = _getDoc(change.table, change.pk)?.fields[change.field];
        final event = MergeEvent(
          table: change.table,
          pk: change.pk,
          field: change.field,
          local: local,
          remote: change,
          conflictDetected: local != null,
        );
        final allowed = _plugins.dispatchBeforeMerge(event);
        if (allowed == null) continue;
        final c = _normalizeKeys(allowed);
        _applyChangeInternal(c);
        affected.add((table: c.table, pk: c.pk));
        if (_quarantined.contains((c.table, c.pk))) {
          _reportStorage(StateError('crdt: document ${c.table}/${c.pk} is quarantined; a remote change is held in memory only'));
        }
        // The merge installed a new document object, so re-read the result.
        final result = _getDoc(c.table, c.pk)?.fields[c.field];
        event.result = result;
        event.winnerNodeId = result?.nodeId;
        _plugins.dispatchAfterMerge(event);
      } on CrdtApplyError catch (e) {
        _reportChange(e, raw);
      } on CrdtMergeError catch (e) {
        _reportChange(e, raw);
      } on StateError catch (e) {
        _reportChange(e, raw);
      } on FormatException catch (e) {
        _reportChange(e, raw);
      }
    }
    for (final k in affected) {
      _markDocument(k.table, k.pk);
      _notifyListeners(k.table, k.pk);
    }
    _requestPersist();
    return affected;
  }

  /// The pushable pending changes, oldest first: rejected ones are left out
  /// until [retryRejected]. A new list each call.
  List<ChangeRecord> getPendingChanges() => [
        for (final p in _pending)
          if (!p.isRejected) p.change,
      ];

  /// Every pending change, rejected ones included, oldest first.
  List<PendingChange> get pending => List.unmodifiable(_pending);

  /// Removes pushed changes from the queue. Pass the records that were pushed
  /// to clear only those (matched by [pendingKey]); clearing everything would
  /// also discard writes made while the push was in flight. With no argument
  /// the whole queue is cleared.
  void clearPendingChanges([Iterable<ChangeRecord>? pushed]) {
    _checkWritable();
    if (pushed == null) {
      _pending.clear();
    } else {
      final keys = {for (final c in pushed) pendingKey(c)};
      _pending.removeWhere((p) => keys.contains(p.key));
    }
    _persistPending();
  }

  /// How many pending changes can be pushed.
  int get pendingCount => _pending.where((p) => !p.isRejected).length;

  /// How many pending changes the server refused.
  int get rejectedCount => _pending.where((p) => p.isRejected).length;

  /// Registers [handler] for changes the pending bound drops. Returns a
  /// function that unregisters it. Overflow means unsynced local work was
  /// discarded: surface it rather than swallow it.
  void Function() onPendingOverflow(void Function(List<ChangeRecord> dropped) handler) {
    _overflowHandlers.add(handler);
    return () => _overflowHandlers.remove(handler);
  }

  /// Throws [PendingQueueFullError] when the store throws on overflow and the
  /// bound is reached. Every mutator calls this first, before it reads the
  /// clock, so a refused write leaves no trace.
  void _assertPendingCapacity([int changes = 1]) {
    if (_throwOnOverflow && _maxPendingChanges > 0 && _pending.length + changes > _maxPendingChanges) {
      throw PendingQueueFullError(_maxPendingChanges);
    }
  }

  /// Queues [change], enforcing the bound.
  void _enqueuePending(ChangeRecord change) {
    _assertPendingCapacity();
    _pending.add(PendingChange(change));
    _markPending();
    _evictOverflow(keepNewest: true);
  }

  /// Trims the queue to the bound and tells the overflow handlers. Returns
  /// whether anything was dropped.
  ///
  /// The bound counts every pending change. Eviction drops the oldest
  /// pushable changes before any rejected one and, with [keepNewest], never
  /// the change just queued. A handler that throws is reported through
  /// `onStorageError` and the others still run, so the write that overflowed
  /// is still persisted and notified.
  bool _evictOverflow({required bool keepNewest}) {
    if (_maxPendingChanges <= 0 || _pending.length <= _maxPendingChanges) return false;
    final excess = _pending.length - _maxPendingChanges;
    final dropped = <ChangeRecord>[];
    final scanEnd = keepNewest ? 1 : 0;
    for (var i = 0; i < _pending.length - scanEnd && dropped.length < excess;) {
      if (_pending[i].isRejected) {
        i++;
      } else {
        dropped.add(_pending.removeAt(i).change);
      }
    }
    while (dropped.length < excess) {
      dropped.add(_pending.removeAt(0).change);
    }
    final view = List<ChangeRecord>.unmodifiable(dropped);
    for (final h in List.of(_overflowHandlers)) {
      try {
        h(view);
      } on Object catch (e) {
        _reportStorage(e);
      }
    }
    return true;
  }

  // --- Subscriptions ---

  /// Calls [listener] after every change. Returns a function that
  /// unsubscribes it.
  void Function() subscribe(void Function() listener) {
    _globalListeners.add(listener);
    return () => _globalListeners.remove(listener);
  }

  /// Calls [listener] after a change to one document. Returns a function that
  /// unsubscribes it.
  void Function() subscribeDocument(String table, String pk, void Function() listener) {
    final t = goString(table);
    final p = goString(pk);
    final byPk = _docListeners[t] ??= {};
    final listeners = byPk[p] ??= {};
    listeners.add(listener);
    return () {
      listeners.remove(listener);
      if (listeners.isEmpty && identical(byPk[p], listeners)) {
        byPk.remove(p);
        if (byPk.isEmpty && identical(_docListeners[t], byPk)) _docListeners.remove(t);
      }
    };
  }

  /// Calls [listener] after a change to any document of [table]. Returns a
  /// function that unsubscribes it.
  void Function() subscribeCollection(String table, void Function() listener) {
    final t = goString(table);
    final listeners = _tableListeners[t] ??= {};
    listeners.add(listener);
    return () {
      listeners.remove(listener);
      if (listeners.isEmpty && identical(_tableListeners[t], listeners)) _tableListeners.remove(t);
    };
  }

  /// One event per changed document per notification: after each write, once
  /// per document after [applyChanges], once per document when a [transact]
  /// ends, and once per hydrated or imported document. A broadcast stream,
  /// closed by [dispose].
  Stream<DocKey> get documentChanges => _events.stream;

  // --- Transactions and batches ---

  /// Runs [fn] as one transaction: persistence and notification are held
  /// until the outermost call returns, so a multi-write batch produces one
  /// notification per document instead of one per write. Nested calls join
  /// the outer transaction. [fn] must be synchronous: writes after an await
  /// run outside the transaction.
  T transact<T>(T Function() fn) {
    _txDepth++;
    try {
      return fn();
    } finally {
      _txDepth--;
      if (_txDepth == 0) _flushTransaction();
    }
  }

  void _flushTransaction() {
    final touched = _txTouched;
    _txTouched = {};
    final global = _txGlobal;
    _txGlobal = false;
    _requestPersist();
    for (final e in touched.entries) {
      for (final pk in e.value) {
        _notifyListenersNow(e.key, pk);
      }
    }
    // Document notifications reach the global listeners already.
    if (global && touched.isEmpty) _notifyGlobal();
  }

  /// A [BatchWriter] queuing writes to one document, applied as one
  /// transaction by [BatchWriter.commit].
  BatchWriter batch(String table, String pk) => BatchWriter._(this, table, pk);

  /// Compacts tombstones older than [before] across every document and
  /// returns the units dropped. A zero [before] is a no-op.
  ///
  /// The horizon is a stability floor the caller guarantees: every replica
  /// has seen everything older, and nothing in flight references an older
  /// address. A horizon that does not hold makes replicas diverge. Compacted
  /// documents are persisted and their subscribers notified, in one
  /// transaction.
  int compact(HLC before) {
    _checkWritable();
    if (before.isZero) return 0;
    var dropped = 0;
    transact(() {
      for (final table in _state.entries.toList()) {
        for (final e in table.value.entries.toList()) {
          final r = compactDocument(e.value, before);
          if (identical(r.doc, e.value)) continue;
          dropped += r.dropped;
          _setDocument(table.key, e.key, r.doc);
          _persistDocument(table.key, e.key);
          _notifyListeners(table.key, e.key);
        }
      }
    });
    return dropped;
  }

  // --- Export / import ---

  /// A deep copy of the whole store.
  StateSnapshot exportState() => StateSnapshot(
        version: 1,
        nodeId: nodeId,
        timestamp: DateTime.now().millisecondsSinceEpoch,
        tables: {
          for (final t in _state.entries) t.key: {for (final d in t.value.entries) d.key: _copyDoc(d.value)},
        },
        pending: [for (final p in _pending) _copyPending(p)],
      );

  /// Replaces the whole store with [snapshot] and notifies every listener.
  ///
  /// Differs from crdt-js: a document the snapshot leaves out is deleted from
  /// storage as well as from memory, so a reload does not bring it back.
  void importState(StateSnapshot snapshot) {
    _checkWritable();
    final removed = <DocKey>{
      for (final t in _state.entries)
        for (final pk in t.value.keys) (table: t.key, pk: pk),
    };
    for (final t in _state.keys) {
      _tableVersions[t] = (_tableVersions[t] ?? 0) + 1;
    }
    _state.clear();
    final imported = <DocKey>{};
    for (final t in snapshot.tables.entries) {
      for (final d in t.value.entries) {
        final table = goString(t.key);
        final pk = goString(d.key);
        _setDocument(table, pk, _copyDoc(d.value));
        imported.add((table: table, pk: pk));
      }
    }
    _pending
      ..clear()
      ..addAll([for (final p in snapshot.pending) _copyPending(p)]);
    _markPending();
    for (final k in {...removed, ...imported}) {
      _markDocument(k.table, k.pk);
    }
    _requestPersist();
    if (_txDepth > 0) {
      // Inside a transaction every affected document notifies when it ends.
      for (final k in {...removed, ...imported}) {
        (_txTouched[k.table] ??= {}).add(k.pk);
      }
      _txGlobal = true;
      return;
    }
    for (final l in List.of(_globalListeners)) {
      l();
    }
    for (final byPk in _docListeners.values.toList()) {
      for (final ls in byPk.values.toList()) {
        for (final l in List.of(ls)) {
          l();
        }
      }
    }
    for (final ls in _tableListeners.values.toList()) {
      for (final l in List.of(ls)) {
        l();
      }
    }
    for (final k in {...removed, ...imported}) {
      _emit(k.table, k.pk);
    }
  }

  /// A deep copy of one table's documents.
  Map<String, DocumentState> exportTable(String table) => {
        for (final d in (_state[goString(table)] ?? const <String, DocumentState>{}).entries) d.key: _copyDoc(d.value),
      };

  // --- Hydration and persistence ---

  Future<void> _hydrate() async {
    // Start both loads before awaiting either, and catch each, so a failure
    // of one is never an unhandled error while the other is awaited.
    Object? failure;
    Future<T?> load<T>(Future<T> Function() call) async {
      try {
        return await call();
      } on Object catch (e) {
        failure ??= e;
        return null;
      }
    }

    final stateFuture = load(() => _storage.loadState(onUnreadable: _reportStorage));
    final pendingFuture = load(_storage.loadPendingChanges);
    final loaded = await stateFuture;
    final loadedPending = await pendingFuture;
    final cause = failure;
    if (cause != null) {
      // Fail closed: without the stored queue the store cannot know what is
      // unpushed, and anything it wrote would overwrite it.
      _unavailable = cause;
      _persistStopped = true;
      _stopTimers();
      _reportStorage(cause);
      throw ReplicaUnavailable(cause);
    }

    // No write can have landed: every write before ready throws.
    final hydrated = <DocKey>[];
    for (final t in loaded!.entries) {
      for (final d in t.value.entries) {
        final DocumentState doc;
        try {
          doc = _plugins.dispatchAfterHydrate(t.key, d.key, _normalizeHlcKeys(d.value));
        } on Object catch (e) {
          // A decryptor that failed: the document is not served, and its
          // stored bytes are left alone.
          _quarantined.add((t.key, d.key));
          _reportStorage(e);
          continue;
        }
        _setDocument(t.key, d.key, doc);
        hydrated.add((table: t.key, pk: d.key));
      }
    }
    _pending.addAll(loadedPending!);

    // Never mint a clock below one this replica already issued or holds. This
    // is the replica's own history, so the drift clamp of update() must not
    // apply.
    final max = maxHlc;
    if (!max.isZero) clock.advanceTo(max);

    _hydrated = true;
    // A stored queue longer than the bound (the bound was lowered) is trimmed
    // through the usual overflow path, and the trimmed queue persisted.
    if (_evictOverflow(keepNewest: false)) _persistPending();

    for (final l in List.of(_globalListeners)) {
      // A listener that throws is reported; it does not fail ready.
      try {
        l();
      } on Object catch (e) {
        _reportStorage(e);
      }
    }
    for (final k in hydrated) {
      _emit(k.table, k.pk);
    }
  }

  void _markDocument(String table, String pk) => (_docQueue[table] ??= {}).add(pk);

  void _markPending() => _pendingDirty = true;

  void _stopTimers() {
    _persistTimer?.cancel();
    _persistTimer = null;
    _retryTimer?.cancel();
    _retryTimer = null;
  }

  /// Flushes now (no debounce), arms the debounce timer, or waits for the
  /// outermost [transact] to end.
  void _requestPersist() {
    if (_txDepth > 0 || _persistStopped || !_hydrated) return;
    if (_persistDebounce <= Duration.zero) {
      _drain();
      return;
    }
    _persistTimer ??= Timer(_persistDebounce, () {
      _persistTimer = null;
      _drain();
    });
  }

  void _persistDocument(String table, String pk) {
    _markDocument(table, pk);
    _requestPersist();
  }

  void _persistPending() {
    _markPending();
    _requestPersist();
  }

  /// Persists a document and the pending queue together.
  void _persistWrite(String table, String pk) {
    _markDocument(table, pk);
    _markPending();
    _requestPersist();
  }

  /// Puts what a failed flush held back on the queue, reports the failure,
  /// and arms a retry. The retry waits a debounce tick (at least
  /// [_minRetryDelay]) and doubles per consecutive failure up to
  /// [_maxRetryDelay], so a storage that stays down is not retried in a hot
  /// loop.
  void _requeue(Iterable<(String, String)> docs, {required bool pending, required Object cause}) {
    for (final k in docs) {
      _markDocument(k.$1, k.$2);
    }
    if (pending) _markPending();
    _failures++;
    _lastFailure = cause;
    _reportStorage(cause);
    if (_persistStopped || _retryTimer != null) return;
    _retryTimer = Timer(_retryDelay, () {
      _retryTimer = null;
      _drain();
    });
    final doubled = _retryDelay * 2;
    _retryDelay = doubled > _maxRetryDelay ? _maxRetryDelay : doubled;
  }

  static const Duration _minRetryDelay = Duration(milliseconds: 50);
  static const Duration _maxRetryDelay = Duration(seconds: 30);

  Duration get _baseRetryDelay => _persistDebounce > _minRetryDelay ? _persistDebounce : _minRetryDelay;

  /// Takes everything queued, reading documents now, and hands it to the
  /// write chain.
  ///
  /// A `beforePersist` hook that throws stops the whole flush: nothing
  /// reaches storage, the error is reported, and what the flush held goes
  /// back on the queue for a retry. A broken encryptor therefore blocks
  /// persistence rather than letting plaintext through.
  void _drain() {
    if (_persistStopped || !_hydrated || (_docQueue.isEmpty && !_pendingDirty)) return;
    final queued = _docQueue;
    _docQueue = {};
    final pendingDirty = _pendingDirty;
    _pendingDirty = false;
    final docs = <(String, String), DocumentState?>{};
    try {
      for (final e in queued.entries) {
        for (final pk in e.value) {
          if (_quarantined.contains((e.key, pk))) continue;
          final doc = _getDoc(e.key, pk);
          docs[(e.key, pk)] = doc == null ? null : _plugins.dispatchBeforePersist(e.key, pk, doc);
        }
      }
    } on Object catch (err) {
      _requeue([
        for (final e in queued.entries)
          for (final pk in e.value) (e.key, pk),
      ], pending: pendingDirty, cause: err);
      return;
    }
    final pending = pendingDirty ? List<PendingChange>.unmodifiable(_pending) : null;
    if (docs.isEmpty && pending == null) return;
    final prev = _writeTail;
    final next = prev == null ? _writeOut(docs, pending) : prev.then((_) => _writeOut(docs, pending));
    _writeTail = next;
    unawaited(next.whenComplete(() {
      if (identical(_writeTail, next)) _writeTail = null;
    }));
  }

  /// Writes one flush and requeues whatever did not reach storage. Never
  /// completes with an error.
  ///
  /// With plain storage the pending queue is written first, and the documents
  /// only once it is stored. A failure part way then leaves an edit queued
  /// (and still pushed) rather than saved in a document with no queue entry.
  Future<void> _writeOut(Map<(String, String), DocumentState?> docs, List<PendingChange>? pending) async {
    final s = _storage;
    if (s is AtomicReplicaStorage) {
      final error = await _attempt(() => s.commit(documents: docs, pending: pending));
      if (error != null) {
        _requeue(docs.keys, pending: pending != null, cause: error);
      } else {
        _retryDelay = _baseRetryDelay;
      }
      return;
    }
    if (pending != null) {
      final error = await _attempt(() => s.savePendingChanges(pending));
      if (error != null) {
        _requeue(docs.keys, pending: true, cause: error);
        return;
      }
    }
    final keys = docs.keys.toList();
    final errors = await Future.wait([
      for (final k in keys)
        _attempt(() {
          final doc = docs[k];
          return doc == null ? s.deleteDocument(k.$1, k.$2) : s.saveDocument(k.$1, k.$2, doc);
        }),
    ]);
    final failed = [
      for (var i = 0; i < keys.length; i++)
        if (errors[i] != null) keys[i],
    ];
    if (failed.isEmpty) {
      _retryDelay = _baseRetryDelay;
    } else {
      _requeue(failed, pending: false, cause: errors.firstWhere((e) => e != null)!);
    }
  }

  /// Runs a storage call and returns the error it failed with, synchronously
  /// or asynchronously, or null.
  Future<Object?> _attempt(Future<void> Function() call) async {
    try {
      await call();
      return null;
    } on Object catch (e) {
      return e;
    }
  }

  /// Writes every queued change now and waits for every write in flight.
  /// Call it before unload, or in tests that assert on storage.
  ///
  /// The pending queue is written only when it changed since the last
  /// successful write of it. Anything that failed before is retried here.
  ///
  /// Completes with a [ReplicaPersistFailed] when a write failed during this
  /// call (including a `beforePersist` hook that threw); what failed stays
  /// queued and is retried. Throws a [StateError] when the replica is
  /// unavailable (see [ReplicaUnavailable]).
  Future<void> flushPersistence() async {
    final unavailable = _unavailable;
    if (unavailable != null) throw StateError('crdt: the replica is unavailable: $unavailable');
    if (!_hydrated) {
      // Nothing may be written before hydration; wait for it, then flush.
      try {
        await ready;
      } on ReplicaUnavailable catch (e) {
        throw StateError('crdt: the replica is unavailable: ${e.cause}');
      }
    }
    final failuresBefore = _failures;
    _stopTimers();
    _drain();
    while (true) {
      final tail = _writeTail;
      if (tail == null) break;
      await tail;
    }
    if (_failures != failuresBefore) throw ReplicaPersistFailed(_lastFailure!);
  }

  /// Stops the timers, makes one final flush attempt, then stops persisting
  /// and closes [documentChanges]. A write after `dispose` is called throws a
  /// [StateError]; reads keep working.
  ///
  /// When the final flush leaves anything unwritten, this completes with a
  /// [ReplicaPersistFailed] after closing, so the caller knows that data did
  /// not reach storage.
  Future<void> dispose() async {
    if (_disposed) return;
    _disposed = true;
    _stopTimers();
    Object? failed;
    if (_unavailable == null) {
      try {
        await flushPersistence();
      } on ReplicaPersistFailed catch (e) {
        failed = e;
      } on StateError {
        // The replica became unavailable while hydrating: nothing to flush.
      }
    }
    _persistStopped = true;
    _stopTimers();
    await _events.close();
    if (failed != null) throw failed;
  }

  void _checkWritable() {
    if (_disposed) throw StateError('crdt: the store is disposed');
    if (!_hydrated && _unavailable == null) throw StateError('await store.ready before writing');
    final unavailable = _unavailable;
    if (unavailable != null) throw StateError('crdt: the replica is unavailable: $unavailable');
  }

  void _checkNotQuarantined(String table, String pk) {
    if (_quarantined.contains((table, pk))) {
      throw StateError('crdt: document $table/$pk is quarantined: its afterHydrate hook failed');
    }
  }

  void _reportStorage(Object error) {
    try {
      _onStorageError?.call(error);
    } on Object {
      // A handler that throws must not break persistence.
    }
  }

  void _reportChange(Object error, ChangeRecord change) {
    try {
      _onError?.call(error, change);
    } on Object {
      // A handler that throws must not abort the batch.
    }
  }

  // --- Internals ---

  FieldState? _captureFieldState(String table, String pk, String field) => _getDoc(table, pk)?.fields[field];

  static ChangeRecord _normalizeKeys(ChangeRecord c) {
    final t = goString(c.table);
    final p = goString(c.pk);
    final f = goString(c.field);
    return identical(t, c.table) && identical(p, c.pk) && identical(f, c.field)
        ? c
        : c.copyWith(table: t, pk: p, field: f);
  }

  /// Folds one change into the replica. A record tombstone (any tombstoned
  /// change that is not a document path delete) sets the sticky tombstone at
  /// the later of the two clocks. Anything else goes through [applyChange].
  /// [prebuilt] supplies a field state already built for a local text edit.
  void _applyChangeInternal(ChangeRecord c, {FieldState Function(FieldState? existing)? prebuilt}) {
    final doc = _docOrEmpty(c.table, c.pk);
    if (_isRecordDelete(c)) {
      final at = doc.tombstone ? hlcMax(doc.tombstoneHlc, c.hlc) : c.hlc;
      _setDocument(c.table, c.pk, doc.copyWith(tombstone: true, tombstoneHlc: at));
      return;
    }
    final existing = doc.fields[c.field];
    final FieldState next;
    if (prebuilt == null) {
      next = applyChange(existing, c);
    } else {
      if (existing != null && existing.type != c.crdtType) {
        throw CrdtApplyError('crdt: cannot apply ${c.crdtType.wire} change onto ${existing.type.wire} field');
      }
      next = prebuilt(existing);
    }
    _setDocument(c.table, c.pk, _withField(doc, c.field, next));
  }

  /// Whether [c] deletes the whole record rather than a path inside a
  /// nested document.
  // Go parity: crdt/server.go:227-228 (MergeChanges) treats a tombstone as a
  // path delete only when it is a document change carrying a value
  // (`CRDTType == TypeDocument && len(Value) > 0`); every other tombstone,
  // a value-less document one included, writes a record tombstone.
  static bool _isRecordDelete(ChangeRecord c) => c.tombstone && !(c.crdtType == CrdtType.document && c.value != null);

  DocumentState? _getDoc(String table, String pk) => _state[table]?[pk];

  /// The current document, or a fresh empty one. Never inserts.
  DocumentState _docOrEmpty(String table, String pk) => _getDoc(table, pk) ?? DocumentState(table: table, pk: pk);

  /// Installs a new document object: the only write path into the state.
  void _setDocument(String table, String pk, DocumentState doc) {
    (_state[table] ??= {})[pk] = doc;
    _tableVersions[table] = (_tableVersions[table] ?? 0) + 1;
  }

  bool _removeDocument(String table, String pk) {
    final docs = _state[table];
    if (docs == null || !docs.containsKey(pk)) return false;
    docs.remove(pk);
    if (docs.isEmpty) _state.remove(table);
    _tableVersions[table] = (_tableVersions[table] ?? 0) + 1;
    return true;
  }

  void _invalidateSnapshots() {
    _docCache = Expando('docCache');
    _listIdCache = Expando('listIdCache');
    _textDeltaCache = Expando('textDeltaCache');
    _collectionCache.clear();
  }

  DocumentState _withField(DocumentState doc, String field, FieldState fs) =>
      doc.copyWith(fields: {...doc.fields, field: fs});

  Map<String, Object?> _resolveDocument(DocumentState doc) => {
        '_table': doc.table,
        '_pk': doc.pk,
        for (final e in doc.fields.entries) e.key: resolveFieldValue(e.value),
      };

  void _emit(String table, String pk) {
    if (!_events.isClosed) _events.add((table: table, pk: pk));
  }

  void _notifyListenersNow(String table, String pk) {
    final docListeners = _docListeners[table]?[pk];
    if (docListeners != null) {
      for (final l in List.of(docListeners)) {
        l();
      }
    }
    final tableListeners = _tableListeners[table];
    if (tableListeners != null) {
      for (final l in List.of(tableListeners)) {
        l();
      }
    }
    _notifyGlobal();
    _emit(table, pk);
  }

  /// Notifies the listeners of a document, or defers to the end of the
  /// outermost [transact].
  void _notifyListeners(String table, String pk) {
    if (_txDepth > 0) {
      (_txTouched[table] ??= {}).add(pk);
      return;
    }
    _notifyListenersNow(table, pk);
  }

  /// Notifies the global listeners, or defers to the end of the outermost
  /// [transact].
  void _notifyGlobalSoon() {
    if (_txDepth > 0) {
      _txGlobal = true;
      return;
    }
    _notifyGlobal();
  }

  /// Notifies the global listeners only.
  void _notifyGlobal() {
    for (final l in List.of(_globalListeners)) {
      l();
    }
  }
}

/// Queues writes to one document and applies them as one transaction. Port of
/// crdt-js `BatchWriter`.
///
/// The queued calls go to the store's own mutators, so batched writes get the
/// same plugin hooks, undo recording and counter totals as direct ones, with
/// one notification per document.
final class BatchWriter {
  BatchWriter._(this._store, this._table, this._pk);

  final CrdtStore _store;
  final String _table;
  final String _pk;
  List<List<ChangeRecord> Function()> _ops = [];

  static List<ChangeRecord> _one(ChangeRecord? c) => c == null ? const [] : [c];

  /// Queues [CrdtStore.setField].
  BatchWriter setField(String field, Object? value) {
    _ops.add(() => _one(_store.setField(_table, _pk, field, value)));
    return this;
  }

  /// Queues [CrdtStore.incrementCounter].
  BatchWriter incrementCounter(String field, [int delta = 1]) {
    _ops.add(() => _one(_store.incrementCounter(_table, _pk, field, delta)));
    return this;
  }

  /// Queues [CrdtStore.decrementCounter].
  BatchWriter decrementCounter(String field, [int delta = 1]) {
    _ops.add(() => _one(_store.decrementCounter(_table, _pk, field, delta)));
    return this;
  }

  /// Queues [CrdtStore.addToSet].
  BatchWriter addToSet(String field, List<Object?> elements) {
    _ops.add(() => _one(_store.addToSet(_table, _pk, field, elements)));
    return this;
  }

  /// Queues [CrdtStore.removeFromSet].
  BatchWriter removeFromSet(String field, List<Object?> elements) {
    _ops.add(() => _one(_store.removeFromSet(_table, _pk, field, elements)));
    return this;
  }

  /// Queues [CrdtStore.insertIntoList].
  BatchWriter insertIntoList(String field, Object? value, {HLC? afterId}) {
    _ops.add(() => _one(_store.insertIntoList(_table, _pk, field, value, afterId: afterId)));
    return this;
  }

  /// Queues [CrdtStore.setDocumentField].
  BatchWriter setDocumentField(String field, String path, Object? value) {
    _ops.add(() => _one(_store.setDocumentField(_table, _pk, field, path, value)));
    return this;
  }

  /// Queues [CrdtStore.insertText].
  BatchWriter insertText(String field, int index, String content) {
    _ops.add(() => _one(_store.insertText(_table, _pk, field, index, content)));
    return this;
  }

  /// Queues [CrdtStore.setText].
  BatchWriter setText(String field, String value) {
    _ops.add(() => _store.setText(_table, _pk, field, value));
    return this;
  }

  /// Applies every queued write as one transaction and returns the changes
  /// made. A write a plugin rejected is left out and does not abort the rest.
  List<ChangeRecord> commit() {
    final ops = _ops;
    _ops = [];
    if (ops.isEmpty) return [];
    return _store.transact(() => [for (final op in ops) ...op()]);
  }
}
