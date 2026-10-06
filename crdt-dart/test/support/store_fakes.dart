/// Test doubles for the store tests: a plugin built from closures and
/// recording storage fakes.
library;

import 'package:grove_crdt/grove_crdt.dart';

/// A plugin whose hooks are closures. A hook left null passes its input
/// through, as [StorePlugin] does.
final class FnPlugin extends StorePlugin {
  /// Creates the plugin.
  FnPlugin(
    this.name, {
    this.onBeforeWrite,
    this.onAfterWrite,
    this.onBeforeMerge,
    this.onAfterMerge,
    this.onTransformDocument,
    this.onTransformCollection,
    this.onBeforePersist,
    this.onAfterHydrate,
  });

  @override
  final String name;

  /// `beforeWrite`.
  final WriteEvent? Function(WriteEvent e)? onBeforeWrite;

  /// `afterWrite`.
  final void Function(WriteEvent e)? onAfterWrite;

  /// `beforeMerge`.
  final ChangeRecord? Function(MergeEvent e)? onBeforeMerge;

  /// `afterMerge`.
  final void Function(MergeEvent e)? onAfterMerge;

  /// `transformDocument`.
  final Map<String, Object?>? Function(String table, String pk, Map<String, Object?> doc)? onTransformDocument;

  /// `transformCollection`.
  final List<Map<String, Object?>> Function(String table, List<Map<String, Object?>> docs)? onTransformCollection;

  /// `beforePersist`.
  final DocumentState Function(String table, String pk, DocumentState doc)? onBeforePersist;

  /// `afterHydrate`.
  final DocumentState Function(String table, String pk, DocumentState doc)? onAfterHydrate;

  @override
  WriteEvent? beforeWrite(WriteEvent e) => onBeforeWrite == null ? e : onBeforeWrite!(e);

  @override
  void afterWrite(WriteEvent e) => onAfterWrite?.call(e);

  @override
  ChangeRecord? beforeMerge(MergeEvent e) => onBeforeMerge == null ? e.remote : onBeforeMerge!(e);

  @override
  void afterMerge(MergeEvent e) => onAfterMerge?.call(e);

  @override
  Map<String, Object?>? transformDocument(String table, String pk, Map<String, Object?> doc) =>
      onTransformDocument == null ? doc : onTransformDocument!(table, pk, doc);

  @override
  List<Map<String, Object?>> transformCollection(String table, List<Map<String, Object?>> docs) =>
      onTransformCollection == null ? docs : onTransformCollection!(table, docs);

  @override
  DocumentState beforePersist(String table, String pk, DocumentState doc) =>
      onBeforePersist == null ? doc : onBeforePersist!(table, pk, doc);

  @override
  DocumentState afterHydrate(String table, String pk, DocumentState doc) =>
      onAfterHydrate == null ? doc : onAfterHydrate!(table, pk, doc);
}

/// One recorded `saveDocument`.
typedef SavedDoc = ({String table, String pk, DocumentState doc});

/// A [ReplicaStorage] that records every call, like crdt-js's
/// `createMockStorage`. Each method can be replaced to throw.
class RecordingStorage implements ReplicaStorage {
  /// Documents returned by [loadState].
  Map<String, Map<String, DocumentState>> preloaded = {};

  /// Pending changes returned by [loadPendingChanges].
  List<PendingChange> preloadedPending = [];

  /// Every `saveDocument`, in order.
  final List<SavedDoc> saved = [];

  /// Every `deleteDocument`, in order.
  final List<(String, String)> deleted = [];

  /// Every `savePendingChanges` argument, in order.
  final List<List<PendingChange>> savedPending = [];

  /// Calls to [loadState].
  int loadStateCalls = 0;

  /// Calls to [loadPendingChanges].
  int loadPendingCalls = 0;

  /// When set, [saveDocument] and [deleteDocument] complete with it.
  Object? failWrites;

  /// When set, [savePendingChanges] completes with it.
  Object? failPending;

  /// When set, [loadState] completes with it.
  Object? failLoad;

  /// When set, [loadPendingChanges] completes with it.
  Object? failLoadPending;

  /// When true, a failing method throws synchronously instead of returning a
  /// failed future.
  bool throwSynchronously = false;

  Future<void> _fail(Object error) => throwSynchronously ? throw error : Future<void>.error(error);

  @override
  Future<Map<String, Map<String, DocumentState>>> loadState({
    void Function(FormatException error)? onUnreadable,
  }) {
    loadStateCalls++;
    final f = failLoad;
    if (f != null) {
      if (throwSynchronously) throw f;
      return Future.error(f);
    }
    return Future.value(preloaded);
  }

  @override
  Future<List<PendingChange>> loadPendingChanges() {
    loadPendingCalls++;
    final f = failLoadPending;
    if (f != null) {
      if (throwSynchronously) throw f;
      return Future.error(f);
    }
    return Future.value(preloadedPending);
  }

  @override
  Future<void> saveDocument(String table, String pk, DocumentState doc) {
    final f = failWrites;
    if (f != null) return _fail(f);
    saved.add((table: table, pk: pk, doc: doc));
    return Future.value();
  }

  @override
  Future<void> deleteDocument(String table, String pk) {
    final f = failWrites;
    if (f != null) return _fail(f);
    deleted.add((table, pk));
    return Future.value();
  }

  @override
  Future<void> savePendingChanges(List<PendingChange> changes) {
    final f = failPending;
    if (f != null) return _fail(f);
    savedPending.add(List.of(changes));
    return Future.value();
  }
}

/// One recorded `commit`.
typedef Commit = ({Map<(String, String), DocumentState?> documents, List<PendingChange>? pending});

/// A [RecordingStorage] that is also an [AtomicReplicaStorage], recording
/// each [commit].
final class RecordingAtomicStorage extends RecordingStorage implements AtomicReplicaStorage {
  /// Every `commit`, in order.
  final List<Commit> commits = [];

  /// When set, [commit] completes with it.
  Object? failCommit;

  @override
  Future<void> commit({
    Map<(String, String), DocumentState?> documents = const {},
    List<PendingChange>? pending,
  }) {
    final f = failCommit;
    if (f != null) return _fail(f);
    commits.add((documents: Map.of(documents), pending: pending == null ? null : List.of(pending)));
    return Future.value();
  }
}
