import 'hlc.dart';
import 'presence_types.dart';
import 'types.dart';

/// The value [StorePlugin.beforePresenceUpdate] returns to cancel an update.
///
/// crdt-js cancels with `null`, but in Dart `null` is also a legitimate payload
/// (a leave), so cancelling needs a value of its own.
const Object presenceRejected = _PresenceRejected();

final class _PresenceRejected {
  const _PresenceRejected();

  @override
  String toString() => 'presenceRejected';
}

/// Event data for a local write. Port of crdt-js `WriteEvent`.
final class WriteEvent {
  /// Creates a write event.
  const WriteEvent({
    required this.table,
    required this.pk,
    required this.field,
    required this.crdtType,
    required this.change,
    required this.previousState,
    this.value,
  });

  /// The table.
  final String table;

  /// The primary key.
  final String pk;

  /// The field.
  final String field;

  /// The field's CRDT type.
  final CrdtType crdtType;

  /// What the caller passed: the plain JSON value, a counter delta, or the set
  /// elements.
  final Object? value;

  /// The change the write produced.
  final ChangeRecord change;

  /// The field state before the write, or null when the field did not exist.
  final FieldState? previousState;
}

/// Event data for a merge of one remote change. Port of crdt-js `MergeEvent`.
///
/// Not immutable: the store fills [result] and [winnerNodeId] after the merge,
/// and [PluginManager.dispatchBeforeMerge] sets [remote] to the change the
/// previous hook returned before it calls the next one.
final class MergeEvent {
  /// Creates a merge event.
  MergeEvent({
    required this.table,
    required this.pk,
    required this.field,
    required this.local,
    required this.remote,
    required this.conflictDetected,
    this.result,
    this.winnerNodeId,
  });

  /// The table.
  final String table;

  /// The primary key.
  final String pk;

  /// The field.
  final String field;

  /// The local field state, or null when the field did not exist.
  final FieldState? local;

  /// The incoming change.
  ChangeRecord remote;

  /// The field state after the merge. Set by the store for `afterMerge`.
  FieldState? result;

  /// The node whose write the merged field now holds. Set by the store for
  /// `afterMerge`.
  String? winnerNodeId;

  /// Whether the field already existed locally.
  final bool conflictDetected;
}

/// Event data for a pull. Port of crdt-js `PullEvent`.
final class PullEvent {
  /// Creates a pull event.
  const PullEvent({required this.tables, this.since});

  /// The tables to pull. Empty means every table.
  final List<String> tables;

  /// Where the pull starts, or null for the beginning.
  final HLC? since;
}

/// A store plugin. Port of crdt-js `StorePlugin` and its six hook interfaces.
///
/// crdt-js detects the hooks a plugin has at runtime. Dart has one base class
/// whose hooks all pass their input through, so a plugin overrides only what it
/// needs and the manager never type-checks.
///
/// A `before*` hook that returns null cancels (see each hook). A hook that
/// throws is caught by the [PluginManager] and treated as a pass-through.
abstract class StorePlugin {
  /// Creates a plugin.
  const StorePlugin();

  /// Unique name for this plugin.
  String get name;

  /// Called when the plugin is registered with a manager. Use it for one-time
  /// setup.
  void init() {}

  /// Called when the plugin is removed or the manager is destroyed. Use it for
  /// cleanup.
  void destroy() {}

  /// Called before a local mutation is applied. Return [e] to proceed, a
  /// modified event to transform the write, or null to reject it.
  WriteEvent? beforeWrite(WriteEvent e) => e;

  /// Called after a local mutation was applied and persisted.
  void afterWrite(WriteEvent e) {}

  /// Called before a remote change is merged. Return the change to proceed, a
  /// modified change to transform it, or null to skip it.
  ChangeRecord? beforeMerge(MergeEvent e) => e.remote;

  /// Called after a merge completes.
  void afterMerge(MergeEvent e) {}

  /// Called before a pull request is sent. Return [e], a modified event, or
  /// null to cancel the pull.
  PullEvent? beforePull(PullEvent e) => e;

  /// Called after a pull completes with the received [changes].
  void afterPull(PullEvent e, List<ChangeRecord> changes) {}

  /// Called before local changes are pushed. Return the changes to push, or
  /// null to cancel the push entirely.
  ///
  /// FILTERING IS NOT SUPPORTED. Return the same records you were given
  /// (reordered, or individually rewritten, is fine) or null. The sync engine
  /// clears the pre-hook snapshot from the pending queue once the push
  /// succeeds, so a record you withhold is dropped from the queue without ever
  /// having been sent, and the write is lost for good. Cancel the whole push
  /// with null and retry later instead.
  List<ChangeRecord>? beforePush(List<ChangeRecord> changes) => changes;

  /// Called after a push completes.
  void afterPush(int pushed, List<ChangeRecord> changes) {}

  /// Called when a document is resolved for reading. Return a transformed
  /// document, or null to hide it.
  Map<String, Object?>? transformDocument(String table, String pk, Map<String, Object?> doc) => doc;

  /// Called when a collection is resolved for reading. Return a filtered or
  /// transformed list.
  List<Map<String, Object?>> transformCollection(String table, List<Map<String, Object?>> docs) => docs;

  /// Called before a presence update is sent. Return [data], modified data
  /// (null is a valid payload), or [presenceRejected] to cancel the update.
  Object? beforePresenceUpdate(String topic, Object? data) => data;

  /// Called when a remote presence event is received.
  void onPresenceEvent(PresenceEvent e) {}

  /// Called before a document is persisted. Return the state to store, for
  /// example an encrypted one.
  DocumentState beforePersist(String table, String pk, DocumentState doc) => doc;

  /// Called after a document is loaded from storage. Return the state to hold
  /// in memory.
  DocumentState afterHydrate(String table, String pk, DocumentState doc) => doc;
}

/// Receives an error a plugin threw, with the plugin's name.
typedef PluginErrorHandler = void Function(Object error, String pluginName);

/// Registers plugins and dispatches events through their hook chains. Port of
/// crdt-js `PluginManager`.
///
/// Dispatch semantics, as in crdt-js:
///
/// - `before*` hooks chain in registration order and the first null cancels,
///   so later plugins do not run.
/// - `after*` hooks all run.
/// - `transform*` and `beforePersist`/`afterHydrate` chain, each hook seeing
///   the previous one's result.
///
/// A hook, `init` or `destroy` that throws is reported through
/// [onPluginError] and treated as a pass-through: the chain goes on with the
/// value it had.
final class PluginManager {
  /// Creates a manager. [onPluginError] hears every error a plugin throws and
  /// defaults to ignoring it. An error the handler itself throws is ignored.
  PluginManager({this.onPluginError});

  /// Hears every error a plugin throws.
  final PluginErrorHandler? onPluginError;

  List<StorePlugin> _plugins = [];

  /// Registers [plugin] and calls its `init`. An `init` that throws is reported
  /// and the plugin stays registered, as in crdt-js.
  void use(StorePlugin plugin) {
    _plugins.add(plugin);
    _run(plugin, plugin.init);
  }

  /// Removes the first plugin called [name] and calls its `destroy`. Does
  /// nothing when there is none.
  void remove(String name) {
    final i = _plugins.indexWhere((p) => p.name == name);
    if (i < 0) return;
    final plugin = _plugins.removeAt(i);
    _run(plugin, plugin.destroy);
  }

  /// The first plugin called [name], or null when there is none or it is not a
  /// [T].
  T? get<T extends StorePlugin>(String name) {
    for (final p in _plugins) {
      if (p.name == name) return p is T ? p : null;
    }
    return null;
  }

  /// Every registered plugin, in registration order. A copy.
  List<StorePlugin> all() => List.unmodifiable(_plugins);

  /// Calls `destroy` on every plugin and unregisters them all.
  void destroy() {
    final plugins = _plugins;
    _plugins = [];
    for (final p in plugins) {
      _run(p, p.destroy);
    }
  }

  void _report(StorePlugin p, Object error) {
    try {
      onPluginError?.call(error, p.name);
    } on Object {
      // A handler that throws must not break the dispatch.
    }
  }

  void _run(StorePlugin p, void Function() call) {
    try {
      call();
    } on Object catch (e) {
      _report(p, e);
    }
  }

  /// Calls [call], or returns [fallback] after reporting the error it threw.
  R _guard<R>(StorePlugin p, R fallback, R Function() call) {
    try {
      return call();
    } on Object catch (e) {
      _report(p, e);
      return fallback;
    }
  }

  // The plugin list is copied per dispatch, so a hook that registers or
  // removes a plugin cannot invalidate the loop.
  List<StorePlugin> get _snapshot => List.of(_plugins, growable: false);

  /// Runs `beforeWrite` through every plugin. Null: a plugin rejected the
  /// write.
  WriteEvent? dispatchBeforeWrite(WriteEvent event) {
    WriteEvent? current = event;
    for (final p in _snapshot) {
      if (current == null) break;
      final input = current;
      current = _guard<WriteEvent?>(p, input, () => p.beforeWrite(input));
    }
    return current;
  }

  /// Runs `afterWrite` through every plugin.
  void dispatchAfterWrite(WriteEvent event) {
    for (final p in _snapshot) {
      _run(p, () => p.afterWrite(event));
    }
  }

  /// Runs `beforeMerge` through every plugin. Before each hook,
  /// [MergeEvent.remote] is set to the change the previous hook returned. Null:
  /// a plugin skipped the change.
  ChangeRecord? dispatchBeforeMerge(MergeEvent event) {
    ChangeRecord? current = event.remote;
    for (final p in _snapshot) {
      if (current == null) break;
      final input = current;
      event.remote = input;
      current = _guard<ChangeRecord?>(p, input, () => p.beforeMerge(event));
    }
    return current;
  }

  /// Runs `afterMerge` through every plugin.
  void dispatchAfterMerge(MergeEvent event) {
    for (final p in _snapshot) {
      _run(p, () => p.afterMerge(event));
    }
  }

  /// Runs `beforePull` through every plugin. Null: a plugin cancelled the pull.
  PullEvent? dispatchBeforePull(PullEvent event) {
    PullEvent? current = event;
    for (final p in _snapshot) {
      if (current == null) break;
      final input = current;
      current = _guard<PullEvent?>(p, input, () => p.beforePull(input));
    }
    return current;
  }

  /// Runs `afterPull` through every plugin.
  void dispatchAfterPull(PullEvent event, List<ChangeRecord> changes) {
    for (final p in _snapshot) {
      _run(p, () => p.afterPull(event, changes));
    }
  }

  /// Runs `beforePush` through every plugin. Null: a plugin cancelled the push.
  /// See [StorePlugin.beforePush] on why a plugin must not filter.
  List<ChangeRecord>? dispatchBeforePush(List<ChangeRecord> changes) {
    List<ChangeRecord>? current = changes;
    for (final p in _snapshot) {
      if (current == null) break;
      final input = current;
      current = _guard<List<ChangeRecord>?>(p, input, () => p.beforePush(input));
    }
    return current;
  }

  /// Runs `afterPush` through every plugin.
  void dispatchAfterPush(int pushed, List<ChangeRecord> changes) {
    for (final p in _snapshot) {
      _run(p, () => p.afterPush(pushed, changes));
    }
  }

  /// Runs `transformDocument` through every plugin. Null: a plugin hid the
  /// document.
  Map<String, Object?>? dispatchTransformDocument(String table, String pk, Map<String, Object?> doc) {
    Map<String, Object?>? current = doc;
    for (final p in _snapshot) {
      if (current == null) break;
      final input = current;
      current = _guard<Map<String, Object?>?>(p, input, () => p.transformDocument(table, pk, input));
    }
    return current;
  }

  /// Runs `transformCollection` through every plugin.
  List<Map<String, Object?>> dispatchTransformCollection(String table, List<Map<String, Object?>> docs) {
    var current = docs;
    for (final p in _snapshot) {
      final input = current;
      current = _guard(p, input, () => p.transformCollection(table, input));
    }
    return current;
  }

  /// Runs `beforePresenceUpdate` through every plugin. [presenceRejected]: a
  /// plugin cancelled the update. Any other value, `null` included, is the
  /// payload to send.
  Object? dispatchBeforePresenceUpdate(String topic, Object? data) {
    var current = data;
    for (final p in _snapshot) {
      if (identical(current, presenceRejected)) break;
      final input = current;
      current = _guard(p, input, () => p.beforePresenceUpdate(topic, input));
    }
    return current;
  }

  /// Runs `onPresenceEvent` through every plugin.
  void dispatchOnPresenceEvent(PresenceEvent event) {
    for (final p in _snapshot) {
      _run(p, () => p.onPresenceEvent(event));
    }
  }

  /// Runs `beforePersist` through every plugin.
  DocumentState dispatchBeforePersist(String table, String pk, DocumentState doc) {
    var current = doc;
    for (final p in _snapshot) {
      final input = current;
      current = _guard(p, input, () => p.beforePersist(table, pk, input));
    }
    return current;
  }

  /// Runs `afterHydrate` through every plugin.
  DocumentState dispatchAfterHydrate(String table, String pk, DocumentState doc) {
    var current = doc;
    for (final p in _snapshot) {
      final input = current;
      current = _guard(p, input, () => p.afterHydrate(table, pk, input));
    }
    return current;
  }
}
