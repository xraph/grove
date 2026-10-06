/// Client-side presence manager for tracking remote peers' ephemeral state.
/// Port of crdt-js `presence.ts`.
///
/// Stores the presence events a change stream delivers and offers per-topic
/// subscriptions that a UI can listen to.
library;

import 'presence_types.dart';
import 'wire_helpers.dart';

/// Shared empty answer: a fresh list per call would make a listener that
/// compares by identity see a change on every read. Unmodifiable, so a
/// consumer that tries to add to an empty result gets an error at once rather
/// than corrupting every other empty answer.
final List<PresenceState> _emptyPresence = List<PresenceState>.unmodifiable(
  const <PresenceState>[],
);

int _systemNowMs() => DateTime.now().millisecondsSinceEpoch;

/// In-memory store for remote peers' presence state.
///
/// This is a read-only store: it holds the presence state the server sent,
/// as events and as snapshots. The user's own presence updates go out through
/// the client, not through here.
///
/// A listener is called synchronously, after the state it is told about has
/// changed. It may read the manager, and may unsubscribe itself or another
/// listener; a listener removed before its turn is not called. When a listener
/// throws, the others are still called and the first error is rethrown once
/// every one has run.
final class PresenceManager {
  /// Creates a manager for the node [localNodeId], whose own state
  /// [getPresence] leaves out. [nowMs] reads the clock in epoch milliseconds
  /// (tests inject it); it stamps events and decides what [prune] drops.
  PresenceManager(this.localNodeId, {int Function()? nowMs})
    : _nowMs = nowMs ?? _systemNowMs;

  /// The local node id, excluded from [getPresence] results.
  final String localNodeId;

  final int Function() _nowMs;

  /// topic -> node id -> state.
  final Map<String, Map<String, PresenceState>> _peers = {};

  /// Cached per-topic answers of [getPresence], dropped when a topic changes.
  final Map<String, List<PresenceState>> _snapshots = {};

  final Map<String, Set<void Function()>> _topicListeners = {};
  final Set<void Function()> _globalListeners = {};

  DateTime _now() => DateTime.fromMillisecondsSinceEpoch(_nowMs(), isUtc: true);

  /// All presence states for [topic], excluding the local node.
  ///
  /// The list is cached and is the same instance until the topic changes, so
  /// a listener can compare by identity. It is unmodifiable. An empty topic
  /// gets one shared empty list.
  List<PresenceState> getPresence(String topic) {
    final hit = _snapshots[topic];
    if (hit != null) return hit;

    final topicMap = _peers[topic];
    if (topicMap == null) return _emptyPresence;

    final result = <PresenceState>[
      for (final entry in topicMap.entries)
        if (entry.key != localNodeId) entry.value,
    ];
    final answer = result.isEmpty
        ? _emptyPresence
        : List<PresenceState>.unmodifiable(result);
    _snapshots[topic] = answer;
    return answer;
  }

  /// One peer's presence on [topic], or `null` when it has none. The local
  /// node's own state is returned too.
  PresenceState? getPeer(String topic, String nodeId) => _peers[topic]?[nodeId];

  /// Applies a presence event from a change stream, then notifies listeners.
  ///
  /// A `join` or `update` stores the event's data (an empty object when it
  /// has none) stamped with the current time. A `leave` drops the peer. Any
  /// other type changes nothing but still notifies, as crdt-js does.
  void applyEvent(PresenceEvent event) {
    final topic = event.topic;
    final nodeId = event.nodeId;
    switch (event.type) {
      case 'join' || 'update':
        _peers.putIfAbsent(topic, () => {})[nodeId] = PresenceState(
          nodeId: nodeId,
          topic: topic,
          data: event.data?.value ?? <String, Object?>{},
          updatedAt: _now(),
        );
      case 'leave':
        final topicMap = _peers[topic];
        if (topicMap != null) {
          topicMap.remove(nodeId);
          if (topicMap.isEmpty) _peers.remove(topic);
        }
    }
    _notify([topic]);
  }

  /// Calls [listener] after each change to [topic]. Returns the function that
  /// unsubscribes it.
  ///
  /// A listener is held once per topic: subscribing the same function twice
  /// is one subscription.
  void Function() subscribe(String topic, void Function() listener) {
    final listeners = _topicListeners.putIfAbsent(topic, () => {});
    listeners.add(listener);
    return () {
      listeners.remove(listener);
      if (listeners.isEmpty && identical(_topicListeners[topic], listeners)) {
        _topicListeners.remove(topic);
      }
    };
  }

  /// Calls [listener] after each change to any topic. Returns the function
  /// that unsubscribes it.
  void Function() subscribeAll(void Function() listener) {
    _globalListeners.add(listener);
    return () => _globalListeners.remove(listener);
  }

  /// Replaces [topic]'s peers with a server snapshot, then notifies.
  ///
  /// Needed on join and after every stream reconnect: a stream only delivers
  /// changes, so peers that were idle when you subscribed stay invisible
  /// until they move. The topic's whole peer map is replaced: the local node's
  /// state survives only if [states] includes it, and [getPresence] leaves it
  /// out either way.
  ///
  /// A state's `updatedAt` is already a [DateTime], so a seeded peer ages the
  /// way an event-applied one does. A state whose time is Go's zero time (the
  /// server sent none) counts as just seen, as an unreadable time does in
  /// crdt-js, so [prune] does not drop it at once.
  void seed(String topic, List<PresenceState> states) {
    final topicMap = <String, PresenceState>{};
    for (final state in states) {
      topicMap[state.nodeId] = isGoZeroTime(state.updatedAt)
          ? PresenceState(
              nodeId: state.nodeId,
              topic: state.topic,
              data: state.data,
              updatedAt: _now(),
              expiresAt: state.expiresAt,
            )
          : state;
    }
    if (topicMap.isEmpty) {
      _peers.remove(topic);
    } else {
      _peers[topic] = topicMap;
    }
    _notify([topic]);
  }

  /// Drops every peer whose last update is older than [maxAge], and notifies
  /// the topics that lost one.
  ///
  /// The server expires presence on its own and broadcasts a leave, but that
  /// only reaches a client with a live stream. A client that was disconnected
  /// across the expiry never sees the leave.
  void prune(Duration maxAge) {
    final cutoff = _nowMs() - maxAge.inMilliseconds;
    final changed = <String>[];
    for (final topic in _peers.keys.toList()) {
      final topicMap = _peers[topic]!;
      final before = topicMap.length;
      topicMap.removeWhere(
        (_, state) => state.updatedAt.millisecondsSinceEpoch < cutoff,
      );
      if (topicMap.isEmpty) _peers.remove(topic);
      if (topicMap.length != before) changed.add(topic);
    }
    // Every topic is settled before the first listener runs, so none can read
    // a peer that was about to be dropped.
    _notify(changed);
  }

  /// Drops all presence state, then notifies the listeners of every topic
  /// that had peers.
  ///
  /// Nothing is left behind, not even a cached answer: by the time the first
  /// listener runs every topic reads as empty, so a listener of one topic
  /// cannot read a peer of another that has not been notified yet. Listeners
  /// stay subscribed. Call it when the account changes.
  void clear() {
    final topics = _peers.keys.toList();
    _peers.clear();
    _snapshots.clear();
    _notify(topics);
  }

  /// Tells the listeners of each of [topics], in order. The cache of each
  /// topic is dropped first.
  void _notify(List<String> topics) {
    Object? firstError;
    StackTrace? firstStack;

    void call(Set<void Function()> from, void Function() listener) {
      // A listener removed by an earlier one is skipped.
      if (!from.contains(listener)) return;
      try {
        listener();
      } on Object catch (error, stack) {
        firstError ??= error;
        firstStack ??= stack;
      }
    }

    for (final topic in topics) {
      _snapshots.remove(topic);
    }
    for (final topic in topics) {
      final topicListeners = _topicListeners[topic];
      if (topicListeners != null) {
        for (final listener in topicListeners.toList()) {
          call(topicListeners, listener);
        }
      }
      for (final listener in _globalListeners.toList()) {
        call(_globalListeners, listener);
      }
    }
    final error = firstError;
    if (error != null) Error.throwWithStackTrace(error, firstStack!);
  }
}
