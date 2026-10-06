/// Transport interfaces. Port of the interfaces in crdt-js `types.ts` (lines
/// 290-403) that the transports implement.
library;

import 'package:meta/meta.dart';

import 'hlc.dart';
import 'presence_types.dart';
import 'sync_types.dart';
import 'types.dart';

/// Carries pull and push requests to a sync server.
///
/// Implement this to use a custom transport (WebSocket, gRPC) in place of the
/// built-in HTTP one.
abstract interface class Transport {
  /// Fetches the changes after `req.since`.
  Future<PullResponse> pull(PullRequest req);

  /// Sends local changes to the server.
  Future<PushResponse> push(PushRequest req);
}

/// A transport that can also publish and read presence.
///
/// crdt-js marks `updatePresence` and `getPresence` optional on `Transport`.
/// Dart has no optional interface members, so a transport that supports
/// presence implements this interface as well and a caller tests `is`.
abstract interface class PresenceTransport {
  /// Publishes this node's presence on a topic.
  Future<void> updatePresence(PresenceUpdate update);

  /// Reads everyone's presence on [topic].
  Future<List<PresenceState>> getPresence(String topic);
}

/// A transport that also supports real-time subscriptions (SSE, WebSocket).
abstract interface class StreamTransport implements Transport {
  /// Opens a subscription. Nothing connects until [CrdtSubscription.connect].
  CrdtSubscription subscribe(StreamConfig config);
}

/// A live subscription returned by [StreamTransport.subscribe].
///
/// crdt-js calls this `StreamSubscription`; the name is changed here so it
/// does not collide with `dart:async`.
abstract interface class CrdtSubscription {
  /// Registers [handler] for every event. Returns a function that removes it.
  void Function() on(void Function(CrdtStreamEvent event) handler);

  /// Starts connecting and keeps reconnecting until [disconnect].
  void connect();

  /// Stops the subscription and cancels any reconnect.
  void disconnect();

  /// Whether the connection is currently up.
  bool get connected;

  /// The newest change clock the subscription has seen, if any.
  HLC? get lastHlc;
}

/// An event from a [CrdtSubscription].
sealed class CrdtStreamEvent {
  const CrdtStreamEvent();
}

/// One change arrived.
final class StreamChange extends CrdtStreamEvent {
  /// Creates the event.
  const StreamChange(this.change);

  /// The change.
  final ChangeRecord change;
}

/// A batch of changes arrived.
final class StreamChanges extends CrdtStreamEvent {
  /// Creates the event.
  const StreamChanges(this.changes);

  /// The changes.
  final List<ChangeRecord> changes;
}

/// A presence event arrived.
final class StreamPresence extends CrdtStreamEvent {
  /// Creates the event.
  const StreamPresence(this.event);

  /// The presence event.
  final PresenceEvent event;
}

/// The stream failed. It may reconnect on its own.
final class StreamError extends CrdtStreamEvent {
  /// Creates the event.
  const StreamError(this.error);

  /// What went wrong.
  final Object error;
}

/// The connection came up.
final class StreamConnected extends CrdtStreamEvent {
  /// Creates the event.
  const StreamConnected();
}

/// The connection went down.
final class StreamDisconnected extends CrdtStreamEvent {
  /// Creates the event.
  const StreamDisconnected();
}

/// Configuration for a stream subscription. Port of crdt-js `StreamConfig`.
@immutable
final class StreamConfig {
  /// Creates a configuration. An empty [tables] subscribes to every table.
  const StreamConfig({
    this.tables = const [],
    this.reconnectDelay = const Duration(seconds: 5),
    this.maxReconnectDelay = const Duration(seconds: 30),
    this.since,
    this.idleTimeout = const Duration(seconds: 45),
    this.nodeId = '',
  });

  /// The tables to subscribe to.
  final List<String> tables;

  /// First delay of the jittered, growing reconnect schedule. It is the first
  /// delay, not a fixed interval.
  final Duration reconnectDelay;

  /// Ceiling of the reconnect schedule.
  final Duration maxReconnectDelay;

  /// Resume after this clock, so a reconnect does not replay history.
  final HLC? since;

  /// Abort and reconnect when no bytes arrive for this long. Zero disables.
  /// Server keep-alive comments reset it.
  final Duration idleTimeout;

  /// This node's id, so the server can skip echoing its own changes back.
  /// New in the Dart port.
  final String nodeId;

  /// A copy with the given fields replaced. [since] cannot be cleared through
  /// this method; build a new [StreamConfig] for that.
  StreamConfig copyWith({
    List<String>? tables,
    Duration? reconnectDelay,
    Duration? maxReconnectDelay,
    HLC? since,
    Duration? idleTimeout,
    String? nodeId,
  }) => StreamConfig(
    tables: tables ?? this.tables,
    reconnectDelay: reconnectDelay ?? this.reconnectDelay,
    maxReconnectDelay: maxReconnectDelay ?? this.maxReconnectDelay,
    since: since ?? this.since,
    idleTimeout: idleTimeout ?? this.idleTimeout,
    nodeId: nodeId ?? this.nodeId,
  );
}
