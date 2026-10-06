/// The CRDT protocol client: pull, push, the change stream and presence. Port
/// of crdt-js `client.ts`.
library;

import 'dart:async';

import 'package:http/http.dart' as http;

import 'auth.dart';
import 'hlc.dart';
import 'http_transport.dart';
import 'plugin.dart';
import 'presence.dart';
import 'presence_types.dart';
import 'store.dart';
import 'sync_types.dart';
import 'transport.dart';
import 'types.dart';

/// Presence settings for a [CrdtClient]. Port of crdt-js `PresenceConfig`.
final class PresenceConfig {
  /// Creates the settings.
  const PresenceConfig({this.heartbeatInterval = const Duration(seconds: 10)});

  /// How often a joined topic's last presence is sent again, so the server
  /// does not expire it.
  final Duration heartbeatInterval;
}

/// Client for Grove's CRDT sync protocol.
///
/// Pulls and pushes through a pluggable [Transport], opens change streams
/// through a [StreamTransport], and publishes presence through a
/// [PresenceTransport]. With a `baseUrl` and no transport it builds an
/// [HttpStreamTransport], as crdt-js does.
///
/// Where this differs from crdt-js:
///
/// - The clock can be shared. Pass the [HybridClock] the [CrdtStore] uses, so
///   the store, the client and a `ClockSkew` agree on one clock. [nodeId]
///   reads `clock.nodeId`, so after a `SyncEngine` clock rebase pulls and
///   pushes carry the new node id.
/// - Presence keeps the node id the client was created with. The
///   [PresenceManager] leaves out its own node by that id, and a clock rebase
///   must not make this device show up as a peer of itself, or leave its old
///   presence entry behind on the server.
/// - The client re-announces every joined topic after a stream it opened
///   reconnects, idle recycles included, because the server removes a node's
///   presence when its stream disconnects.
/// - [leaveAllPresence] and [dispose] also clear the [PresenceManager], so
///   nothing from the old session stays visible after an account switch.
final class CrdtClient {
  /// Creates a client.
  ///
  /// Give either a [transport] or a [baseUrl]; with only a [baseUrl] the
  /// client builds an [HttpStreamTransport] from it, [headers], [auth] and
  /// [httpClient], and closes it in [dispose]. [streamTransport] defaults to
  /// [transport] when that can stream. [clock] defaults to a new
  /// [HybridClock] for [nodeId]; a given clock must have that node id.
  ///
  /// Throws an [ArgumentError] when neither [transport] nor [baseUrl] is
  /// given, or when [clock] belongs to another node.
  CrdtClient({
    required String nodeId,
    Transport? transport,
    Uri? baseUrl,
    List<String> tables = const [],
    Map<String, String> headers = const {},
    CrdtAuthProvider? auth,
    StreamTransport? streamTransport,
    PresenceConfig? presence,
    HybridClock? clock,
    http.Client? httpClient,
  }) : clock = clock ?? HybridClock(nodeId),
       presence = PresenceManager(nodeId),
       _presenceNodeId = nodeId,
       _tables = List.unmodifiable(tables),
       _presenceConfig = presence ?? const PresenceConfig(),
       _transport =
           transport ?? _httpTransport(baseUrl, headers, auth, httpClient),
       _ownsTransport = transport == null {
    if (clock != null && clock.nodeId != nodeId) {
      throw ArgumentError.value(
        nodeId,
        'nodeId',
        'must equal the clock node id "${clock.nodeId}"',
      );
    }
    final t = _transport;
    _streamTransport = streamTransport ?? (t is StreamTransport ? t : null);
  }

  static Transport _httpTransport(
    Uri? baseUrl,
    Map<String, String> headers,
    CrdtAuthProvider? auth,
    http.Client? httpClient,
  ) {
    if (baseUrl == null) {
      throw ArgumentError(
        'CrdtClient requires either `baseUrl` or `transport`.',
      );
    }
    return HttpStreamTransport(
      baseUrl: baseUrl,
      client: httpClient,
      headers: headers,
      auth: auth,
    );
  }

  /// The clock stamping this client's requests. Share it with the store.
  final HybridClock clock;

  /// Remote peers' presence, as streams and snapshots deliver it.
  final PresenceManager presence;

  final String _presenceNodeId;
  final List<String> _tables;
  final PresenceConfig _presenceConfig;
  final Transport _transport;
  final bool _ownsTransport;
  late final StreamTransport? _streamTransport;
  final Map<String, Timer> _heartbeats = {};
  final Map<String, Object?> _lastPresence = {};
  final List<void Function()> _streamHandlers = [];
  CrdtStore? _store;
  bool _disposed = false;

  /// The node id pulls and pushes carry: `clock.nodeId`.
  String get nodeId => clock.nodeId;

  /// Pulls the changes after [since] for [tables] (the client's tables when
  /// null), narrowed by [filter]. The response's `latestHlc` is merged into
  /// [clock], as crdt-js does.
  Future<PullResponse> pull({
    List<String>? tables,
    HLC? since,
    SyncFilter? filter,
  }) async {
    final response = await _transport.pull(
      PullRequest(
        tables: tables ?? _tables,
        since: since,
        nodeId: nodeId,
        filter: filter,
      ),
    );
    if (!response.latestHlc.isZero) clock.update(response.latestHlc);
    return response;
  }

  /// Pushes [changes]. An empty list sends nothing and answers with zero
  /// merged and a fresh clock value, as crdt-js does. The response's
  /// `latestHlc` is merged into [clock].
  Future<PushResponse> push(List<ChangeRecord> changes) async {
    if (changes.isEmpty) return PushResponse(merged: 0, latestHlc: clock.now());
    final response = await _transport.push(
      PushRequest(changes: changes, nodeId: nodeId),
    );
    if (!response.latestHlc.isZero) clock.update(response.latestHlc);
    return response;
  }

  /// Opens a change stream with [config], whose every field is forwarded.
  /// Nothing connects until [CrdtSubscription.connect].
  ///
  /// An empty `tables` (the [StreamConfig] default) becomes the client's
  /// tables, so a config that leaves them out streams what the client syncs.
  /// crdt-js tells an omitted list from an empty one; Dart's default cannot,
  /// so an explicit empty list also gets the client's tables, and is empty
  /// only when the client has none.
  ///
  /// When the stream reconnects (any connection after its first, idle
  /// recycles included), every joined topic is announced again.
  ///
  /// Throws a [StateError] when no stream transport is available.
  CrdtSubscription stream([StreamConfig? config]) {
    final transport = _streamTransport;
    if (transport == null) {
      throw StateError(
        'No stream transport available. Provide a `streamTransport` or use a '
        'transport that supports streaming.',
      );
    }
    final base = config ?? const StreamConfig();
    final sub = transport.subscribe(
      base.tables.isEmpty ? base.copyWith(tables: _tables) : base,
    );
    var connectedBefore = false;
    _streamHandlers.add(
      sub.on((event) {
        if (event is! StreamConnected) return;
        if (connectedBefore) _reannounce();
        connectedBefore = true;
      }),
    );
    return sub;
  }

  // --- Presence ---

  /// Links [store], so presence runs through its plugin chain. Optional:
  /// presence works without it, without hooks.
  void attachStore(CrdtStore store) => _store = store;

  /// Applies an inbound presence event, running the `onPresenceEvent` hooks
  /// first. Prefer this to calling `presence.applyEvent` directly.
  void applyPresenceEvent(PresenceEvent event) {
    _store?.pluginManager.dispatchOnPresenceEvent(event);
    presence.applyEvent(event);
  }

  PresenceTransport _presenceTransport() {
    final t = _transport;
    if (t is PresenceTransport) return t as PresenceTransport;
    throw UnsupportedError(
      'Transport does not support presence. Use HttpTransport or implement '
      'PresenceTransport.',
    );
  }

  /// Publishes this node's presence on [topic] and keeps it alive with a
  /// heartbeat every `heartbeatInterval`. A `beforePresenceUpdate` hook may
  /// rewrite [data] or cancel the update (then nothing is sent).
  ///
  /// Throws an [UnsupportedError] when the transport has no presence.
  Future<void> updatePresence(String topic, Object? data) async {
    final transport = _presenceTransport();
    final store = _store;
    final hooked = store == null
        ? data
        : store.pluginManager.dispatchBeforePresenceUpdate(topic, data);
    if (identical(hooked, presenceRejected)) return;
    _lastPresence[topic] = hooked;
    await transport.updatePresence(
      PresenceUpdate(nodeId: _presenceNodeId, topic: topic, data: hooked),
    );
    // Left or disposed while the update was in flight: no heartbeat.
    if (_disposed || !_lastPresence.containsKey(topic)) return;
    _startHeartbeat(topic);
  }

  /// Joins [topic]: publishes presence, then seeds [presence] from the
  /// server's snapshot, so peers already present show at once.
  ///
  /// The seed needs [PresenceTransport.getPresence]. The WebSocket transport
  /// cannot answer it (the Go server has no `presence_get` handler), so over
  /// a socket this is [updatePresence] alone, as in crdt-js.
  Future<void> joinPresence(String topic, Object? data) async {
    await updatePresence(topic, data);
    final List<PresenceState> states;
    try {
      states = await _presenceTransport().getPresence(topic);
    } on UnsupportedError {
      return;
    }
    if (_disposed) return;
    presence.seed(topic, states);
  }

  /// Leaves [topic]: stops its heartbeat and sends `data: null`.
  Future<void> leavePresence(String topic) async {
    _stopHeartbeat(topic);
    _lastPresence.remove(topic);
    final t = _transport;
    if (t is PresenceTransport) {
      await (t as PresenceTransport).updatePresence(
        PresenceUpdate(nodeId: _presenceNodeId, topic: topic),
      );
    }
  }

  /// Reads every presence state on [topic] from the server.
  ///
  /// Throws an [UnsupportedError] when the transport has no presence.
  Future<List<PresenceState>> getPresence(String topic) async =>
      _presenceTransport().getPresence(topic);

  /// Leaves every joined topic, stops every heartbeat and clears [presence].
  ///
  /// The heartbeats stop and [presence] is cleared before anything is sent,
  /// so a failed leave cannot leave a timer running or a peer visible. Every
  /// leave is attempted; the first failure is rethrown after the last.
  Future<void> leaveAllPresence() async {
    final topics = {..._heartbeats.keys, ..._lastPresence.keys}.toList();
    for (final topic in topics) {
      _stopHeartbeat(topic);
    }
    _lastPresence.clear();
    presence.clear();
    Object? failure;
    StackTrace? trace;
    for (final topic in topics) {
      try {
        await leavePresence(topic);
      } on Object catch (e, s) {
        failure ??= e;
        trace ??= s;
      }
    }
    if (failure != null) Error.throwWithStackTrace(failure, trace!);
  }

  /// Leaves every topic, stops every heartbeat, clears [presence], stops
  /// re-announcing on the streams this client opened, and closes the
  /// transport the client built from `baseUrl`.
  ///
  /// The leaves are best effort: a leave that fails (the account was
  /// switched away and the auth provider cancels) is ignored, because the
  /// local state is already gone and the server expires presence on its own.
  Future<void> dispose() async {
    if (_disposed) return;
    _disposed = true;
    for (final remove in _streamHandlers) {
      remove();
    }
    _streamHandlers.clear();
    try {
      await leaveAllPresence();
    } on Object {
      // Best effort; see the doc comment.
    }
    final t = _transport;
    if (_ownsTransport && t is HttpTransport) t.close();
  }

  void _reannounce() {
    if (_disposed) return;
    final t = _transport;
    if (t is! PresenceTransport) return;
    for (final entry in _lastPresence.entries.toList()) {
      unawaited(
        (t as PresenceTransport)
            .updatePresence(
              PresenceUpdate(
                nodeId: _presenceNodeId,
                topic: entry.key,
                data: entry.value,
              ),
            )
            .then<void>((_) {}, onError: (Object _) {}),
      );
    }
  }

  void _startHeartbeat(String topic) {
    _stopHeartbeat(topic);
    _heartbeats[topic] = Timer.periodic(_presenceConfig.heartbeatInterval, (_) {
      if (_disposed || !_lastPresence.containsKey(topic)) return;
      final t = _transport;
      if (t is! PresenceTransport) return;
      unawaited(
        (t as PresenceTransport)
            .updatePresence(
              PresenceUpdate(
                nodeId: _presenceNodeId,
                topic: topic,
                data: _lastPresence[topic],
              ),
            )
            .then<void>((_) {}, onError: (Object _) {}),
      );
    });
  }

  void _stopHeartbeat(String topic) => _heartbeats.remove(topic)?.cancel();
}
