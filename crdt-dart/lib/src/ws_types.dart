import 'package:meta/meta.dart';

import 'wire_helpers.dart';

/// WebSocket message types. Mirrors Go `crdt.WSMessageType`.
enum WsMessageType {
  /// Client to server pull.
  pullRequest('pull_request'),

  /// Server reply to a pull.
  pullResponse('pull_response'),

  /// Client to server push.
  pushRequest('push_request'),

  /// Server reply to a push.
  pushResponse('push_response'),

  /// One streamed change.
  change('change'),

  /// A batch of streamed changes (declared by Go, never sent by its server).
  changes('changes'),

  /// Presence update from a client.
  presenceUpdate('presence_update'),

  /// Presence broadcast.
  presenceEvent('presence_event'),

  /// Presence snapshot request (declared, not handled by the Go server).
  presenceGet('presence_get'),

  /// Presence snapshot (declared, never sent by the Go server).
  presenceSnapshot('presence_snapshot'),

  /// Start streaming the given tables.
  subscribe('subscribe'),

  /// Stop streaming (declared, not handled by the Go server).
  unsubscribe('unsubscribe'),

  /// Error, correlated by request id when it answers a request.
  error('error'),

  /// Keep-alive.
  ping('ping'),

  /// Keep-alive reply.
  pong('pong');

  const WsMessageType(this.wire);

  /// The wire string.
  final String wire;

  /// Decodes a wire string; unknown strings return null.
  static WsMessageType? fromWire(String s) {
    for (final t in values) {
      if (t.wire == s) return t;
    }
    return null;
  }
}

/// The JSON frame of the multiplexed WebSocket. Mirrors Go
/// `crdt.WebSocketMessage`.
@immutable
final class WebSocketMessage {
  /// Creates a frame.
  const WebSocketMessage(this.type, {this.payload, this.requestId = ''});

  /// The message type.
  final WsMessageType type;

  /// The decoded payload; Go always emits the key, `null` when empty.
  final Object? payload;

  /// The correlation id; emitted only when non-empty.
  final String requestId;

  /// Go wire form.
  Map<String, Object?> toJson() => {
    'type': type.wire,
    'payload': payload,
    if (requestId.isNotEmpty) 'request_id': requestId,
  };

  /// Decodes the Go wire form. Throws [FormatException] for an unknown type.
  static WebSocketMessage fromJson(Object? j) {
    final m = wireObj(j);
    final raw = wireStr(m, 'type');
    final type =
        WsMessageType.fromWire(raw) ??
        (throw FormatException('crdt: unknown ws message type $raw'));
    return WebSocketMessage(
      type,
      payload: m['payload'],
      requestId: wireStr(m, 'request_id'),
    );
  }
}
