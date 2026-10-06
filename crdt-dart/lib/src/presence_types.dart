import 'package:meta/meta.dart';

import 'go_json.dart';
import 'types.dart';
import 'wire_helpers.dart';

JsonValue? _present(Map<String, Object?> m, String k) =>
    m.containsKey(k) ? JsonValue(m[k]) : null;

String _time(DateTime? t) => formatRfc3339Nano(t ?? goZeroTime);

/// One participant's presence on a topic. Mirrors Go `crdt.PresenceState`.
@immutable
final class PresenceState {
  /// Creates a presence state.
  const PresenceState({
    required this.nodeId,
    required this.topic,
    this.data,
    required this.updatedAt,
    this.expiresAt,
  });

  /// The participant's node id.
  final String nodeId;

  /// The topic (room) it is present on.
  final String topic;

  /// Arbitrary decoded presence data.
  final Object? data;

  /// When the state was last written.
  final DateTime updatedAt;

  /// When the state lapses; `null` for Go's zero time.
  final DateTime? expiresAt;

  /// Go wire form. `expires_at` is Go's zero time when [expiresAt] is `null`.
  Map<String, Object?> toJson() => {
    'node_id': nodeId,
    'topic': topic,
    'data': data,
    'updated_at': _time(updatedAt),
    'expires_at': _time(expiresAt),
  };

  /// Decodes the Go wire form. Go's zero `expires_at` decodes as `null`.
  static PresenceState fromJson(Object? j) {
    final m = wireObj(j);
    final expires = wireTime(m, 'expires_at');
    return PresenceState(
      nodeId: wireStr(m, 'node_id'),
      topic: wireStr(m, 'topic'),
      data: m['data'],
      updatedAt: wireTime(m, 'updated_at'),
      expiresAt: isGoZeroTime(expires) ? null : expires,
    );
  }
}

/// A client's presence write. Mirrors Go `crdt.PresenceUpdate`.
@immutable
final class PresenceUpdate {
  /// Creates an update. A `null` [data] means the participant leaves.
  const PresenceUpdate({required this.nodeId, required this.topic, this.data});

  /// The participant's node id.
  final String nodeId;

  /// The topic.
  final String topic;

  /// The new presence data; `null` means leave.
  final Object? data;

  /// Go wire form. `data` is always emitted.
  Map<String, Object?> toJson() => {
    'node_id': nodeId,
    'topic': topic,
    'data': data,
  };

  /// Decodes the Go wire form.
  static PresenceUpdate fromJson(Object? j) {
    final m = wireObj(j);
    return PresenceUpdate(
      nodeId: wireStr(m, 'node_id'),
      topic: wireStr(m, 'topic'),
      data: m['data'],
    );
  }
}

/// A presence broadcast. Mirrors Go `crdt.PresenceEvent`.
@immutable
final class PresenceEvent {
  /// Creates an event.
  const PresenceEvent({
    required this.type,
    required this.nodeId,
    required this.topic,
    this.data,
  });

  /// `join`, `update` or `leave`.
  final String type;

  /// The participant's node id.
  final String nodeId;

  /// The topic.
  final String topic;

  /// The presence data, absent on a leave.
  final JsonValue? data;

  /// Go wire form.
  Map<String, Object?> toJson() => {
    'type': type,
    'node_id': nodeId,
    'topic': topic,
    if (data != null) 'data': data!.value,
  };

  /// Decodes the Go wire form.
  static PresenceEvent fromJson(Object? j) {
    final m = wireObj(j);
    return PresenceEvent(
      type: wireStr(m, 'type'),
      nodeId: wireStr(m, 'node_id'),
      topic: wireStr(m, 'topic'),
      data: _present(m, 'data'),
    );
  }
}

/// Every participant's state on a topic. Mirrors Go `crdt.PresenceSnapshot`.
@immutable
final class PresenceSnapshot {
  /// Creates a snapshot.
  const PresenceSnapshot({required this.topic, this.states = const []});

  /// The topic.
  final String topic;

  /// The participants' states.
  final List<PresenceState> states;

  /// Go wire form.
  Map<String, Object?> toJson() => {
    'topic': topic,
    'states': [for (final s in states) s.toJson()],
  };

  /// Decodes the Go wire form.
  static PresenceSnapshot fromJson(Object? j) {
    final m = wireObj(j);
    return PresenceSnapshot(
      topic: wireStr(m, 'topic'),
      states: wireList(m['states'], PresenceState.fromJson),
    );
  }
}

/// A collaboration room. Mirrors Go `crdt.Room`.
@immutable
final class Room {
  /// Creates a room.
  const Room({
    required this.id,
    this.type = '',
    this.metadata,
    this.maxParticipants = 0,
    required this.createdAt,
    this.createdBy = '',
  });

  /// The room id.
  final String id;

  /// The room kind.
  final String type;

  /// Arbitrary metadata, absent when not set.
  final JsonValue? metadata;

  /// The participant cap (0: unlimited).
  final int maxParticipants;

  /// When the room was created.
  final DateTime createdAt;

  /// The node that created it.
  final String createdBy;

  /// Go wire form.
  Map<String, Object?> toJson() => {
    'id': id,
    if (type.isNotEmpty) 'type': type,
    if (metadata != null) 'metadata': metadata!.value,
    if (maxParticipants != 0) 'max_participants': maxParticipants,
    'created_at': formatRfc3339Nano(createdAt),
    if (createdBy.isNotEmpty) 'created_by': createdBy,
  };

  /// Decodes the Go wire form.
  static Room fromJson(Object? j) {
    final m = wireObj(j);
    return Room(
      id: wireStr(m, 'id'),
      type: wireStr(m, 'type'),
      metadata: _present(m, 'metadata'),
      maxParticipants: wireInt(m, 'max_participants'),
      createdAt: wireTime(m, 'created_at'),
      createdBy: wireStr(m, 'created_by'),
    );
  }
}

/// A room with its live participants. Mirrors Go `crdt.RoomInfo`, which embeds
/// `Room`, so the room keys sit at the top level.
@immutable
final class RoomInfo {
  /// Creates room info.
  const RoomInfo({
    required this.room,
    required this.participantCount,
    this.participants = const [],
  });

  /// The room.
  final Room room;

  /// The number of participants.
  final int participantCount;

  /// The participants' states.
  final List<PresenceState> participants;

  /// Go wire form: the [Room] keys flattened, then the participant keys.
  Map<String, Object?> toJson() => {
    ...room.toJson(),
    'participant_count': participantCount,
    'participants': [for (final p in participants) p.toJson()],
  };

  /// Decodes the Go wire form.
  static RoomInfo fromJson(Object? j) {
    final m = wireObj(j);
    return RoomInfo(
      room: Room.fromJson(m),
      participantCount: wireInt(m, 'participant_count'),
      participants: wireList(m['participants'], PresenceState.fromJson),
    );
  }
}

/// A cursor or selection. Mirrors Go `crdt.CursorPosition`.
@immutable
final class CursorPosition {
  /// Creates a cursor position.
  const CursorPosition({
    this.x = 0,
    this.y = 0,
    this.offset = 0,
    this.line = 0,
    this.column = 0,
    this.selectionStart = 0,
    this.selectionEnd = 0,
    this.field = '',
  });

  /// Pointer x coordinate.
  final double x;

  /// Pointer y coordinate.
  final double y;

  /// Text offset.
  final int offset;

  /// Line number.
  final int line;

  /// Column number.
  final int column;

  /// Selection start offset.
  final int selectionStart;

  /// Selection end offset.
  final int selectionEnd;

  /// The field the cursor is in.
  final String field;

  /// Go wire form: each key only when non-zero.
  Map<String, Object?> toJson() => {
    if (x != 0) 'x': x,
    if (y != 0) 'y': y,
    if (offset != 0) 'offset': offset,
    if (line != 0) 'line': line,
    if (column != 0) 'column': column,
    if (selectionStart != 0) 'selection_start': selectionStart,
    if (selectionEnd != 0) 'selection_end': selectionEnd,
    if (field.isNotEmpty) 'field': field,
  };

  /// Decodes the Go wire form.
  static CursorPosition fromJson(Object? j) {
    final m = wireObj(j);
    return CursorPosition(
      x: wireDouble(m, 'x'),
      y: wireDouble(m, 'y'),
      offset: wireInt(m, 'offset'),
      line: wireInt(m, 'line'),
      column: wireInt(m, 'column'),
      selectionStart: wireInt(m, 'selection_start'),
      selectionEnd: wireInt(m, 'selection_end'),
      field: wireStr(m, 'field'),
    );
  }
}

/// The conventional shape of presence data. Mirrors Go `crdt.ParticipantData`.
@immutable
final class ParticipantData {
  /// Creates participant data.
  const ParticipantData({
    this.name = '',
    this.color = '',
    this.avatar = '',
    this.cursor,
    this.isTyping = false,
    this.activeField = '',
    this.status = '',
    this.extra = const {},
  });

  /// Display name.
  final String name;

  /// Display colour.
  final String color;

  /// Avatar URL.
  final String avatar;

  /// The cursor, when set.
  final CursorPosition? cursor;

  /// Whether the participant is typing.
  final bool isTyping;

  /// The field the participant is editing.
  final String activeField;

  /// A free-form status.
  final String status;

  /// Application-defined extras.
  final Map<String, Object?> extra;

  /// Go wire form: each key only when set.
  Map<String, Object?> toJson() => {
    if (name.isNotEmpty) 'name': name,
    if (color.isNotEmpty) 'color': color,
    if (avatar.isNotEmpty) 'avatar': avatar,
    if (cursor != null) 'cursor': cursor!.toJson(),
    if (isTyping) 'is_typing': true,
    if (activeField.isNotEmpty) 'active_field': activeField,
    if (status.isNotEmpty) 'status': status,
    if (extra.isNotEmpty) 'extra': extra,
  };

  /// Decodes the Go wire form.
  static ParticipantData fromJson(Object? j) {
    final m = wireObj(j);
    return ParticipantData(
      name: wireStr(m, 'name'),
      color: wireStr(m, 'color'),
      avatar: wireStr(m, 'avatar'),
      cursor: m['cursor'] == null ? null : CursorPosition.fromJson(m['cursor']),
      isTyping: wireBool(m, 'is_typing'),
      activeField: wireStr(m, 'active_field'),
      status: wireStr(m, 'status'),
      extra: wireObj(m['extra']),
    );
  }
}
