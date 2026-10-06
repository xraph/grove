import 'package:meta/meta.dart';

import 'hlc.dart';
import 'types.dart';
import 'wire_helpers.dart';

/// Stable identity of a local change.
///
/// Unique because one node never issues the same HLC twice. The parts are
/// joined with U+0000, which cannot appear in a table, key or field name that
/// came off the wire.
String pendingKey(ChangeRecord c) => '${c.table}\u0000${c.pk}\u0000${c.field}\u0000${hlcString(c.hlc)}';

String _requiredString(Map<String, Object?> m, String key, String owner) {
  final v = m[key];
  if (v is String) return v;
  throw FormatException('crdt: $owner "$key" must be a string, got ${v == null ? 'nothing' : v.runtimeType}');
}

/// Why the server refused a pending change.
@immutable
final class PendingRejection {
  /// Creates a rejection mark.
  const PendingRejection({required this.kind, required this.reason});

  /// One of `hook`, `validation`, `drift`, `bad_request`, `server`.
  final String kind;

  /// The server's text.
  final String reason;

  /// JSON form.
  Map<String, Object?> toJson() => {'kind': kind, 'reason': reason};

  /// Decodes the JSON form. A missing or non-string `kind` or `reason` throws
  /// a [FormatException].
  static PendingRejection fromJson(Object? j) {
    final m = wireObj(j);
    return PendingRejection(
      kind: _requiredString(m, 'kind', 'rejection'),
      reason: _requiredString(m, 'reason', 'rejection'),
    );
  }

  @override
  bool operator ==(Object other) => other is PendingRejection && other.kind == kind && other.reason == reason;

  @override
  int get hashCode => Object.hash(kind, reason);

  @override
  String toString() => 'PendingRejection($kind: $reason)';
}

/// A local change waiting to be pushed, with its rejection mark.
@immutable
final class PendingChange {
  /// Wraps [change].
  const PendingChange(this.change, {this.rejection, this.restamps = 0});

  /// The change.
  final ChangeRecord change;

  /// Set when the server refused the change. A rejected change is not pushed
  /// again until it is retried.
  final PendingRejection? rejection;

  /// How many times the change was re-stamped after a clock correction.
  final int restamps;

  /// [pendingKey] of [change].
  String get key => pendingKey(change);

  /// Whether the server refused this change.
  bool get isRejected => rejection != null;

  /// A copy with the given fields replaced. A null [change], [rejection] or
  /// [restamps] keeps the current value; [clearRejection] removes the mark.
  PendingChange copyWith({
    ChangeRecord? change,
    PendingRejection? rejection,
    bool clearRejection = false,
    int? restamps,
  }) =>
      PendingChange(
        change ?? this.change,
        rejection: clearRejection ? null : (rejection ?? this.rejection),
        restamps: restamps ?? this.restamps,
      );

  /// JSON form.
  Map<String, Object?> toJson() => {
        'change': change.toJson(),
        if (rejection != null) 'rejection': rejection!.toJson(),
        if (restamps != 0) 'restamps': restamps,
      };

  /// Decodes the JSON form. A missing `change`, or a value of the wrong type,
  /// throws a [FormatException].
  static PendingChange fromJson(Object? j) {
    final m = wireObj(j);
    if (m['change'] == null) throw const FormatException('crdt: pending change has no "change"');
    return PendingChange(
      ChangeRecord.fromJson(m['change']),
      rejection: m['rejection'] == null ? null : PendingRejection.fromJson(m['rejection']),
      restamps: wireInt(m, 'restamps'),
    );
  }
}
