import 'package:meta/meta.dart';

import 'go_json.dart';
import 'hlc.dart';
import 'types.dart';
import 'wire_helpers.dart';

/// Stable identity of a local change.
///
/// Unique because one node never issues the same HLC twice, and the HLC string
/// carries the node id. The parts are joined with U+0000 only as a separator;
/// uniqueness does not rest on U+0000 being absent from a name.
String pendingKey(ChangeRecord c) =>
    '${c.table}\u0000${c.pk}\u0000${c.field}\u0000${hlcString(c.hlc)}';

String _requiredString(Map<String, Object?> m, String key, String owner) {
  final v = m[key];
  if (v is String) return v;
  throw FormatException(
    'crdt: $owner "$key" must be a string, got ${v == null ? 'nothing' : v.runtimeType}',
  );
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
  bool operator ==(Object other) =>
      other is PendingRejection && other.kind == kind && other.reason == reason;

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
  }) => PendingChange(
    change ?? this.change,
    rejection: clearRejection ? null : (rejection ?? this.rejection),
    restamps: restamps ?? this.restamps,
  );

  /// JSON form.
  ///
  /// A set element held as a [RawJson] (the exact key bytes of a remove) is
  /// also listed under `raw_elements` by position, as its JSON text, so
  /// [fromJson] restores it as a [RawJson] and a push after a restart sends
  /// the same bytes.
  Map<String, Object?> toJson() {
    final elements = change.setOp?.elements ?? const <Object?>[];
    final hasRaw = elements.any((e) => e is RawJson);
    return {
      'change': change.toJson(),
      if (rejection != null) 'rejection': rejection!.toJson(),
      if (restamps != 0) 'restamps': restamps,
      if (hasRaw)
        'raw_elements': [
          for (final e in elements) e is RawJson ? e.json : null,
        ],
    };
  }

  /// Decodes the JSON form. A missing `change`, a `raw_elements` list that
  /// does not match the set elements, or a value of the wrong type throws a
  /// [FormatException].
  static PendingChange fromJson(Object? j) {
    final m = wireObj(j);
    if (m['change'] == null) {
      throw const FormatException('crdt: pending change has no "change"');
    }
    var change = ChangeRecord.fromJson(m['change']);
    final raw = m['raw_elements'];
    if (raw != null) {
      final texts = wireList<String?>(raw, (e) {
        if (e == null || e is String) return e as String?;
        throw FormatException(
          'crdt: pending "raw_elements" entries must be strings or null, got $e',
        );
      });
      final op = change.setOp;
      if (op == null || op.elements.length != texts.length) {
        throw const FormatException(
          'crdt: pending "raw_elements" does not match the set elements',
        );
      }
      change = change.copyWith(
        setOp: SetOperation(op.op, [
          for (var i = 0; i < texts.length; i++)
            texts[i] == null ? op.elements[i] : RawJson(texts[i]!),
        ], tags: op.tags),
      );
    }
    return PendingChange(
      change,
      rejection: m['rejection'] == null
          ? null
          : PendingRejection.fromJson(m['rejection']),
      restamps: wireInt(m, 'restamps'),
    );
  }
}
