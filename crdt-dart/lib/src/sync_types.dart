import 'package:meta/meta.dart';

import 'hlc.dart';
import 'types.dart';
import 'wire_helpers.dart';

/// Narrows a pull to some documents or fields. Mirrors Go `crdt.SyncFilter`.
@immutable
final class SyncFilter {
  /// Creates a filter.
  const SyncFilter({this.pkFilter = const [], this.fieldFilter = const []});

  /// Only these primary keys (all when empty).
  final List<String> pkFilter;

  /// Only these fields (all when empty).
  final List<String> fieldFilter;

  /// Go wire form.
  Map<String, Object?> toJson() => {
    if (pkFilter.isNotEmpty) 'pk_filter': pkFilter,
    if (fieldFilter.isNotEmpty) 'field_filter': fieldFilter,
  };

  /// Decodes the Go wire form.
  static SyncFilter fromJson(Object? j) {
    final m = wireObj(j);
    String s(Object? v) => v is String
        ? v
        : throw FormatException('crdt: filter entries must be strings, got $v');
    return SyncFilter(
      pkFilter: wireList(m['pk_filter'], s),
      fieldFilter: wireList(m['field_filter'], s),
    );
  }
}

String _string(Object? v) =>
    v is String ? v : throw FormatException('crdt: expected a string, got $v');

/// A pull request. Mirrors Go `crdt.PullRequest`.
@immutable
final class PullRequest {
  /// Creates a pull request. [since] defaults to [HLC.zero].
  PullRequest({
    required this.tables,
    HLC? since,
    required this.nodeId,
    this.filter,
  }) : since = since ?? HLC.zero;

  /// The tables to pull.
  final List<String> tables;

  /// Only changes after this clock.
  final HLC since;

  /// The requesting node.
  final String nodeId;

  /// Optional narrowing.
  final SyncFilter? filter;

  /// Go wire form. `since` is a struct, so it is always emitted.
  Map<String, Object?> toJson() => {
    'tables': tables,
    'since': since.toJson(),
    'node_id': nodeId,
    if (filter != null) 'filter': filter!.toJson(),
  };

  /// Decodes the Go wire form.
  static PullRequest fromJson(Object? j) {
    final m = wireObj(j);
    return PullRequest(
      tables: wireList(m['tables'], _string),
      since: HLC.fromJson(m['since']),
      nodeId: wireStr(m, 'node_id'),
      filter: m['filter'] == null ? null : SyncFilter.fromJson(m['filter']),
    );
  }
}

/// A pull response. Mirrors Go `crdt.PullResponse`.
@immutable
final class PullResponse {
  /// Creates a pull response. [latestHlc] defaults to [HLC.zero].
  PullResponse({this.changes = const [], HLC? latestHlc})
    : latestHlc = latestHlc ?? HLC.zero;

  /// The changes since the requested clock.
  final List<ChangeRecord> changes;

  /// The newest clock the server holds.
  final HLC latestHlc;

  /// Go wire form.
  Map<String, Object?> toJson() => {
    'changes': [for (final c in changes) c.toJson()],
    'latest_hlc': latestHlc.toJson(),
  };

  /// Decodes the Go wire form. A `null` `changes` decodes as empty.
  static PullResponse fromJson(Object? j) {
    final m = wireObj(j);
    return PullResponse(
      changes: wireList(m['changes'], ChangeRecord.fromJson),
      latestHlc: HLC.fromJson(m['latest_hlc']),
    );
  }
}

/// A push request. Mirrors Go `crdt.PushRequest`.
@immutable
final class PushRequest {
  /// Creates a push request.
  const PushRequest({required this.changes, required this.nodeId});

  /// The changes to merge.
  final List<ChangeRecord> changes;

  /// The pushing node.
  final String nodeId;

  /// Go wire form.
  Map<String, Object?> toJson() => {
    'changes': [for (final c in changes) c.toJson()],
    'node_id': nodeId,
  };

  /// Decodes the Go wire form.
  static PushRequest fromJson(Object? j) {
    final m = wireObj(j);
    return PushRequest(
      changes: wireList(m['changes'], ChangeRecord.fromJson),
      nodeId: wireStr(m, 'node_id'),
    );
  }
}

/// A push response. Mirrors Go `crdt.PushResponse`.
@immutable
final class PushResponse {
  /// Creates a push response. [latestHlc] defaults to [HLC.zero].
  PushResponse({required this.merged, HLC? latestHlc})
    : latestHlc = latestHlc ?? HLC.zero;

  /// How many changes the server merged.
  final int merged;

  /// The newest clock the server holds.
  final HLC latestHlc;

  /// Go wire form.
  Map<String, Object?> toJson() => {
    'merged': merged,
    'latest_hlc': latestHlc.toJson(),
  };

  /// Decodes the Go wire form.
  static PushResponse fromJson(Object? j) {
    final m = wireObj(j);
    return PushResponse(
      merged: wireInt(m, 'merged'),
      latestHlc: HLC.fromJson(m['latest_hlc']),
    );
  }
}

/// The outcome of one sync run. Mirrors Go `crdt.SyncReport`, plus the
/// Dart-only [rejected] count.
@immutable
final class SyncReport {
  /// Creates a report.
  const SyncReport({
    this.pulled = 0,
    this.pushed = 0,
    this.merged = 0,
    this.conflicts = 0,
    this.rejected = 0,
  });

  /// Changes pulled.
  final int pulled;

  /// Changes pushed.
  final int pushed;

  /// Changes merged.
  final int merged;

  /// Conflicts resolved.
  final int conflicts;

  /// Changes the server rejected and this run marked. Dart only.
  final int rejected;

  /// Wire form.
  Map<String, Object?> toJson() => {
    'pulled': pulled,
    'pushed': pushed,
    'merged': merged,
    'conflicts': conflicts,
    'rejected': rejected,
  };

  /// Decodes the wire form.
  static SyncReport fromJson(Object? j) {
    final m = wireObj(j);
    return SyncReport(
      pulled: wireInt(m, 'pulled'),
      pushed: wireInt(m, 'pushed'),
      merged: wireInt(m, 'merged'),
      conflicts: wireInt(m, 'conflicts'),
      rejected: wireInt(m, 'rejected'),
    );
  }
}
