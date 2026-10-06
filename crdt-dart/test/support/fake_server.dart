/// An in-memory [Transport] with the Go server's pull and push semantics, for
/// the sync engine tests.
library;

import 'dart:async';

import 'package:grove_crdt/grove_crdt.dart';

/// An in-memory Transport with the Go server's pull cursor semantics (node id
/// ignored, LIMIT per table) and scriptable push failures.
///
/// Pull mirrors Go `SyncController.HandlePull` over the SQL stores: per table,
/// the changes whose `(ts, c)` is after `since` (the node id is not
/// compared), ordered by HLC, at most [pageLimit]; the filter applies after
/// the limit. Push mirrors `HandlePush`: the whole request is validated first
/// (`ValidatePushRequest`), then each change runs the inbound hook and is
/// merged. By default a hook rejection fails the push after merging the
/// changes before it, which is grove v1.7.0; [atomicHooks] fails it with
/// nothing merged, which is `fix/crdt-sync-defects`. Drift is measured in
/// both directions (v1.7.0) unless [futureDriftOnly] (the fix).
final class FakeServer implements Transport {
  /// Creates a server.
  FakeServer({
    this.pageLimit = 10000,
    this.maxChangesPerPush = 10000,
    this.validateDrift,
    this.atomicHooks = false,
    this.futureDriftOnly = false,
  });

  /// Changes per table per pull.
  final int pageLimit;

  /// Go `ValidationConfig.MaxChangesPerPush`.
  final int maxChangesPerPush;

  /// When set, a pushed change whose ts differs from this by more than one
  /// hour fails validation the way Go's ValidateChangeRecord does.
  final BigInt Function()? validateDrift;

  /// Fail a hook-rejected push with nothing merged (fix/crdt-sync-defects).
  final bool atomicHooks;

  /// Reject only future-dated drift (fix/crdt-sync-defects).
  final bool futureDriftOnly;

  /// Every change the server holds, in arrival order.
  final List<ChangeRecord> log = [];

  /// Every push request's changes, failed ones included.
  final List<List<ChangeRecord>> pushes = [];

  /// Every pull request.
  final List<PullRequest> pulls = [];

  /// Answer every request with this status.
  int? failStatus;

  /// The inbound hook refuses a change to this field.
  String? rejectField;

  /// Successful pushes.
  int serverClockCalls = 0;

  /// When set, a push waits for it after being recorded.
  Completer<void>? pushGate;

  /// When set, a pull waits for it after being recorded.
  Completer<void>? pullGate;

  /// Called with each pull after it is recorded; may throw.
  void Function(PullRequest req)? onPull;

  /// Called with each push after it is recorded; may throw.
  void Function(PushRequest req)? onPush;

  /// Go's `BeforeOutboundRead` hook: hides a pulled change after the page's
  /// `latest_hlc` was computed.
  bool Function(ChangeRecord change)? hide;

  /// The push response's `latest_hlc` (the server's clock).
  HLC Function()? serverClock;

  /// Pushes that have been recorded but not answered yet.
  int pushesInFlight = 0;

  /// Requests made so far, pulls and pushes.
  int get requests => pulls.length + pushes.length;

  static int _cmp(HLC a, HLC b) {
    final t = a.ts.compareTo(b.ts);
    return t != 0 ? t : a.c.compareTo(b.c);
  }

  TransportError _fail(int status) => TransportError(
    'fail',
    statusCode: status,
    body: {'error': status == 404 || status == 410 ? 'gone' : 'failed'},
  );

  @override
  Future<PullResponse> pull(PullRequest req) async {
    pulls.add(req);
    onPull?.call(req);
    final gate = pullGate;
    if (gate != null) await gate.future;
    final status = failStatus;
    if (status != null) throw _fail(status);
    final since = req.since;
    final out = <ChangeRecord>[];
    for (final table in req.tables) {
      final rows =
          log
              .where(
                (c) =>
                    c.table == table &&
                    (c.hlc.ts > since.ts ||
                        (c.hlc.ts == since.ts && c.hlc.c > since.c)),
              )
              .toList()
            ..sort((a, b) => _cmp(a.hlc, b.hlc));
      out.addAll(rows.take(pageLimit));
    }
    var latest = HLC.zero;
    for (final c in out) {
      if (c.hlc.isAfter(latest)) latest = c.hlc;
    }
    final f = req.filter;
    final filtered = f == null
        ? out
        : [
            for (final c in out)
              if ((f.pkFilter.isEmpty || f.pkFilter.contains(c.pk)) &&
                  (f.fieldFilter.isEmpty || f.fieldFilter.contains(c.field)))
                c,
          ];
    final h = hide;
    final visible = h == null
        ? filtered
        : [
            for (final c in filtered)
              if (!h(c)) c,
          ];
    return PullResponse(changes: visible, latestHlc: latest);
  }

  @override
  Future<PushResponse> push(PushRequest req) async {
    pushes.add(req.changes);
    onPush?.call(req);
    pushesInFlight++;
    try {
      final gate = pushGate;
      if (gate != null) await gate.future;
    } finally {
      pushesInFlight--;
    }
    final status = failStatus;
    if (status != null) throw _fail(status);
    if (req.changes.length > maxChangesPerPush) {
      throw _error(
        'crdt: push exceeds max changes (${req.changes.length} > $maxChangesPerPush)',
      );
    }
    for (var i = 0; i < req.changes.length; i++) {
      final c = req.changes[i];
      if (c.pk.isEmpty) {
        throw _error('crdt: change[$i]: crdt: change pk is required');
      }
      final drift = validateDrift;
      if (drift != null) {
        final d = c.hlc.ts - drift();
        final tooFar = futureDriftOnly ? d > _hour : d.abs() > _hour;
        if (tooFar) {
          throw _error(
            'crdt: change[$i]: crdt: change HLC timestamp drift too large (2h0m0s)',
          );
        }
      }
    }
    if (atomicHooks) {
      for (final c in req.changes) {
        if (c.field == rejectField) throw _hook(c);
      }
    }
    var merged = 0;
    for (final c in req.changes) {
      if (c.field == rejectField) throw _hook(c);
      log.add(c);
      merged++;
    }
    serverClockCalls++;
    return PushResponse(merged: merged, latestHlc: serverClock?.call());
  }

  static final BigInt _hour = BigInt.from(3600) * BigInt.from(1000000000);

  static TransportError _error(String msg) =>
      TransportError('push failed', statusCode: 500, body: {'error': msg});

  static TransportError _hook(ChangeRecord c) =>
      _error('crdt: inbound change hook: ${c.field} is locked');

  /// Adds [count] server changes to [table], one per row, at consecutive
  /// timestamps from [startTs].
  void seed(String table, int count, {String node = 'srv', int startTs = 1}) {
    for (var i = 0; i < count; i++) {
      log.add(
        ChangeRecord(
          table: table,
          pk: 'r$i',
          field: 'f',
          crdtType: CrdtType.lww,
          hlc: HLC(BigInt.from(startTs + i), 0, node),
          nodeId: node,
          value: JsonValue(i),
        ),
      );
    }
  }
}
