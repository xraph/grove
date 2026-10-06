/// The sync engine: pull, clock correction and push for one replica. Starts
/// as a port of crdt-js `sync.ts` and adds what an offline device needs:
/// per-table paging, batching, rejection handling, clock correction, terminal
/// states and cancellation.
library;

import 'dart:async';
import 'dart:math' as math;

import 'backoff.dart';
import 'client.dart';
import 'clock_skew.dart';
import 'errors.dart';
import 'hlc.dart';
import 'pending.dart';
import 'plugin.dart';
import 'rejection.dart';
import 'retry.dart';
import 'storage.dart';
import 'store.dart';
import 'sync_types.dart';
import 'transport.dart';
import 'types.dart';

/// Something the engine wants its owner to know.
sealed class SyncEngineEvent {
  const SyncEngineEvent();
}

/// A run finished.
final class SyncSucceeded extends SyncEngineEvent {
  /// Creates the event.
  const SyncSucceeded(this.report);

  /// What the run did.
  final SyncReport report;
}

/// A run stopped on a retryable failure. The next run tries again.
final class SyncInterrupted extends SyncEngineEvent {
  /// Creates the event.
  const SyncInterrupted(this.error);

  /// The failure.
  final Object error;
}

/// The server refused one change; it stays in the replica, marked.
final class ChangeRejected extends SyncEngineEvent {
  /// Creates the event.
  const ChangeRejected(this.change, this.rejection);

  /// The refused change, as it was before it was marked.
  final PendingChange change;

  /// Why.
  final PushRejection rejection;
}

/// The server answered 404 or 410; the engine stopped until `resume`.
final class DatasetGone extends SyncEngineEvent {
  /// Creates the event.
  const DatasetGone(this.status, this.message);

  /// HTTP status.
  final int status;

  /// Server text.
  final String message;
}

/// The server answered 401 or 403; the engine stopped until `resume`.
final class AuthRequired extends SyncEngineEvent {
  /// Creates the event.
  const AuthRequired(this.status);

  /// HTTP status.
  final int status;
}

/// The clock moved to a new node id after a backwards correction.
final class ClockRebased extends SyncEngineEvent {
  /// Creates the event.
  const ClockRebased(this.nodeId, this.offset);

  /// The new node id.
  final String nodeId;

  /// The applied wall clock correction.
  final Duration offset;
}

/// Remote changes were applied to these documents.
final class ChangesApplied extends SyncEngineEvent {
  /// Creates the event.
  const ChangesApplied(this.affected);

  /// Documents that changed.
  final Set<DocKey> affected;
}

/// Where the engine is.
enum SyncEngineState {
  /// Waiting for the next run.
  idle,

  /// A run is in flight.
  syncing,

  /// The last run failed on a retryable error.
  offline,

  /// The server no longer has this dataset. Runs stop until `resume`.
  gone,

  /// The server refused the credentials. Runs stop until `resume`.
  unauthorized,
}

/// The run hit a terminal state (gone, unauthorized). Caught by the run.
final class _Stop implements Exception {
  _Stop(this.error);

  /// The response that ended the run.
  final TransportError error;

  /// What the push leg had done before the stop, for the partial report.
  _Tally carried = _none;
}

/// The run was cancelled by `stop` or `dispose`. Caught by the run.
final class _Cancelled implements Exception {
  const _Cancelled();
}

/// A drift correction re-stamped pending changes, so the batch being bisected
/// holds stale records. Caught by the push leg, which re-reads the queue.
final class _Restart implements Exception {
  const _Restart();
}

typedef _Tally = ({int pushed, int merged, int rejected});
const _Tally _none = (pushed: 0, merged: 0, rejected: 0);
const _Tally _oneRejected = (pushed: 0, merged: 0, rejected: 1);
_Tally _sum(_Tally a, _Tally b) => (
  pushed: a.pushed + b.pushed,
  merged: a.merged + b.merged,
  rejected: a.rejected + b.rejected,
);

/// One pending change and the record pushed for it (what `beforePush`
/// returned, which may be rewritten, for example encrypted).
typedef _Pair = ({PendingChange pending, ChangeRecord record});

/// State shared by one bisection.
final class _Bisection {
  /// What the bisection pushed and marked so far.
  _Tally total = _none;

  /// Single changes that failed without a verdict, to mark at the end.
  final List<(PendingChange, PushRejection)> deferred = [];

  TransportError? lastUnclassified;
  StackTrace? lastTrace;

  void add(_Tally t) => total = _sum(total, t);
}

/// Drives pull, clock correction and push for one replica, on demand or on a
/// timer.
///
/// A run:
///
/// 1. Awaits `store.ready`. A [ReplicaUnavailable] is surfaced (a
///    [SyncInterrupted] and the error from [sync]) and nothing is sent.
/// 2. Pulls one table at a time, page by page, applying each page and
///    persisting the table's cursor after it (see [cursor]).
/// 3. Corrects the clock when a [ClockSkew] says the device runs ahead.
/// 4. Pushes the pushable pending changes in batches of [pushBatchSize] and
///    clears exactly the pushed keys, so a write made during a push stays
///    pending (crdt-js D11).
///
/// Push failures are read with [classifyPushError] and the HTTP status:
///
/// - 401 and 403 move the engine to [SyncEngineState.unauthorized] (the
///   transport has already tried its one `onUnauthorized` refresh); 404 and
///   410 to [SyncEngineState.gone]. Both emit an event, keep the queue as it
///   is, and stop every later run until [resume].
/// - 408, 429, 502, 503 and 504, a [NetworkError] and an `AuthError` abort the
///   run with a [SyncInterrupted]. Nothing is marked.
/// - 413 and a [BatchTooLargeRejection] halve the batch. After a batch goes
///   through, the size doubles back, up to the configured size and the
///   server's known limit. A single change refused with either is marked.
/// - A [ValidationRejection] marks its change and the rest are pushed again.
/// - A [DriftRejection] corrects the clock and re-stamps the change once; it
///   is marked only when it cannot be re-stamped or is refused again.
/// - A [HookRejection] and an [UnclassifiedRejection] (a 400 or 422) bisect
///   the batch until single changes fail, marking each with its reason.
/// - A failure with no HTTP status (a WebSocket error frame) is classified as
///   status 0, like Go's own error text over HTTP: a recognized rejection is
///   handled exactly as above. An unrecognized one is transient: the run
///   aborts, nothing is marked or counted.
/// - Any other failure (an unclassified 500, an unexpected status) aborts the
///   run and is retried by the next. After [serverErrorLimit] consecutive
///   failures the batch is bisected. A single change that keeps failing that
///   way is marked only when this run has evidence that the server works:
///   another change merged, or got a verdict. Without it (a queue of one
///   change, or a server failing every request) nothing is marked; the run
///   aborts with a [SyncInterrupted] and later runs retry, with backoff when
///   driven by [start], for as long as it takes.
///
/// Only a server verdict on a change marks it. A marked change stays in the
/// replica and in the queue until [retryRejected] or [discardRejected].
///
/// Server behaviour is grove v1.7.0's: a hook rejection fails the push after
/// merging the changes before the refused one, and drift is measured in both
/// directions. `fix/crdt-sync-defects` fails the whole push with nothing
/// merged and rejects only future-dated changes. Bisection gives the same
/// result under both, because a re-pushed change is idempotent.
///
/// A late answer abandoned by a cancellation changes nothing the engine owns.
/// [CrdtClient] has already merged its `latestHlc` into the shared clock by
/// then; that only moves the clock forward (and Go's drift clamp bounds it),
/// so no issued or future HLC is affected.
///
/// Cancellation: [stop] and [dispose] cancel the run in flight, and a
/// [discardRejected] in flight. The run checks
/// for cancellation before every transport call, after each pull page, after
/// each push batch and after each bisection step, and a request in flight is
/// abandoned at once (its answer, when it lands, changes nothing). A
/// cancelled run persists nothing it had not persisted, advances no cursor
/// past what it applied, and clears from the queue only the batches whose
/// confirmation arrived before the cancellation. A [CrdtError] with
/// [CrdtErrorCode.cancelled] from a transport call (an auth provider whose
/// account was switched away) ends the run the same way, with no retry, no
/// further request and no state change. Either way [sync] completes with
/// that cancellation as an error: the transport's own [CrdtError], or a new
/// one with [CrdtErrorCode.cancelled].
final class SyncEngine {
  /// Creates an engine over [client] and [store], which must share one
  /// [HybridClock]. A [skew] is attached to that clock.
  ///
  /// [cursors] persists the pull cursors and the clock epoch; without it they
  /// live in memory. [futureTolerance] is how far past corrected time a
  /// pending change may be before it is re-stamped. [baseNodeId] is the node
  /// id a rebase builds on (`<baseNodeId>~<epoch>`); it defaults to the
  /// store's node id up to its first `~`. A [skew] requires [cursors].
  ///
  /// When a run driven by [start] fails, the next one waits longer: the
  /// interval, then a jittered delay that doubles up to [maxRetryDelay].
  /// [random] replaces the jitter source, for tests.
  SyncEngine(
    this.client,
    this.store, {
    required List<String> tables,
    this._cursors,
    this.pushBatchSize = 500,
    this._skew,
    this.futureTolerance = const Duration(minutes: 1),
    this.serverErrorLimit = 3,
    String? baseNodeId,
    this.maxRetryDelay = const Duration(minutes: 5),
    this._random,
  }) : tables = List.unmodifiable(tables),
       _configuredBatchSize = pushBatchSize,
       _baseNodeId = baseNodeId ?? store.nodeId.split('~').first {
    assert(
      identical(client.clock, store.clock),
      'client and store must share one HybridClock',
    );
    if (pushBatchSize < 1) {
      throw ArgumentError.value(pushBatchSize, 'pushBatchSize', 'must be >= 1');
    }
    if (_skew != null && _cursors == null) {
      // The clock epoch must survive a restart: an epoch kept in memory would
      // start again at 1 and reuse `<base>~1`, whose HLCs are already issued.
      throw ArgumentError.value(
        _cursors,
        'cursors',
        'a ClockSkew needs a cursors store to persist the clock epoch',
      );
    }
    _skew?.attach(store.clock);
    store.ready.then<void>((_) => _storeReady = true, onError: (Object _) {});
  }

  /// The protocol client.
  final CrdtClient client;

  /// The replica.
  final CrdtStore store;

  /// Tables this engine syncs.
  final List<String> tables;

  /// Changes per push; shrinks when the server says the batch is too big.
  int pushBatchSize;

  /// How far past corrected time a pending change may be before it is
  /// re-stamped.
  final Duration futureTolerance;

  /// Consecutive unclassified failures tolerated before bisecting.
  final int serverErrorLimit;

  /// Ceiling of the retry delay after failed runs driven by [start].
  final Duration maxRetryDelay;

  final int _configuredBatchSize;
  final double Function()? _random;
  int? _serverLimit;
  Duration? _interval;
  Backoff? _retry;
  bool _evidence = false;
  final Set<Future<void>> _discards = {};
  Future<void>? _disposing;

  final SyncCursorStore? _cursors;
  final ClockSkew? _skew;
  final String _baseNodeId;
  final Map<String, HLC> _cursorCache = {};
  final _events = StreamController<SyncEngineEvent>.broadcast(sync: true);
  final Set<Completer<Object?>> _waiting = {};
  final List<void Function()> _streamHandlers = [];
  SyncEngineState _state = SyncEngineState.idle;
  Future<SyncReport>? _inFlight;
  Timer? _timer;
  int _serverErrors = 0;
  int _generation = 0;
  int? _epoch;
  Set<String>? _eligible;
  bool _halted = false;
  bool _disposed = false;
  bool _storeReady = false;

  /// When the last run finished.
  DateTime? lastSyncTime;

  /// Engine events. Closed by [dispose].
  Stream<SyncEngineEvent> get events => _events.stream;

  /// Current state.
  SyncEngineState get state => _state;

  /// The pull cursor for [table]: the highest `(ts, c)` applied from it. The
  /// next pull asks from just below it, because the server compares only
  /// `(ts, c)` and two changes may share them across a page boundary.
  HLC? cursor(String table) => _cursorCache[table];

  void _emit(SyncEngineEvent e) {
    if (!_events.isClosed) _events.add(e);
  }

  /// Runs one sync, or joins the run in flight.
  ///
  /// Completes with the report, including a partial one after a terminal
  /// state or rejections. Completes with the error after emitting
  /// [SyncInterrupted] for a retryable failure, and with a [CrdtError] of
  /// code [CrdtErrorCode.cancelled] when the run was cancelled. In the `gone`
  /// and `unauthorized` states it sends nothing and reports nothing.
  ///
  /// Throws a [StateError] after [dispose].
  Future<SyncReport> sync() {
    final running = _inFlight;
    if (running != null) return running;
    if (_disposed) {
      return Future.error(StateError('crdt: the sync engine is disposed'));
    }
    final done = Completer<SyncReport>();
    final future = done.future;
    // Set before the run starts: an event listener that calls sync() from
    // inside the run joins it instead of starting a second one.
    _inFlight = future;
    void settle() {
      if (identical(_inFlight, future)) _inFlight = null;
    }

    unawaited(
      _run(_generation).then<void>(
        (report) {
          settle();
          done.complete(report);
        },
        onError: (Object e, StackTrace s) {
          settle();
          done.completeError(e, s);
        },
      ),
    );
    return future;
  }

  /// Leaves the `gone` or `unauthorized` state.
  void resume() {
    if (_state == SyncEngineState.gone ||
        _state == SyncEngineState.unauthorized) {
      _state = SyncEngineState.idle;
    }
  }

  /// Syncs every [interval] and returns [stop]. Failures are swallowed, as in
  /// crdt-js: the queue is durable and the next tick retries; read [events]
  /// or [state] for status. crdt-js also syncs on the browser's `online`
  /// event; Dart has no such event, so the owner calls [sync] itself.
  Future<void> Function() start({
    Duration interval = const Duration(seconds: 30),
  }) {
    if (_disposed) throw StateError('crdt: the sync engine is disposed');
    _halted = false;
    _interval = interval;
    _retry = Backoff(
      initialDelay: interval,
      maxDelay: maxRetryDelay < interval ? interval : maxRetryDelay,
      random: _random,
    );
    _schedule(interval);
    return stop;
  }

  void _schedule(Duration delay) {
    _timer?.cancel();
    _timer = Timer(delay, _tick);
  }

  void _tick() {
    _timer = null;
    if (_disposed || _halted) return;
    final gen = _generation;
    unawaited(
      sync()
          .then<Duration?>(
            (_) {
              _retry?.reset();
              return _interval;
            },
            onError: (Object e) {
              final interval = _interval;
              final retry = _retry;
              if (interval == null || retry == null || _isCancellation(e)) {
                return interval;
              }
              final wait = retry.next();
              return wait < interval ? interval : wait;
            },
          )
          .then<void>((delay) {
            if (delay == null || _disposed || _halted) return;
            if (gen != _generation || _timer != null) return;
            _schedule(delay);
          }),
    );
  }

  void _syncQuietly() {
    if (_disposed || _halted) return;
    unawaited(sync().then<void>((_) {}, onError: (Object _) {}));
  }

  /// Stops the timer and cancels the run in flight and any [discardRejected]
  /// in flight. Completes when they have finished. A request in flight is
  /// abandoned at once, so this does not wait on the network.
  ///
  /// Until [start] is called again, a stream attached with [attachStream]
  /// neither starts runs nor applies changes. [sync] still runs when called.
  /// An account switch calls [dispose] (or [stop] and detaches the stream),
  /// so nothing from the old account reaches the store.
  Future<void> stop() async {
    _timer?.cancel();
    _timer = null;
    _halted = true;
    _generation++;
    for (final c in _waiting.toList()) {
      if (!c.isCompleted) c.completeError(const _Cancelled());
    }
    _waiting.clear();
    final running = [?_inFlight, ..._discards];
    for (final f in running) {
      try {
        await f;
      } on Object {
        // The outcome belongs to whoever called sync() or discardRejected().
      }
    }
  }

  /// Applies the changes [subscription] delivers and syncs when it
  /// reconnects. Returns a function that detaches it.
  ///
  /// A [StreamConnected] with [ConnectionReason.normal] starts a run, which
  /// pulls what the stream missed while it was down. An idle recycle
  /// ([ConnectionReason.idle]) is not an error and loses nothing, so it starts
  /// no run. A [StreamError] is left to the stream, which reconnects on its
  /// own. Changes arriving before `store.ready`, or after [stop] until the
  /// next [start], are dropped; the next pull fetches them. Stream changes do
  /// not move the pull cursors.
  void Function() attachStream(CrdtSubscription subscription) {
    if (_disposed) throw StateError('crdt: the sync engine is disposed');
    late final void Function() remove;
    void detach() {
      remove();
      _streamHandlers.remove(detach);
    }

    remove = subscription.on((event) {
      if (_disposed) return;
      switch (event) {
        case StreamChange(:final change):
          _applyStreamed([change]);
        case StreamChanges(:final changes):
          _applyStreamed(changes);
        case StreamConnected(reason: ConnectionReason.normal):
          _syncQuietly();
        case StreamConnected() ||
            StreamDisconnected() ||
            StreamError() ||
            StreamPresence():
          break;
      }
    });
    _streamHandlers.add(detach);
    return detach;
  }

  void _applyStreamed(List<ChangeRecord> changes) {
    if (!_storeReady || _halted || changes.isEmpty) return;
    final Set<DocKey> affected;
    try {
      affected = store.applyChanges(changes);
    } on StateError {
      // The store was disposed or became unavailable; the stream outlived it.
      return;
    }
    if (affected.isNotEmpty) _emit(ChangesApplied(affected));
  }

  /// Makes a rejected change pushable again.
  void retryRejected(String key) => store.retryRejected(key);

  void _checkCancelled(int gen) {
    if (gen != _generation) throw const _Cancelled();
  }

  /// One transport call, abandoned at once when the run is cancelled.
  Future<T> _call<T>(int gen, Future<T> Function() f) async {
    _checkCancelled(gen);
    final waiter = Completer<T>();
    _waiting.add(waiter);
    final Future<T> call;
    try {
      call = f();
    } on Object {
      _waiting.remove(waiter);
      rethrow;
    }
    unawaited(
      call.then<void>(
        (v) {
          if (!waiter.isCompleted) waiter.complete(v);
        },
        onError: (Object e, StackTrace s) {
          if (!waiter.isCompleted) waiter.completeError(e, s);
        },
      ),
    );
    try {
      final value = await waiter.future;
      _checkCancelled(gen);
      return value;
    } on TransportError catch (e) {
      _checkCancelled(gen);
      final s = e.statusCode;
      if (s == 404 || s == 410) {
        _state = SyncEngineState.gone;
        _emit(DatasetGone(s!, serverMessage(e.body) ?? e.message));
        throw _Stop(e);
      }
      if (s == 401 || s == 403) {
        _state = SyncEngineState.unauthorized;
        _emit(AuthRequired(s!));
        throw _Stop(e);
      }
      rethrow;
    } finally {
      _waiting.remove(waiter);
    }
  }

  static bool _isCancellation(Object e) =>
      e is _Cancelled || (e is CrdtError && e.code == CrdtErrorCode.cancelled);

  Future<SyncReport> _run(int gen) async {
    if (_state == SyncEngineState.gone ||
        _state == SyncEngineState.unauthorized) {
      return const SyncReport();
    }
    _state = SyncEngineState.syncing;
    _evidence = false;
    final pulledChanges = <ChangeRecord>[];
    var tally = _none;
    SyncReport partial() => SyncReport(
      pulled: pulledChanges.length,
      pushed: tally.pushed,
      merged: tally.merged,
      rejected: tally.rejected,
    );
    try {
      await store.ready;
      _storeReady = true;
      _checkCancelled(gen);
      final plugins = store.pluginManager;
      final pullEvent = plugins.dispatchBeforePull(PullEvent(tables: tables));
      if (pullEvent != null) {
        for (final table
            in pullEvent.tables.isEmpty ? tables : pullEvent.tables) {
          await _pullTable(gen, table, pulledChanges);
        }
        plugins.dispatchAfterPull(pullEvent, List.unmodifiable(pulledChanges));
      }
      await _correctClock(gen);
      tally = await _pushAll(gen, (t) => tally = t);
      _checkCancelled(gen);
      lastSyncTime = DateTime.now();
      final report = partial();
      _state = SyncEngineState.idle;
      _emit(SyncSucceeded(report));
      return report;
    } on _Stop {
      return partial();
    } on Object catch (e) {
      // An error surfacing after a cancellation (a failed cursor write, a
      // store that became unavailable) belongs to a cancelled run.
      if (_isCancellation(e) || gen != _generation) {
        if (_state == SyncEngineState.syncing) _state = SyncEngineState.idle;
        if (e is CrdtError) rethrow;
        throw CrdtError(
          'crdt: the sync run was cancelled',
          code: CrdtErrorCode.cancelled,
        );
      }
      _state = SyncEngineState.offline;
      _emit(SyncInterrupted(e));
      rethrow;
    }
  }

  static int _cmpTsc(HLC a, HLC b) {
    final t = a.ts.compareTo(b.ts);
    return t != 0 ? t : a.c.compareTo(b.c);
  }

  /// The `(ts, c)` just below [h], so a pull from it returns [h] again and
  /// everything sharing its `(ts, c)`.
  static HLC _below(HLC h) =>
      h.c > 0 ? HLC(h.ts, h.c - 1, '') : HLC(h.ts - BigInt.one, 0xFFFFFFFF, '');

  Future<void> _pullTable(int gen, String table, List<ChangeRecord> out) async {
    var cursor = _cursorCache[table];
    if (cursor == null) {
      cursor = await _cursors?.readCursor(table);
      _checkCancelled(gen);
      if (cursor != null) _cursorCache[table] = cursor;
    }
    await _paged(gen, table, cursor, (changes, next) async {
      if (changes.isNotEmpty) {
        final affected = store.applyChanges(changes);
        if (affected.isNotEmpty) _emit(ChangesApplied(affected));
        out.addAll(changes);
      }
      if (next == null) return;
      _cursorCache[table] = next;
      await _cursors?.writeCursor(table, next);
      _checkCancelled(gen);
    });
  }

  /// Pulls [table] page by page from just below [from] (the start when
  /// null), narrowed by [filter], until a page makes no progress. Each page
  /// goes to [onPage] with the cursor it advances to, or null on the last
  /// page. Returns the final cursor.
  ///
  /// A non-empty page advances to the highest `(ts, c)` among its changes.
  /// When that makes no progress, or the page is empty, it advances to the
  /// page's `latestHlc` if that is past the cursor: Go computes it over the
  /// rows it read before its filter (applied after the LIMIT) and its
  /// `BeforeOutboundRead` hook hid any of them, so a page whose rows were all
  /// hidden still moves the cursor on instead of ending the pull.
  ///
  /// A page shorter than its limit is not taken as the end: the engine does
  /// not know the server's limit. More changes sharing one `(ts, c)` than fit
  /// in a page (which needs that many distinct nodes) would stop the pull at
  /// that group.
  Future<HLC?> _paged(
    int gen,
    String table,
    HLC? from,
    Future<void> Function(List<ChangeRecord> changes, HLC? next) onPage, {
    SyncFilter? filter,
  }) async {
    var cursor = from;
    while (true) {
      final since = cursor == null ? null : _below(cursor);
      final resp = await _call(
        gen,
        () => client.pull(tables: [table], since: since, filter: filter),
      );
      HLC? next;
      if (resp.changes.isNotEmpty) {
        var pageMax = resp.changes.first.hlc;
        for (final c in resp.changes) {
          if (_cmpTsc(c.hlc, pageMax) > 0) pageMax = c.hlc;
        }
        if (cursor == null || _cmpTsc(pageMax, cursor) > 0) next = pageMax;
      }
      if (next == null) {
        final latest = _latestCursor(resp);
        if (latest != null && (cursor == null || _cmpTsc(latest, cursor) > 0)) {
          next = latest;
        }
      }
      await onPage(resp.changes, next);
      if (next == null) return cursor;
      cursor = next;
    }
  }

  /// A double holds 53 significant bits. An int64 nanosecond timestamp is
  /// below 2^63, where a double's ulp is at most 2^10 = 1024 ns, so rounding
  /// moves it by at most 512 ns (128 ns for timestamps before 2043, which are
  /// below 2^61). One microsecond covers every int64 magnitude.
  static final BigInt _roundingMargin = BigInt.from(1000);

  /// The cursor a page's `latestHlc` allows, or null when it is zero. An
  /// exact value is used as it is. One that may have rounded is moved down
  /// by [_roundingMargin] with counter 0, so it is never past the real one;
  /// the rows in between are pulled again, which is idempotent.
  static HLC? _latestCursor(PullResponse resp) {
    final latest = resp.latestHlc;
    if (latest.isZero) return null;
    if (resp.latestHlcExact) return HLC(latest.ts, latest.c, '');
    final ts = latest.ts - _roundingMargin;
    return ts > BigInt.zero ? HLC(ts, 0, '') : null;
  }

  Future<void> _correctClock(int gen) async {
    final skew = _skew;
    if (skew == null || !skew.corrected) return;
    final limit =
        skew.nowNs + BigInt.from(futureTolerance.inMicroseconds) * _nsPerUs;
    final future = [
      for (final p in store.pending)
        if (!p.isRejected && p.change.hlc.ts > limit) p.key,
    ];
    if (store.clock.last.ts > limit) {
      // The clock has issued values past corrected time. It never steps back
      // under one node id, so it moves to a fresh one and starts from
      // corrected time there.
      final stored = int.tryParse(
        await _cursors?.readMeta('clock_epoch') ?? '',
      );
      _checkCancelled(gen);
      final epoch = math.max(stored ?? 0, _epoch ?? 0) + 1;
      await _cursors?.writeMeta('clock_epoch', '$epoch');
      _checkCancelled(gen);
      _epoch = epoch;
      final node = '$_baseNodeId~$epoch';
      store.clock.rebase(node);
      _emit(ClockRebased(node, skew.offset));
    }
    for (final key in future) {
      _restamp(key);
    }
  }

  /// Re-stamps a pending change, keeping it in the push leg under its new
  /// key. Null when it cannot be re-stamped.
  ChangeRecord? _restamp(String key) {
    final fresh = store.restampPending(key);
    if (fresh != null) _eligible?.add(pendingKey(fresh));
    return fresh;
  }

  static final BigInt _nsPerUs = BigInt.from(1000);

  String _signature() {
    final b = StringBuffer('$pushBatchSize|');
    for (final p in store.pending) {
      b
        ..write(p.isRejected ? 'r' : 'p')
        ..write(p.key)
        ..write('\u0001');
    }
    return b.toString();
  }

  /// Pushes what was pushable when the push leg started. A write made
  /// during the leg waits for the next run, so a user who keeps typing
  /// cannot keep one run going.
  Future<_Tally> _pushAll(int gen, void Function(_Tally) progress) async {
    final eligible = {
      for (final p in store.pending)
        if (!p.isRejected) p.key,
    };
    _eligible = eligible;
    try {
      return await _pushEligible(gen, eligible, progress);
    } finally {
      if (identical(_eligible, eligible)) _eligible = null;
    }
  }

  Future<_Tally> _pushEligible(
    int gen,
    Set<String> eligible,
    void Function(_Tally) progress,
  ) async {
    var tally = _none;
    while (true) {
      _checkCancelled(gen);
      final batch = store.pending
          .where((p) => !p.isRejected && eligible.contains(p.key))
          .take(pushBatchSize)
          .toList();
      if (batch.isEmpty) return tally;
      final before = _signature();
      final _Tally? step;
      try {
        step = await _pushBatch(gen, batch);
      } on _Stop catch (stop) {
        progress(_sum(tally, stop.carried));
        rethrow;
      }
      if (step == null) return tally;
      tally = _sum(tally, step);
      progress(tally);
      _checkCancelled(gen);
      // Every step clears, marks or re-stamps something, or changes the
      // batch size. Guard anyway, so the loop can never spin.
      if (_signature() == before) return tally;
    }
  }

  /// Pairs each pushed record with its pending change by [pendingKey]. Null
  /// when a `beforePush` hook added, dropped or re-keyed records, so a
  /// verdict cannot be traced back to a change.
  static List<_Pair>? _pair(
    List<PendingChange> batch,
    List<ChangeRecord> records,
  ) {
    if (records.length != batch.length) return null;
    final byKey = {for (final p in batch) p.key: p};
    final pairs = <_Pair>[];
    for (final r in records) {
      final p = byKey.remove(pendingKey(r));
      if (p == null) return null;
      pairs.add((pending: p, record: r));
    }
    return pairs;
  }

  /// The batch size after a batch went through: double, up to the
  /// configured size and the server's known limit.
  void _growBatch() {
    var cap = _configuredBatchSize;
    final limit = _serverLimit;
    if (limit != null && limit >= 1 && limit < cap) cap = limit;
    pushBatchSize = math.min(cap, math.max(pushBatchSize, pushBatchSize * 2));
  }

  void _pushed(List<ChangeRecord> records, PushResponse resp) {
    _skew?.observeHlc(resp.latestHlc);
    store.pluginManager.dispatchAfterPush(records.length, records);
    _serverErrors = 0;
    _evidence = true;
  }

  /// One top-level batch. Null when `beforePush` cancelled the push.
  Future<_Tally?> _pushBatch(int gen, List<PendingChange> batch) async {
    final records = store.pluginManager.dispatchBeforePush([
      for (final p in batch) p.change,
    ]);
    if (records == null) return null;
    final PushResponse resp;
    try {
      resp = await _call(gen, () => client.push(records));
    } on TransportError catch (e, s) {
      return _batchFailed(gen, batch, records, e, s);
    }
    // Clears the pre-hook snapshot, as crdt-js does (see
    // StorePlugin.beforePush on why a hook must not filter).
    store.clearPendingChanges([for (final p in batch) p.change]);
    _pushed(records, resp);
    _growBatch();
    return (pushed: records.length, merged: resp.merged, rejected: 0);
  }

  /// The status a failure is classified with: a missing one (a WebSocket
  /// error frame) reads as 0, which [classifyPushError] treats like Go's
  /// 500.
  static int _statusOf(TransportError e) => e.statusCode ?? 0;

  /// Whether a failure is transient: a retry status, or a failure with no
  /// HTTP status whose text names no rejection (a closed socket, a timeout).
  static bool _transient(TransportError e) {
    final status = _statusOf(e);
    if (isTransientStatus(status)) return true;
    return status == 0 && classifyPushError(0, e.body) == null;
  }

  Future<_Tally> _batchFailed(
    int gen,
    List<PendingChange> batch,
    List<ChangeRecord> records,
    TransportError e,
    StackTrace s,
  ) async {
    if (_transient(e)) Error.throwWithStackTrace(e, s);
    final status = _statusOf(e);
    if (status == 413) {
      if (batch.length == 1) return _verdict(batch.single, _tooLarge(e));
      pushBatchSize = math.max(1, batch.length ~/ 2);
      return _none;
    }
    final rejection = classifyPushError(status, e.body);
    if (rejection case BatchTooLargeRejection(:final limit)) {
      if (batch.length == 1) return _verdict(batch.single, rejection);
      _evidence = true;
      _serverLimit = limit;
      pushBatchSize = math.max(1, math.min(limit, batch.length ~/ 2));
      return _none;
    }
    final pairs = _pair(batch, records);
    if (pairs == null) {
      // A hook re-keyed the batch: no verdict can be traced to a change.
      Error.throwWithStackTrace(e, s);
    }
    PendingChange? blame(int i) {
      if (i < 0 || i >= records.length) return null;
      final key = pendingKey(records[i]);
      for (final p in pairs) {
        if (p.pending.key == key) return p.pending;
      }
      return null;
    }

    switch (rejection) {
      case ValidationRejection(:final index) when blame(index) != null:
        return _verdict(blame(index)!, rejection);
      case DriftRejection(:final index) when blame(index) != null:
        _evidence = true;
        return _handleDrift(gen, blame(index)!, rejection);
      case HookRejection() ||
          UnclassifiedRejection() ||
          ValidationRejection() ||
          DriftRejection():
        _evidence = true;
        return _bisect(gen, pairs, e, s);
      case BatchTooLargeRejection():
        // Handled above.
        return _none;
      case null:
        if (++_serverErrors < serverErrorLimit) {
          Error.throwWithStackTrace(e, s);
        }
        _serverErrors = 0;
        return _bisect(gen, pairs, e, s);
    }
  }

  static UnclassifiedRejection _tooLarge(TransportError e) =>
      UnclassifiedRejection(413, serverMessage(e.body) ?? e.message);

  /// Marks [p] for a server verdict, which is also evidence the server works.
  _Tally _verdict(PendingChange p, PushRejection r) {
    _evidence = true;
    return _mark(p, r);
  }

  _Tally _mark(PendingChange p, PushRejection r) {
    store.markRejected(p.key, PendingRejection(kind: r.kind, reason: r.reason));
    _emit(ChangeRejected(p, r));
    return _oneRejected;
  }

  Future<_Tally> _handleDrift(
    int gen,
    PendingChange p,
    DriftRejection r,
  ) async {
    await _correctClock(gen);
    final current = store.pending.where((x) => x.key == p.key).firstOrNull;
    // The correction re-stamped it already; the next batch pushes it.
    if (current == null) return _none;
    if (current.restamps == 0 && _restamp(current.key) != null) {
      return _none;
    }
    return _mark(current, r);
  }

  /// Bisects [pairs], whose push just failed with [e]: pushes each half,
  /// splits a half that fails again, and marks single changes that the
  /// server refuses.
  ///
  /// A single change that fails without a verdict (an unclassified 500) is
  /// marked only when this run has evidence that the server works: another
  /// change merged or got a verdict. Otherwise the last failure is rethrown
  /// and nothing more is marked: a server failing every request is not a
  /// verdict on the queue, and neither is one change failing alone.
  Future<_Tally> _bisect(
    int gen,
    List<_Pair> pairs,
    TransportError e,
    StackTrace s,
  ) async {
    final b = _Bisection();
    try {
      await _split(gen, b, pairs, e);
    } on _Restart {
      return b.total;
    } on _Stop catch (stop) {
      stop.carried = _sum(stop.carried, b.total);
      rethrow;
    }
    if (b.deferred.isEmpty) return b.total;
    if (!_evidence) {
      Error.throwWithStackTrace(b.lastUnclassified ?? e, b.lastTrace ?? s);
    }
    for (final (p, r) in b.deferred) {
      b.add(_mark(p, r));
    }
    return b.total;
  }

  Future<void> _split(
    int gen,
    _Bisection b,
    List<_Pair> pairs,
    TransportError e,
  ) async {
    if (pairs.length == 1) return _single(gen, b, pairs.single, e);
    final mid = pairs.length ~/ 2;
    await _pushPairs(gen, b, pairs.sublist(0, mid));
    _checkCancelled(gen);
    await _pushPairs(gen, b, pairs.sublist(mid));
    _checkCancelled(gen);
  }

  Future<void> _pushPairs(int gen, _Bisection b, List<_Pair> pairs) async {
    final records = [for (final p in pairs) p.record];
    final PushResponse resp;
    try {
      resp = await _call(gen, () => client.push(records));
    } on TransportError catch (e, s) {
      if (_transient(e)) Error.throwWithStackTrace(e, s);
      final status = _statusOf(e);
      if (status != 413 && classifyPushError(status, e.body) == null) {
        b.lastUnclassified = e;
        b.lastTrace = s;
      } else {
        _evidence = true;
      }
      return _split(gen, b, pairs, e);
    }
    store.clearPendingChanges([for (final p in pairs) p.pending.change]);
    _pushed(records, resp);
    b.add((pushed: records.length, merged: resp.merged, rejected: 0));
  }

  /// A single change whose push failed with [e].
  Future<void> _single(
    int gen,
    _Bisection b,
    _Pair pair,
    TransportError e,
  ) async {
    final status = _statusOf(e);
    if (status == 413) {
      b.add(_verdict(pair.pending, _tooLarge(e)));
      return;
    }
    final r = classifyPushError(status, e.body);
    switch (r) {
      case DriftRejection():
        _evidence = true;
        b.add(await _handleDrift(gen, pair.pending, r));
        // The correction may have re-stamped other changes in this batch,
        // so its remaining records are stale.
        throw const _Restart();
      case final PushRejection verdict:
        b.add(_verdict(pair.pending, verdict));
      case null:
        b.deferred.add((
          pair.pending,
          UnclassifiedRejection(status, serverMessage(e.body) ?? e.message),
        ));
    }
  }

  /// Drops a rejected change and restores the server's value for its field
  /// (or its whole document, for a record delete), then re-applies later
  /// pending changes on it.
  ///
  /// The server's value is fetched first, page by page with a pk and field
  /// filter until the pages are exhausted (Go filters after its LIMIT, so a
  /// single request could miss it). Only when that succeeds is the change
  /// discarded, the field dropped, the fetched changes applied and the later
  /// pending changes re-applied, in one store transaction. When the fetch
  /// fails (offline, 401, 404, a cancellation) nothing changes: the change
  /// stays marked rejected and the error propagates (a [TransportError],
  /// [NetworkError], or a [CrdtError] with [CrdtErrorCode.cancelled]).
  ///
  /// Waits for a run in flight first, and is itself cancelled and awaited by
  /// [stop] and [dispose]. A key that is not a rejected change is ignored.
  /// Throws a [StateError] in the `gone` and `unauthorized` states.
  Future<void> discardRejected(String key) {
    if (_disposed) {
      return Future.error(StateError('crdt: the sync engine is disposed'));
    }
    final gen = _generation;
    final future = _discard(gen, key);
    _discards.add(future);
    return future.whenComplete(() => _discards.remove(future));
  }

  Future<void> _discard(int gen, String key) async {
    try {
      final running = _inFlight;
      if (running != null) {
        try {
          await running;
        } on Object {
          // The run's outcome belongs to whoever called sync().
        }
      }
      await store.ready;
      _checkCancelled(gen);
      if (_state == SyncEngineState.gone ||
          _state == SyncEngineState.unauthorized) {
        throw StateError(
          'crdt: the sync engine is ${_state.name}; resume it first',
        );
      }
      final target = store.pending.where((p) => p.key == key).firstOrNull;
      if (target == null || !target.isRejected) return;
      final c = target.change;
      final recordDelete =
          c.tombstone && !(c.crdtType == CrdtType.document && c.value != null);
      final fetched = <ChangeRecord>[];
      await _paged(
        gen,
        c.table,
        null,
        (changes, _) async => fetched.addAll(changes),
        filter: SyncFilter(
          pkFilter: [c.pk],
          fieldFilter: recordDelete ? const [] : [c.field],
        ),
      );
      _checkCancelled(gen);
      final current = store.pending.where((p) => p.key == key).firstOrNull;
      if (current == null || !current.isRejected) return;
      store.transact(() {
        store.discardPending(key);
        if (recordDelete) {
          store.dropDocument(c.table, c.pk);
        } else {
          store.dropField(c.table, c.pk, c.field);
        }
        store.applyChanges(fetched);
        store.applyChanges([
          for (final q in store.pending)
            if (q.change.table == c.table &&
                q.change.pk == c.pk &&
                (recordDelete || q.change.field == c.field))
              q.change,
        ]);
      });
      _emit(ChangesApplied({(table: c.table, pk: c.pk)}));
    } on _Stop catch (stop) {
      // The state and event already say gone or unauthorized.
      throw stop.error;
    } on _Cancelled {
      throw CrdtError(
        'crdt: the discard was cancelled',
        code: CrdtErrorCode.cancelled,
      );
    }
  }

  /// Stops the engine (see [stop]), waits for the run and any discard in
  /// flight, detaches every attached stream and closes [events]. A second
  /// call returns the first call's future.
  Future<void> dispose() => _disposing ??= _dispose();

  Future<void> _dispose() async {
    _disposed = true;
    await stop();
    for (final detach in _streamHandlers.toList()) {
      detach();
    }
    await _events.close();
  }
}
