// SyncEngine behaviour crdt-js lacks: per-table paging, batching, rejection
// handling, clock correction, terminal states, cancellation and stream
// reconnects. Runs against FakeServer, which models the Go server.
import 'dart:async';

import 'package:fake_async/fake_async.dart';
import 'package:grove_crdt/grove_crdt.dart';
import 'package:test/test.dart';

import 'support/fake_server.dart';
import 'support/store_fakes.dart';

({CrdtStore store, SyncEngine engine, FakeServer server, MapReplicaKeyValue kv})
setup({
  FakeServer? server,
  int pushBatchSize = 500,
  int Function()? now,
  ClockSkew? skew,
  MapReplicaKeyValue? kv,
  int serverErrorLimit = 3,
  Transport? transport,
  ReplicaStorage? storage,
}) {
  final srv = server ?? FakeServer();
  final store_ = kv ?? MapReplicaKeyValue();
  final clock = HybridClock('dev', nowMs: now ?? () => 1000);
  final client = CrdtClient(
    nodeId: 'dev',
    transport: transport ?? srv,
    clock: clock,
  );
  final store = CrdtStore(
    'dev',
    clock,
    persistDebounce: Duration.zero,
    storage: storage,
  );
  final engine = SyncEngine(
    client,
    store,
    tables: const ['notes'],
    cursors: KeyValueReplicaStorage(store_),
    pushBatchSize: pushBatchSize,
    skew: skew,
    serverErrorLimit: serverErrorLimit,
  );
  return (store: store, engine: engine, server: srv, kv: store_);
}

TransportError _status(int status, [String error = 'failed']) =>
    TransportError('fail', statusCode: status, body: {'error': error});

final Matcher _isCancelled = isA<CrdtError>().having(
  (e) => e.code,
  'code',
  CrdtErrorCode.cancelled,
);

Future<void> _until(bool Function() done) async {
  for (var i = 0; i < 200 && !done(); i++) {
    await pumpEventQueue(times: 1);
  }
  expect(done(), isTrue, reason: 'condition never held');
}

void main() {
  test('pull drains a multi-page backlog per table', () async {
    final srv = FakeServer(pageLimit: 10)..seed('notes', 25);
    final s = setup(server: srv);
    final report = await s.engine.sync();
    expect(s.store.getCollection('notes'), hasLength(25));
    expect(report.pulled, greaterThanOrEqualTo(25));
    expect(srv.pulls.every((p) => p.tables.length == 1), isTrue);
  });

  test('changes sharing (ts, c) across a page boundary all arrive', () async {
    // Page one ends on (7, 0, a); (7, 0, b) shares its (ts, c). A cursor of
    // (7, 0) would skip b, because the server ignores the node id.
    final srv = FakeServer(pageLimit: 2);
    for (final (ts, node) in [(6, 'x'), (7, 'a'), (7, 'b')]) {
      srv.log.add(
        ChangeRecord(
          table: 'notes',
          pk: node,
          field: 'f',
          crdtType: CrdtType.lww,
          hlc: HLC(BigInt.from(ts), 0, node),
          nodeId: node,
          value: const JsonValue(1),
        ),
      );
    }
    final s = setup(server: srv);
    await s.engine.sync();
    expect(
      s.store.getCollection('notes').map((d) => d['_pk']),
      unorderedEquals(['x', 'a', 'b']),
    );
  });

  test('cursors persist and the next run resumes from them', () async {
    final srv = FakeServer()..seed('notes', 5);
    final first = setup(server: srv);
    await first.engine.sync();
    final again = setup(server: srv, kv: first.kv);
    srv.pulls.clear();
    await again.engine.sync();
    // Just below the stored cursor (5, 0).
    expect(srv.pulls.first.since.ts, BigInt.from(4));
    expect(srv.pulls.first.since.c, 4294967295);
  });

  test(
    'pushes in batches of pushBatchSize and clears only pushed keys',
    () async {
      final s = setup(pushBatchSize: 2);
      for (var i = 0; i < 5; i++) {
        s.store.setField('notes', 'n$i', 'title', 't$i');
      }
      final report = await s.engine.sync();
      expect(s.server.pushes.map((p) => p.length), [2, 2, 1]);
      expect(report.pushed, 5);
      expect(s.store.pendingCount, 0);
    },
  );

  for (final atomic in [false, true]) {
    final behaviour = atomic ? 'fix/crdt-sync-defects' : 'grove v1.7.0';
    test('a hook rejection is isolated by bisection ($behaviour)', () async {
      final s = setup(server: FakeServer(atomicHooks: atomic));
      final events = <SyncEngineEvent>[];
      s.engine.events.listen(events.add);
      s.store.setField('notes', 'n1', 'title', 'a');
      final locked = s.store.setField('notes', 'n1', 'locked', 'x')!;
      s.store.setField('notes', 'n2', 'title', 'b');
      s.server.rejectField = 'locked';
      final report = await s.engine.sync();
      expect(report.rejected, 1);
      expect(s.store.pendingCount, 0);
      expect(s.store.pending.single.key, pendingKey(locked));
      expect(s.store.pending.single.rejection!.reason, 'locked is locked');
      expect(s.store.pending.single.rejection!.kind, 'hook');
      expect(s.server.log.map((c) => (c.pk, c.field)).toSet(), {
        ('n1', 'title'),
        ('n2', 'title'),
      });
      expect(
        events.whereType<ChangeRejected>().single.rejection,
        isA<HookRejection>(),
      );
      expect(s.store.getDocument('notes', 'n1')!['locked'], 'x');
    });
  }

  test(
    'bisection pushes the records beforePush rewrote, never the raw ones',
    () async {
      final s = setup();
      s.store.use(_Upper());
      s.store.setField('notes', 'n1', 'title', 'a');
      s.store.setField('notes', 'n1', 'locked', 'x');
      s.store.setField('notes', 'n2', 'title', 'b');
      s.server.rejectField = 'locked';
      await s.engine.sync();
      expect(s.server.pushes.expand((p) => p).map((c) => c.value).toSet(), {
        const JsonValue('A'),
        const JsonValue('X'),
        const JsonValue('B'),
      });
      expect(s.store.rejectedCount, 1);
      expect(s.store.pendingCount, 0);
    },
  );

  test(
    'a validation rejection marks that change and pushes the rest',
    () async {
      final s = setup();
      final srv = s.server;
      final bad = s.store.setField('notes', '', 'title', 'no pk')!;
      s.store.setField('notes', 'n2', 'title', 'ok');
      var first = true;
      final wrapped = _Intercept(srv, (req) {
        if (first) {
          first = false;
          throw TransportError(
            'v',
            statusCode: 500,
            body: {'error': 'crdt: change[0]: crdt: change pk is required'},
          );
        }
      });
      final s2 = SyncEngine(
        CrdtClient(nodeId: 'dev', transport: wrapped, clock: s.store.clock),
        s.store,
        tables: const ['notes'],
      );
      await s2.sync();
      expect(s.store.pending.single.key, pendingKey(bad));
      expect(s.store.pending.single.rejection!.kind, 'validation');
      expect(srv.log.single.pk, 'n2');
    },
  );

  test(
    'a drift rejection re-stamps the change once and the retry succeeds',
    () async {
      var now = DateTime.utc(2026, 10, 1).millisecondsSinceEpoch;
      final serverNow =
          BigInt.from(DateTime.utc(2026, 10, 4).millisecondsSinceEpoch) *
          BigInt.from(1000000);
      final srv = FakeServer(validateDrift: () => serverNow);
      final s = setup(server: srv, now: () => now);
      final old = s.store.setField('notes', 'n1', 'title', 'offline edit')!;
      now = DateTime.utc(2026, 10, 4).millisecondsSinceEpoch;
      await s.engine.sync();
      expect(s.store.pending, isEmpty);
      expect(srv.log.single.hlc.isAfter(old.hlc), isTrue);
      expect(srv.log.single.value, const JsonValue('offline edit'));
    },
  );

  test('a past-dated change goes through as it is when the server only '
      'rejects future drift (fix/crdt-sync-defects)', () async {
    var now = DateTime.utc(2026, 10, 1).millisecondsSinceEpoch;
    final serverNow =
        BigInt.from(DateTime.utc(2026, 10, 4).millisecondsSinceEpoch) *
        BigInt.from(1000000);
    final srv = FakeServer(
      validateDrift: () => serverNow,
      futureDriftOnly: true,
    );
    final s = setup(server: srv, now: () => now);
    final old = s.store.setField('notes', 'n1', 'title', 'offline edit')!;
    now = DateTime.utc(2026, 10, 4).millisecondsSinceEpoch;
    await s.engine.sync();
    expect(s.store.pending, isEmpty);
    expect(srv.log.single.hlc, old.hlc);
  });

  test(
    'a change refused for drift again after its re-stamp is marked',
    () async {
      final s = setup();
      final edit = s.store.setField('notes', 'n1', 'title', 'x')!;
      s.server.onPush = (req) => throw _status(
        500,
        'crdt: change[0]: crdt: change HLC timestamp drift too large (2h0m0s)',
      );
      final report = await s.engine.sync();
      expect(report.rejected, 1);
      expect(s.server.pushes, hasLength(2));
      final p = s.store.pending.single;
      expect(p.restamps, 1);
      expect(p.rejection!.kind, 'drift');
      expect(p.change.hlc.isAfter(edit.hlc), isTrue);
    },
  );

  test('future-dated pending changes are re-stamped before push after a clock rebase', () async {
    final real = DateTime.utc(2026, 10, 4, 12).millisecondsSinceEpoch;
    final ahead = real + const Duration(hours: 3).inMilliseconds;
    final skew = ClockSkew(systemNowMs: () => ahead);
    final s = setup(now: () => ahead, skew: skew);
    final events = <SyncEngineEvent>[];
    s.engine.events.listen(events.add);
    final edit = s.store.setField('notes', 'n1', 'title', 'from the future')!;
    skew.observe(DateTime.fromMillisecondsSinceEpoch(real, isUtc: true));
    await s.engine.sync();
    final pushed = s.server.log.single;
    expect(pushed.hlc.ts < edit.hlc.ts, isTrue);
    expect(pushed.nodeId, 'dev~1');
    expect(events.whereType<ClockRebased>().single.nodeId, 'dev~1');
    expect(s.store.setField('notes', 'n2', 'title', 'new')!.nodeId, 'dev~1');
  });

  test(
    'the clock epoch persists, so a later rebase never reuses a node id',
    () async {
      final real = DateTime.utc(2026, 10, 4, 12).millisecondsSinceEpoch;
      final ahead = real + const Duration(hours: 3).inMilliseconds;
      final kv = MapReplicaKeyValue();
      for (final expected in ['dev~1', 'dev~2']) {
        final skew = ClockSkew(systemNowMs: () => ahead);
        final s = setup(now: () => ahead, skew: skew, kv: kv);
        s.store.setField('notes', 'n1', 'title', 'x');
        skew.observe(DateTime.fromMillisecondsSinceEpoch(real, isUtc: true));
        await s.engine.sync();
        expect(s.server.log.single.nodeId, expected);
      }
    },
  );

  test(
    'a forward correction re-stamps nothing and never rewinds the clock',
    () async {
      // The device runs an hour behind: the correction moves time forward, so
      // no stamp is in the future and the node id stays.
      final real = DateTime.utc(2026, 10, 4, 12).millisecondsSinceEpoch;
      final behind = real - const Duration(hours: 1).inMilliseconds;
      final skew = ClockSkew(systemNowMs: () => behind);
      final s = setup(now: () => behind, skew: skew);
      final events = <SyncEngineEvent>[];
      s.engine.events.listen(events.add);
      final edit = s.store.setField('notes', 'n1', 'title', 'x')!;
      skew.observe(DateTime.fromMillisecondsSinceEpoch(real, isUtc: true));
      await s.engine.sync();
      expect(events.whereType<ClockRebased>(), isEmpty);
      expect(s.server.log.single.hlc, edit.hlc);
      final next = s.store.setField('notes', 'n2', 'title', 'y')!;
      expect(next.nodeId, 'dev');
      expect(next.hlc.isAfter(edit.hlc), isTrue);
    },
  );

  test('404 stops the engine and keeps pending', () async {
    final s = setup();
    s.store.setField('notes', 'n1', 'title', 'x');
    s.server.failStatus = 404;
    final events = <SyncEngineEvent>[];
    s.engine.events.listen(events.add);
    await s.engine.sync();
    expect(s.engine.state, SyncEngineState.gone);
    expect(events.whereType<DatasetGone>(), hasLength(1));
    final calls = s.server.pulls.length + s.server.pushes.length;
    await s.engine.sync();
    expect(s.server.pulls.length + s.server.pushes.length, calls);
    expect(s.store.pendingCount, 1);
  });

  test('401 requires auth until resume', () async {
    final s = setup();
    s.server.failStatus = 401;
    await s.engine.sync();
    expect(s.engine.state, SyncEngineState.unauthorized);
    s.server.failStatus = null;
    s.engine.resume();
    await s.engine.sync();
    expect(s.engine.state, SyncEngineState.idle);
  });

  test('an oversized batch shrinks to the server limit', () async {
    final srv = FakeServer(maxChangesPerPush: 3);
    final s = setup(server: srv, pushBatchSize: 10);
    for (var i = 0; i < 7; i++) {
      s.store.setField('notes', 'n$i', 'title', 't$i');
    }
    await s.engine.sync();
    expect(s.store.pendingCount, 0);
    expect(srv.log, hasLength(7));
  });

  test('discardRejected restores the server value and re-applies later pending edits', () async {
    final s = setup();
    s.server.log.add(
      ChangeRecord(
        table: 'notes',
        pk: 'n1',
        field: 'locked',
        crdtType: CrdtType.lww,
        hlc: HLC(BigInt.from(1), 0, 'srv'),
        nodeId: 'srv',
        value: const JsonValue('server'),
      ),
    );
    await s.engine.sync();
    final rejected = s.store.setField('notes', 'n1', 'locked', 'mine')!;
    s.server.rejectField = 'locked';
    await s.engine.sync();
    s.server.rejectField = null;
    await s.engine.discardRejected(pendingKey(rejected));
    expect(s.store.getDocument('notes', 'n1')!['locked'], 'server');
    expect(s.store.pending, isEmpty);
  });

  group('HTTP status mapping', () {
    for (final status in [401, 403]) {
      test(
        '$status on a push moves to unauthorized and keeps the queue',
        () async {
          final s = setup();
          final events = <SyncEngineEvent>[];
          s.engine.events.listen(events.add);
          s.store.setField('notes', 'n1', 'title', 'x');
          s.server.onPush = (_) => throw _status(status);
          final report = await s.engine.sync();
          expect(report.pushed, 0);
          expect(s.engine.state, SyncEngineState.unauthorized);
          expect(events.whereType<AuthRequired>().single.status, status);
          expect(s.store.pendingCount, 1);
          expect(s.store.rejectedCount, 0);
        },
      );
    }

    for (final status in [404, 410]) {
      test('$status on a push moves to gone and keeps the queue', () async {
        final s = setup();
        final events = <SyncEngineEvent>[];
        s.engine.events.listen(events.add);
        s.store.setField('notes', 'n1', 'title', 'x');
        s.server.onPush = (_) => throw _status(status, 'no such dataset');
        await s.engine.sync();
        expect(s.engine.state, SyncEngineState.gone);
        final gone = events.whereType<DatasetGone>().single;
        expect(gone.status, status);
        expect(gone.message, 'no such dataset');
        expect(s.store.pendingCount, 1);
        expect(s.store.rejectedCount, 0);
      });
    }

    for (final status in [408, 429, 502, 503, 504]) {
      test('$status aborts the run and marks nothing', () async {
        final s = setup();
        final events = <SyncEngineEvent>[];
        s.engine.events.listen(events.add);
        s.store.setField('notes', 'n1', 'title', 'x');
        s.store.setField('notes', 'n2', 'title', 'y');
        s.server.onPush = (_) => throw _status(status);
        await expectLater(
          s.engine.sync(),
          throwsA(
            isA<TransportError>().having(
              (e) => e.statusCode,
              'statusCode',
              status,
            ),
          ),
        );
        expect(s.server.pushes, hasLength(1));
        expect(s.engine.state, SyncEngineState.offline);
        expect(events.whereType<SyncInterrupted>(), hasLength(1));
        expect(s.store.pendingCount, 2);
        expect(s.store.rejectedCount, 0);
      });
    }

    test('a network error aborts the run and marks nothing', () async {
      final s = setup();
      s.store.setField('notes', 'n1', 'title', 'x');
      s.server.onPush = (_) => throw NetworkError('offline');
      await expectLater(s.engine.sync(), throwsA(isA<NetworkError>()));
      expect(s.engine.state, SyncEngineState.offline);
      expect(s.store.pendingCount, 1);
    });

    test('413 halves the batch until it fits', () async {
      final s = setup(pushBatchSize: 10);
      for (var i = 0; i < 5; i++) {
        s.store.setField('notes', 'n$i', 'title', 't$i');
      }
      s.server.onPush = (req) {
        if (req.changes.length > 2) throw _status(413, 'request too large');
      };
      await s.engine.sync();
      expect(s.engine.pushBatchSize, 2);
      expect(s.server.log, hasLength(5));
      expect(s.store.pendingCount, 0);
    });

    test('413 on a single change marks it, so the rest still flow', () async {
      final s = setup();
      final big = s.store.setField('notes', 'n1', 'body', 'huge')!;
      s.store.setField('notes', 'n2', 'title', 'small');
      s.server.onPush = (req) {
        if (req.changes.any((c) => c.field == 'body')) {
          throw _status(413, 'request too large');
        }
      };
      final report = await s.engine.sync();
      expect(report.rejected, 1);
      expect(s.store.pending.single.key, pendingKey(big));
      expect(s.store.pending.single.rejection!.kind, 'bad_request');
      expect(s.server.log.single.pk, 'n2');
    });

    test('an unclassified 500 retries next run and is bisected after serverErrorLimit', () async {
      final s = setup(serverErrorLimit: 3);
      s.store.setField('notes', 'n1', 'title', 'fine');
      final poison = s.store.setField('notes', 'n2', 'title', 'poison')!;
      s.server.onPush = (req) {
        if (req.changes.any((c) => c.pk == 'n2')) {
          throw _status(500, 'database is locked');
        }
      };
      for (var run = 1; run < 3; run++) {
        await expectLater(s.engine.sync(), throwsA(isA<TransportError>()));
        expect(s.store.pendingCount, 2, reason: 'run $run marks nothing');
      }
      final report = await s.engine.sync();
      expect(report.rejected, 1);
      expect(s.store.pending.single.key, pendingKey(poison));
      expect(s.store.pending.single.rejection!.kind, 'server');
      expect(s.server.log.single.pk, 'n1');
    });

    test('a server failing every push never gets the queue marked', () async {
      final s = setup(serverErrorLimit: 2);
      for (var i = 0; i < 4; i++) {
        s.store.setField('notes', 'n$i', 'title', 't$i');
      }
      s.server.onPush = (_) => throw _status(500, 'panic');
      for (var run = 0; run < 6; run++) {
        await expectLater(s.engine.sync(), throwsA(isA<TransportError>()));
      }
      expect(s.store.pendingCount, 4);
      expect(s.store.rejectedCount, 0);
      // Runs 2, 4 and 6 bisected before giving up.
      expect(s.server.pushes.length, greaterThan(6));
    });

    test(
      'a 400 without a recognized message bisects and marks bad_request',
      () async {
        final s = setup();
        s.store.setField('notes', 'n1', 'title', 'a');
        final bad = s.store.setField('notes', 'n2', 'title', 'b')!;
        s.server.onPush = (req) {
          if (req.changes.any((c) => c.pk == 'n2')) {
            throw _status(400, 'malformed');
          }
        };
        final report = await s.engine.sync();
        expect(report.rejected, 1);
        expect(s.store.pending.single.key, pendingKey(bad));
        expect(s.store.pending.single.rejection!.kind, 'bad_request');
        expect(s.server.log.single.pk, 'n1');
      },
    );

    test(
      'a transient status during bisection aborts without marking',
      () async {
        final s = setup();
        s.store.setField('notes', 'n1', 'locked', 'a');
        s.store.setField('notes', 'n2', 'title', 'b');
        s.server.rejectField = 'locked';
        var calls = 0;
        s.server.onPush = (req) {
          if (++calls == 2) throw _status(503);
        };
        await expectLater(s.engine.sync(), throwsA(isA<TransportError>()));
        expect(s.store.rejectedCount, 0);
        expect(s.store.pendingCount, 2);
      },
    );
  });

  group('store readiness', () {
    test('a run waits for store.ready before applying or pushing', () async {
      final storage = RecordingStorage()
        ..preloadedPending = [
          PendingChange(
            ChangeRecord(
              table: 'notes',
              pk: 'n1',
              field: 'title',
              crdtType: CrdtType.lww,
              hlc: HLC(BigInt.from(500), 0, 'dev'),
              nodeId: 'dev',
              value: const JsonValue('queued offline'),
            ),
          ),
        ];
      final srv = FakeServer()..seed('notes', 2);
      final s = setup(server: srv, storage: storage);
      await s.engine.sync();
      expect(s.store.getCollection('notes'), hasLength(2));
      expect(
        srv.log.map((c) => c.value),
        contains(const JsonValue('queued offline')),
      );
      expect(s.store.pending, isEmpty);
    });

    test(
      'an unreadable replica surfaces ReplicaUnavailable and sends nothing',
      () async {
        final storage = RecordingStorage()
          ..failLoadPending = StateError('disk');
        final s = setup(storage: storage);
        final events = <SyncEngineEvent>[];
        s.engine.events.listen(events.add);
        await expectLater(s.engine.sync(), throwsA(isA<ReplicaUnavailable>()));
        expect(s.server.requests, 0);
        expect(events.single, isA<SyncInterrupted>());
      },
    );
  });

  group('cancellation', () {
    test('stop while a push is held open sends nothing more and the late '
        'answer clears nothing', () async {
      final s = setup(pushBatchSize: 1);
      s.store.setField('notes', 'n1', 'title', 'a');
      s.store.setField('notes', 'n2', 'title', 'b');
      final gate = s.server.pushGate = Completer<void>();
      final events = <SyncEngineEvent>[];
      s.engine.events.listen(events.add);

      final run = s.engine.sync();
      Object? runError;
      unawaited(run.then<void>((_) {}, onError: (Object e) => runError = e));
      await _until(() => s.server.pushesInFlight == 1);
      final requestsAtStop = s.server.requests;

      await s.engine.stop();
      expect(runError, _isCancelled);
      expect(s.engine.state, SyncEngineState.idle);

      // The held push lands after stop: the server merged it, but the run
      // is over, so nothing is cleared.
      gate.complete();
      await pumpEventQueue();
      expect(s.server.log, hasLength(1));
      expect(s.store.pendingCount, 2);
      expect(s.server.requests, requestsAtStop);
      expect(events.whereType<SyncInterrupted>(), isEmpty);
      expect(events.whereType<SyncSucceeded>(), isEmpty);
    });

    test('a transport that throws CrdtError(cancelled) mid-pull ends the run '
        'at the last applied page', () async {
      final srv = FakeServer(pageLimit: 2)..seed('notes', 6);
      final s = setup(server: srv);
      s.store.setField('notes', 'mine', 'title', 'pending');
      final cancel = CrdtError('switched away', code: CrdtErrorCode.cancelled);
      srv.onPull = (req) {
        if (srv.pulls.length == 2) throw cancel;
      };
      final events = <SyncEngineEvent>[];
      s.engine.events.listen(events.add);

      await expectLater(s.engine.sync(), throwsA(same(cancel)));
      expect(srv.pulls, hasLength(2));
      expect(srv.pushes, isEmpty);
      expect(s.engine.cursor('notes'), HLC(BigInt.from(2), 0, 'srv'));
      final stored = await KeyValueReplicaStorage(s.kv).readCursor('notes');
      expect(stored, HLC(BigInt.from(2), 0, 'srv'));
      expect(s.store.getCollection('notes'), hasLength(3));
      expect(s.store.pendingCount, 1);
      expect(s.engine.state, SyncEngineState.idle);
      expect(events.whereType<SyncInterrupted>(), isEmpty);
    });

    test('a cancellation from a push ends the run with no retry', () async {
      final s = setup(pushBatchSize: 1);
      s.store.setField('notes', 'n1', 'title', 'a');
      s.store.setField('notes', 'n2', 'title', 'b');
      s.server.onPush = (_) =>
          throw CrdtError('switched', code: CrdtErrorCode.cancelled);
      await expectLater(s.engine.sync(), throwsA(_isCancelled));
      expect(s.server.pushes, hasLength(1));
      expect(s.store.pendingCount, 2);
      expect(s.engine.state, SyncEngineState.idle);
    });

    test('a pull answer that lands after stop applies nothing', () async {
      final srv = FakeServer()..seed('notes', 3);
      final s = setup(server: srv);
      final gate = srv.pullGate = Completer<void>();
      final run = s.engine.sync()..ignore();
      await _until(() => srv.pulls.length == 1);
      await s.engine.stop();
      gate.complete();
      await pumpEventQueue();
      await expectLater(run, throwsA(_isCancelled));
      expect(s.store.getCollection('notes'), isEmpty);
      expect(s.engine.cursor('notes'), isNull);
    });

    test('dispose awaits the in-flight run', () async {
      final srv = FakeServer()..seed('notes', 2);
      final gate = Completer<void>();
      final cursors = _GatedCursors(gate.future);
      final clock = HybridClock('dev', nowMs: () => 1000);
      final store = CrdtStore('dev', clock, persistDebounce: Duration.zero);
      final engine = SyncEngine(
        CrdtClient(nodeId: 'dev', transport: srv, clock: clock),
        store,
        tables: const ['notes'],
        cursors: cursors,
      );
      var runSettled = false;
      unawaited(
        engine.sync().then<void>(
          (_) => runSettled = true,
          onError: (Object _) => runSettled = true,
        ),
      );
      await _until(() => cursors.writes == 1);

      var disposed = false;
      final disposing = engine.dispose().then((_) => disposed = true);
      await pumpEventQueue();
      expect(disposed, isFalse, reason: 'the run is still writing its cursor');
      expect(runSettled, isFalse);

      gate.complete();
      await disposing;
      expect(runSettled, isTrue);
      expect(srv.pulls, hasLength(1), reason: 'no page after the cancel');
      expect(cursors.writes, 1);
      expect(() => engine.sync(), throwsStateError);
    });

    test('dispose closes events and a stopped engine can sync again', () async {
      final s = setup();
      var closed = false;
      s.engine.events.listen((_) {}, onDone: () => closed = true);
      await s.engine.stop();
      s.store.setField('notes', 'n1', 'title', 'x');
      await s.engine.sync();
      expect(s.store.pendingCount, 0);
      await s.engine.dispose();
      expect(closed, isTrue);
    });
  });

  group('timer', () {
    test(
      'start syncs on the interval, stop halts it and swallows failures',
      () {
        fakeAsync((async) {
          final s = setup();
          final stop = s.engine.start(interval: const Duration(seconds: 30));
          s.store.setField('notes', 'n1', 'title', 'x');
          async.elapse(const Duration(seconds: 29));
          expect(s.server.pushes, isEmpty);
          async.elapse(const Duration(seconds: 2));
          expect(s.server.pushes, hasLength(1));

          s.server.failStatus = 503;
          s.store.setField('notes', 'n2', 'title', 'y');
          async.elapse(const Duration(seconds: 30));
          expect(s.engine.state, SyncEngineState.offline);

          s.server.failStatus = null;
          stop();
          async.flushMicrotasks();
          final requests = s.server.requests;
          async.elapse(const Duration(minutes: 5));
          expect(s.server.requests, requests);
          expect(s.store.pendingCount, 1);
        });
      },
    );
  });

  group('stream', () {
    test('an idle reconnect starts no run; a normal reconnect does', () async {
      final s = setup();
      final sub = _Subscription();
      s.engine.attachStream(sub);
      await pumpEventQueue();

      sub
        ..emit(const StreamDisconnected(reason: ConnectionReason.idle))
        ..emit(const StreamConnected(reason: ConnectionReason.idle));
      await pumpEventQueue();
      expect(s.server.requests, 0);
      expect(s.engine.state, SyncEngineState.idle);

      sub
        ..emit(const StreamDisconnected())
        ..emit(const StreamConnected());
      await pumpEventQueue();
      expect(s.server.pulls, isNotEmpty);
    });

    test('streamed changes are applied and announced', () async {
      final s = setup();
      final sub = _Subscription();
      final events = <SyncEngineEvent>[];
      s.engine.events.listen(events.add);
      s.engine.attachStream(sub);
      await pumpEventQueue();
      sub.emit(
        StreamChange(
          ChangeRecord(
            table: 'notes',
            pk: 'n1',
            field: 'title',
            crdtType: CrdtType.lww,
            hlc: HLC(BigInt.from(7), 0, 'peer'),
            nodeId: 'peer',
            value: const JsonValue('live'),
          ),
        ),
      );
      expect(s.store.getDocument('notes', 'n1')!['title'], 'live');
      expect(events.whereType<ChangesApplied>().single.affected, {
        (table: 'notes', pk: 'n1'),
      });
      expect(s.engine.cursor('notes'), isNull);
    });

    test('a stream error is left to the stream; after stop or detach a '
        'reconnect starts nothing', () async {
      final s = setup();
      final sub = _Subscription();
      final detach = s.engine.attachStream(sub);
      await pumpEventQueue();
      sub.emit(StreamError(NetworkError('blip')));
      await pumpEventQueue();
      expect(s.engine.state, SyncEngineState.idle);
      expect(s.server.requests, 0);

      await s.engine.stop();
      sub.emit(const StreamConnected());
      await pumpEventQueue();
      expect(s.server.requests, 0);

      s.engine.start(interval: const Duration(hours: 1));
      detach();
      expect(sub.handlers, isEmpty);
      await s.engine.dispose();
    });
  });
}

/// A beforePush hook that rewrites every value, as an encryptor would.
final class _Upper extends StorePlugin {
  @override
  String get name => 'upper';

  @override
  List<ChangeRecord>? beforePush(List<ChangeRecord> changes) => [
    for (final c in changes)
      c.copyWith(value: JsonValue((c.value!.value! as String).toUpperCase())),
  ];
}

final class _Intercept implements Transport {
  _Intercept(this.inner, this.beforePush);
  final Transport inner;
  final void Function(PushRequest req) beforePush;

  @override
  Future<PullResponse> pull(PullRequest req) => inner.pull(req);

  @override
  Future<PushResponse> push(PushRequest req) {
    beforePush(req);
    return inner.push(req);
  }
}

/// A cursor store whose writes wait on a gate.
final class _GatedCursors implements SyncCursorStore {
  _GatedCursors(this.gate);

  final Future<void> gate;
  final Map<String, HLC> cursors = {};
  int writes = 0;

  @override
  Future<HLC?> readCursor(String table) async => cursors[table];

  @override
  Future<void> writeCursor(String table, HLC cursor) async {
    writes++;
    await gate;
    cursors[table] = cursor;
  }

  @override
  Future<String?> readMeta(String key) async => null;

  @override
  Future<void> writeMeta(String key, String value) async {}

  @override
  Future<void> clearAll() async {}
}

/// A subscription the test drives by hand.
final class _Subscription implements CrdtSubscription {
  final handlers = <void Function(CrdtStreamEvent)>[];

  void emit(CrdtStreamEvent e) {
    for (final h in handlers.toList()) {
      h(e);
    }
  }

  @override
  void Function() on(void Function(CrdtStreamEvent event) handler) {
    handlers.add(handler);
    return () => handlers.remove(handler);
  }

  @override
  void connect() {}

  @override
  void disconnect() {}

  @override
  bool get connected => false;

  @override
  HLC? get lastHlc => null;
}
