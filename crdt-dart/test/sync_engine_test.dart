// SyncEngine behaviour crdt-js lacks: per-table paging, batching, rejection
// handling, clock correction, terminal states, cancellation and stream
// reconnects. Runs against FakeServer, which models the Go server.
import 'dart:async';

import 'package:fake_async/fake_async.dart';
import 'package:grove_crdt/grove_crdt.dart';
import 'package:test/test.dart';

import 'support/fake_server.dart';
import 'support/store_fakes.dart';

typedef Setup = ({
  CrdtStore store,
  SyncEngine engine,
  FakeServer server,
  MapReplicaKeyValue kv,
});

Setup setup({
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
      expect(s.server.log, hasLength(5));
      expect(s.store.pendingCount, 0);
      expect(
        s.server.pushes.where((p) => p.length <= 2).expand((p) => p),
        hasLength(5),
      );
    });

    test(
      'the batch size doubles back to the configured size after a 413',
      () async {
        final s = setup(pushBatchSize: 8);
        var limit = 2;
        s.server.onPush = (req) {
          if (req.changes.length > limit) {
            throw _status(413, 'request too large');
          }
        };
        for (var i = 0; i < 3; i++) {
          s.store.setField('notes', 'a$i', 'title', 't$i');
        }
        await s.engine.sync();
        // 3 refused, then 1 and 2 went through, doubling 1 to 4.
        expect(s.server.pushes.map((p) => p.length), [3, 1, 2]);
        expect(s.engine.pushBatchSize, 4);
        limit = 100;
        for (var i = 0; i < 40; i++) {
          s.store.setField('notes', 'b$i', 'title', 't$i');
        }
        await s.engine.sync();
        expect(s.engine.pushBatchSize, 8);
        expect(s.store.pendingCount, 0);
      },
    );

    test(
      'growth stops at the server limit a BatchTooLargeRejection named',
      () async {
        final srv = FakeServer(maxChangesPerPush: 3);
        final s = setup(server: srv, pushBatchSize: 10);
        for (var i = 0; i < 20; i++) {
          s.store.setField('notes', 'n$i', 'title', 't$i');
        }
        await s.engine.sync();
        expect(srv.log, hasLength(20));
        expect(s.engine.pushBatchSize, 3);
        // Only the first oversized batch was refused.
        expect(srv.pushes.where((p) => p.length > 3), hasLength(1));
      },
    );

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

  group('WebSocket error frames (no HTTP status)', () {
    test(
      'a statusless hook rejection is classified and the queue drains past it',
      () async {
        final srv = FakeServer();
        final s = setup(server: srv, transport: _WsLike(srv));
        s.store.setField('notes', 'n1', 'title', 'fine');
        s.store.setField('notes', 'n2', 'locked', 'x');
        srv.rejectField = 'locked';
        final report = await s.engine.sync();
        expect(report.rejected, 1);
        expect(s.store.rejectedCount, 1);
        expect(s.store.pending.single.rejection!.kind, 'hook');
        // v1.7.0 merged n1 before the refusal; bisection pushed it again.
        expect(srv.log.map((c) => c.pk).toSet(), {'n1'});
        expect(s.store.pendingCount, 0);
      },
    );

    test('a statusless validation rejection marks its change', () async {
      final srv = FakeServer();
      final s = setup(server: srv, transport: _WsLike(srv));
      final bad = s.store.setField('notes', '', 'title', 'no pk')!;
      s.store.setField('notes', 'n2', 'title', 'ok');
      await s.engine.sync();
      expect(s.store.pending.single.key, pendingKey(bad));
      expect(s.store.pending.single.rejection!.kind, 'validation');
      expect(srv.log.single.pk, 'n2');
    });

    test('a statusless too-many error shrinks the batch', () async {
      final srv = FakeServer(maxChangesPerPush: 3);
      final s = setup(server: srv, transport: _WsLike(srv), pushBatchSize: 10);
      for (var i = 0; i < 7; i++) {
        s.store.setField('notes', 'n$i', 'title', 't$i');
      }
      await s.engine.sync();
      expect(srv.log, hasLength(7));
      expect(s.store.pendingCount, 0);
    });

    test('a statusless failure naming no rejection is transient: nothing is '
        'marked, counted or bisected', () async {
      final s = setup(serverErrorLimit: 2);
      s.store.setField('notes', 'n1', 'title', 'a');
      s.store.setField('notes', 'n2', 'title', 'b');
      s.server.onPush = (_) =>
          throw TransportError('CRDT ws: the WebSocket transport is closed');
      for (var run = 0; run < 6; run++) {
        await expectLater(s.engine.sync(), throwsA(isA<TransportError>()));
      }
      expect(s.server.pushes, hasLength(6));
      expect(s.store.rejectedCount, 0);
      expect(s.store.pendingCount, 2);
    });
  });

  group('discardRejected never loses the server value', () {
    test('it restores a value beyond the first page', () async {
      final srv = FakeServer(pageLimit: 5)..seed('notes', 8);
      final s = setup(server: srv);
      await s.engine.sync();
      expect(s.store.getCollection('notes'), hasLength(8));
      // r7 holds the newest server row, beyond the first page; Go filters
      // after its LIMIT, so a single filtered request would miss it.
      final rejected = s.store.setField('notes', 'r7', 'f', 'mine')!;
      srv.rejectField = 'f';
      await s.engine.sync();
      expect(s.store.rejectedCount, 1);
      srv.rejectField = null;
      await s.engine.discardRejected(pendingKey(rejected));
      expect(s.store.getDocument('notes', 'r7')?['f'], 7);
      expect(s.store.pending, isEmpty);
    });

    Future<({ChangeRecord rejected, Setup s})> rejectedOverServerValue() async {
      final s = setup();
      s.server.log
        ..add(
          ChangeRecord(
            table: 'notes',
            pk: 'n1',
            field: 'locked',
            crdtType: CrdtType.lww,
            hlc: HLC(BigInt.from(1), 0, 'srv'),
            nodeId: 'srv',
            value: const JsonValue('server'),
          ),
        )
        ..add(
          ChangeRecord(
            table: 'notes',
            pk: 'n2',
            field: 'x',
            crdtType: CrdtType.lww,
            hlc: HLC(BigInt.from(5), 0, 'srv'),
            nodeId: 'srv',
            value: const JsonValue('later'),
          ),
        );
      await s.engine.sync();
      final rejected = s.store.setField('notes', 'n1', 'locked', 'mine')!;
      s.server.rejectField = 'locked';
      await s.engine.sync();
      s.server.rejectField = null;
      return (rejected: rejected, s: s);
    }

    test('a discard whose re-pull fails offline changes nothing', () async {
      final r = await rejectedOverServerValue();
      final s = r.s;
      s.server.onPull = (_) => throw NetworkError('offline');
      await expectLater(
        s.engine.discardRejected(pendingKey(r.rejected)),
        throwsA(isA<NetworkError>()),
      );
      expect(s.store.pending.single.isRejected, isTrue);
      expect(s.store.getDocument('notes', 'n1')!['locked'], 'mine');

      s.server.onPull = null;
      await s.engine.sync();
      await s.engine.discardRejected(pendingKey(r.rejected));
      expect(s.store.getDocument('notes', 'n1')!['locked'], 'server');
      expect(s.store.pending, isEmpty);
    });

    test('a discard answered 401 changes nothing and needs auth', () async {
      final r = await rejectedOverServerValue();
      final s = r.s;
      s.server.onPull = (_) => throw _status(401);
      await expectLater(
        s.engine.discardRejected(pendingKey(r.rejected)),
        throwsA(
          isA<TransportError>().having((e) => e.statusCode, 'status', 401),
        ),
      );
      expect(s.engine.state, SyncEngineState.unauthorized);
      expect(s.store.pending.single.isRejected, isTrue);
      expect(s.store.getDocument('notes', 'n1')!['locked'], 'mine');
      await expectLater(
        s.engine.discardRejected(pendingKey(r.rejected)),
        throwsStateError,
      );
    });

    test(
      'a discard re-applies a later pending edit on the same field',
      () async {
        final r = await rejectedOverServerValue();
        final s = r.s;
        final later = s.store.setField('notes', 'n1', 'locked', 'later')!;
        await s.engine.discardRejected(pendingKey(r.rejected));
        expect(s.store.getDocument('notes', 'n1')!['locked'], 'later');
        expect(s.store.pending.single.key, pendingKey(later));
      },
    );

    test(
      'a newer value streamed during the fetch survives the discard',
      () async {
        final srv = FakeServer();
        final held = _Held(srv);
        final s = setup(server: srv, transport: held);
        final sub = _Subscription();
        s.engine.attachStream(sub);
        srv.log.add(
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
        srv.rejectField = 'locked';
        await s.engine.sync();
        srv.rejectField = null;
        final gate = held.gate = Completer<void>();
        // The discard's second page: its answer was computed before the edit.
        held.gateAt = srv.pulls.length + 2;
        final discard = s.engine.discardRejected(pendingKey(rejected));
        await _until(() => srv.pulls.length == held.gateAt);
        await pumpEventQueue();
        // Another user edits the field; the server streams it during the fetch.
        final newer = ChangeRecord(
          table: 'notes',
          pk: 'n1',
          field: 'locked',
          crdtType: CrdtType.lww,
          hlc: HLC(BigInt.two.pow(50), 0, 'srv'),
          nodeId: 'srv',
          value: const JsonValue('newer'),
        );
        srv.log.add(newer);
        sub.emit(StreamChange(newer));
        held.gate = null;
        gate.complete();
        await discard;
        expect(s.store.getDocument('notes', 'n1')?['locked'], 'newer');
        expect(s.store.pending, isEmpty);
      },
    );

    test(
      'a newer value during the fetch survives a transforming beforeMerge',
      () async {
        final s = setup();
        s.store.use(_Decrypt());
        final rejected = await _rejectLocked(s);
        expect(s.store.getDocument('notes', 'n1')?['locked'], 'mine');
        final gate = s.server.pullGate = Completer<void>();
        final discard = s.engine.discardRejected(pendingKey(rejected));
        await _until(() => s.server.pulls.length == 3);
        // Delivered as a stream would be: straight into applyChanges.
        s.store.applyChanges([
          ChangeRecord(
            table: 'notes',
            pk: 'n1',
            field: 'locked',
            crdtType: CrdtType.lww,
            hlc: HLC(BigInt.two.pow(50), 0, 'srv'),
            nodeId: 'srv',
            value: const JsonValue('newer'),
          ),
        ]);
        expect(s.store.getDocument('notes', 'n1')?['locked'], 'dec:newer');
        s.server.pullGate = null;
        // This fetch answer holds only the server's older value.
        gate.complete();
        await discard;
        expect(s.store.getDocument('notes', 'n1')?['locked'], 'dec:newer');
      },
    );

    test('a later pending edit is re-applied without beforeMerge', () async {
      final s = setup();
      s.store.use(_Decrypt());
      final rejected = await _rejectLocked(s);
      s.store.setField('notes', 'n1', 'locked', 'later');
      await s.engine.discardRejected(pendingKey(rejected));
      expect(s.store.getDocument('notes', 'n1')?['locked'], 'later');
    });

    for (final viaDispose in [false, true]) {
      final how = viaDispose ? 'dispose' : 'stop';
      test('$how during a discard sends nothing after it returns', () async {
        final s = setup();
        final rejected = s.store.setField('notes', 'n1', 'locked', 'mine')!;
        s.server.rejectField = 'locked';
        await s.engine.sync();
        expect(s.store.rejectedCount, 1);
        s.server.rejectField = null;
        // A run in flight, held on its pull.
        final gate = s.server.pullGate = Completer<void>();
        final run = s.engine.sync()..ignore();
        await _until(() => s.server.pulls.length == 2);
        // The user discards while the run is in flight; the discard waits.
        final discard = s.engine.discardRejected(pendingKey(rejected))
          ..ignore();
        await pumpEventQueue();
        s.server.pullGate = null;
        if (viaDispose) {
          await s.engine.dispose();
        } else {
          await s.engine.stop();
        }
        final at = s.server.requests;
        gate.complete();
        await expectLater(discard, throwsA(_isCancelled));
        await expectLater(run, throwsA(_isCancelled));
        await pumpEventQueue();
        expect(s.server.requests, at, reason: 'a pull was sent after $how');
        expect(s.store.pending.single.isRejected, isTrue);
      });
    }
  });

  group('no mark without evidence the server works', () {
    test(
      'with pushBatchSize 1 the queue drains past a change failing with 500',
      () async {
        final s = setup(pushBatchSize: 1);
        s.store.setField('notes', 'n1', 'title', 'poison');
        s.store.setField('notes', 'n2', 'title', 'good');
        s.server.onPush = (req) {
          if (req.changes.any((c) => c.pk == 'n1')) {
            throw _status(500, 'boom');
          }
        };
        for (var i = 0; i < 8; i++) {
          await expectLater(s.engine.sync(), throwsA(isA<TransportError>()));
        }
        expect(s.server.log.map((c) => c.pk), ['n2']);
        // After n2 went, no later run has evidence: n1 stays pushable.
        expect(s.store.rejectedCount, 0);
        expect(s.store.pendingCount, 1);
      },
    );

    test('a skipped change is marked once the rest of the run shows the server works', () async {
      final s = setup(pushBatchSize: 1, serverErrorLimit: 1);
      final events = <SyncEngineEvent>[];
      s.engine.events.listen(events.add);
      final poison = s.store.setField('notes', 'n1', 'title', 'poison')!;
      s.store.setField('notes', 'n2', 'title', 'good');
      s.server.onPush = (req) {
        if (req.changes.any((c) => c.pk == 'n1')) {
          throw _status(500, 'boom');
        }
      };
      final report = await s.engine.sync();
      expect(report.pushed, 1);
      expect(report.rejected, 1);
      expect(s.server.log.map((c) => c.pk), ['n2']);
      expect(s.store.pending.single.key, pendingKey(poison));
      expect(s.store.pending.single.rejection!.kind, 'server');
      expect(events.whereType<SyncInterrupted>(), isEmpty);
    });

    test('a later change on the skipped field never overtakes it', () async {
      final s = setup(pushBatchSize: 1);
      s.store.setField('notes', 'n1', 'title', 'poison');
      final later = s.store.setField('notes', 'n1', 'title', 'later')!;
      s.store.setField('notes', 'n1', 'body', 'other field');
      s.store.setField('notes', 'n2', 'title', 'other doc');
      s.server.onPush = (req) {
        if (req.changes.any((c) => c.value == const JsonValue('poison'))) {
          throw _status(500, 'boom');
        }
      };
      await expectLater(s.engine.sync(), throwsA(isA<TransportError>()));
      expect(
        s.server.log.map((c) => (c.pk, c.field)),
        unorderedEquals([('n1', 'body'), ('n2', 'title')]),
      );
      expect(
        s.server.pushes.expand((p) => p).map((c) => c.hlc),
        isNot(contains(later.hlc)),
      );
      expect(s.store.pendingCount, 2);
    });

    test(
      'a lone change failing with 500 is never marked, however many runs',
      () async {
        final s = setup(serverErrorLimit: 3);
        final events = <SyncEngineEvent>[];
        s.engine.events.listen(events.add);
        s.store.setField('notes', 'n1', 'title', 'only edit');
        s.server.onPush = (_) =>
            throw _status(500, 'crdt: read state: db down');
        for (var i = 0; i < 10; i++) {
          await expectLater(s.engine.sync(), throwsA(isA<TransportError>()));
        }
        expect(s.store.rejectedCount, 0);
        expect(s.store.pendingCount, 1);
        expect(events.whereType<SyncInterrupted>(), hasLength(10));
        expect(events.whereType<ChangeRejected>(), isEmpty);

        s.server.onPush = null;
        await s.engine.sync();
        expect(s.store.pendingCount, 0);
      },
    );

    test('a change merged earlier in the same run is evidence', () async {
      final s = setup(pushBatchSize: 1, serverErrorLimit: 1);
      s.store.setField('notes', 'n1', 'title', 'fine');
      final poison = s.store.setField('notes', 'n2', 'title', 'poison')!;
      s.server.onPush = (req) {
        if (req.changes.any((c) => c.pk == 'n2')) {
          throw _status(500, 'panic');
        }
      };
      final report = await s.engine.sync();
      expect(report.pushed, 1);
      expect(report.rejected, 1);
      expect(s.store.pending.single.key, pendingKey(poison));
      expect(s.store.pending.single.rejection!.kind, 'server');
    });

    test('failed timer runs back off, and a success restores the interval', () {
      fakeAsync((async) {
        final srv = FakeServer()..failStatus = 503;
        final clock = HybridClock('dev', nowMs: () => 1000);
        final store = CrdtStore('dev', clock, persistDebounce: Duration.zero);
        final engine = SyncEngine(
          CrdtClient(nodeId: 'dev', transport: srv, clock: clock),
          store,
          tables: const ['notes'],
          maxRetryDelay: const Duration(seconds: 80),
          random: () => 1.0,
        );
        engine.start(interval: const Duration(seconds: 10));
        int at(int seconds) {
          async.elapse(Duration(seconds: seconds) - async.elapsed);
          return srv.pulls.length;
        }

        // Fails at 10s, then waits 10, 20, 40 and 80 (the ceiling) seconds.
        expect(at(10), 1);
        expect(at(19), 1);
        expect(at(20), 2);
        expect(at(39), 2);
        expect(at(40), 3);
        expect(at(79), 3);
        expect(at(80), 4);
        expect(at(159), 4);
        srv.failStatus = null;
        expect(at(160), 5);
        expect(engine.state, SyncEngineState.idle);
        expect(at(170), 6);
        engine.stop();
        async.flushMicrotasks();
      });
    });
  });

  group('pull paging and latest_hlc', () {
    test('a page the outbound hook hid entirely moves the cursor on', () async {
      final srv = FakeServer(pageLimit: 3)..seed('notes', 5);
      // Go's BeforeOutboundRead hides the first three rows; latest_hlc still
      // covers them.
      srv.hide = (c) => c.hlc.ts <= BigInt.from(3);
      final s = setup(server: srv);
      await s.engine.sync();
      expect(srv.pulls.first.since.isZero, isTrue);
      expect(srv.pulls[1].since, HLC(BigInt.from(2), 4294967295, ''));
      expect(
        s.store.getCollection('notes').map((d) => d['_pk']),
        unorderedEquals(['r3', 'r4']),
      );
      expect(s.engine.cursor('notes'), HLC(BigInt.from(5), 0, 'srv'));
    });

    test(
      'a rounded latestHlc backs off a microsecond before it is used',
      () async {
        // Real nanosecond timestamps, one microsecond and more apart. The
        // envelope rounds latest_hlc up by 300 ns, past the next visible row.
        final base = BigInt.parse('1760000000000000000');
        final srv = FakeServer(pageLimit: 3);
        for (final (offset, pk) in [
          (0, 'h0'),
          (10000, 'h1'),
          (20000, 'h2'),
          (20100, 'v'),
        ]) {
          srv.log.add(
            ChangeRecord(
              table: 'notes',
              pk: pk,
              field: 'f',
              crdtType: CrdtType.lww,
              hlc: HLC(base + BigInt.from(offset), 0, 'srv'),
              nodeId: 'srv',
              value: const JsonValue(1),
            ),
          );
        }
        srv.hide = (c) => c.pk.startsWith('h');
        final s = setup(server: srv, transport: _Rounding(srv));
        await s.engine.sync();
        expect(s.store.getDocument('notes', 'v'), isNotNull);
        // The first cursor came from the rounded value minus the margin.
        expect(
          srv.pulls[1].since,
          HLC(base + BigInt.from(20300 - 1000 - 1), 4294967295, ''),
        );
      },
    );
  });

  group('cancellation (review probes)', () {
    test(
      'stop while page 2 of a pull is held: the cursor stays at page 1',
      () async {
        final srv = FakeServer(pageLimit: 2)..seed('notes', 6);
        final s = setup(server: srv);
        s.store.setField('notes', 'mine', 'title', 'x');
        Completer<void>? gate;
        srv.onPull = (req) {
          if (srv.pulls.length == 2) srv.pullGate = gate = Completer<void>();
        };
        final run = s.engine.sync()..ignore();
        await _until(() => srv.pulls.length == 2);
        final at = srv.requests;
        await s.engine.stop();
        gate!.complete();
        await pumpEventQueue();
        await expectLater(run, throwsA(_isCancelled));
        expect(srv.requests, at);
        expect(s.engine.cursor('notes'), HLC(BigInt.from(2), 0, 'srv'));
        expect(
          await KeyValueReplicaStorage(s.kv).readCursor('notes'),
          HLC(BigInt.from(2), 0, 'srv'),
        );
        expect(s.store.pendingCount, 1);
      },
    );

    test('stop while a bisection step is held: nothing more is sent', () async {
      final s = setup();
      s.store.setField('notes', 'n1', 'title', 'a');
      s.store.setField('notes', 'n2', 'locked', 'b');
      s.store.setField('notes', 'n3', 'title', 'c');
      s.store.setField('notes', 'n4', 'title', 'd');
      s.server.rejectField = 'locked';
      Completer<void>? gate;
      s.server.onPush = (req) {
        if (s.server.pushes.length == 2) {
          s.server.pushGate = gate = Completer<void>();
        }
      };
      final run = s.engine.sync()..ignore();
      await _until(() => s.server.pushesInFlight == 1 && gate != null);
      final at = s.server.requests;
      final pendingAt = s.store.pendingCount;
      await s.engine.stop();
      s.server.pushGate = null;
      gate!.complete();
      await pumpEventQueue();
      await expectLater(run, throwsA(_isCancelled));
      expect(s.server.requests, at);
      expect(s.store.pendingCount, pendingAt);
      expect(s.store.rejectedCount, 0);
    });

    test('CrdtError(cancelled) during bisection marks nothing more', () async {
      final s = setup();
      s.store.setField('notes', 'n1', 'title', 'a');
      s.store.setField('notes', 'n2', 'locked', 'b');
      s.server.rejectField = 'locked';
      s.server.onPush = (req) {
        if (s.server.pushes.length == 2) {
          throw CrdtError('switched', code: CrdtErrorCode.cancelled);
        }
      };
      await expectLater(s.engine.sync(), throwsA(_isCancelled));
      expect(s.server.pushes, hasLength(2));
      expect(s.store.rejectedCount, 0);
      expect(s.store.pendingCount, 2);
      expect(s.engine.state, SyncEngineState.idle);
    });
  });

  group('review minors', () {
    test(
      'a second dispose returns the first one, which waits for the run',
      () async {
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
        engine.sync().ignore();
        await _until(() => cursors.writes == 1);
        final first = engine.dispose();
        final second = engine.dispose();
        expect(identical(first, second), isTrue);
        var done = false;
        unawaited(second.then((_) => done = true));
        await pumpEventQueue();
        expect(done, isFalse);
        gate.complete();
        await second;
        expect(done, isTrue);
      },
    );

    test('after stop an attached stream applies nothing until start', () async {
      final s = setup();
      final sub = _Subscription();
      s.engine.attachStream(sub);
      await pumpEventQueue();
      await s.engine.stop();
      ChangeRecord change(String pk) => ChangeRecord(
        table: 'notes',
        pk: pk,
        field: 'title',
        crdtType: CrdtType.lww,
        hlc: HLC(BigInt.from(7), 0, 'peer'),
        nodeId: 'peer',
        value: const JsonValue('live'),
      );
      sub.emit(StreamChange(change('old-account')));
      expect(s.store.getDocument('notes', 'old-account'), isNull);
      s.engine.start(interval: const Duration(hours: 1));
      sub.emit(StreamChange(change('n1')));
      expect(s.store.getDocument('notes', 'n1'), isNotNull);
      await s.engine.dispose();
    });

    test('an error surfacing after stop belongs to a cancelled run', () async {
      final srv = FakeServer()..seed('notes', 2);
      final gate = Completer<void>();
      final cursors = _GatedCursors(gate.future, fail: StateError('disk'));
      final clock = HybridClock('dev', nowMs: () => 1000);
      final store = CrdtStore('dev', clock, persistDebounce: Duration.zero);
      final engine = SyncEngine(
        CrdtClient(nodeId: 'dev', transport: srv, clock: clock),
        store,
        tables: const ['notes'],
        cursors: cursors,
      );
      final events = <SyncEngineEvent>[];
      engine.events.listen(events.add);
      final run = engine.sync()..ignore();
      await _until(() => cursors.writes == 1);
      final stopping = engine.stop();
      gate.complete();
      await stopping;
      await expectLater(run, throwsA(_isCancelled));
      expect(engine.state, SyncEngineState.idle);
      expect(events.whereType<SyncInterrupted>(), isEmpty);
    });

    test('a ClockSkew without a cursors store is refused', () {
      final clock = HybridClock('dev');
      expect(
        () => SyncEngine(
          CrdtClient(nodeId: 'dev', transport: FakeServer(), clock: clock),
          CrdtStore('dev', clock),
          tables: const ['notes'],
          skew: ClockSkew(),
        ),
        throwsArgumentError,
      );
    });

    test(
      'a stop during bisection still reports what the bisection pushed',
      () async {
        final s = setup();
        s.store.setField('notes', 'n1', 'title', 'a');
        s.store.setField('notes', 'n2', 'locked', 'b');
        s.store.setField('notes', 'n3', 'title', 'c');
        s.server.rejectField = 'locked';
        s.server.onPush = (req) {
          // The whole batch fails on the hook; [n1] goes through; then 401.
          if (s.server.pushes.length == 3) throw _status(401);
        };
        final report = await s.engine.sync();
        expect(report.pushed, 1);
        expect(s.engine.state, SyncEngineState.unauthorized);
      },
    );
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
  _GatedCursors(this.gate, {this.fail});

  final Future<void> gate;
  final Object? fail;
  final Map<String, HLC> cursors = {};
  int writes = 0;

  @override
  Future<HLC?> readCursor(String table) async => cursors[table];

  @override
  Future<void> writeCursor(String table, HLC cursor) async {
    writes++;
    await gate;
    final f = fail;
    if (f != null) throw f;
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

/// Mirrors WebSocketTransport: a failed push arrives as an error frame, a
/// [TransportError] with no HTTP status and the server's text as its body.
final class _WsLike implements Transport {
  _WsLike(this.inner);
  final Transport inner;

  @override
  Future<PullResponse> pull(PullRequest req) => inner.pull(req);

  @override
  Future<PushResponse> push(PushRequest req) async {
    try {
      return await inner.push(req);
    } on TransportError catch (e) {
      throw TransportError(
        'CRDT ws error: ${serverMessage(e.body)}',
        body: e.body,
      );
    }
  }
}

/// A camel-DTO-like envelope on the web: latest_hlc arrives as a double,
/// here rounded up by 300 ns, and is marked inexact.
final class _Rounding implements Transport {
  _Rounding(this.inner);
  final Transport inner;

  @override
  Future<PullResponse> pull(PullRequest req) async {
    final resp = await inner.pull(req);
    final l = resp.latestHlc;
    return PullResponse(
      changes: resp.changes,
      latestHlc: l.isZero ? l : HLC(l.ts + BigInt.from(300), l.c, l.node),
      latestHlcExact: false,
    );
  }

  @override
  Future<PushResponse> push(PushRequest req) => inner.push(req);
}

/// Computes each pull at once and holds its answer on [gate] from pull
/// number [gateAt].
final class _Held implements Transport {
  _Held(this.inner);
  final FakeServer inner;
  Completer<void>? gate;
  int gateAt = 1 << 30;

  @override
  Future<PullResponse> pull(PullRequest req) async {
    final r = await inner.pull(req);
    final g = gate;
    if (g != null && inner.pulls.length >= gateAt) await g.future;
    return r;
  }

  @override
  Future<PushResponse> push(PushRequest req) => inner.push(req);
}

/// Seeds the server value `server` for notes/n1.locked, then makes a local
/// `mine` that the server's hook rejects. Returns the rejected change.
Future<ChangeRecord> _rejectLocked(Setup s) async {
  s.server.log.add(
    ChangeRecord(
      table: 'notes',
      pk: 'n1',
      field: 'locked',
      crdtType: CrdtType.lww,
      hlc: HLC(BigInt.one, 0, 'srv'),
      nodeId: 'srv',
      value: const JsonValue('server'),
    ),
  );
  await s.engine.sync();
  final rejected = s.store.setField('notes', 'n1', 'locked', 'mine')!;
  s.server.rejectField = 'locked';
  await s.engine.sync();
  s.server.rejectField = null;
  return rejected;
}

/// Transforms remote values on merge the way a decryptor would, and throws
/// on its own output, as a decryptor fed plaintext does.
final class _Decrypt extends StorePlugin {
  @override
  String get name => 'decrypt';

  @override
  ChangeRecord? beforeMerge(MergeEvent e) {
    final v = e.remote.value?.value;
    if (e.remote.field != 'locked' || v is! String) return e.remote;
    if (v.startsWith('dec:')) throw StateError('already decrypted');
    return e.remote.copyWith(value: JsonValue('dec:$v'));
  }
}
