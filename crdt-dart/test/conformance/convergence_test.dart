@TestOn('vm')
@Tags(['conformance'])
library;

import 'dart:math';

import 'package:grove_crdt/grove_crdt.dart';
import 'package:test/test.dart';

import 'conformance_server.dart';

({CrdtStore store, SyncEngine engine}) replica(
  String node,
  Uri syncUrl, {
  int Function()? nowMs,
}) {
  final clock = HybridClock(node, nowMs: nowMs);
  final client = CrdtClient(
    nodeId: node,
    clock: clock,
    transport: HttpTransport(baseUrl: syncUrl),
  );
  final store = CrdtStore(node, clock, persistDebounce: Duration.zero);
  return (
    store: store,
    engine: SyncEngine(client, store, tables: const ['notes']),
  );
}

void randomEdits(CrdtStore s, Random rng, int count) {
  final pks = ['p0', 'p1', 'p2'];
  for (var i = 0; i < count; i++) {
    final pk = pks[rng.nextInt(pks.length)];
    switch (rng.nextInt(7)) {
      case 0:
        s.setField('notes', pk, 'title', 't${rng.nextInt(1000)}');
      case 1:
        s.incrementCounter('notes', pk, 'views', rng.nextInt(5) + 1);
      case 2:
        s.addToSet('notes', pk, 'tags', ['g${rng.nextInt(4)}']);
      case 3:
        s.removeFromSet('notes', pk, 'tags', ['g${rng.nextInt(4)}']);
      case 4:
        s.insertIntoList('notes', pk, 'items', 'i${rng.nextInt(100)}');
      case 5:
        final len = s.getText('notes', pk, 'body').runes.length;
        s.insertText(
          'notes',
          pk,
          'body',
          rng.nextInt(len + 1),
          'w${rng.nextInt(10)}',
        );
      case 6:
        s.setDocumentField(
          'notes',
          pk,
          'meta',
          'a.b${rng.nextInt(3)}',
          rng.nextInt(9),
        );
    }
  }
}

void main() {
  group('convergence', skip: ConformanceServer.skipReason(), () {
    late ConformanceServer server;
    setUp(() async => server = await ConformanceServer.start());
    tearDown(() => server.stop());

    test(
      'two devices edit the same field offline converge to the later write',
      () async {
        const aNow = 1000000;
        var bNow = 1000000;
        final a = replica(
          'dev-a',
          server.syncUrl,
          nowMs: () => DateTime.now().millisecondsSinceEpoch + aNow,
        );
        final b = replica(
          'dev-b',
          server.syncUrl,
          nowMs: () => DateTime.now().millisecondsSinceEpoch + bNow,
        );
        a.store.setField('notes', 'n1', 'title', 'from a');
        bNow += 5000;
        b.store.setField('notes', 'n1', 'title', 'from b (later)');
        await a.engine.sync();
        await b.engine.sync();
        await a.engine.sync();
        expect(a.store.getDocument('notes', 'n1')!['title'], 'from b (later)');
        expect(b.store.getDocument('notes', 'n1')!['title'], 'from b (later)');
        expect(
          resolveFieldValue(
            (await server.state('notes', 'n1'))!.fields['title']!,
          ),
          'from b (later)',
        );
      },
    );

    // The two devices run on fake clocks, device B always later than A, so
    // every HLC order is fixed and the cases do not depend on how many
    // milliseconds a test step takes. The sync order follows the stamps:
    // the device whose edits carry the earlier clock pushes first. The
    // reverse order is the grove v1.7.0 limitation pinned (skipped) below.
    for (var seed = 1; seed <= 20; seed++) {
      test('interleaved offline edits converge (seed $seed)', () async {
        final rng = Random(seed);
        final base = DateTime.now().millisecondsSinceEpoch;
        var aNow = base;
        var bNow = base + 1000;
        final a = replica('dev-a', server.syncUrl, nowMs: () => aNow);
        final b = replica('dev-b', server.syncUrl, nowMs: () => bNow);
        randomEdits(a.store, rng, 30);
        randomEdits(b.store, rng, 30);
        await a.engine.sync();
        await b.engine.sync();
        await a.engine.sync();
        aNow = base + 2000;
        bNow = base + 3000;
        randomEdits(a.store, rng, 10);
        randomEdits(b.store, rng, 10);
        await a.engine.sync();
        await b.engine.sync();
        await a.engine.sync();
        for (final pk in ['p0', 'p1', 'p2']) {
          final local = a.store.getDocument('notes', pk);
          expect(b.store.getDocument('notes', pk), local, reason: 'pk $pk');
          final serverDoc = await server.state('notes', pk);
          if (local == null) {
            expect(serverDoc, isNull, reason: 'pk $pk is untouched');
            continue;
          }
          final localState = a.store.getDocumentState('notes', pk)!;
          Object? value(DocumentState d, String f) =>
              d.fields[f] == null ? null : resolveFieldValue(d.fields[f]!);
          for (final f in {
            ...localState.fields.keys,
            ...serverDoc!.fields.keys,
          }) {
            expect(
              value(localState, f),
              value(serverDoc, f),
              reason: 'pk $pk field $f',
            );
          }
        }
      });
    }

    test('a change pushed late with an older stamp reaches a device that synced past it', () async {
      final base = DateTime.now().millisecondsSinceEpoch;
      final a = replica('dev-a', server.syncUrl, nowMs: () => base);
      final b = replica('dev-b', server.syncUrl, nowMs: () => base + 1000);
      a.store.incrementCounter('notes', 'n1', 'views', 3);
      b.store.incrementCounter('notes', 'n1', 'views', 7);
      // B syncs first with the later stamp. A then pushes an earlier one,
      // and B's pull cursor is already past it.
      await b.engine.sync();
      await a.engine.sync();
      await b.engine.sync();
      expect(a.store.getDocument('notes', 'n1')!['views'], 10);
      expect(b.store.getDocument('notes', 'n1')!['views'], 10);
    });
  });
}
