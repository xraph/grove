import 'dart:async';

import 'package:grove_crdt/grove_crdt.dart';
import 'package:test/test.dart';

import 'websocket_test.dart' show Connector;

class DelayedPresence implements Transport, PresenceTransport {
  final first = Completer<void>();
  int count = 0;
  Object? server;
  @override
  Future<PullResponse> pull(PullRequest r) async => PullResponse();
  @override
  Future<PushResponse> push(PushRequest r) async => PushResponse(merged: 0);
  @override
  Future<List<PresenceState>> getPresence(String topic) async => [];
  @override
  Future<void> updatePresence(PresenceUpdate u) async {
    if (++count == 1) await first.future;
    server = u.data;
  }
}

class DelayedPush implements Transport {
  final accepted = Completer<void>();
  final ack = Completer<void>();
  @override
  Future<PullResponse> pull(PullRequest r) async => PullResponse();
  @override
  Future<PushResponse> push(PushRequest r) async {
    accepted.complete();
    await ack.future;
    return PushResponse(merged: r.changes.length);
  }
}

void main() {
  additionalRegressions();
  test(
    'regression: an in-flight tombstone is no longer safely undoable',
    () async {
      final tr = DelayedPush();
      final clock = HybridClock('me');
      final client = CrdtClient(nodeId: 'me', transport: tr, clock: clock);
      final store = CrdtStore('me', clock);
      await store.ready;
      final engine = SyncEngine(client, store, tables: ['docs']);
      addTearDown(engine.dispose);
      addTearDown(store.dispose);
      addTearDown(client.dispose);
      store.setField('docs', '1', 'title', 'kept');
      store.clearPendingChanges();
      store.deleteDocument('docs', '1');
      final run = engine.sync();
      await tr.accepted.future;
      final undone = store.undo();
      tr.ack.complete();
      await run;
      expect(undone, isFalse);
    },
  );
  test(
    'regression: removing an unseen set element preserves concurrent additions',
    () async {
      final a = CrdtStore('a', HybridClock('a', nowMs: () => 1000));
      final b = CrdtStore('b', HybridClock('b', nowMs: () => 1001));
      addTearDown(a.dispose);
      addTearDown(b.dispose);
      await Future.wait([a.ready, b.ready]);
      final add = a.addToSet('docs', '1', 'tags', ['x'])!;
      final remove = b.removeFromSet('docs', '1', 'tags', ['x'])!;
      a.applyChanges([remove]);
      b.applyChanges([add]);
      expect(
        a.getDocument('docs', '1')?['tags'],
        b.getDocument('docs', '1')?['tags'],
      );
    },
  );
  test('regression: streamed clocks precede subsequent local writes', () async {
    final clock = HybridClock('me', nowMs: () => 1000);
    final store = CrdtStore('me', clock);
    await store.ready;
    addTearDown(store.dispose);
    store.applyChanges([
      ChangeRecord(
        table: 'docs',
        pk: '1',
        field: 'title',
        crdtType: CrdtType.lww,
        nodeId: 'peer',
        hlc: HLC(BigInt.from(1001000000), 0, 'peer'),
        value: const JsonValue('remote'),
      ),
    ]);
    store.setField('docs', '1', 'title', 'my later edit');
    expect(store.getDocument('docs', '1')?['title'], 'my later edit');
  });
  test('regression: delayed cursor cannot overtake a newer cursor', () async {
    final tr = DelayedPresence();
    final client = CrdtClient(nodeId: 'me', transport: tr);
    addTearDown(client.dispose);
    final old = client.updatePresence('room', {'x': 1});
    final newer = client.updatePresence('room', {'x': 2});
    await Future<void>.delayed(Duration.zero);
    tr.first.complete();
    await Future.wait([old, newer]);
    expect(tr.server, {'x': 2});
  });
  test('regression: leaving waits for an earlier cursor publication', () async {
    final tr = DelayedPresence();
    final client = CrdtClient(nodeId: 'me', transport: tr);
    addTearDown(client.dispose);
    final old = client.updatePresence('room', {'x': 1});
    final leave = client.leavePresence('room');
    await Future<void>.delayed(Duration.zero);
    tr.first.complete();
    await Future.wait([old, leave]);
    expect(tr.server, isNull);
  });
  test(
    'regression: websocket stops database delivery after disconnect',
    () async {
      final c = Connector();
      final tr = WebSocketTransport(
        url: Uri.parse('ws://test'),
        connect: c.call,
      );
      addTearDown(tr.close);
      final sub = tr.subscribe(const StreamConfig(tables: ['docs']));
      final events = <CrdtStreamEvent>[];
      sub.on(events.add);
      sub.connect();
      await Future<void>.delayed(Duration.zero);
      sub.disconnect();
      events.clear();
      c.latest.serverSends({'type': 'changes', 'payload': []});
      await Future<void>.delayed(Duration.zero);
      expect(events.whereType<StreamChanges>(), isEmpty);
    },
  );
  test('regression: websocket transmits requested resume cursor', () async {
    final c = Connector();
    final tr = WebSocketTransport(url: Uri.parse('ws://test'), connect: c.call);
    addTearDown(tr.close);
    final since = HLC(BigInt.from(99), 0, 'peer');
    tr.subscribe(StreamConfig(tables: ['docs'], since: since)).connect();
    await Future<void>.delayed(Duration.zero);
    final sent = c.latest.ofType('subscribe').single;
    expect((sent['payload'] as Map)['since'], since.toJson());
  });
}

void additionalRegressions() {
  test(
    'exact empty tags and legacy omitted tags survive JSON distinctly',
    () async {
      for (final exact in [false, true]) {
        final store = CrdtStore('me', HybridClock('me', nowMs: () => 1000));
        addTearDown(store.dispose);
        await store.ready;
        store.addToSet('docs', '1', 'tags', ['x']);
        final op = SetOperation.fromJson(
          SetOperation(SetOpType.remove, const [
            'x',
          ], tags: exact ? [] : null).toJson(),
        );
        expect(op.hasTags, exact);
        store.applyChanges([
          ChangeRecord(
            table: 'docs',
            pk: '1',
            field: 'tags',
            crdtType: CrdtType.set,
            nodeId: 'peer',
            hlc: HLC(BigInt.from(1001000000), 0, 'peer'),
            setOp: op,
          ),
        ]);
        expect(
          store.getDocument('docs', '1')?['tags'],
          exact ? ['x'] : isEmpty,
        );
      }
    },
  );
  test(
    'WebSocket scopes share the socket and reconnect from their own cursor',
    () async {
      final c = Connector();
      final tr = WebSocketTransport(
        url: Uri.parse('ws://test'),
        connect: c.call,
        pingInterval: Duration.zero,
        sleep: (_) async {},
      );
      addTearDown(tr.close);
      final docs = tr.subscribe(
        StreamConfig(
          tables: const ['docs'],
          since: HLC(BigInt.from(10), 0, 'peer'),
        ),
      );
      final tasks = tr.subscribe(
        StreamConfig(
          tables: const ['tasks'],
          since: HLC(BigInt.from(5), 0, 'peer'),
        ),
      );
      final events = <CrdtStreamEvent>[];
      tasks.on(events.add);
      docs.connect();
      await pumpEventQueue();
      tasks.connect();
      await pumpEventQueue();
      expect((c.latest.ofType('subscribe').last['payload'] as Map)['tables'], [
        'docs',
        'tasks',
      ]);
      Map<String, Object?> change(String table, int ts) => ChangeRecord(
        table: table,
        pk: '1',
        field: 'title',
        crdtType: CrdtType.lww,
        nodeId: 'peer',
        hlc: HLC(BigInt.from(ts), 0, 'peer'),
        value: const JsonValue('v'),
      ).toJson();
      c.latest.serverSends({
        'type': 'changes',
        'payload': [change('docs', 20), change('tasks', 15)],
      });
      await pumpEventQueue();
      expect(
        events.whereType<StreamChanges>().single.changes.single.table,
        'tasks',
      );
      docs.disconnect();
      expect(tasks.connected, isTrue);
      c.latest.drop();
      await pumpEventQueue();
      expect(c.sockets, hasLength(2));
      expect(c.latest.ofType('subscribe').single['payload'], {
        'tables': ['tasks'],
        'since': HLC(BigInt.from(15), 0, 'peer').toJson(),
      });
      tasks.disconnect();
      expect(c.latest.ofType('unsubscribe'), hasLength(1));
      c.latest.drop();
      await pumpEventQueue();
      expect(c.sockets, hasLength(2));
    },
  );
  test(
    'presence queue recovers from failure and dispose ends after queued writes',
    () async {
      final tr = DelayedPresence();
      final client = CrdtClient(nodeId: 'me', transport: tr);
      final first = client.updatePresence('room', {'x': 1});
      final failed = expectLater(first, throwsStateError);
      final next = client.updatePresence('room', {'x': 2});
      final disposed = client.dispose();
      tr.first.completeError(StateError('network failure'));
      await failed;
      await next;
      await disposed;
      expect(tr.server, isNull);
      await expectLater(client.updatePresence('room', {}), throwsStateError);
    },
  );
  test(
    'a lost deletion acknowledgement keeps the pending tombstone non-undoable',
    () async {
      final tr = DelayedPush();
      final clock = HybridClock('me');
      final client = CrdtClient(nodeId: 'me', transport: tr, clock: clock);
      final store = CrdtStore('me', clock);
      await store.ready;
      final engine = SyncEngine(client, store, tables: ['docs']);
      addTearDown(engine.dispose);
      addTearDown(store.dispose);
      addTearDown(client.dispose);
      store.setField('docs', '1', 'title', 'kept');
      store.clearPendingChanges();
      store.deleteDocument('docs', '1');
      final run = engine.sync();
      final failed = expectLater(run, throwsA(isA<Object>()));
      await tr.accepted.future;
      tr.ack.completeError(StateError('lost acknowledgement'));
      await failed;
      expect(store.pendingCount, 1);
      expect(store.undo(), isFalse);
      expect(store.getDocument('docs', '1'), isNull);
    },
  );
}
