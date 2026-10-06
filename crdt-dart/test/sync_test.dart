// Port of crdt-js src/__tests__/sync.test.ts, case for case.
//
// The `SyncHook` and `PresenceHook` cases of plugin.test.ts (lines 305-412)
// drive a bare PluginManager and are already ported in
// plugin_manager_test.dart (Task 9); the cases here drive the same hooks
// through the engine and the client. The `joinPresence seeds from the server
// snapshot` case of presence-seed.test.ts is ported in
// presence_seed_test.dart, next to its siblings.
import 'dart:async';

import 'package:grove_crdt/grove_crdt.dart';
import 'package:test/test.dart';

/// A transport answering with empty pulls and full merges.
final class _Transport implements Transport, PresenceTransport {
  _Transport({this.onPush, this.pushGate});

  final void Function(PushRequest req)? onPush;
  final Future<void>? pushGate;
  final sent = <Object?>[];
  var pushStarted = 0;
  var presenceCalls = 0;

  @override
  Future<PullResponse> pull(PullRequest req) async => PullResponse();

  @override
  Future<PushResponse> push(PushRequest req) async {
    pushStarted++;
    onPush?.call(req);
    final gate = pushGate;
    if (gate != null) await gate;
    return PushResponse(merged: req.changes.length);
  }

  @override
  Future<void> updatePresence(PresenceUpdate u) async {
    presenceCalls++;
    sent.add(u.data);
  }

  @override
  Future<List<PresenceState>> getPresence(String topic) async => const [];
}

/// A transport without presence, like the TS case's literal.
final class _PlainTransport implements Transport {
  @override
  Future<PullResponse> pull(PullRequest req) async => PullResponse();

  @override
  Future<PushResponse> push(PushRequest req) async => PushResponse(merged: 0);
}

({CrdtClient client, CrdtStore store, SyncEngine engine}) _mk(
  Transport transport,
) {
  final client = CrdtClient(nodeId: 'n1', transport: transport);
  final store = CrdtStore('n1', client.clock, persistDebounce: Duration.zero);
  return (
    client: client,
    store: store,
    engine: SyncEngine(client, store, tables: const ['t']),
  );
}

final class _SyncSpy extends StorePlugin {
  _SyncSpy(this.name, this.calls);

  @override
  final String name;
  final List<String> calls;

  @override
  List<ChangeRecord>? beforePush(List<ChangeRecord> changes) {
    calls.add('before:${changes.length}');
    return changes;
  }

  @override
  void afterPush(int pushed, List<ChangeRecord> changes) =>
      calls.add('after:$pushed');
}

final class _PullSpy extends StorePlugin {
  _PullSpy(this.calls);

  @override
  String get name => 'pull-spy';
  final List<String> calls;

  @override
  PullEvent? beforePull(PullEvent e) {
    calls.add('before');
    return e;
  }

  @override
  void afterPull(PullEvent e, List<ChangeRecord> changes) =>
      calls.add('after:${changes.length}');
}

final class _Veto extends StorePlugin {
  @override
  String get name => 'veto';

  @override
  List<ChangeRecord>? beforePush(List<ChangeRecord> changes) => null;

  @override
  Object? beforePresenceUpdate(String topic, Object? data) => presenceRejected;
}

final class _Redact extends StorePlugin {
  @override
  String get name => 'redact';

  @override
  Object? beforePresenceUpdate(String topic, Object? data) => {
    ...(data! as Map<String, Object?>),
    'redacted': true,
  };
}

final class _PresenceSpy extends StorePlugin {
  _PresenceSpy(this.seen);

  @override
  String get name => 'spy';
  final List<String> seen;

  @override
  void onPresenceEvent(PresenceEvent e) => seen.add(e.type);
}

void main() {
  group('SyncEngine', () {
    test('pushes pending changes and clears them', () async {
      final m = _mk(_Transport());
      m.store.setField('t', 'p', 'f', 1);
      final report = await m.engine.sync();
      expect(report.pushed, 1);
      expect(m.store.pendingCount, 0);
    });

    test('does not lose writes made while a push is in flight (D11)', () async {
      final gate = Completer<void>();
      final transport = _Transport(pushGate: gate.future);
      final m = _mk(transport);

      m.store.setField('t', 'p', 'a', 1);
      final inFlight = m.engine.sync();
      // crdt-js snapshots the queue when sync() is called, so its write right
      // after the call already lands mid-round-trip. This engine reads the
      // queue when its push leg starts, after the pull, so the write waits
      // until the push is really in flight.
      while (transport.pushStarted == 0) {
        await pumpEventQueue(times: 1);
      }
      // This write lands while the push is still awaiting the gate.
      m.store.setField('t', 'p', 'b', 2);
      gate.complete();
      await inFlight;

      expect(m.store.pendingCount, 1);
      expect(m.store.getPendingChanges()[0].field, 'b');
    });

    test('runs beforePush and afterPush hooks (D5)', () async {
      final m = _mk(_Transport());
      final calls = <String>[];
      m.store.use(_SyncSpy('sync-spy', calls));
      m.store.setField('t', 'p', 'f', 1);
      await m.engine.sync();
      expect(calls, ['before:1', 'after:1']);
    });

    test('runs beforePull and afterPull hooks (D5)', () async {
      final m = _mk(_Transport());
      final calls = <String>[];
      m.store.use(_PullSpy(calls));
      await m.engine.sync();
      expect(calls, ['before', 'after:0']);
    });

    test(
      'beforePush returning null cancels the push and keeps pending',
      () async {
        var pushes = 0;
        final m = _mk(_Transport(onPush: (_) => pushes++));
        m.store.use(_Veto());
        m.store.setField('t', 'p', 'f', 1);
        await m.engine.sync();
        expect(pushes, 0);
        expect(m.store.pendingCount, 1);
      },
    );
  });

  group('presence hooks', () {
    test('beforePresenceUpdate can rewrite outgoing data (D5)', () async {
      final transport = _Transport();
      final m = _mk(transport);
      m.client.attachStore(m.store);
      m.store.use(_Redact());
      await m.client.updatePresence('t', {'name': 'Alice'});
      expect(transport.sent[0], {'name': 'Alice', 'redacted': true});
      await m.client.leaveAllPresence();
    });

    test('beforePresenceUpdate returning null cancels the update', () async {
      final transport = _Transport();
      final m = _mk(transport);
      m.client.attachStore(m.store);
      // Changed from crdt-js: null is a valid payload in Dart (a leave), so a
      // hook cancels with `presenceRejected`.
      m.store.use(_Veto());
      await m.client.updatePresence('t', {'name': 'Alice'});
      expect(transport.presenceCalls, 0);
    });

    test('onPresenceEvent fires for inbound events', () {
      final m = _mk(_PlainTransport());
      m.client.attachStore(m.store);
      final seen = <String>[];
      m.store.use(_PresenceSpy(seen));
      m.client.applyPresenceEvent(
        const PresenceEvent(
          type: 'join',
          nodeId: 'peer',
          topic: 't',
          data: JsonValue(<String, Object?>{}),
        ),
      );
      expect(seen, ['join']);
    });
  });
}
