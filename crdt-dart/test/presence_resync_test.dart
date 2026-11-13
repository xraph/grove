import 'dart:async';

import 'package:fake_async/fake_async.dart';
import 'package:grove_crdt/grove_crdt.dart';
import 'package:test/test.dart';

const resync = PresenceEvent(type: 'resync', nodeId: '', topic: '');
PresenceState peer(String id) => PresenceState(
  nodeId: id,
  topic: 'room',
  data: const {},
  updatedAt: DateTime.now().toUtc(),
);

final class ResyncTransport implements Transport, PresenceTransport {
  int reads = 0;
  Future<List<PresenceState>> Function() snapshot = () async => [peer('idle')];
  @override
  Future<PullResponse> pull(PullRequest request) async => PullResponse();
  @override
  Future<PushResponse> push(PushRequest request) async =>
      PushResponse(merged: 0);
  @override
  Future<void> updatePresence(PresenceUpdate update) async {}
  @override
  Future<List<PresenceState>> getPresence(String topic) {
    reads++;
    return snapshot();
  }
}

void main() {
  test('heartbeats do not invalidate a slow recovery snapshot', () {
    fakeAsync((async) {
      final transport = ResyncTransport();
      final client = CrdtClient(
        nodeId: 'me',
        transport: transport,
        presence: const PresenceConfig(
          heartbeatInterval: Duration(milliseconds: 100),
        ),
      );
      unawaited(client.updatePresence('room', const {}));
      async.flushMicrotasks();
      final result = Completer<List<PresenceState>>();
      transport.snapshot = () => result.future;
      client.applyPresenceEvent(resync);
      async.flushMicrotasks();
      async.elapse(const Duration(milliseconds: 500));
      result.complete([peer('idle')]);
      async.flushMicrotasks();
      expect(transport.reads, 1);
      expect(client.presence.getPresence('room').map((p) => p.nodeId), [
        'idle',
      ]);
      unawaited(client.dispose());
      async.flushMicrotasks();
      expect(async.periodicTimerCount, 0);
    });
  });

  test(
    'overflow clears ghosts and restores idle peers without reconnect',
    () async {
      final transport = ResyncTransport();
      final client = CrdtClient(nodeId: 'me', transport: transport);
      addTearDown(client.dispose);
      await client.updatePresence('room', const {});
      client.presence.seed('room', [peer('gone')]);
      client.applyPresenceEvent(resync);
      expect(client.presence.getPresence('room'), isEmpty);
      // Joining the in-flight refresh coalesces instead of racing it.
      await client.refreshPresence('room');
      expect(client.presence.getPresence('room').map((p) => p.nodeId), [
        'idle',
      ]);
    },
  );

  test(
    'repeated overflow invalidates a pending snapshot with a bounded refetch',
    () async {
      final transport = ResyncTransport();
      final client = CrdtClient(nodeId: 'me', transport: transport);
      addTearDown(client.dispose);
      await client.updatePresence('room', const {});
      final first = Completer<List<PresenceState>>();
      transport.snapshot = () =>
          transport.reads == 1 ? first.future : Future.value([peer('idle')]);
      client.applyPresenceEvent(resync);
      await Future<void>.value();
      for (var i = 0; i < 20; i++) {
        client.applyPresenceEvent(resync);
      }
      expect(transport.reads, 1);
      final observed = <List<String>>[];
      client.presence.subscribe(
        'room',
        () => observed.add(
          client.presence.getPresence('room').map((p) => p.nodeId).toList(),
        ),
      );
      final done = client.refreshPresence('room');
      first.complete([peer('gone')]);
      await done;
      expect(transport.reads, 2);
      expect(observed, [
        ['idle'],
      ]);
    },
  );

  test(
    'resync preserves stream leaves and cannot re-seed after leaving',
    () async {
      final transport = ResyncTransport();
      final client = CrdtClient(nodeId: 'me', transport: transport);
      addTearDown(client.dispose);
      await client.updatePresence('room', const {});
      var result = Completer<List<PresenceState>>();
      transport.snapshot = () => result.future;
      final refresh = client.refreshPresence('room');
      await Future<void>.value();
      client.applyPresenceEvent(
        const PresenceEvent(type: 'leave', nodeId: 'gone', topic: 'room'),
      );
      result.complete([peer('gone'), peer('idle')]);
      await refresh;
      expect(client.presence.getPresence('room').map((p) => p.nodeId), [
        'idle',
      ]);
      result = Completer<List<PresenceState>>();
      client.applyPresenceEvent(resync);
      final pending = client.refreshPresence('room');
      await client.leavePresence('room');
      result.complete([peer('gone')]);
      await pending;
      expect(client.presence.getPresence('room'), isEmpty);
    },
  );

  test(
    'failed overflow recovery retries on heartbeat without resurrecting ghosts',
    () {
      fakeAsync((async) {
        final transport = ResyncTransport();
        final client = CrdtClient(
          nodeId: 'me',
          transport: transport,
          presence: const PresenceConfig(
            heartbeatInterval: Duration(milliseconds: 100),
          ),
        );
        unawaited(client.updatePresence('room', const {}));
        async.flushMicrotasks();
        transport.snapshot = () async {
          if (transport.reads == 1) throw StateError('offline');
          return [peer('idle')];
        };
        client.presence.seed('room', [peer('gone')]);
        client.applyPresenceEvent(resync);
        async.flushMicrotasks();
        expect(client.presence.getPresence('room'), isEmpty);
        async.elapse(const Duration(milliseconds: 100));
        async.flushMicrotasks();
        expect(client.presence.getPresence('room').map((p) => p.nodeId), [
          'idle',
        ]);
        expect(transport.reads, 2);
        unawaited(client.dispose());
        async.flushMicrotasks();
        expect(async.periodicTimerCount, 0);
      });
    },
  );
}
