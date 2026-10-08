// Port of crdt-js src/__tests__/presence-seed.test.ts, case for case, with an
// injected clock in place of `Date.now()`.
//
// `PresenceState.updatedAt` is a `DateTime` here (the wire's RFC 3339 string is
// parsed when the state is decoded), so the TS normalisation to epoch
// milliseconds has nothing left to do inside `seed`. The two cases about it
// keep their descriptions and assert what remains true: a state decoded from
// the wire ages like an event-applied one.
//
// The last case drives `CrdtClient` against a recording presence transport.
import 'package:grove_crdt/grove_crdt.dart';
import 'package:test/test.dart';

PresenceEvent _event(String type, String nodeId, String topic, Object? data) =>
    PresenceEvent(
      type: type,
      nodeId: nodeId,
      topic: topic,
      data: JsonValue(data),
    );

PresenceState _state(String nodeId, int atMs, [Object? data = const {}]) =>
    PresenceState(
      nodeId: nodeId,
      topic: 't',
      data: data,
      updatedAt: DateTime.fromMillisecondsSinceEpoch(atMs, isUtc: true),
    );

void main() {
  late int now;
  late PresenceManager pm;

  setUp(() {
    now = 1760000000000;
    pm = PresenceManager('me', nowMs: () => now);
  });

  group('presence', () {
    test('getPresence is referentially stable between events', () {
      pm.applyEvent(_event('join', 'peer', 't', {'a': 1}));
      expect(pm.getPresence('t'), same(pm.getPresence('t')));
    });

    test('getPresence returns a new reference after an event', () {
      pm.applyEvent(_event('join', 'peer', 't', {'a': 1}));
      final first = pm.getPresence('t');
      pm.applyEvent(_event('update', 'peer', 't', {'a': 2}));
      expect(pm.getPresence('t'), isNot(same(first)));
    });

    test('an empty topic returns a stable empty array', () {
      expect(pm.getPresence('nope'), same(pm.getPresence('nope')));
    });

    test('seed populates peers already present (D6)', () {
      final states = [
        _state('bob', now, {'name': 'Bob'}),
        _state('me', now),
      ];
      pm.seed('t', states);
      final peers = pm.getPresence('t');
      expect(peers, hasLength(1));
      expect(peers[0].nodeId, 'bob');
    });

    test('prune drops peers older than maxAge', () {
      pm.seed('t', [_state('stale', now - 60000), _state('fresh', now)]);
      pm.prune(const Duration(milliseconds: 30000));
      expect(pm.getPresence('t').map((p) => p.nodeId), ['fresh']);
    });

    test(
      'prune drops a seeded peer whose updated_at arrived as an RFC3339 string',
      () {
        // crdt/presence_types.go declares UpdatedAt as time.Time, so the wire
        // carries an RFC 3339 string. The TS manager stored that string and
        // `updated_at < cutoff` was then always false, so a seeded peer never
        // expired. Here the string is parsed when the state is decoded, and
        // the case checks the decoded state ages like an applied one.
        final staleIso = DateTime.fromMillisecondsSinceEpoch(
          now - 60000,
          isUtc: true,
        ).toIso8601String();
        final stale = PresenceState.fromJson({
          'node_id': 'stale',
          'topic': 't',
          'data': <String, Object?>{},
          'updated_at': staleIso,
        });
        pm.seed('t', [stale, _state('fresh', now)]);

        // Held as a DateTime that is the instant the string named.
        expect(pm.getPeer('t', 'stale')!.updatedAt, DateTime.parse(staleIso));
        expect(
          pm.getPeer('t', 'stale')!.updatedAt.millisecondsSinceEpoch,
          now - 60000,
        );

        pm.prune(const Duration(milliseconds: 30000));
        expect(pm.getPresence('t').map((p) => p.nodeId), ['fresh']);
      },
    );

    test('seed leaves a numeric updated_at untouched', () {
      final at = now - 1234;
      pm.seed('t', [_state('bob', at)]);
      expect(
        pm.getPeer('t', 'bob')!.updatedAt,
        DateTime.fromMillisecondsSinceEpoch(at, isUtc: true),
      );
    });

    test('joinPresence seeds from the server snapshot', () async {
      final transport = _SeedTransport([
        _state('bob', now, {'name': 'Bob'}),
      ]);
      final client = CrdtClient(nodeId: 'me', transport: transport);
      await client.joinPresence('t', {'name': 'Me'});
      expect(transport.sent, hasLength(1));
      expect(client.presence.getPresence('t').map((p) => p.nodeId), ['bob']);
      await client.leaveAllPresence();
    });

    // Not in the TS file. A state the server sent without a time decodes as
    // Go's zero time, which is far older than any cutoff. crdt-js treats a time
    // it cannot read as just seen, so seeding does the same.
    test('seed counts a state with Go\'s zero time as just seen, and keeps its '
        'expiry', () {
      final expires = DateTime.utc(2026, 10, 4, 12, 0, 30);
      pm.seed('t', [
        PresenceState(
          nodeId: 'untimed',
          topic: 't',
          data: const {'a': 1},
          updatedAt: DateTime.utc(1),
          expiresAt: expires,
        ),
      ]);
      pm.prune(const Duration(seconds: 30));
      final peer = pm.getPeer('t', 'untimed')!;
      expect(peer.updatedAt.millisecondsSinceEpoch, now);
      expect(peer.expiresAt, expires);
      expect(peer.data, {'a': 1});

      now += 31000;
      pm.prune(const Duration(seconds: 30));
      expect(pm.getPresence('t'), isEmpty);
    });
  });
}

/// A transport answering presence reads with a fixed snapshot.
final class _SeedTransport implements Transport, PresenceTransport {
  _SeedTransport(this.snapshot);

  final List<PresenceState> snapshot;
  final sent = <PresenceUpdate>[];

  @override
  Future<PullResponse> pull(PullRequest req) async => PullResponse();

  @override
  Future<PushResponse> push(PushRequest req) async => PushResponse(merged: 0);

  @override
  Future<void> updatePresence(PresenceUpdate u) async => sent.add(u);

  @override
  Future<List<PresenceState>> getPresence(String topic) async => snapshot;
}
