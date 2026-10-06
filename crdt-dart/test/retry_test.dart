// Port of crdt-js src/__tests__/retry.test.ts, case for case, with the Dart
// shape of `withRetry`: it returns a `RetryingTransport` that exposes the
// wrapped transport as `inner` instead of proxying its members.
import 'package:grove_crdt/grove_crdt.dart';
import 'package:test/test.dart';

PullResponse _empty() => PullResponse();
PushResponse _pushed() => PushResponse(merged: 0);
PullRequest _pull() => PullRequest(tables: const [], nodeId: 'n1');

/// `{ retries: 3, backoff: { initialDelay: 1, jitter: false } }`, with the wait
/// itself injected so no test sleeps.
final _slept = <Duration>[];
Future<void> _sleep(Duration d) async => _slept.add(d);
Backoff _backoff() =>
    Backoff(initialDelay: const Duration(milliseconds: 1), jitter: false);

RetryingTransport _wrap(Transport t, {int retries = 3}) =>
    withRetry(t, retries: retries, backoff: _backoff, sleep: _sleep);

/// A transport whose pull fails with [failures] then succeeds.
class _Flaky implements Transport {
  _Flaky(this.failures, this.error);
  final int failures;
  final Object error;
  int calls = 0;

  @override
  Future<PullResponse> pull(PullRequest req) async {
    calls++;
    if (calls <= failures) throw error;
    return _empty();
  }

  @override
  Future<PushResponse> push(PushRequest req) async => _pushed();
}

class _Subscription implements CrdtSubscription {
  @override
  void Function() on(void Function(CrdtStreamEvent event) handler) => () {};
  @override
  void connect() {}
  @override
  void disconnect() {}
  @override
  bool get connected => false;
  @override
  HLC? get lastHlc => null;
}

/// `class FakeWs implements StreamTransport`.
class _FakeWs implements StreamTransport {
  bool closed = false;
  List<String>? subscribedTables;
  int pulls = 0;

  @override
  Future<PullResponse> pull(PullRequest req) async {
    pulls++;
    if (pulls == 1) throw TransportError('boom', statusCode: 503);
    return _empty();
  }

  @override
  Future<PushResponse> push(PushRequest req) async => _pushed();

  @override
  CrdtSubscription subscribe(StreamConfig config) {
    subscribedTables = config.tables;
    return _Subscription();
  }

  /// Not part of Transport: the whole point of the test that reads it.
  void close() => closed = true;
}

class _PresenceOnly implements Transport, PresenceTransport {
  int seen = 0;

  @override
  Future<PullResponse> pull(PullRequest req) async => _empty();

  @override
  Future<PushResponse> push(PushRequest req) async => _pushed();

  @override
  Future<void> updatePresence(PresenceUpdate update) async => seen++;

  @override
  Future<List<PresenceState>> getPresence(String topic) async => const [];
}

class _Bare implements Transport {
  @override
  Future<PullResponse> pull(PullRequest req) async => _empty();

  @override
  Future<PushResponse> push(PushRequest req) async => _pushed();
}

void main() {
  setUp(_slept.clear);

  group('withRetry', () {
    test(
      'retries a retryable failure from ANY transport, not just HTTP',
      () async {
        final flaky = _Flaky(2, TransportError('boom', statusCode: 503));
        final resp = await _wrap(flaky).pull(_pull());
        expect(flaky.calls, 3);
        expect(resp.changes, isEmpty);
      },
    );

    test('does not retry a non-retryable failure', () async {
      final bad = _Flaky(99, TransportError('bad request', statusCode: 400));
      await expectLater(
        _wrap(bad).pull(_pull()),
        throwsA(isA<TransportError>()),
      );
      expect(bad.calls, 1);
    });

    test('preserves streaming capability', () {
      final wrapped = _wrap(_FakeWs());
      expect(isStreamTransport(wrapped), isTrue);
      expect(wrapped.subscribe(const StreamConfig()), isA<CrdtSubscription>());
    });

    // crdt-js returns a Proxy, so `withRetry(ws).close()` reaches the wrapped
    // object. Dart cannot proxy arbitrary members: the wrapped transport is
    // `inner`, and the capability checks and the retrying pull still hold.
    test('keeps members outside the Transport interface reachable', () async {
      final inner = _FakeWs();
      final wrapped = _wrap(inner);

      expect(wrapped.inner, same(inner));
      (wrapped.inner as _FakeWs).close();
      expect(inner.closed, isTrue);

      // Streaming goes to the real instance, untouched.
      wrapped.subscribe(const StreamConfig(tables: ['docs']));
      expect(inner.subscribedTables, ['docs']);

      // The capabilities the wrapper must keep honouring. A stream-only inner
      // has no presence, and asking for it throws instead of vanishing.
      expect(isStreamTransport(wrapped), isTrue);
      expect(wrapped.supportsStream, isTrue);
      expect(wrapped.supportsPresence, isFalse);
      expect(isPresenceTransport(wrapped), isFalse);
      expect(
        () => wrapped.updatePresence(
          const PresenceUpdate(nodeId: 'n1', topic: 't'),
        ),
        throwsUnsupportedError,
      );
      expect(() => wrapped.getPresence('t'), throwsUnsupportedError);

      // Plain data members are read through `inner`.
      expect(inner.closed, isTrue);

      // pull is still the retrying wrapper: the first call fails with a 503.
      expect((await wrapped.pull(_pull())).changes, isEmpty);
      expect(inner.pulls, 2);
    });

    // crdt-js leaves `updatePresence` undefined when the inner transport lacks
    // it. A Dart class cannot gain a method at runtime, so the wrapper always
    // has the methods, `supportsPresence` is false and the call throws.
    test(
      'passes optional presence methods through only when present',
      () async {
        final bare = _wrap(_Bare());
        expect(bare.supportsPresence, isFalse);
        expect(isPresenceTransport(bare), isFalse);
        expect(
          () => bare.updatePresence(
            const PresenceUpdate(nodeId: 'n1', topic: 't'),
          ),
          throwsUnsupportedError,
        );

        final inner = _PresenceOnly();
        final withPresence = _wrap(inner);
        expect(withPresence.supportsPresence, isTrue);
        await withPresence.updatePresence(
          const PresenceUpdate(nodeId: 'n1', topic: 't'),
        );
        expect(inner.seen, 1);
      },
    );
  });

  group('Dart retry behaviour', () {
    test('streaming is absent when the inner transport cannot stream', () {
      final wrapped = _wrap(_Bare());
      expect(isStreamTransport(wrapped), isFalse);
      expect(wrapped.supportsStream, isFalse);
      expect(
        () => wrapped.subscribe(const StreamConfig()),
        throwsUnsupportedError,
      );
    });

    test('capability checks see through a wrapper of a wrapper', () {
      expect(isStreamTransport(_wrap(_wrap(_FakeWs()))), isTrue);
      expect(isStreamTransport(_wrap(_wrap(_Bare()))), isFalse);
      expect(isPresenceTransport(_wrap(_wrap(_PresenceOnly()))), isTrue);
      expect(isPresenceTransport(_wrap(_wrap(_Bare()))), isFalse);
    });

    test('isStreamTransport and isPresenceTransport on plain transports', () {
      expect(isStreamTransport(_FakeWs()), isTrue);
      expect(isStreamTransport(_Bare()), isFalse);
      expect(isPresenceTransport(_PresenceOnly()), isTrue);
      expect(isPresenceTransport(_Bare()), isFalse);
    });

    test('waits a growing backoff step between attempts', () async {
      final flaky = _Flaky(3, TransportError('boom', statusCode: 503));
      await _wrap(flaky).pull(_pull());
      expect(_slept, [
        const Duration(milliseconds: 1),
        const Duration(milliseconds: 2),
        const Duration(milliseconds: 4),
      ]);
    });

    test(
      'gives up after the configured retries and throws the last error',
      () async {
        final flaky = _Flaky(99, TransportError('boom', statusCode: 503));
        await expectLater(
          _wrap(flaky, retries: 2).pull(_pull()),
          throwsA(
            isA<TransportError>().having((e) => e.statusCode, 'status', 503),
          ),
        );
        expect(flaky.calls, 3);
      },
    );

    // Go parity: Go answers a deterministic push failure with a 500, so the
    // default policy does not retry one, though TransportError marks it
    // retryable.
    test('does not retry a 500 by default', () async {
      final flaky = _Flaky(99, TransportError('hook said no', statusCode: 500));
      await expectLater(
        _wrap(flaky).pull(_pull()),
        throwsA(isA<TransportError>()),
      );
      expect(flaky.calls, 1);
    });

    // Differs from crdt-js: it retries any Error. Retrying an unknown error
    // would call the auth provider again after a cancellation.
    test('does not retry an arbitrary exception or Error', () async {
      for (final error in [
        StateError('a bug'),
        const FormatException('garbled'),
        Exception('unknown'),
      ]) {
        final bad = _Flaky(99, error);
        await expectLater(_wrap(bad).pull(_pull()), throwsA(same(error)));
        expect(bad.calls, 1, reason: '$error');
      }
    });

    test('retries a NetworkError only when it is marked retryable', () async {
      final down = _Flaky(1, NetworkError('unreachable'));
      await _wrap(down).pull(_pull());
      expect(down.calls, 2);

      final fixed = _Flaky(99, NetworkError('refused', retryable: false));
      await expectLater(
        _wrap(fixed).pull(_pull()),
        throwsA(isA<NetworkError>()),
      );
      expect(fixed.calls, 1);
    });

    test('never retries a cancelled error, whatever its type says', () async {
      for (final error in [
        _CancelledCrdt(),
        NetworkError('cancelled', code: CrdtErrorCode.cancelled),
      ]) {
        final bad = _Flaky(99, error);
        await expectLater(_wrap(bad).pull(_pull()), throwsA(same(error)));
        expect(bad.calls, 1);
      }
    });

    test('isRetryable replaces the default policy', () async {
      final flaky = _Flaky(99, TransportError('bad request', statusCode: 400));
      final t = withRetry(
        flaky,
        retries: 2,
        backoff: _backoff,
        sleep: _sleep,
        isRetryable: (_) => true,
      );
      await expectLater(t.pull(_pull()), throwsA(isA<TransportError>()));
      expect(flaky.calls, 3);
    });

    test('honours Retry-After on a 429 or 503, capped', () async {
      final flaky = _Flaky(
        2,
        TransportError(
          'slow down',
          statusCode: 429,
          headers: const {'retry-after': '7'},
        ),
      );
      await _wrap(flaky).pull(_pull());
      expect(_slept, [const Duration(seconds: 7), const Duration(seconds: 7)]);

      _slept.clear();
      final huge = _Flaky(
        1,
        TransportError(
          'busy',
          statusCode: 503,
          headers: const {'retry-after': '86400'},
        ),
      );
      await _wrap(huge).pull(_pull());
      expect(_slept, [maxRetryAfter]);
    });

    test(
      'ignores Retry-After on other statuses and when the backoff is longer',
      () async {
        final bad = _Flaky(
          1,
          TransportError(
            'gateway',
            statusCode: 502,
            headers: const {'retry-after': '30'},
          ),
        );
        await _wrap(bad).pull(_pull());
        expect(_slept, [const Duration(milliseconds: 1)]);

        _slept.clear();
        final longBackoff = withRetry(
          _Flaky(
            1,
            TransportError(
              'busy',
              statusCode: 503,
              headers: const {'retry-after': '1'},
            ),
          ),
          retries: 1,
          backoff: () =>
              Backoff(initialDelay: const Duration(seconds: 5), jitter: false),
          sleep: _sleep,
        );
        await longBackoff.pull(_pull());
        expect(_slept, [const Duration(seconds: 5)]);
      },
    );

    test('retries presence calls too', () async {
      var calls = 0;
      final inner = _CountingPresence(() {
        calls++;
        if (calls == 1) throw TransportError('boom', statusCode: 503);
      });
      await _wrap(inner)
          .updatePresence(const PresenceUpdate(nodeId: 'n1', topic: 't'));
      expect(calls, 2);
    });
  });
}

class _CountingPresence extends _Bare implements PresenceTransport {
  _CountingPresence(this.onCall);
  final void Function() onCall;

  @override
  Future<void> updatePresence(PresenceUpdate update) async => onCall();

  @override
  Future<List<PresenceState>> getPresence(String topic) async => const [];
}

final class _CancelledCrdt extends CrdtError {
  _CancelledCrdt()
    : super('cancelled', code: CrdtErrorCode.cancelled, retryable: true);
}
