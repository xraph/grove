// Port of the `HttpTransport` and `timeout, retry, empty body` describes of
// crdt-js src/__tests__/transport.test.ts, case for case, with
// `package:http/testing.dart`'s MockClient in place of `fetch` mocks. The
// `TransportError` cases of that file live in errors_test.dart, and the two
// `HttpStreamTransport` cases land with that class in Task 13.
import 'dart:async';
import 'dart:convert';

import 'package:fake_async/fake_async.dart';
import 'package:grove_crdt/grove_crdt.dart';
import 'package:http/http.dart' as http;
import 'package:http/testing.dart';
import 'package:test/test.dart';

const _pullOk = '{"changes":[],"latest_hlc":{"ts":"1","c":0,"node":"s"}}';
const _pushOk = '{"merged":1,"latest_hlc":{"ts":"1","c":0,"node":"s"}}';

/// A MockClient that records what it was sent.
final class _Server {
  _Server(this._respond);

  /// Always answers with [body] as JSON.
  _Server.json(Object body, [int status = 200])
    : _respond = ((r, n) =>
          http.Response(body is String ? body : jsonEncode(body), status));

  final FutureOr<http.Response> Function(http.Request request, int call)
  _respond;
  final requests = <http.Request>[];

  int get calls => requests.length;

  late final http.Client client = MockClient((r) async {
    requests.add(r);
    return _respond(r, requests.length);
  });
}

/// `getHeaders: vi.fn(() => ({ Authorization: "Bearer dynamic" }))`
class _CountingAuth implements CrdtAuthProvider {
  _CountingAuth(this.headers);
  final Map<String, String> headers;
  int calls = 0;

  @override
  Map<String, String> getHeaders() {
    calls++;
    return headers;
  }
}

class _AsyncAuth implements CrdtAuthProvider {
  @override
  Future<Map<String, String>> getHeaders() async => {
    'Authorization': 'Bearer async',
  };
}

final class _TokenAuth implements CrdtAuthProvider {
  _TokenAuth(this.token);
  final String Function() token;

  @override
  Map<String, String> getHeaders() => {'authorization': 'Bearer ${token()}'};
}

/// What a provider throws when its account was switched away.
final class _Cancelled implements Exception {
  @override
  String toString() => 'cancelled';
}

final class _CancelledCrdt extends CrdtError {
  _CancelledCrdt()
    : super('cancelled', code: CrdtErrorCode.cancelled, retryable: true);
}

class _FnAuth implements CrdtAuthProvider {
  _FnAuth(this.fn);
  final Map<String, String> Function() fn;

  @override
  Map<String, String> getHeaders() => fn();
}

class _ThrowingAuth implements CrdtAuthProvider {
  _ThrowingAuth({required this.throwOnCall});
  final int throwOnCall;
  int calls = 0;
  final Object error = _Cancelled();

  @override
  Map<String, String> getHeaders() {
    calls++;
    if (calls >= throwOnCall) throw error;
    return {'authorization': 'Bearer t$calls'};
  }
}

Backoff _fast() =>
    Backoff(initialDelay: const Duration(milliseconds: 1), jitter: false);

Future<void> _noSleep(Duration _) async {}

Uri _base([String path = '/sync']) => Uri.parse('https://api.example.com$path');

PullRequest _pullReq() => PullRequest(tables: const [], nodeId: 'n1');

PushRequest _pushReq() => const PushRequest(changes: [], nodeId: 'a');

void main() {
  group('HttpTransport', () {
    test('sends POST to /pull with correct body', () async {
      final server = _Server.json(_pullOk);
      final transport = HttpTransport(baseUrl: _base(), client: server.client);

      await transport.pull(PullRequest(tables: const ['users'], nodeId: 'n1'));

      expect(server.calls, 1);
      final r = server.requests.single;
      expect(r.url.toString(), 'https://api.example.com/sync/pull');
      expect(r.method, 'POST');
      final body = jsonDecode(r.body) as Map<String, Object?>;
      expect(body['tables'], ['users']);
      expect(body['node_id'], 'n1');
    });

    test('sends POST to /push with correct body', () async {
      final server = _Server.json(_pushOk);
      final transport = HttpTransport(baseUrl: _base(), client: server.client);

      final changes = [
        ChangeRecord(
          table: 'users',
          pk: '1',
          field: 'name',
          crdtType: CrdtType.lww,
          hlc: HLC(BigInt.from(100), 0, 'n1'),
          nodeId: 'n1',
          value: const JsonValue('Alice'),
        ),
      ];
      await transport.push(PushRequest(changes: changes, nodeId: 'n1'));

      final r = server.requests.single;
      expect(r.url.toString(), 'https://api.example.com/sync/push');
      final body = jsonDecode(r.body) as Map<String, Object?>;
      expect(body['changes'], hasLength(1));
      expect(body['node_id'], 'n1');
    });

    test('strips trailing slashes from baseURL', () async {
      final server = _Server.json(_pullOk);
      final transport = HttpTransport(
        baseUrl: _base('/sync///'),
        client: server.client,
      );

      await transport.pull(_pullReq());
      expect(
        server.requests.single.url.toString(),
        'https://api.example.com/sync/pull',
      );
      expect(transport.baseUrl.toString(), 'https://api.example.com/sync');
    });

    test('includes static headers', () async {
      final server = _Server.json(_pullOk);
      final transport = HttpTransport(
        baseUrl: _base(),
        client: server.client,
        headers: const {'X-Custom': 'value'},
      );

      await transport.pull(_pullReq());
      final headers = server.requests.single.headers;
      expect(headers['X-Custom'], 'value');
      expect(headers['Content-Type'], 'application/json');
      expect(headers['Accept'], 'application/json');
    });

    test('resolves auth headers dynamically', () async {
      final server = _Server.json(_pullOk);
      final auth = _CountingAuth({'Authorization': 'Bearer dynamic'});
      final transport = HttpTransport(
        baseUrl: _base(),
        client: server.client,
        auth: auth,
      );

      await transport.pull(_pullReq());
      expect(auth.calls, 1);
      expect(server.requests.single.headers['Authorization'], 'Bearer dynamic');
    });

    test('auth headers take precedence over static headers', () async {
      final server = _Server.json(_pullOk);
      final transport = HttpTransport(
        baseUrl: _base(),
        client: server.client,
        headers: const {'Authorization': 'Bearer static-loses'},
        auth: _CountingAuth({'Authorization': 'Bearer auth-wins'}),
      );

      await transport.pull(_pullReq());
      expect(
        server.requests.single.headers['Authorization'],
        'Bearer auth-wins',
      );
    });

    test('supports async auth getHeaders()', () async {
      final server = _Server.json(_pullOk);
      final transport = HttpTransport(
        baseUrl: _base(),
        client: server.client,
        auth: _AsyncAuth(),
      );

      await transport.pull(_pullReq());
      expect(server.requests.single.headers['Authorization'], 'Bearer async');
    });

    test('throws TransportError on non-ok response', () async {
      final server = _Server.json('server error', 500);
      final transport = HttpTransport(
        baseUrl: _base(),
        client: server.client,
        retries: 0,
      );

      await expectLater(
        transport.pull(_pullReq()),
        throwsA(isA<TransportError>()),
      );
    });

    test('error includes status code and message', () async {
      final server = _Server.json('bad request', 400);
      final transport = HttpTransport(baseUrl: _base(), client: server.client);

      try {
        await transport.pull(_pullReq());
        fail('should have thrown');
      } on TransportError catch (err) {
        expect(err.message, contains('400'));
        expect(err.statusCode, 400);
      }
    });

    test('auth getHeaders called per request', () async {
      final server = _Server(
        (r, n) => http.Response(n == 1 ? _pullOk : _pushOk, 200),
      );
      final auth = _CountingAuth({'Authorization': 'Bearer token'});
      final transport = HttpTransport(
        baseUrl: _base(),
        client: server.client,
        auth: auth,
      );

      await transport.pull(_pullReq());
      await transport.push(_pushReq());
      expect(auth.calls, 2);
    });
  });

  group('timeout, retry, empty body', () {
    test('retries a 503 and then succeeds', () async {
      final server = _Server(
        (r, n) =>
            n < 3 ? http.Response('busy', 503) : http.Response(_pullOk, 200),
      );
      final t = HttpTransport(
        baseUrl: Uri.parse('http://x'),
        client: server.client,
        retries: 3,
        backoff: _fast,
        sleep: _noSleep,
      );
      final resp = await t.pull(PullRequest(tables: const ['a'], nodeId: 'n1'));
      expect(server.calls, 3);
      expect(resp.changes, isEmpty);
    });

    test('does not retry a 400', () async {
      final server = _Server.json('bad', 400);
      final t = HttpTransport(
        baseUrl: Uri.parse('http://x'),
        client: server.client,
        retries: 3,
        backoff: _fast,
        sleep: _noSleep,
      );
      await expectLater(
        t.pull(PullRequest(tables: const ['a'], nodeId: 'n1')),
        throwsA(isA<TransportError>()),
      );
      expect(server.calls, 1);
    });

    test('tolerates a 204 with no body on presence update', () async {
      final client = MockClient((r) async => http.Response('', 204));
      final t = HttpTransport(baseUrl: Uri.parse('http://x'), client: client);
      await expectLater(
        t.updatePresence(
          const PresenceUpdate(
            nodeId: 'n1',
            topic: 't',
            data: <String, Object?>{},
          ),
        ),
        completes,
      );
    });

    // The TS case aborts a fetch through an AbortController after 20 ms of real
    // time. Here the clock is fake, and the case also checks the request was
    // really aborted rather than abandoned.
    test('aborts after the timeout', () {
      fakeAsync((async) {
        final hung = _Hung();
        final t = HttpTransport(
          baseUrl: Uri.parse('http://x'),
          client: hung,
          timeout: const Duration(milliseconds: 20),
          retries: 0,
        );
        Object? error;
        t
            .pull(PullRequest(tables: const ['a'], nodeId: 'n1'))
            .then<void>((_) {}, onError: (Object e) => error = e);

        async.elapse(const Duration(milliseconds: 19));
        expect(error, isNull);
        async.elapse(const Duration(milliseconds: 2));

        expect(
          error,
          isA<NetworkError>()
              .having((e) => e.code, 'code', CrdtErrorCode.syncTimeout)
              .having((e) => e.retryable, 'retryable', isTrue)
              .having(
                (e) => e.message,
                'message',
                'CRDT /pull timed out after 20ms',
              ),
        );
        var aborted = false;
        hung.request!.abortTrigger!.then<void>((_) => aborted = true);
        async.flushMicrotasks();
        expect(aborted, isTrue);
      });
    });
  });

  group('Dart transport behaviour', () {
    test(
      'does not retry a 500, which carries a deterministic crdt error',
      () async {
        var calls = 0;
        final t = HttpTransport(
          baseUrl: Uri.parse('http://x/sync'),
          client: MockClient((r) async {
            calls++;
            return http.Response(
              '{"error":"crdt: inbound change hook: no"}',
              500,
            );
          }),
          sleep: (_) async {},
        );
        await expectLater(
          t.push(const PushRequest(changes: [], nodeId: 'a')),
          throwsA(
            isA<TransportError>()
                .having((e) => e.statusCode, 'status', 500)
                .having((e) => serverMessage(e.body), 'body', contains('hook')),
          ),
        );
        expect(calls, 1);
      },
    );

    test('reports the server Date on success and on failure', () async {
      final seen = <DateTime>[];
      final t = HttpTransport(
        baseUrl: Uri.parse('http://x/sync'),
        client: MockClient(
          (r) async => http.Response(
            '{"error":"x"}',
            500,
            headers: {'date': 'Sun, 04 Oct 2026 12:00:00 GMT'},
          ),
        ),
        onServerTime: seen.add,
      );
      await expectLater(
        t.push(const PushRequest(changes: [], nodeId: 'a')),
        throwsA(
          isA<TransportError>().having(
            (e) => e.serverTime,
            'serverTime',
            DateTime.utc(2026, 10, 4, 12),
          ),
        ),
      );
      expect(seen, [DateTime.utc(2026, 10, 4, 12)]);

      seen.clear();
      final ok = HttpTransport(
        baseUrl: Uri.parse('http://x/sync'),
        client: MockClient(
          (r) async => http.Response(
            _pushOk,
            200,
            headers: {'date': 'Sun, 04 Oct 2026 12:00:01 GMT'},
          ),
        ),
        onServerTime: seen.add,
      );
      await ok.push(const PushRequest(changes: [], nodeId: 'a'));
      expect(seen, [DateTime.utc(2026, 10, 4, 12, 0, 1)]);
    });

    test('refreshes auth once on 401 and retries', () async {
      var token = 'old';
      var refreshed = 0;
      final t = HttpTransport(
        baseUrl: Uri.parse('http://x/sync'),
        auth: _TokenAuth(() => token),
        onUnauthorized: () async {
          refreshed++;
          token = 'new';
          return true;
        },
        client: MockClient(
          (r) async => r.headers['authorization'] == 'Bearer new'
              ? http.Response(
                  '{"changes":null,"latest_hlc":{"ts":"0","c":0,"node":""}}',
                  200,
                )
              : http.Response('', 401),
        ),
      );
      final resp = await t.pull(PullRequest(tables: const ['t'], nodeId: 'a'));
      expect(resp.changes, isEmpty);
      expect(refreshed, 1);
    });

    test('sends bodies through the envelope byte for byte', () async {
      late String body;
      final t = HttpTransport(
        baseUrl: Uri.parse('http://x/sync'),
        client: MockClient((r) async {
          body = r.body;
          return http.Response(
            '{"merged":1,"latest_hlc":{"ts":"5","c":0,"node":"s"}}',
            200,
          );
        }),
      );
      await t.push(
        PushRequest(
          nodeId: 'a',
          changes: [
            ChangeRecord(
              table: 't',
              pk: '1',
              field: 's',
              crdtType: CrdtType.set,
              hlc: HLC(BigInt.one, 0, 'a'),
              nodeId: 'a',
              setOp: const SetOperation(SetOpType.add, [
                {'b': 1, 'a': '<'},
              ]),
            ),
          ],
        ),
      );
      // Go parity: `json.Marshal` escapes `<` as \u003c, and so does the
      // envelope. The elements are sorted by key.
      expect(body, contains(r'"elements":[{"a":"\u003c","b":1}]'));
    });
  });

  group('HttpTransport retry policy', () {
    test('retries 408, 429, 502, 503 and 504 once each', () async {
      for (final status in [408, 429, 502, 503, 504]) {
        final server = _Server(
          (r, n) =>
              n == 1 ? http.Response('', status) : http.Response(_pullOk, 200),
        );
        final t = HttpTransport(
          baseUrl: _base(),
          client: server.client,
          sleep: _noSleep,
          backoff: _fast,
        );
        await t.pull(_pullReq());
        expect(server.calls, 2, reason: 'status $status');
      }
    });

    test('never retries a 500, a 501 or another 4xx', () async {
      for (final status in [400, 401, 403, 404, 409, 410, 413, 422, 500, 501]) {
        final server = _Server.json('{"error":"x"}', status);
        final t = HttpTransport(
          baseUrl: _base(),
          client: server.client,
          sleep: _noSleep,
          backoff: _fast,
        );
        await expectLater(
          t.pull(_pullReq()),
          throwsA(
            isA<TransportError>().having((e) => e.statusCode, 'status', status),
          ),
          reason: 'status $status',
        );
        expect(server.calls, 1, reason: 'status $status');
      }
    });

    test(
      'waits the backoff schedule between attempts and then gives up',
      () async {
        final server = _Server.json('busy', 503);
        final slept = <Duration>[];
        final t = HttpTransport(
          baseUrl: _base(),
          client: server.client,
          retries: 3,
          backoff: () => Backoff(
            initialDelay: const Duration(milliseconds: 10),
            jitter: false,
          ),
          sleep: (d) async => slept.add(d),
        );
        await expectLater(
          t.pull(_pullReq()),
          throwsA(
            isA<TransportError>().having((e) => e.statusCode, 'status', 503),
          ),
        );
        expect(server.calls, 4);
        expect(slept, [
          const Duration(milliseconds: 10),
          const Duration(milliseconds: 20),
          const Duration(milliseconds: 40),
        ]);
      },
    );

    test('retries a network error and then succeeds', () async {
      var calls = 0;
      final t = HttpTransport(
        baseUrl: _base(),
        client: MockClient((r) async {
          calls++;
          if (calls < 3) throw http.ClientException('connection reset');
          return http.Response(_pullOk, 200);
        }),
        sleep: _noSleep,
        backoff: _fast,
      );
      await t.pull(_pullReq());
      expect(calls, 3);
    });

    test(
      'throws NetworkError, with its cause, when no attempt gets a response',
      () async {
        var calls = 0;
        final cause = http.ClientException('unreachable');
        final t = HttpTransport(
          baseUrl: _base(),
          client: MockClient((r) async {
            calls++;
            throw cause;
          }),
          sleep: _noSleep,
          backoff: _fast,
        );
        await expectLater(
          t.pull(_pullReq()),
          throwsA(
            isA<NetworkError>()
                .having((e) => e.cause, 'cause', same(cause))
                .having((e) => e.code, 'code', CrdtErrorCode.networkUnreachable)
                .having((e) => e.retryable, 'retryable', isTrue),
          ),
        );
        expect(calls, 3);
      },
    );

    test(
      'a programming Error from the client is not turned into a network error',
      () async {
        final t = HttpTransport(
          baseUrl: _base(),
          client: MockClient((r) async => throw StateError('bug')),
          sleep: _noSleep,
        );
        await expectLater(t.pull(_pullReq()), throwsStateError);
      },
    );

    test('a timed out attempt is retried', () {
      fakeAsync((async) {
        var calls = 0;
        final t = HttpTransport(
          baseUrl: _base(),
          client: MockClient((r) {
            calls++;
            return calls == 1
                ? Completer<http.Response>().future
                : Future.value(http.Response(_pullOk, 200));
          }),
          timeout: const Duration(milliseconds: 20),
          retries: 1,
          sleep: _noSleep,
          backoff: _fast,
        );
        PullResponse? resp;
        t.pull(_pullReq()).then((r) => resp = r);
        async.elapse(const Duration(milliseconds: 25));
        expect(calls, 2);
        expect(resp, isNotNull);
      });
    });

    test(
      'honours Retry-After on a 429 and a 503, capped at maxRetryAfter',
      () async {
        for (final (status, header, expected) in [
          (429, '7', const Duration(seconds: 7)),
          (503, '7', const Duration(seconds: 7)),
          (429, '86400', maxRetryAfter),
          (503, ' 3 ', const Duration(seconds: 3)),
          (429, 'soon', const Duration(milliseconds: 1)),
          (502, '30', const Duration(milliseconds: 1)),
        ]) {
          final server = _Server(
            (r, n) => n == 1
                ? http.Response('', status, headers: {'retry-after': header})
                : http.Response(_pullOk, 200),
          );
          final slept = <Duration>[];
          final t = HttpTransport(
            baseUrl: _base(),
            client: server.client,
            backoff: _fast,
            sleep: (d) async => slept.add(d),
          );
          await t.pull(_pullReq());
          expect(slept, [expected], reason: '$status retry-after "$header"');
        }
      },
    );

    test('measures an HTTP-date Retry-After from the response Date', () async {
      final server = _Server(
        (r, n) => n == 1
            ? http.Response(
                '',
                429,
                headers: {
                  'date': 'Sun, 04 Oct 2026 12:00:00 GMT',
                  'retry-after': 'Sun, 04 Oct 2026 12:00:12 GMT',
                },
              )
            : http.Response(_pullOk, 200),
      );
      final slept = <Duration>[];
      final t = HttpTransport(
        baseUrl: _base(),
        client: server.client,
        backoff: _fast,
        sleep: (d) async => slept.add(d),
      );
      await t.pull(_pullReq());
      expect(slept, [const Duration(seconds: 12)]);
    });

    test('reports the server Date on every attempt', () async {
      final server = _Server(
        (r, n) => n == 1
            ? http.Response(
                '',
                503,
                headers: {'date': 'Sun, 04 Oct 2026 12:00:00 GMT'},
              )
            : http.Response(
                _pullOk,
                200,
                headers: {'date': 'Sun, 04 Oct 2026 12:00:03 GMT'},
              ),
      );
      final seen = <DateTime>[];
      final t = HttpTransport(
        baseUrl: _base(),
        client: server.client,
        sleep: _noSleep,
        backoff: _fast,
        onServerTime: seen.add,
      );
      await t.pull(_pullReq());
      expect(seen, [
        DateTime.utc(2026, 10, 4, 12),
        DateTime.utc(2026, 10, 4, 12, 0, 3),
      ]);
    });

    test('does not report a Date header that is not an HTTP date', () async {
      final seen = <DateTime>[];
      final t = HttpTransport(
        baseUrl: _base(),
        client: MockClient(
          (r) async => http.Response(
            _pullOk,
            200,
            headers: {'date': '2026-10-04T12:00:00Z'},
          ),
        ),
        onServerTime: seen.add,
      );
      await t.pull(_pullReq());
      expect(seen, isEmpty);
    });
  });

  group('HttpTransport auth failures', () {
    test(
      'a 401 without onUnauthorized throws TransportError and is not retried',
      () async {
        final server = _Server.json('', 401);
        final t = HttpTransport(
          baseUrl: _base(),
          client: server.client,
          sleep: _noSleep,
        );
        await expectLater(
          t.pull(_pullReq()),
          throwsA(
            isA<TransportError>().having((e) => e.statusCode, 'status', 401),
          ),
        );
        expect(server.calls, 1);
      },
    );

    test('a 401 is refreshed once per request, then reported', () async {
      final server = _Server.json('', 401);
      var refreshed = 0;
      final t = HttpTransport(
        baseUrl: _base(),
        client: server.client,
        sleep: _noSleep,
        onUnauthorized: () async {
          refreshed++;
          return true;
        },
      );
      await expectLater(
        t.pull(_pullReq()),
        throwsA(
          isA<TransportError>().having((e) => e.statusCode, 'status', 401),
        ),
      );
      expect(refreshed, 1);
      expect(server.calls, 2);
    });

    test('onUnauthorized returning false sends nothing more', () async {
      final server = _Server.json('', 401);
      final t = HttpTransport(
        baseUrl: _base(),
        client: server.client,
        sleep: _noSleep,
        onUnauthorized: () async => false,
      );
      await expectLater(t.pull(_pullReq()), throwsA(isA<TransportError>()));
      expect(server.calls, 1);
    });

    test('re-reads the auth headers for every attempt', () async {
      var n = 0;
      final server = _Server(
        (r, call) =>
            call < 3 ? http.Response('', 503) : http.Response(_pullOk, 200),
      );
      final t = HttpTransport(
        baseUrl: _base(),
        client: server.client,
        auth: _TokenAuth(() => 't${++n}'),
        sleep: _noSleep,
        backoff: _fast,
      );
      await t.pull(_pullReq());
      expect(server.requests.map((r) => r.headers['authorization']), [
        'Bearer t1',
        'Bearer t2',
        'Bearer t3',
      ]);
    });

    // Privacy: an account switch makes the auth provider throw a cancellation.
    // It must stop the request for good.
    test(
      'auth that throws on the second attempt stops the retries and propagates',
      () async {
        final server = _Server.json('busy', 503);
        final auth = _ThrowingAuth(throwOnCall: 2);
        final slept = <Duration>[];
        final t = HttpTransport(
          baseUrl: _base(),
          client: server.client,
          auth: auth,
          retries: 5,
          sleep: (d) async => slept.add(d),
          backoff: _fast,
        );
        await expectLater(t.pull(_pullReq()), throwsA(same(auth.error)));
        expect(server.calls, 1);
        expect(auth.calls, 2);
        expect(slept, hasLength(1));
      },
    );

    test('a cancelled error from auth is called once and not retried, even through withRetry', () async {
      final server = _Server.json(_pullOk);
      final cancelled = _CancelledCrdt();
      var calls = 0;
      final t = withRetry(
        HttpTransport(
          baseUrl: _base(),
          client: server.client,
          auth: _FnAuth(() {
            calls++;
            throw cancelled;
          }),
          sleep: _noSleep,
        ),
        sleep: _noSleep,
      );
      await expectLater(t.pull(_pullReq()), throwsA(same(cancelled)));
      expect(calls, 1);
      expect(server.calls, 0);
    });

    test(
      'a cancelled error thrown by the client is not wrapped or retried',
      () async {
        final cancelled = _CancelledCrdt();
        final network = NetworkError('gone', code: CrdtErrorCode.cancelled);
        for (final error in [cancelled, network]) {
          var calls = 0;
          final t = HttpTransport(
            baseUrl: _base(),
            client: MockClient((r) async {
              calls++;
              throw error;
            }),
            sleep: _noSleep,
          );
          await expectLater(t.pull(_pullReq()), throwsA(same(error)));
          expect(calls, 1);
        }
      },
    );

    test('auth that throws on the first attempt sends no request', () async {
      final server = _Server.json(_pullOk);
      final auth = _ThrowingAuth(throwOnCall: 1);
      final t = HttpTransport(
        baseUrl: _base(),
        client: server.client,
        auth: auth,
      );
      await expectLater(t.pull(_pullReq()), throwsA(same(auth.error)));
      expect(server.calls, 0);
    });

    test('auth that throws during a 401 refresh is not retried', () async {
      final server = _Server.json('', 401);
      final auth = _ThrowingAuth(throwOnCall: 2);
      final t = HttpTransport(
        baseUrl: _base(),
        client: server.client,
        auth: auth,
        sleep: _noSleep,
        onUnauthorized: () async => true,
      );
      await expectLater(t.pull(_pullReq()), throwsA(same(auth.error)));
      expect(server.calls, 1);
    });

    test(
      'onUnauthorized that throws is not retried and propagates unchanged',
      () async {
        final server = _Server.json('', 401);
        final cancelled = _Cancelled();
        final t = HttpTransport(
          baseUrl: _base(),
          client: server.client,
          sleep: _noSleep,
          onUnauthorized: () async => throw cancelled,
        );
        await expectLater(t.pull(_pullReq()), throwsA(same(cancelled)));
        expect(server.calls, 1);
      },
    );

    test('onUnauthorized that throws a retryable NetworkError is still not retried', () async {
      final server = _Server.json('', 401);
      final error = NetworkError('account switched');
      final t = HttpTransport(
        baseUrl: _base(),
        client: server.client,
        sleep: _noSleep,
        onUnauthorized: () async => throw error,
      );
      await expectLater(t.pull(_pullReq()), throwsA(same(error)));
      expect(server.calls, 1);
    });
  });

  group('HttpTransport bodies and errors', () {
    test(
      'a failed response carries the status, decoded body, headers and Date',
      () async {
        final t = HttpTransport(
          baseUrl: _base(),
          client: MockClient(
            (r) async => http.Response(
              '{"error":"nope"}',
              422,
              headers: {
                'x-trace': 'abc',
                'date': 'Sun, 04 Oct 2026 12:00:00 GMT',
              },
            ),
          ),
        );
        await expectLater(
          t.pull(_pullReq()),
          throwsA(
            isA<TransportError>()
                .having((e) => e.statusCode, 'status', 422)
                .having((e) => e.body, 'body', {'error': 'nope'})
                .having((e) => e.headers['x-trace'], 'header', 'abc')
                .having(
                  (e) => e.serverTime,
                  'serverTime',
                  DateTime.utc(2026, 10, 4, 12),
                ),
          ),
        );
      },
    );

    test('a failed response with a text body keeps the text, an empty one has none', () async {
      final text = HttpTransport(
        baseUrl: _base(),
        client: _Server.json('plain failure', 400).client,
      );
      await expectLater(
        text.pull(_pullReq()),
        throwsA(
          isA<TransportError>().having((e) => e.body, 'body', 'plain failure'),
        ),
      );
      final empty = HttpTransport(
        baseUrl: _base(),
        client: _Server.json('', 400).client,
      );
      await expectLater(
        empty.pull(_pullReq()),
        throwsA(isA<TransportError>().having((e) => e.body, 'body', isNull)),
      );
    });

    test('a 200 whose body is not valid for the envelope is a TransportError, not retried', () async {
      for (final body in ['not json', '', '[1,2]', '{"changes":"x"}']) {
        final server = _Server.json(body);
        final t = HttpTransport(
          baseUrl: _base(),
          client: server.client,
          sleep: _noSleep,
        );
        await expectLater(
          t.pull(_pullReq()),
          throwsA(
            isA<TransportError>()
                .having((e) => e.statusCode, 'status', 200)
                .having((e) => e.retryable, 'retryable', isFalse),
          ),
          reason: 'body "$body"',
        );
        expect(server.calls, 1, reason: 'body "$body"');
      }
    });

    test('sends and reads UTF-8 whatever the response charset says', () async {
      final server = _Server(
        (r, n) => http.Response.bytes(
          utf8.encode(
            '{"topic":"t","states":[{"node_id":"n","topic":"t","data":{"name":"héllo ☃"},"updated_at":"2026-10-04T12:00:00Z","expires_at":"0001-01-01T00:00:00Z"}]}',
          ),
          200,
          headers: {'content-type': 'application/json'},
        ),
      );
      final t = HttpTransport(baseUrl: _base(), client: server.client);
      final states = await t.getPresence('t');
      expect((states.single.data as Map<String, Object?>)['name'], 'héllo ☃');

      await t.updatePresence(
        const PresenceUpdate(
          nodeId: 'n',
          topic: 't',
          data: {'name': 'héllo ☃'},
        ),
      );
      expect(utf8.decode(server.requests.last.bodyBytes), contains('héllo ☃'));
      expect(server.requests.last.headers['content-type'], 'application/json');
    });

    test(
      'getPresence sends a GET with the topic as a query parameter',
      () async {
        final server = _Server.json('{"topic":"a b","states":[]}');
        final t = HttpTransport(
          baseUrl: _base(),
          client: server.client,
          auth: _CountingAuth({'Authorization': 'Bearer p'}),
        );
        expect(await t.getPresence('a b'), isEmpty);
        final r = server.requests.single;
        expect(r.method, 'GET');
        expect(r.url.path, '/sync/presence');
        expect(r.url.queryParameters, {'topic': 'a b'});
        expect(r.headers['Authorization'], 'Bearer p');
        expect(r.headers.containsKey('content-type'), isFalse);
      },
    );

    test('getPresence reads an empty answer as nobody present', () async {
      final t = HttpTransport(
        baseUrl: _base(),
        client: MockClient((r) async => http.Response('', 204)),
      );
      expect(await t.getPresence('t'), isEmpty);
    });

    test(
      'updatePresence posts the envelope body to the presence path',
      () async {
        final server = _Server.json('', 204);
        final t = HttpTransport(baseUrl: _base(), client: server.client);
        await t.updatePresence(
          const PresenceUpdate(nodeId: 'n1', topic: 't', data: {'x': 1}),
        );
        final r = server.requests.single;
        expect(r.url.path, '/sync/presence');
        expect(r.body, '{"data":{"x":1},"node_id":"n1","topic":"t"}');
      },
    );

    test('uses the configured envelope and paths', () async {
      final server = _Server.json(
        '{"changes":[],"latestHlc":{"ts":7,"counter":1,"nodeId":"s"}}',
      );
      final t = HttpTransport(
        baseUrl: _base(),
        client: server.client,
        envelope: camelDtoEnvelope,
        pullPath: 'api/pull',
      );
      final resp = await t.pull(
        PullRequest(
          tables: const ['x'],
          since: HLC(BigInt.from(9), 2, 'n'),
          nodeId: 'n',
        ),
      );
      expect(server.requests.single.url.path, '/sync/api/pull');
      expect(
        server.requests.single.body,
        '{"since":{"counter":2,"nodeId":"n","ts":9}}',
      );
      expect(resp.latestHlc.ts, BigInt.from(7));
    });

    test('an injected client is not closed by close()', () {
      var closed = false;
      final client = _TrackedClient(() => closed = true);
      HttpTransport(baseUrl: _base(), client: client).close();
      expect(closed, isFalse);
    });
  });

  group('TransportError retryAfter', () {
    test('reads seconds and dates, null otherwise', () {
      Duration? read(Map<String, String> h, {DateTime? at}) => TransportError(
        'x',
        statusCode: 429,
        headers: h,
        serverTime: at,
      ).retryAfter;
      expect(read({'retry-after': '5'}), const Duration(seconds: 5));
      expect(read({'retry-after': '0'}), Duration.zero);
      expect(read({'retry-after': '-5'}), isNull);
      expect(read({'retry-after': '1.5'}), isNull);
      expect(read({'retry-after': ''}), isNull);
      expect(read(const {}), isNull);
      final at = DateTime.utc(2026, 10, 4, 12);
      expect(
        read({'retry-after': 'Sun, 04 Oct 2026 12:00:09 GMT'}, at: at),
        const Duration(seconds: 9),
      );
      expect(
        read({'retry-after': 'Sun, 04 Oct 2026 11:59:00 GMT'}, at: at),
        Duration.zero,
      );
    });
  });
}

/// A client that never answers, and keeps the request it was sent.
final class _Hung extends http.BaseClient {
  http.AbortableRequest? request;

  @override
  Future<http.StreamedResponse> send(http.BaseRequest request) {
    this.request = request as http.AbortableRequest;
    return Completer<http.StreamedResponse>().future;
  }
}

final class _TrackedClient extends http.BaseClient {
  _TrackedClient(this.onClose);
  final void Function() onClose;

  @override
  Future<http.StreamedResponse> send(http.BaseRequest request) =>
      throw StateError('unused');

  @override
  void close() => onClose();
}
