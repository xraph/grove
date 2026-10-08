// There is no `room.test.ts` in crdt-js. These cases cover `room.ts` against
// the Go room handlers (`crdt/room.go` `RoomHTTPHandler` and the Forge
// extension's room routes), which are the authority for paths, body keys and
// status codes.
import 'dart:async';
import 'dart:convert';
import 'dart:math';

import 'package:grove_crdt/grove_crdt.dart';
import 'package:http/http.dart' as http;
import 'package:http/testing.dart';
import 'package:test/test.dart';

void main() {
  late List<http.Request> requests;
  RoomClient client(
    http.Response Function(http.Request r) reply, {
    Uri? baseUrl,
    String roomsPath = '/rooms',
    Map<String, String> headers = const {},
    CrdtAuthProvider? auth,
  }) {
    requests = [];
    return RoomClient(
      baseUrl: baseUrl ?? Uri.parse('http://x/sync'),
      roomsPath: roomsPath,
      headers: headers,
      auth: auth,
      client: MockClient((r) async {
        requests.add(r);
        return reply(r);
      }),
    );
  }

  const roomJson =
      '{"id":"documents:doc-1","type":"document","created_at":"2026-10-04T12:00:00Z","participant_count":1,'
      '"participants":[{"node_id":"a","topic":"documents:doc-1","data":{"name":"Ada"},"updated_at":"2026-10-04T12:00:00Z","expires_at":"2026-10-04T12:00:30Z"}]}';
  final json = {'content-type': 'application/json'};

  test('documentRoomId matches Go DocumentRoomID', () {
    expect(documentRoomId('documents', 'doc-1'), 'documents:doc-1');
  });

  test('joinDocumentRoom creates then joins with Go room keys', () async {
    final c = client(
      (r) => http.Response(
        r.url.path.endsWith('/join') ? roomJson : '{}',
        200,
        headers: {'content-type': 'application/json'},
      ),
    );
    final info = await c.joinDocumentRoom(
      'documents',
      'doc-1',
      'a',
      const ParticipantData(name: 'Ada', isTyping: true),
    );
    expect(requests.map((r) => '${r.method} ${r.url.path}'), [
      'POST /sync/rooms',
      'POST /sync/rooms/documents%3Adoc-1/join',
    ]);
    expect(jsonDecode(requests[0].body), {
      'id': 'documents:doc-1',
      'type': 'document',
      'metadata': {'pk': 'doc-1', 'table': 'documents'},
    });
    expect(jsonDecode(requests[1].body), {
      'node_id': 'a',
      'data': {'is_typing': true, 'name': 'Ada'},
    });
    expect(info.participantCount, 1);
    expect(info.participants.single.nodeId, 'a');
  });

  test('updateCursor sends node_id and cursor', () async {
    final c = client((r) => http.Response('', 204));
    await c.updateCursor('r1', 'a', const CursorPosition(line: 3, column: 4));
    expect(requests.single.url.path, '/sync/rooms/r1/cursor');
    expect(jsonDecode(requests.single.body), {
      'node_id': 'a',
      'cursor': {'line': 3, 'column': 4},
    });
  });

  test('getRoom returns null on a 404', () async {
    final c = client((r) => http.Response('{"error":"room not found"}', 404));
    expect(await c.getRoom('missing'), isNull);
  });

  test('randomColor picks from the palette', () {
    expect(randomColor(), startsWith('#'));
  });

  group('requests', () {
    test('the body is the Go-encoded JSON, keys sorted', () async {
      final c = client((r) => http.Response(roomJson, 200, headers: json));
      await c.joinDocumentRoom('documents', 'doc-1', 'a');
      expect(
        requests[0].body,
        '{"id":"documents:doc-1","metadata":{"pk":"doc-1","table":"documents"},'
        '"type":"document"}',
      );
      expect(requests[1].body, '{"node_id":"a"}');
      expect(requests[1].headers['content-type'], 'application/json');
    });

    test('a GET carries no body or content type', () async {
      final c = client((r) => http.Response('[]', 200, headers: json));
      await c.listRooms();
      expect(requests.single.method, 'GET');
      expect(requests.single.body, isEmpty);
      expect(requests.single.headers.containsKey('content-type'), isFalse);
    });

    test(
      'listRooms filters by type and reads Go\'s null as no rooms',
      () async {
        final c = client((r) => http.Response('null', 200, headers: json));
        expect(await c.listRooms(type: 'document'), isEmpty);
        expect(requests.single.url.path, '/sync/rooms');
        expect(requests.single.url.queryParameters, {'type': 'document'});

        expect(await c.listRooms(type: ''), isEmpty);
        expect(requests.last.url.hasQuery, isFalse);
      },
    );

    test('listRooms decodes the flattened room keys', () async {
      final c = client((r) => http.Response('[$roomJson]', 200, headers: json));
      final rooms = await c.listRooms();
      expect(rooms.single.room.id, 'documents:doc-1');
      expect(rooms.single.room.type, 'document');
      expect(rooms.single.participants.single.data, {'name': 'Ada'});
    });

    test('getRoom returns the room, with the id escaped in the path', () async {
      final c = client((r) => http.Response(roomJson, 200, headers: json));
      final info = await c.getRoom('a/b c');
      expect(info!.participantCount, 1);
      expect(requests.single.url.path, '/sync/rooms/a%2Fb%20c');
    });

    test('getRoom throws on a failure that is not a 404', () async {
      final c = client((r) => http.Response('{"error":"nope"}', 500));
      await expectLater(
        c.getRoom('r'),
        throwsA(
          isA<TransportError>().having((e) => e.statusCode, 'status', 500),
        ),
      );
      final unauthorized = client((r) => http.Response('', 401));
      await expectLater(
        unauthorized.getRoom('r'),
        throwsA(
          isA<TransportError>().having((e) => e.statusCode, 'status', 401),
        ),
      );
    });

    test('createRoom sends only the keys that are set', () async {
      final c = client(
        (r) => http.Response(
          '{"id":"r","created_at":"2026-10-04T12:00:00Z"}',
          201,
          headers: json,
        ),
      );
      final room = await c.createRoom('r');
      expect(jsonDecode(requests.single.body), {'id': 'r'});
      expect(room.id, 'r');

      await c.createRoom(
        'r',
        type: 'canvas',
        metadata: {'title': 'T'},
        maxParticipants: 4,
        createdBy: 'a',
      );
      expect(jsonDecode(requests.last.body), {
        'id': 'r',
        'type': 'canvas',
        'metadata': {'title': 'T'},
        'max_participants': 4,
        'created_by': 'a',
      });
    });

    test(
      'joinRoom without data sends no data key, and a full room is a 409',
      () async {
        var full = false;
        final c = client(
          (r) => full
              ? http.Response(
                  '{"error":"crdt: room r is full (2/2)"}',
                  409,
                  headers: json,
                )
              : http.Response(roomJson, 200, headers: json),
        );
        await c.joinRoom('r', 'a');
        expect(jsonDecode(requests.single.body), {'node_id': 'a'});

        full = true;
        await expectLater(
          c.joinRoom('r', 'b'),
          throwsA(
            isA<TransportError>()
                .having((e) => e.statusCode, 'status', 409)
                .having((e) => e.body, 'body', {
                  'error': 'crdt: room r is full (2/2)',
                }),
          ),
        );
      },
    );

    test('joinRoom with an empty body is an unreadable-body error', () async {
      final c = client((r) => http.Response('', 200));
      await expectLater(c.joinRoom('r', 'a'), throwsA(isA<TransportError>()));
    });

    test('leaveRoom posts node_id', () async {
      final c = client((r) => http.Response('', 204));
      await c.leaveRoom('r1', 'a');
      expect(requests.single.method, 'POST');
      expect(requests.single.url.path, '/sync/rooms/r1/leave');
      expect(jsonDecode(requests.single.body), {'node_id': 'a'});
    });

    test('leaveDocumentRoom leaves the document room', () async {
      final c = client((r) => http.Response('', 204));
      await c.leaveDocumentRoom('documents', 'doc-1', 'a');
      expect(requests.single.url.path, '/sync/rooms/documents%3Adoc-1/leave');
    });

    test('updateTyping posts node_id and is_typing', () async {
      final c = client((r) => http.Response('', 204));
      await c.updateTyping('r1', 'a', true);
      expect(requests.single.url.path, '/sync/rooms/r1/typing');
      expect(jsonDecode(requests.single.body), {
        'node_id': 'a',
        'is_typing': true,
      });
      await c.updateTyping('r1', 'a', false);
      expect(jsonDecode(requests.last.body), {
        'node_id': 'a',
        'is_typing': false,
      });
    });

    test('updateMetadata PUTs the metadata as the whole body', () async {
      // Go parity: RoomHTTPHandler and the Forge extension serve
      // `PUT /rooms/{id}/metadata` and decode the whole body as the metadata
      // (a json.RawMessage). crdt-js POSTs `{"metadata": ...}`, which Go
      // answers with a 405.
      final c = client((r) => http.Response('', 204));
      await c.updateMetadata('r1', {'title': 'Plan'});
      expect(requests.single.method, 'PUT');
      expect(requests.single.url.path, '/sync/rooms/r1/metadata');
      expect(jsonDecode(requests.single.body), {'title': 'Plan'});
    });

    test('updateMetadata on an unknown room is a 404', () async {
      final c = client(
        (r) => http.Response('{"error":"room not found"}', 404, headers: json),
      );
      await expectLater(
        c.updateMetadata('nope', 'x'),
        throwsA(
          isA<TransportError>().having((e) => e.statusCode, 'status', 404),
        ),
      );
      expect(requests.single.body, '"x"');
    });

    test('getParticipants decodes the states, and null as nobody', () async {
      final c = client(
        (r) => http.Response(
          '[{"node_id":"a","topic":"r1","data":{"name":"Ada"},'
          '"updated_at":"2026-10-04T12:00:00Z","expires_at":"0001-01-01T00:00:00Z"}]',
          200,
          headers: json,
        ),
      );
      final states = await c.getParticipants('r1');
      expect(requests.single.url.path, '/sync/rooms/r1/participants');
      expect(states.single.nodeId, 'a');
      expect(states.single.expiresAt, isNull);

      final empty = client((r) => http.Response('null', 200, headers: json));
      expect(await empty.getParticipants('r1'), isEmpty);
    });

    test('a trailing slash on the base and a custom rooms path', () async {
      final c = client(
        (r) => http.Response('[]', 200, headers: json),
        baseUrl: Uri.parse('http://x/sync//'),
        roomsPath: '/collab/rooms',
      );
      await c.listRooms();
      expect(requests.single.url.path, '/sync/collab/rooms');
    });

    test(
      'joinDocumentRoom still joins when creating the room failed',
      () async {
        final c = client(
          (r) => r.url.path.endsWith('/join')
              ? http.Response(roomJson, 200, headers: json)
              : http.Response('{"error":"boom"}', 500),
        );
        final info = await c.joinDocumentRoom('documents', 'doc-1', 'a');
        expect(info.participantCount, 1);
        expect(requests, hasLength(2));
      },
    );

    test('a failure to reach the server is a NetworkError', () async {
      final c = RoomClient(
        baseUrl: Uri.parse('http://x/sync'),
        client: MockClient((r) async => throw http.ClientException('refused')),
      );
      await expectLater(c.listRooms(), throwsA(isA<NetworkError>()));
    });
  });

  group('auth', () {
    test('static headers go out, and the provider\'s headers win', () async {
      final c = client(
        (r) => http.Response('[]', 200, headers: json),
        headers: {'x-app': 'one', 'authorization': 'Bearer static'},
        auth: StaticAuthProvider({'Authorization': 'Bearer fresh'}),
      );
      await c.listRooms();
      expect(requests.single.headers['x-app'], 'one');
      expect(requests.single.headers['authorization'], 'Bearer fresh');
    });

    test('headers are read for every request', () async {
      var n = 0;
      final c = client(
        (r) => r.method == 'GET'
            ? http.Response('[]', 200, headers: json)
            : http.Response('', 204),
        auth: _FnAuth(() => {'Authorization': 'Bearer t${++n}'}),
      );
      await c.listRooms();
      await c.leaveRoom('r', 'a');
      await c.updateTyping('r', 'a', true);
      expect(n, 3);
      expect(requests.map((r) => r.headers['authorization']), [
        'Bearer t1',
        'Bearer t2',
        'Bearer t3',
      ]);
    });

    test('an asynchronous provider is awaited', () async {
      final c = client(
        (r) => http.Response('[]', 200, headers: json),
        auth: _LaterAuth({'Authorization': 'Bearer later'}),
      );
      await c.listRooms();
      expect(requests.single.headers['authorization'], 'Bearer later');
    });

    test(
      'an error from the provider is an AuthError, sent nowhere, tried once',
      () async {
        final auth = _FnAuth(
          () => throw const FormatException('identity provider down'),
        );
        final c = client((r) => http.Response('[]', 200), auth: auth);
        await expectLater(
          c.listRooms(),
          throwsA(
            isA<AuthError>()
                .having((e) => e.retryable, 'retryable', isFalse)
                .having((e) => e.cause, 'cause', isA<FormatException>()),
          ),
        );
        expect(requests, isEmpty);
        expect(auth.calls, 1);
      },
    );

    test('a retryable NetworkError from the provider is not retryable out of '
        'a RoomClient', () async {
      final auth = _FnAuth(() => throw NetworkError('idp unreachable'));
      final c = client((r) => http.Response('[]', 200), auth: auth);
      await expectLater(
        c.getRoom('r'),
        throwsA(
          isA<AuthError>().having((e) => e.retryable, 'retryable', isFalse),
        ),
      );
      expect(requests, isEmpty);
      expect(auth.calls, 1);
    });

    test('a cancellation from the provider propagates as thrown, for every '
        'call', () async {
      final cancelled = CrdtError(
        'account switched',
        code: CrdtErrorCode.cancelled,
        retryable: true,
      );
      final auth = _FnAuth(() => throw cancelled);
      final c = client((r) => http.Response('[]', 200), auth: auth);
      final calls = <Future<Object?> Function()>[
        () => c.listRooms(),
        () => c.getRoom('r'),
        () => c.createRoom('r'),
        () => c.joinRoom('r', 'a'),
        () => c.leaveRoom('r', 'a'),
        () => c.updateCursor('r', 'a', const CursorPosition()),
        () => c.updateTyping('r', 'a', true),
        () => c.updateMetadata('r', 1),
        () => c.getParticipants('r'),
        () => c.joinDocumentRoom('t', 'p', 'a'),
        () => c.leaveDocumentRoom('t', 'p', 'a'),
      ];
      for (final call in calls) {
        await expectLater(call(), throwsA(same(cancelled)));
      }
      expect(requests, isEmpty);
    });

    test('joinDocumentRoom does not swallow a cancellation while creating, '
        'and sends no join', () async {
      final cancelled = CrdtError('gone', code: CrdtErrorCode.cancelled);
      var n = 0;
      final c = client(
        (r) => http.Response('{}', 200, headers: json),
        auth: _FnAuth(() {
          if (++n == 1) throw cancelled;
          return {};
        }),
      );
      await expectLater(
        c.joinDocumentRoom('t', 'p', 'a'),
        throwsA(same(cancelled)),
      );
      expect(requests, isEmpty);
      expect(n, 1);
    });

    test(
      'joinDocumentRoom does not swallow an auth failure while creating',
      () async {
        var n = 0;
        final c = client(
          (r) => http.Response('{}', 200, headers: json),
          auth: _FnAuth(() {
            if (++n == 1) throw const FormatException('idp down');
            return {};
          }),
        );
        await expectLater(
          c.joinDocumentRoom('t', 'p', 'a'),
          throwsA(isA<AuthError>()),
        );
        expect(requests, isEmpty);
      },
    );

    test(
      'a cancellation thrown while the request is in flight propagates',
      () async {
        final cancelled = CrdtError('gone', code: CrdtErrorCode.cancelled);
        final c = RoomClient(
          baseUrl: Uri.parse('http://x/sync'),
          client: MockClient((r) async => throw cancelled),
        );
        await expectLater(c.listRooms(), throwsA(same(cancelled)));
      },
    );

    test(
      'a request in flight is not turned into a retry or a 401 refresh',
      () async {
        final auth = _FnAuth(() => {'Authorization': 'Bearer t'});
        final c = client((r) => http.Response('no', 401), auth: auth);
        await expectLater(
          c.listRooms(),
          throwsA(
            isA<TransportError>().having((e) => e.statusCode, 'status', 401),
          ),
        );
        expect(requests, hasLength(1));
        expect(auth.calls, 1);
      },
    );
  });

  group('error text never carries a credential', () {
    const token = 'tok-9f8e7d6c5b4a';

    void expectClean(Object error) {
      final shown = StringBuffer(error.toString());
      if (error is CrdtError) shown.write(error.message);
      if (error is TransportError) {
        shown.write(error.body);
        shown.write(error.headers);
      }
      if (error is NetworkError) shown.write(error.cause);
      expect(shown.toString(), isNot(contains(token)));
      expect(shown.toString().toLowerCase(), isNot(contains(token)));
    }

    test('a server answer that echoes the Authorization value', () async {
      final c = client(
        (r) => http.Response(
          '{"error":"bad credential Bearer $token for '
          'http://x/sync/rooms?access_token=$token&type=a"}',
          500,
          headers: json,
        ),
        auth: StaticAuthProvider({'Authorization': 'Bearer $token'}),
      );
      Object? caught;
      try {
        await c.listRooms(type: 'a');
      } on TransportError catch (e) {
        caught = e;
      }
      expect(caught, isA<TransportError>());
      expectClean(caught!);
      expect((caught as TransportError).message, contains('REDACTED'));
      expect(caught.statusCode, 500);
    });

    test('the echoed token alone, without its scheme', () async {
      final c = client(
        (r) => http.Response('denied: $token', 403),
        auth: StaticAuthProvider({'Authorization': 'Bearer $token'}),
      );
      Object? caught;
      try {
        await c.getParticipants('r');
      } on TransportError catch (e) {
        caught = e;
      }
      expectClean(caught!);
      expect((caught as TransportError).message, contains('403'));
    });

    test('a static header value is removed too', () async {
      final c = client(
        (r) => http.Response('key $token rejected', 400),
        headers: {'x-api-key': token},
      );
      Object? caught;
      try {
        await c.leaveRoom('r', 'a');
      } on TransportError catch (e) {
        caught = e;
      }
      expectClean(caught!);
    });

    test(
      'a connection failure whose message names the URL with a query token',
      () async {
        requests = [];
        final c = RoomClient(
          baseUrl: Uri.parse('http://x/sync?access_token=$token'),
          auth: StaticAuthProvider({'Authorization': 'Bearer $token'}),
          client: MockClient(
            (r) async => throw http.ClientException(
              'Connection refused, uri=${r.url}',
              r.url,
            ),
          ),
        );
        Object? caught;
        try {
          await c.listRooms(type: 'document');
        } on NetworkError catch (e) {
          caught = e;
        }
        expect(caught, isA<NetworkError>());
        expectClean(caught!);
        final text = (caught as NetworkError).message;
        expect(text, contains('http://x/sync/rooms?'));
        expect(text, contains('REDACTED'));
        expect(text, isNot(contains('document')));
      },
    );

    test(
      'a failure that is not an Exception passes through untouched',
      () async {
        final c = RoomClient(
          baseUrl: Uri.parse('http://x/sync'),
          client: MockClient((r) async => throw StateError('bug')),
        );
        await expectLater(c.listRooms(), throwsA(isA<StateError>()));
        final errorClient = RoomClient(
          baseUrl: Uri.parse('http://x/sync'),
          client: MockClient(
            (r) async => throw ArgumentError('programmer error'),
          ),
        );
        await expectLater(
          errorClient.listRooms(),
          throwsA(isA<ArgumentError>()),
        );
      },
    );

    test('an unreadable success body does not echo a credential', () async {
      final c = client(
        (r) => http.Response('<html>$token', 200),
        auth: StaticAuthProvider({'Authorization': 'Bearer $token'}),
      );
      Object? caught;
      try {
        await c.getRoom('r');
      } on TransportError catch (e) {
        caught = e;
      }
      expect(caught, isA<TransportError>());
      expectClean(caught!);
    });
  });

  group('a success whose body is not read', () {
    final voidCalls = <String, Future<void> Function(RoomClient c)>{
      'leaveRoom': (c) => c.leaveRoom('r', 'a'),
      'updateCursor': (c) =>
          c.updateCursor('r', 'a', const CursorPosition(line: 1)),
      'updateTyping': (c) => c.updateTyping('r', 'a', true),
      'updateMetadata': (c) => c.updateMetadata('r', {'a': 1}),
      'leaveDocumentRoom': (c) => c.leaveDocumentRoom('t', 'p', 'a'),
    };

    for (final entry in voidCalls.entries) {
      test('${entry.key} completes on a 2xx with a text, broken or empty '
          'body', () async {
        for (final body in ['ok', '{"half":', '', '<html>done</html>']) {
          final c = client((r) => http.Response(body, 200));
          await entry.value(c);
          expect(requests, hasLength(1), reason: 'body "$body"');
        }
        final proxy = client(
          (r) => http.Response(
            'accepted',
            202,
            headers: {'content-type': 'text/plain'},
          ),
        );
        await entry.value(proxy);
      });
    }

    test('a call that returns data still rejects an unreadable body', () async {
      final c = client((r) => http.Response('ok', 200));
      await expectLater(c.getParticipants('r'), throwsA(isA<TransportError>()));
    });
  });

  group('room ids and path segments', () {
    final badIds = ['', '.', '..'];
    final idCalls = <String, Future<Object?> Function(RoomClient c, String id)>{
      'getRoom': (c, id) => c.getRoom(id),
      'joinRoom': (c, id) => c.joinRoom(id, 'a'),
      'leaveRoom': (c, id) => c.leaveRoom(id, 'a'),
      'updateCursor': (c, id) =>
          c.updateCursor(id, 'a', const CursorPosition()),
      'updateTyping': (c, id) => c.updateTyping(id, 'a', true),
      'updateMetadata': (c, id) => c.updateMetadata(id, 1),
      'getParticipants': (c, id) => c.getParticipants(id),
    };

    for (final entry in idCalls.entries) {
      test(
        '${entry.key} rejects an empty, "." or ".." id before any request',
        () async {
          final c = client((r) => http.Response('{}', 200, headers: json));
          for (final id in badIds) {
            await expectLater(
              entry.value(c, id),
              throwsA(isA<ArgumentError>()),
              reason: 'id "$id"',
            );
          }
          expect(requests, isEmpty);
        },
      );
    }

    test('document rooms reject an empty, "." or ".." table or pk', () async {
      final c = client((r) => http.Response('{}', 200, headers: json));
      for (final bad in badIds) {
        await expectLater(
          c.joinDocumentRoom(bad, 'pk', 'a'),
          throwsA(isA<ArgumentError>()),
        );
        await expectLater(
          c.joinDocumentRoom('t', bad, 'a'),
          throwsA(isA<ArgumentError>()),
        );
        await expectLater(
          c.leaveDocumentRoom(bad, 'pk', 'a'),
          throwsA(isA<ArgumentError>()),
        );
        await expectLater(
          c.leaveDocumentRoom('t', bad, 'a'),
          throwsA(isA<ArgumentError>()),
        );
      }
      expect(requests, isEmpty);
    });

    test('an id cannot climb out of the rooms path', () async {
      final c = client((r) => http.Response('', 204));
      for (final id in [
        'a/../b',
        '../x',
        'x/..',
        '%2e%2e',
        '..%2f..',
        'a:..',
        '...',
        'a?b=c#d',
      ]) {
        await c.leaveRoom(id, 'n');
        final url = requests.last.url;
        expect(url.path, startsWith('/sync/rooms/'), reason: id);
        expect(url.path, endsWith('/leave'), reason: id);
        expect(url.hasQuery, isFalse, reason: id);
        expect(url.hasFragment, isFalse, reason: id);
        // One segment between the rooms path and `leave`.
        expect(url.pathSegments, hasLength(4), reason: id);
        expect(url.pathSegments[2], id);
      }
    });

    test(
      'a document room with "." inside its parts is still one segment',
      () async {
        final c = client((r) => http.Response('', 204));
        await c.leaveDocumentRoom('a.b', 'c/..', 'n');
        expect(requests.single.url.pathSegments, hasLength(4));
      },
    );
  });

  group('redaction of the URL, the cause and the reason phrase', () {
    const token = 'qry-5c4b3a2918f7';

    test(
      'a secret in the failing URL query is hidden in message and cause',
      () async {
        final c = RoomClient(
          baseUrl: Uri.parse('http://x/sync'),
          client: MockClient(
            (r) async => throw http.ClientException(
              'Connection refused, uri=${r.url}',
              r.url,
            ),
          ),
        );
        Object? caught;
        try {
          await c.listRooms(type: token);
        } on NetworkError catch (e) {
          caught = e;
        }
        final error = caught! as NetworkError;
        expect(error.message, isNot(contains(token)));
        expect(error.message, contains('type=REDACTED'));
        expect(error.cause.toString(), isNot(contains(token)));
        expect(error.cause.toString(), contains('type=REDACTED'));
        expect(error.cause, isNot(isA<http.ClientException>()));
        expect(error.toString(), isNot(contains(token)));
      },
    );

    test(
      'a secret in the cause of a CrdtError thrown by the client is hidden',
      () async {
        final c = RoomClient(
          baseUrl: Uri.parse('http://x/sync'),
          auth: StaticAuthProvider({'Authorization': 'Bearer $token'}),
          client: MockClient(
            (r) async => throw NetworkError('dial http://h/p?k=$token failed'),
          ),
        );
        Object? caught;
        try {
          await c.listRooms();
        } on NetworkError catch (e) {
          caught = e;
        }
        final error = caught! as NetworkError;
        expect(error.message, isNot(contains(token)));
        expect(error.cause.toString(), isNot(contains(token)));
        expect(error.cause.toString(), contains('k=REDACTED'));
      },
    );

    test('the HTTP reason phrase is redacted', () async {
      final c = client(
        (r) => http.Response('', 500, reasonPhrase: 'Denied $token'),
        auth: StaticAuthProvider({'Authorization': 'Bearer $token'}),
      );
      Object? caught;
      try {
        await c.listRooms();
      } on TransportError catch (e) {
        caught = e;
      }
      final error = caught! as TransportError;
      expect(error.message, isNot(contains(token)));
      expect(error.toString(), isNot(contains(token)));
      expect(error.message, contains('500 Denied REDACTED'));
    });

    test('a reason phrase with a URL query value is redacted', () async {
      final c = client(
        (r) => http.Response('', 502, reasonPhrase: 'via http://h/p?k=$token'),
      );
      Object? caught;
      try {
        await c.listRooms();
      } on TransportError catch (e) {
        caught = e;
      }
      expect((caught! as TransportError).message, isNot(contains(token)));
    });
  });

  group('randomColor', () {
    test('answers a six digit hex colour', () {
      for (var i = 0; i < 50; i++) {
        expect(randomColor(), matches(RegExp(r'^#[0-9a-f]{6}$')));
      }
    });

    test('the injected random picks the entry, over the whole palette', () {
      final seen = <String>{};
      for (var i = 0; i < 16; i++) {
        seen.add(randomColor(_Fixed(i)));
      }
      expect(seen, hasLength(16));
      expect(randomColor(_Fixed(0)), '#e57373');
      expect(randomColor(_Fixed(15)), '#dce775');
    });

    test('a seeded random is repeatable', () {
      expect(randomColor(Random(7)), randomColor(Random(7)));
    });
  });
}

/// A provider that runs [fn] on every read.
final class _FnAuth implements CrdtAuthProvider {
  _FnAuth(this.fn);

  final Map<String, String> Function() fn;
  int calls = 0;

  @override
  Map<String, String> getHeaders() {
    calls++;
    return fn();
  }
}

/// A provider that answers after a tick.
final class _LaterAuth implements CrdtAuthProvider {
  _LaterAuth(this.headers);

  final Map<String, String> headers;

  @override
  Future<Map<String, String>> getHeaders() =>
      Future.delayed(Duration.zero, () => headers);
}

/// A [Random] whose `nextInt` answers one fixed value.
final class _Fixed implements Random {
  _Fixed(this.value);
  final int value;

  @override
  int nextInt(int max) => value;

  @override
  bool nextBool() => false;

  @override
  double nextDouble() => 0;
}
