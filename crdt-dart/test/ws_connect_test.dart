// The native WebSocket connector (`ws_connect_io.dart`) through a real
// WebSocketTransport against a loopback server. It binds only to 127.0.0.1.
// The web connector (`ws_connect_web.dart`) needs a browser WebSocket, so it is
// covered by the web compile check and by the `webSocketUrl` test in
// websocket_test.dart, not here.
@TestOn('vm')
library;

import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:grove_crdt/grove_crdt.dart';
import 'package:test/test.dart';

void main() {
  test('speaks to a local server: handshake headers, a pull, a binary '
      'change frame', () async {
    final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    addTearDown(() => server.close(force: true));
    final handshake = Completer<HttpRequest>();
    server.listen((request) async {
      handshake.complete(request);
      final socket = await WebSocketTransformer.upgrade(
        request,
        protocolSelector: (offered) => offered.first,
      );
      socket.listen((message) {
        final frame = jsonDecode(message as String) as Map<String, Object?>;
        switch (frame['type']) {
          case 'pull_request':
            socket.add(
              jsonEncode({
                'type': 'pull_response',
                'request_id': frame['request_id'],
                'payload': {
                  'changes': <Object?>[],
                  'latest_hlc': {'ts': '9', 'c': 0, 'node': 's'},
                },
              }),
            );
          case 'subscribe':
            // A binary frame: the transport decodes it as UTF-8 text.
            socket.add(
              utf8.encode(
                jsonEncode({
                  'type': 'change',
                  'payload': {
                    'table': 'docs',
                    'pk': '1',
                    'field': 'f',
                    'crdt_type': 'lww',
                    'hlc': {'ts': '4', 'c': 0, 'node': 's'},
                    'node_id': 's',
                    'value': 1,
                  },
                }),
              ),
            );
        }
      });
    });

    final transport = WebSocketTransport(
      url: Uri.parse('ws://127.0.0.1:${server.port}/sync/ws'),
      protocols: const ['grove'],
      auth: StaticAuthProvider({'Authorization': 'Bearer tok'}),
      pingInterval: Duration.zero,
    );
    addTearDown(transport.close);

    final response = await transport
        .pull(PullRequest(tables: const ['docs'], nodeId: 'n'))
        .timeout(const Duration(seconds: 20));
    expect(response.latestHlc.ts, BigInt.from(9));
    final request = await handshake.future;
    expect(request.headers.value('authorization'), 'Bearer tok');
    expect(request.headers.value('sec-websocket-protocol'), 'grove');

    final sub = transport.subscribe(const StreamConfig(tables: ['docs']));
    final got = Completer<ChangeRecord>();
    sub.on((e) {
      if (e is StreamChange && !got.isCompleted) got.complete(e.change);
    });
    sub.connect();
    final change = await got.future.timeout(const Duration(seconds: 20));
    expect(change.pk, '1');
    expect(sub.lastHlc!.ts, BigInt.from(4));
    sub.disconnect();
  });

  test(
    'a refused connection is a NetworkError that names no query value',
    () async {
      // Bind and release a port so nothing listens on it.
      final probe = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
      final port = probe.port;
      await probe.close();
      final transport = WebSocketTransport(
        url: Uri.parse('ws://127.0.0.1:$port/ws?token=sekrit-value'),
        pingInterval: Duration.zero,
      );
      addTearDown(transport.close);
      await expectLater(
        transport.pull(PullRequest(tables: const [], nodeId: 'n')),
        throwsA(
          isA<NetworkError>().having(
            (e) => e.toString(),
            'text',
            allOf(
              isNot(contains('sekrit-value')),
              contains('connection failed'),
            ),
          ),
        ),
      );
    },
  );

  test('a 403 on the upgrade leaves no query value in the message or the '
      'cause', () async {
    final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    addTearDown(() => server.close(force: true));
    server.listen((request) {
      request.response
        ..statusCode = HttpStatus.forbidden
        ..close();
    });
    final transport = WebSocketTransport(
      url: Uri.parse('ws://127.0.0.1:${server.port}/ws?token=sekrit-value'),
      auth: StaticAuthProvider({'Authorization': 'Bearer hdr-sekrit-value'}),
      pingInterval: Duration.zero,
    );
    addTearDown(transport.close);
    Object? caught;
    try {
      await transport.pull(PullRequest(tables: const [], nodeId: 'n'));
    } on Object catch (e) {
      caught = e;
    }
    final error = caught! as NetworkError;
    expect(error.message, isNot(contains('sekrit-value')));
    expect('${error.cause}', isNot(contains('sekrit-value')));
    expect('$error', contains('connection failed'));
  });
}
