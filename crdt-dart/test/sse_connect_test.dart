// The native SSE connector (`sse_io.dart`) against a real loopback server and
// against a streaming `MockClient`. It binds only to 127.0.0.1. The web
// connector (`sse_web.dart`) needs a browser `fetch`, so it is covered by the
// web compile check, not here.
@TestOn('vm')
library;

import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:grove_crdt/grove_crdt.dart';
import 'package:http/http.dart' as http;
import 'package:http/testing.dart';
import 'package:test/test.dart';

void main() {
  group('native SSE connect', () {
    test('streams events from a local server, sends headers, and closes the '
        'connection on disconnect', () async {
      // A raw socket, so the test sees the client hang up: an HttpServer only
      // notices a closed client on its next write.
      final server = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
      addTearDown(server.close);
      final requestText = Completer<String>();
      final clientGone = Completer<void>();
      server.listen((socket) {
        final head = StringBuffer();
        socket.listen(
          (bytes) {
            head.write(latin1.decode(bytes));
            if (head.toString().contains('\r\n\r\n') &&
                !requestText.isCompleted) {
              requestText.complete(head.toString());
              socket.write(
                'HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\n'
                'connection: close\r\n\r\n'
                ': hello\n\n'
                'event: change\ndata: {"table":"t","pk":"1","field":"f",'
                '"crdt_type":"lww","hlc":{"ts":"5","c":0,"node":"s"},'
                '"node_id":"s","value":1}\n\n',
              );
            }
          },
          onDone: () {
            if (!clientGone.isCompleted) clientGone.complete();
            socket.destroy();
          },
          onError: (Object _) {
            if (!clientGone.isCompleted) clientGone.complete();
          },
        );
      });

      final stream = CrdtStream(
        baseUrl: Uri.parse('http://127.0.0.1:${server.port}/sync'),
        headers: const {'X-App': 'demo'},
        auth: StaticAuthProvider({'Authorization': 'Bearer tok'}),
      );
      final change = Completer<void>();
      stream.on((e) {
        if (e is StreamChange && !change.isCompleted) change.complete();
      });
      stream.connect();
      await change.future.timeout(const Duration(seconds: 20));

      final request = (await requestText.future).toLowerCase();
      expect(request, startsWith('get /sync/stream '));
      expect(request, contains('accept: text/event-stream'));
      expect(request, contains('authorization: bearer tok'));
      expect(request, contains('x-app: demo'));
      expect(stream.lastHlc!.ts, BigInt.from(5));

      stream.disconnect();
      await clientGone.future.timeout(const Duration(seconds: 20));
    });

    test('passes the status and lower-case headers through', () async {
      late http.BaseRequest seen;
      final client = MockClient.streaming((request, body) async {
        seen = request;
        return http.StreamedResponse(
          Stream.value(utf8.encode(': x\n\n')),
          200,
          headers: {'Date': 'Sun, 04 Oct 2026 12:00:00 GMT'},
        );
      });
      final connect = defaultSseConnect(client: client);
      final response = await connect(Uri.parse('http://x/stream?a=1'), const {
        'authorization': 'Bearer t',
      }, Completer<void>().future);
      expect(seen.method, 'GET');
      expect(seen.url.toString(), 'http://x/stream?a=1');
      expect(seen.headers['accept'], 'text/event-stream');
      expect(seen.headers['authorization'], 'Bearer t');
      expect(response.status, 200);
      expect(response.headers['date'], 'Sun, 04 Oct 2026 12:00:00 GMT');
      expect(await response.body.toList(), isNotEmpty);
    });
  });
}
