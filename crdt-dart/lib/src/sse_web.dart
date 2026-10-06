/// Web SSE connect over `fetch` and a `ReadableStreamDefaultReader`.
///
/// `EventSource` cannot set request headers, so the web uses `fetch`. The
/// request carries its headers for a same-origin or CORS-enabled server.
library;

import 'dart:async';
import 'dart:js_interop';

import 'package:http/http.dart' as http;
import 'package:web/web.dart' as web;

import 'sse.dart';

@JS('fetch')
external JSPromise<web.Response> _fetch(JSString url, [web.RequestInit? init]);

/// `Headers.forEach`, which `package:web` leaves out.
extension type _IterableHeaders(JSObject _) implements JSObject {
  external void forEach(JSFunction callback);
}

/// Web SSE connector. [client] is ignored: the browser makes the request.
SseConnect platformSseConnect({http.Client? client}) =>
    (url, headers, abort) async {
      final controller = web.AbortController();
      unawaited(abort.then((_) => controller.abort()));
      final requestHeaders = web.Headers();
      for (final e in {
        'accept': 'text/event-stream',
        'cache-control': 'no-cache',
        ...headers,
      }.entries) {
        requestHeaders.append(e.key, e.value);
      }
      final response = await _fetch(
        url.toString().toJS,
        web.RequestInit(
          method: 'GET',
          headers: requestHeaders,
          signal: controller.signal,
        ),
      ).toDart;
      final received = <String, String>{};
      _IterableHeaders(response.headers).forEach(
        ((JSString value, JSString key, JSAny? _) {
          received[key.toDart.toLowerCase()] = value.toDart;
        }).toJS,
      );
      return SseResponse(
        response.status,
        _bodyStream(response.body),
        headers: received,
      );
    };

/// The response body as a byte stream, read chunk by chunk from a
/// `ReadableStreamDefaultReader`. Cancelling the subscription cancels the
/// reader.
Stream<List<int>> _bodyStream(web.ReadableStream? body) {
  if (body == null) return const Stream.empty();
  final reader = body.getReader() as web.ReadableStreamDefaultReader;
  var cancelled = false;
  late final StreamController<List<int>> controller;
  Future<void> pump() async {
    try {
      while (!cancelled) {
        final result = await reader.read().toDart;
        if (result.done) break;
        final value = result.value;
        if (value != null) controller.add((value as JSUint8Array).toDart);
      }
    } on Object catch (error, stack) {
      if (!cancelled) controller.addError(error, stack);
    } finally {
      if (!controller.isClosed) unawaited(controller.close());
    }
  }

  controller = StreamController<List<int>>(
    onListen: () => unawaited(pump()),
    onCancel: () async {
      cancelled = true;
      try {
        await reader.cancel().toDart;
      } on Object {
        // The reader is already closed or errored.
      }
    },
  );
  return controller.stream;
}
