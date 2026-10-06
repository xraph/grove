/// Native SSE connect over a streamed `package:http` GET.
library;

import 'dart:async';

import 'package:http/http.dart' as http;

import 'sse.dart';

/// Native SSE connector: a streamed GET through [client], or through a new
/// client per connection (closed when the body ends or [abort] completes)
/// when [client] is null.
SseConnect platformSseConnect({http.Client? client}) =>
    (url, headers, abort) async {
      final owned = client == null;
      final c = client ?? http.Client();
      if (owned) unawaited(abort.whenComplete(c.close));
      final request = http.AbortableRequest('GET', url, abortTrigger: abort)
        ..headers.addAll({
          'accept': 'text/event-stream',
          'cache-control': 'no-cache',
          ...headers,
        });
      try {
        final response = await c.send(request);
        return SseResponse(
          response.statusCode,
          owned ? _closing(response.stream, c) : response.stream,
          headers: {
            for (final e in response.headers.entries)
              e.key.toLowerCase(): e.value,
          },
        );
      } on Object {
        if (owned) c.close();
        rethrow;
      }
    };

Stream<List<int>> _closing(Stream<List<int>> body, http.Client client) async* {
  try {
    yield* body;
  } finally {
    client.close();
  }
}
