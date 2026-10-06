/// Web WebSocket connector over the browser `WebSocket`.
library;

import 'package:web_socket_channel/html.dart';

import 'websocket.dart';

/// Web connector. A browser handshake cannot carry custom headers, so each
/// auth header is added to the URL as a lower-cased query parameter (the
/// crdt-js convention), through `webSocketUrl`.
///
/// Warning: tokens in a URL can be logged by proxies and servers. Use
/// short-lived tokens.
WebSocketConnector platformWebSocketConnector() =>
    (url, headers, protocols) async {
      final channel = HtmlWebSocketChannel.connect(
        webSocketUrl(url, headers),
        protocols: protocols,
      );
      await channel.ready;
      return wrapChannel(channel);
    };
