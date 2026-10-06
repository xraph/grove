/// Native WebSocket connector over `IOWebSocketChannel`.
library;

import 'package:web_socket_channel/io.dart';

import 'websocket.dart';

/// Native connector: the auth headers go in the handshake, and the connection
/// is awaited until it is ready to send.
WebSocketConnector platformWebSocketConnector() =>
    (url, headers, protocols) async {
      final channel = IOWebSocketChannel.connect(
        url,
        headers: headers,
        protocols: protocols,
      );
      await channel.ready;
      return wrapChannel(channel);
    };
