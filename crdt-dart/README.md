# grove_crdt

A pure Dart client for Grove's CRDT sync protocol, the Dart sibling of
`crdt-js`. It speaks the same wire format as the Go `crdt` package: pull and
push over HTTP, the change stream over SSE, and the multiplexed WebSocket.

```bash
dart pub add grove_crdt
```

```dart
import 'package:grove_crdt/grove_crdt.dart';

final client = CrdtClient(
  nodeId: 'device-1',
  transport: HttpStreamTransport(baseUrl: Uri.parse('https://api.example.com/sync')),
  tables: ['documents'],
);
final store = CrdtStore('device-1', client.clock);
final engine = SyncEngine(client, store, tables: ['documents']);
store.setField('documents', 'doc-1', 'title', 'Hello');
await engine.sync();
```

Where `crdt-js` and the Go server disagree, this package follows the Go
server. The differences are listed in `doc/go-parity.md`.
