@TestOn('vm')
@Tags(['conformance'])
library;

import 'package:grove_crdt/grove_crdt.dart';
import 'package:test/test.dart';

import 'conformance_server.dart';

void main() {
  test(
    'Go and Dart preserve an unobserved concurrent set add across sync',
    () async {
      final server = await ConformanceServer.start();
      addTearDown(server.stop);
      final aClock = HybridClock('a', nowMs: () => 1000);
      final bClock = HybridClock('b', nowMs: () => 1001);
      final a = CrdtStore('a', aClock);
      final b = CrdtStore('b', bClock);
      final client = CrdtClient(
        nodeId: 'a',
        baseUrl: server.syncUrl,
        clock: aClock,
      );
      addTearDown(client.dispose);
      addTearDown(a.dispose);
      addTearDown(b.dispose);
      await Future.wait([a.ready, b.ready]);
      final add = a.addToSet('notes', '1', 'tags', ['x'])!;
      final remove = b.removeFromSet('notes', '1', 'tags', ['x'])!;
      await client.push([add]);
      await client.push([remove]);
      final pulled = await client.pull(tables: ['notes']);
      a.applyChanges(pulled.changes);
      b.applyChanges(pulled.changes);
      expect(a.getDocument('notes', '1')?['tags'], ['x']);
      expect(b.getDocument('notes', '1')?['tags'], ['x']);
    },
    skip: ConformanceServer.skipReason(),
  );
}
