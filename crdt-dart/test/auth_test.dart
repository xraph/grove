// Port of crdt-js src/__tests__/auth.test.ts, case for case.
import 'dart:async';

import 'package:grove_crdt/grove_crdt.dart';
import 'package:test/test.dart';

/// `getHeaders: () => ({...})`
class _SyncAuth implements CrdtAuthProvider {
  @override
  Map<String, String> getHeaders() => {'Authorization': 'Bearer sync-token'};
}

/// `getHeaders: async () => ({...})`
class _AsyncAuth implements CrdtAuthProvider {
  @override
  Future<Map<String, String>> getHeaders() async => {
    'Authorization': 'Bearer async-token',
  };
}

/// `getHeaders: vi.fn(...)`, with the call count `toHaveBeenCalledTimes` reads.
class _CountingAuth implements CrdtAuthProvider {
  int calls = 0;

  @override
  Map<String, String> getHeaders() {
    calls++;
    return {'X-Request-ID': '$calls'};
  }
}

/// The token refresh pattern: the token is rebuilt on every call.
class _RefreshingAuth implements CrdtAuthProvider {
  String token = 'initial';

  @override
  Future<Map<String, String>> getHeaders() async {
    token = 'refreshed-${DateTime.now().millisecondsSinceEpoch}';
    return {'Authorization': 'Bearer $token'};
  }
}

void main() {
  group('StaticAuthProvider', () {
    test('returns configured headers', () {
      final auth = StaticAuthProvider({'Authorization': 'Bearer abc'});
      expect(auth.getHeaders(), {'Authorization': 'Bearer abc'});
    });

    test('returns a copy each time (not the same reference)', () {
      final auth = StaticAuthProvider({'X-Key': 'val'});
      final a = auth.getHeaders();
      final b = auth.getHeaders();
      expect(a, b);
      expect(identical(a, b), isFalse);
    });

    test('mutations to returned object do not affect provider', () {
      final auth = StaticAuthProvider({'X-Key': 'val'});
      final headers = auth.getHeaders();
      headers['X-Key'] = 'mutated';
      expect(auth.getHeaders()['X-Key'], 'val');
    });

    test('returns empty object when constructed with empty headers', () {
      final auth = StaticAuthProvider({});
      expect(auth.getHeaders(), isEmpty);
    });

    test('implements AuthProvider interface', () async {
      final CrdtAuthProvider auth = StaticAuthProvider({'key': 'value'});
      expect(await auth.getHeaders(), {'key': 'value'});
    });

    test('is not changed by edits to the map it was built from', () {
      final source = {'k': 'v'};
      final auth = StaticAuthProvider(source);
      source['k'] = 'changed';
      expect(auth.getHeaders(), {'k': 'v'});
    });
  });

  group('Dynamic AuthProvider', () {
    test('supports sync getHeaders()', () async {
      final CrdtAuthProvider auth = _SyncAuth();
      expect(await auth.getHeaders(), {'Authorization': 'Bearer sync-token'});
      expect(_SyncAuth().getHeaders(), {'Authorization': 'Bearer sync-token'});
    });

    test('supports async getHeaders()', () async {
      final CrdtAuthProvider auth = _AsyncAuth();
      final headers = await auth.getHeaders();
      expect(headers, {'Authorization': 'Bearer async-token'});
    });

    test('can be called multiple times with vi.fn()', () {
      final auth = _CountingAuth();

      final h1 = auth.getHeaders();
      final h2 = auth.getHeaders();
      expect(h1, {'X-Request-ID': '1'});
      expect(h2, {'X-Request-ID': '2'});
      expect(auth.calls, 2);
    });

    test('supports token refresh pattern', () async {
      final CrdtAuthProvider auth = _RefreshingAuth();

      final h1 = await auth.getHeaders();
      expect(h1['Authorization'], contains('Bearer refreshed-'));
    });

    test('a FutureOr result awaits the same either way', () async {
      final providers = <CrdtAuthProvider>[_SyncAuth(), _AsyncAuth()];
      for (final p in providers) {
        final FutureOr<Map<String, String>> r = p.getHeaders();
        expect((await r).keys, ['Authorization']);
      }
    });
  });
}
