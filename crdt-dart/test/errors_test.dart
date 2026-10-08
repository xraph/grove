// Ports the CRDTError cases of crdt-js src/__tests__/client.test.ts and the
// TransportError cases of src/__tests__/transport.test.ts, then covers the
// rest of errors.ts.
import 'package:grove_crdt/grove_crdt.dart';
import 'package:test/test.dart';

void main() {
  group('CRDTError', () {
    test('has correct name property', () {
      final err = CrdtError('test');
      expect(err.name, 'CrdtError');
    });

    test('includes statusCode', () {
      final err = CrdtError('test', statusCode: 404);
      expect(err.statusCode, 404);
    });

    // The TS case checks `instanceof Error`. Dart errors thrown on purpose
    // are Exceptions, and `CrdtError implements Exception`.
    test('extends Error', () {
      final err = CrdtError('test');
      expect(err, isA<Exception>());
    });

    test('has undefined statusCode when not provided', () {
      final err = CrdtError('test');
      expect(err.statusCode, isNull);
    });
  });

  group('TransportError', () {
    test('has correct name property', () {
      final err = TransportError('test');
      expect(err.name, 'TransportError');
    });

    test('includes statusCode', () {
      final err = TransportError('test', statusCode: 404);
      expect(err.statusCode, 404);
    });

    test('extends Error', () {
      final err = TransportError('test');
      expect(err, isA<Exception>());
    });

    test('extends CRDTError for backward compatibility', () {
      final err = TransportError('test', statusCode: 500);
      expect(err, isA<CrdtError>());
      expect(err.statusCode, 500);
    });
  });

  group('error taxonomy', () {
    test('a bare CrdtError is an invalid, non-retryable state', () {
      final err = CrdtError('test');
      expect(err.code, CrdtErrorCode.invalidState);
      expect(err.retryable, isFalse);
      expect(err.message, 'test');
    });

    test('codes carry the crdt-js strings', () {
      expect(
        {for (final c in CrdtErrorCode.values) c.name: c.value},
        {
          'networkUnreachable': 'NETWORK_UNREACHABLE',
          'syncTimeout': 'SYNC_TIMEOUT',
          'mergeConflict': 'MERGE_CONFLICT',
          'validationFailed': 'VALIDATION_FAILED',
          'unauthorized': 'UNAUTHORIZED',
          'rateLimited': 'RATE_LIMITED',
          'offlineQueueFull': 'OFFLINE_QUEUE_FULL',
          'pluginRejected': 'PLUGIN_REJECTED',
          'storageError': 'STORAGE_ERROR',
          'invalidState': 'INVALID_STATE',
          // Dart only: crdt-js has no cancellation code.
          'cancelled': 'CANCELLED',
        },
      );
    });

    test('toString names the class and the message', () {
      expect(CrdtError('boom').toString(), 'CrdtError: boom');
      expect(TransportError('down').toString(), 'TransportError: down');
    });

    test('isRetryableStatus: no status, 5xx, 429 and 408 are transient', () {
      expect(isRetryableStatus(null), isTrue);
      expect(isRetryableStatus(500), isTrue);
      expect(isRetryableStatus(503), isTrue);
      expect(isRetryableStatus(429), isTrue);
      expect(isRetryableStatus(408), isTrue);
      expect(isRetryableStatus(400), isFalse);
      expect(isRetryableStatus(401), isFalse);
      expect(isRetryableStatus(404), isFalse);
      expect(isRetryableStatus(200), isFalse);
    });

    test('TransportError is retryable per the status', () {
      expect(TransportError('x').retryable, isTrue);
      expect(TransportError('x', statusCode: 503).retryable, isTrue);
      expect(TransportError('x', statusCode: 404).retryable, isFalse);
      expect(
        TransportError('x', statusCode: 404).code,
        CrdtErrorCode.networkUnreachable,
      );
    });

    test('TransportError keeps the response body and server time', () {
      final at = DateTime.utc(2026, 10, 4, 12);
      final err = TransportError(
        'x',
        statusCode: 500,
        body: {'error': 'e'},
        serverTime: at,
      );
      expect(err.body, {'error': 'e'});
      expect(err.serverTime, at);
    });

    test('NetworkError is retryable and unreachable by default', () {
      final err = NetworkError('offline');
      expect(err, isA<CrdtError>());
      expect(err.name, 'NetworkError');
      expect(err.code, CrdtErrorCode.networkUnreachable);
      expect(err.retryable, isTrue);
    });

    test('NetworkError can carry another code, as a timeout does', () {
      final cause = Exception('socket');
      final err = NetworkError(
        'timed out',
        code: CrdtErrorCode.syncTimeout,
        cause: cause,
      );
      expect(err.code, CrdtErrorCode.syncTimeout);
      expect(err.retryable, isTrue);
      expect(err.cause, same(cause));
    });

    test('ValidationError names the field and is not retryable', () {
      final err = ValidationError('bad', field: 'title');
      expect(err.name, 'ValidationError');
      expect(err.field, 'title');
      expect(err.code, CrdtErrorCode.validationFailed);
      expect(err.retryable, isFalse);
      expect(err.statusCode, isNull);
      expect(ValidationError('bad').field, isNull);
    });

    test('SyncError defaults to a retryable timeout and keeps its cause', () {
      final cause = StateError('merge');
      final err = SyncError('pull failed', phase: SyncPhase.pull, cause: cause);
      expect(err.name, 'SyncError');
      expect(err.code, CrdtErrorCode.syncTimeout);
      expect(err.retryable, isTrue);
      expect(err.phase, SyncPhase.pull);
      expect(err.cause, same(cause));
      expect(
        SyncError('m', code: CrdtErrorCode.mergeConflict).code,
        CrdtErrorCode.mergeConflict,
      );
    });

    test('PluginError names the plugin and is not retryable', () {
      final err = PluginError('rejected', 'audit');
      expect(err.name, 'PluginError');
      expect(err.pluginName, 'audit');
      expect(err.code, CrdtErrorCode.pluginRejected);
      expect(err.retryable, isFalse);
    });

    test('every subclass is a CrdtError', () {
      expect(<CrdtError>[
        TransportError('a'),
        NetworkError('a'),
        ValidationError('a'),
        SyncError('a'),
        PluginError('a', 'p'),
      ], everyElement(isA<CrdtError>()));
    });
  });
}
