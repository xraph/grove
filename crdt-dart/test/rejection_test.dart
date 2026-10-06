import 'package:grove_crdt/grove_crdt.dart';
import 'package:test/test.dart';

void main() {
  test('classifies a drift validation failure with its index', () {
    final r = classifyPushError(500, {
      'error': 'crdt: change[3]: crdt: change HLC timestamp drift too large (2h0m0s)',
    });
    expect(r, isA<DriftRejection>().having((d) => d.index, 'index', 3));
    expect(r.kind, 'drift');
  });

  test('classifies another validation failure with its index', () {
    final r = classifyPushError(500, {
      'error': 'crdt: change[0]: crdt: change pk is required',
    });
    expect(r, isA<ValidationRejection>().having((v) => v.index, 'index', 0));
    expect(r.reason, 'crdt: change pk is required');
  });

  test('classifies an oversized batch with the server limit', () {
    final r = classifyPushError(500, {
      'error': 'crdt: push exceeds max changes (12000 > 10000)',
    });
    expect(
      r,
      isA<BatchTooLargeRejection>().having((b) => b.limit, 'limit', 10000),
    );
  });

  test('classifies a sync hook rejection and keeps the reason', () {
    final r = classifyPushError(500, {
      'error': 'crdt: inbound change hook: title is locked',
    });
    expect(r, isA<HookRejection>());
    expect(r.reason, 'title is locked');
    expect(r.kind, 'hook');
  });

  test('reads forge HTTPError bodies', () {
    final r = classifyPushError(500, {
      'code': 'INTERNAL',
      'message': 'crdt: inbound change hook: nope',
    });
    expect(r, isA<HookRejection>());
  });

  test('reads a WebSocket error payload', () {
    expect(
      classifyPushError(0, {'error': 'crdt: inbound change hook: nope'}),
      isA<HookRejection>(),
    );
  });

  test('falls back to unclassified, bad_request for 4xx', () {
    expect(
      classifyPushError(500, 'Internal Server Error'),
      isA<UnclassifiedRejection>().having((u) => u.kind, 'kind', 'server'),
    );
    expect(
      classifyPushError(400, {'error': 'invalid request'}).kind,
      'bad_request',
    );
  });

  group('beyond the brief', () {
    test(
      'an oversized batch maps to the validation kind and keeps the text',
      () {
        final r = classifyPushError(500, {
          'error': 'crdt: push exceeds max changes (501 > 500)',
        });
        expect(r.kind, 'validation');
        expect(r.reason, 'crdt: push exceeds max changes (501 > 500)');
      },
    );

    test('a validation failure maps to the validation kind', () {
      expect(
        classifyPushError(500, {'error': 'crdt: change[2]: x'}).kind,
        'validation',
      );
    });

    // Go wraps the hook error as `crdt: inbound change hook: %w`, so the
    // reason is free application text. A hook that wraps a validation error
    // must still be blamed on the hook, never on the index it quotes.
    test(
      'a hook reason that quotes a change index is still a hook rejection',
      () {
        final r = classifyPushError(500, {
          'error': 'crdt: inbound change hook: crdt: change[7]: crdt: change pk is required',
        });
        expect(r, isA<HookRejection>());
        expect(r.reason, 'crdt: change[7]: crdt: change pk is required');
      },
    );

    test('a hook reason that quotes the batch limit is still a hook rejection', () {
      final r = classifyPushError(500, {
        'error':
            'crdt: inbound change hook: crdt: push exceeds max changes (2 > 1)',
      });
      expect(r, isA<HookRejection>());
    });

    test('a hook reason that quotes drift text is still a hook rejection', () {
      final r = classifyPushError(500, {
        'error':
            'crdt: inbound change hook: HLC timestamp drift too large for me',
      });
      expect(r, isA<HookRejection>());
    });

    test('a hook prefix inside another wrapper is found', () {
      final r = classifyPushError(500, {
        'error': 'push failed: crdt: inbound change hook: locked',
      });
      expect(r, isA<HookRejection>());
      expect(r.reason, 'locked');
    });

    test('a multi-line inner message is kept whole', () {
      final r = classifyPushError(500, {
        'error': 'crdt: change[1]: line one\nline two',
      });
      expect(r.reason, 'line one\nline two');
    });

    test('an absurd index does not throw', () {
      final r = classifyPushError(500, {
        'error': 'crdt: change[99999999999999999999999]: x',
      });
      expect(r, isA<UnclassifiedRejection>());
    });

    test(
      'no body, a non-map body and a non-string message are unclassified',
      () {
        expect(classifyPushError(502, null), isA<UnclassifiedRejection>());
        expect(classifyPushError(502, null).reason, '');
        expect(classifyPushError(502, 42).reason, '');
        expect(classifyPushError(500, {'error': 7}).reason, '');
        expect(classifyPushError(502, null).kind, 'server');
      },
    );

    test('status picks the kind of an unclassified failure', () {
      expect(classifyPushError(399, 'x').kind, 'server');
      expect(classifyPushError(400, 'x').kind, 'bad_request');
      expect(classifyPushError(413, 'x').kind, 'bad_request');
      expect(classifyPushError(499, 'x').kind, 'bad_request');
      expect(classifyPushError(500, 'x').kind, 'server');
      expect(classifyPushError(0, 'x').kind, 'server');
    });

    test('every kind is one a PendingRejection accepts', () {
      const kinds = {'hook', 'validation', 'drift', 'bad_request', 'server'};
      final all = <PushRejection>[
        const ValidationRejection(0, 'a'),
        const DriftRejection(0, 'a'),
        const BatchTooLargeRejection(1, 'a'),
        const HookRejection('a'),
        const UnclassifiedRejection(400, 'a'),
        const UnclassifiedRejection(500, 'a'),
      ];
      expect(all.map((r) => r.kind).toSet(), kinds);
    });

    test('serverMessage reads error, message and a raw string', () {
      expect(serverMessage({'error': 'a'}), 'a');
      expect(serverMessage({'message': 'b'}), 'b');
      expect(serverMessage({'error': 'a', 'message': 'b'}), 'a');
      expect(serverMessage('raw'), 'raw');
      expect(serverMessage(null), isNull);
      expect(serverMessage({'other': 'x'}), isNull);
      expect(serverMessage(<Object?>['x']), isNull);
    });

    test('an exhaustive switch covers every rejection', () {
      String describe(PushRejection r) => switch (r) {
        ValidationRejection(:final index) => 'validation $index',
        DriftRejection(:final index) => 'drift $index',
        BatchTooLargeRejection(:final limit) => 'limit $limit',
        HookRejection() => 'hook',
        UnclassifiedRejection(:final status) => 'other $status',
      };
      expect(describe(const DriftRejection(4, 'x')), 'drift 4');
      expect(describe(const HookRejection('x')), 'hook');
    });
  });
}
