import 'package:grove_crdt/grove_crdt.dart';
import 'package:test/test.dart';

const _hook = 'crdt: inbound change hook: title is locked';

void main() {
  test('classifies a drift validation failure with its index', () {
    final r = classifyPushError(500, {
      'error': 'crdt: change[3]: crdt: change HLC timestamp drift too large (2h0m0s)',
    })!;
    expect(r, isA<DriftRejection>().having((d) => d.index, 'index', 3));
    expect(r.kind, 'drift');
  });

  test('classifies another validation failure with its index', () {
    final r = classifyPushError(500, {
      'error': 'crdt: change[0]: crdt: change pk is required',
    })!;
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
    final r = classifyPushError(500, {'error': _hook})!;
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

  // Changed from the brief, by ruling: it expected a plain 500 to classify as
  // an UnclassifiedRejection of kind `server`. A 500 with no recognized text
  // may be a crash and means "try again", so it is no verdict (null).
  test('falls back to unclassified, bad_request for 4xx', () {
    expect(classifyPushError(500, 'Internal Server Error'), isNull);
    final r = classifyPushError(400, {'error': 'invalid request'});
    expect(r, isA<UnclassifiedRejection>());
    expect(r!.kind, 'bad_request');
  });

  group('statuses that mean try again are not rejections', () {
    // 401 and 403 are auth: refresh, then retry. 408, 429 and 5xx other than
    // 500 are transient: back off, then retry. None may mark a change.
    for (final status in [401, 403, 408, 429, 503]) {
      test('$status stays pending, not rejected', () {
        expect(classifyPushError(status, {'error': 'nope'}), isNull);
        expect(classifyPushError(status, 'text body'), isNull);
        expect(classifyPushError(status, null), isNull);
      });

      test('$status stays pending even when the body quotes a verdict', () {
        // A proxy or gateway can say anything; the status decides.
        expect(classifyPushError(status, {'error': _hook}), isNull);
        expect(
          classifyPushError(status, {'error': 'crdt: change[1]: x'}),
          isNull,
        );
        expect(
          classifyPushError(status, {
            'error': 'crdt: push exceeds max changes (2 > 1)',
          }),
          isNull,
        );
      });
    }

    for (final status in [404, 410, 413, 502, 504, 599]) {
      test('$status is left to the engine, not a rejection', () {
        expect(classifyPushError(status, {'error': 'x'}), isNull);
        expect(classifyPushError(status, {'error': _hook}), isNull);
      });
    }

    test('a 400 with a hook string becomes a hook rejection', () {
      final r = classifyPushError(400, {'error': _hook});
      expect(r, isA<HookRejection>());
      expect(r!.reason, 'title is locked');
    });

    test('a 422 with a validation string becomes a validation rejection', () {
      final r = classifyPushError(422, {'error': 'crdt: change[4]: bad'});
      expect(r, isA<ValidationRejection>().having((v) => v.index, 'index', 4));
    });

    test('a 422 with no recognized text is a bad_request', () {
      expect(classifyPushError(422, {'error': 'x'})!.kind, 'bad_request');
    });

    test('a 500 or an error frame with no recognized text is no verdict', () {
      expect(
        classifyPushError(500, {'error': 'crdt: merge field: boom'}),
        isNull,
      );
      expect(classifyPushError(500, null), isNull);
      expect(classifyPushError(0, {'error': 'invalid push request'}), isNull);
    });
  });

  group('beyond the brief', () {
    test(
      'an oversized batch maps to the validation kind and keeps the text',
      () {
        final r = classifyPushError(500, {
          'error': 'crdt: push exceeds max changes (501 > 500)',
        })!;
        expect(r.kind, 'validation');
        expect(r.reason, 'crdt: push exceeds max changes (501 > 500)');
      },
    );

    test('a validation failure maps to the validation kind', () {
      expect(
        classifyPushError(500, {'error': 'crdt: change[2]: x'})!.kind,
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
        })!;
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

    // The reverse of the three above: the server's own prefix comes first, so
    // a hook prefix inside a validation message must not take it over.
    test('a change index that comes before a hook prefix wins', () {
      final r = classifyPushError(500, {
        'error': 'crdt: change[2]: crdt: unknown crdt type "crdt: inbound change hook: x"',
      });
      expect(r, isA<ValidationRejection>().having((v) => v.index, 'index', 2));
    });

    test('the batch limit that comes before a hook prefix wins', () {
      final r = classifyPushError(500, {
        'error': 'crdt: push exceeds max changes (3 > 2) crdt: inbound change hook: x',
      });
      expect(
        r,
        isA<BatchTooLargeRejection>().having((b) => b.limit, 'limit', 2),
      );
    });

    test('a hook prefix that comes first wins over a later change index', () {
      final r = classifyPushError(500, {
        'error': 'crdt: inbound change hook: x crdt: change[1]: y',
      });
      expect(r, isA<HookRejection>());
    });

    // Only the full phrase marks drift: a validation message that merely
    // mentions drift is an ordinary validation failure.
    test(
      'a change message that mentions drift without the phrase is validation',
      () {
        final r = classifyPushError(500, {
          'error': 'crdt: change[1]: crdt: unknown crdt type "drift"',
        });
        expect(r, isA<ValidationRejection>());
        final r2 = classifyPushError(500, {
          'error': 'crdt: change[1]: HLC timestamp drift too small',
        });
        expect(r2, isA<ValidationRejection>());
      },
    );

    test('a hook prefix inside another wrapper is found', () {
      final r = classifyPushError(500, {
        'error': 'push failed: crdt: inbound change hook: locked',
      })!;
      expect(r, isA<HookRejection>());
      expect(r.reason, 'locked');
    });

    test('a multi-line inner message is kept whole', () {
      final r = classifyPushError(500, {
        'error': 'crdt: change[1]: line one\nline two',
      })!;
      expect(r.reason, 'line one\nline two');
    });

    test('an absurd index does not throw and is not blamed on a change', () {
      final r = classifyPushError(500, {
        'error': 'crdt: change[99999999999999999999999]: x',
      });
      expect(r, isNull);
    });

    test('no body, a non-map body and a non-string message carry no text', () {
      expect(classifyPushError(400, null)!.reason, '');
      expect(classifyPushError(400, 42)!.reason, '');
      expect(classifyPushError(400, {'error': 7})!.reason, '');
      expect(classifyPushError(502, null), isNull);
    });

    test('an unclassified rejection takes its kind from its status', () {
      expect(const UnclassifiedRejection(399, 'x').kind, 'server');
      expect(const UnclassifiedRejection(400, 'x').kind, 'bad_request');
      expect(const UnclassifiedRejection(499, 'x').kind, 'bad_request');
      expect(const UnclassifiedRejection(500, 'x').kind, 'server');
      expect(const UnclassifiedRejection(0, 'x').kind, 'server');
    });

    // PendingRejection.kind is a free string, so this pins the strings the
    // rejection classes produce against the five its docs list.
    test(
      'the rejection classes produce the five kinds PendingRejection documents',
      () {
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
      },
    );

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
