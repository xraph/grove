// New in the Dart port: crdt-js speaks only Grove's native framing, so there is
// no TS file to port. These cases cover both envelopes.
import 'dart:convert';

import 'package:grove_crdt/grove_crdt.dart';
import 'package:test/test.dart';

void main() {
  final since = HLC(BigInt.parse('1712345678901234567'), 4, 'srv');

  test('grove envelope writes the native pull body', () {
    final body = jsonDecode(
      groveEnvelope.encodePull(
        PullRequest(tables: const ['documents'], since: since, nodeId: 'dev'),
      ),
    );
    expect(body, {
      'tables': ['documents'],
      'since': {'ts': '1712345678901234567', 'c': 4, 'node': 'srv'},
      'node_id': 'dev',
    });
  });

  test(
    'camel DTO envelope writes foundry\'s pull body with an exact numeric ts',
    () {
      final text = camelDtoEnvelope.encodePull(
        PullRequest(
          tables: const ['ignored'],
          since: since,
          nodeId: 'dev',
          filter: const SyncFilter(pkFilter: ['r1']),
        ),
      );
      expect(
        text,
        '{"filter":{"pk_filter":["r1"]},"since":{"counter":4,"nodeId":"srv","ts":1712345678901234567}}',
      );
    },
  );

  test('camel DTO envelope omits a zero since', () {
    expect(
      camelDtoEnvelope.encodePull(PullRequest(tables: const [], nodeId: 'dev')),
      '{}',
    );
  });

  test('camel DTO envelope decodes foundry\'s pull response', () {
    final resp = camelDtoEnvelope.decodePull({
      'changes': [
        {
          'table': 'ds_x',
          'pk': '1',
          'field': 'f',
          'crdt_type': 'lww',
          'hlc': {'ts': '9', 'c': 0, 'node': 'n'},
          'node_id': 'n',
          'value': 1,
        },
      ],
      'latestHlc': {'ts': 9, 'counter': 0, 'nodeId': 'n'},
    });
    expect(resp.changes.single.table, 'ds_x');
    expect(resp.latestHlc.ts, BigInt.from(9));
  });

  test('camel DTO envelope writes foundry\'s push and presence bodies', () {
    expect(
      camelDtoEnvelope.encodePush(
        const PushRequest(changes: [], nodeId: 'dev'),
      ),
      '{"changes":[],"nodeId":"dev"}',
    );
    expect(
      camelDtoEnvelope.encodePresenceUpdate(
        const PresenceUpdate(nodeId: 'dev', topic: 'ds', data: {'x': 1}),
      ),
      '{"data":{"x":1},"nodeId":"dev"}',
    );
  });

  // The brief wrote `PullResponse.latestHlc` as nullable, but the real type
  // (Task 4) is non-null and defaults to the zero HLC, as Go's struct does.
  test('camel DTO envelope reads an omitted latestHlc as the zero HLC', () {
    expect(
      camelDtoEnvelope.decodePull({'changes': null}).latestHlc.isZero,
      isTrue,
    );
    expect(camelDtoEnvelope.decodePush({'merged': 2}).latestHlc.isZero, isTrue);
  });

  test('camel DTO envelope decodes foundry\'s push response', () {
    final resp = camelDtoEnvelope.decodePush({
      'merged': 3,
      'latestHlc': {'ts': '1712345678901234567', 'counter': 2, 'nodeId': 's'},
    });
    expect(resp.merged, 3);
    expect(resp.latestHlc.ts, BigInt.parse('1712345678901234567'));
    expect(resp.latestHlc.c, 2);
    expect(resp.latestHlc.node, 's');
  });

  test('camel DTO envelope rejects a value of the wrong shape', () {
    expect(
      () => camelDtoEnvelope.decodePull({
        'latestHlc': {'ts': true},
      }),
      throwsFormatException,
    );
    expect(() => camelDtoEnvelope.decodePush([1]), throwsFormatException);
    expect(
      () => camelDtoEnvelope.decodePull({
        'latestHlc': {'ts': 'abc'},
      }),
      throwsFormatException,
    );
  });

  test('grove envelope decodes the native responses', () {
    final pull = groveEnvelope.decodePull({
      'changes': null,
      'latest_hlc': {'ts': '5', 'c': 1, 'node': 's'},
    });
    expect(pull.changes, isEmpty);
    expect(pull.latestHlc.ts, BigInt.from(5));
    expect(
      groveEnvelope.decodePush({
        'merged': 4,
        'latest_hlc': {'ts': '5', 'c': 1, 'node': 's'},
      }).merged,
      4,
    );
  });

  test('grove envelope writes the native push and presence bodies', () {
    expect(
      groveEnvelope.encodePush(const PushRequest(changes: [], nodeId: 'dev')),
      '{"changes":[],"node_id":"dev"}',
    );
    expect(
      groveEnvelope.encodePresenceUpdate(
        const PresenceUpdate(nodeId: 'dev', topic: 'ds', data: {'x': 1}),
      ),
      '{"data":{"x":1},"node_id":"dev","topic":"ds"}',
    );
  });

  test('a pull latest_hlc is exact as a string and possibly rounded as a '
      'number', () {
    Map<String, Object?> grove(Object ts) => {
      'changes': <Object?>[],
      'latest_hlc': {'ts': ts, 'c': 1, 'node': 's'},
    };
    Map<String, Object?> camel(Object ts) => {
      'changes': <Object?>[],
      'latestHlc': {'ts': ts, 'counter': 1, 'nodeId': 's'},
    };
    expect(groveEnvelope.decodePull(grove('5')).latestHlcExact, isTrue);
    expect(groveEnvelope.decodePull(grove(5)).latestHlcExact, isFalse);
    expect(camelDtoEnvelope.decodePull(camel('5')).latestHlcExact, isTrue);
    expect(camelDtoEnvelope.decodePull(camel(5)).latestHlcExact, isFalse);
    expect(
      camelDtoEnvelope.decodePull({'changes': null}).latestHlcExact,
      isTrue,
    );
  });
}
