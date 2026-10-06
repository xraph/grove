import 'dart:convert';

import 'package:grove_crdt/grove_crdt.dart';
import 'package:test/test.dart';

import 'support/fixture.dart';
import 'support/json_equivalent.dart';

final Map<String, Object? Function(Object?)> roundTrip = {
  'HLC': (j) => HLC.fromJson(j).toJson(),
  'ChangeRecord': (j) => ChangeRecord.fromJson(j).toJson(),
  'FieldState': (j) => FieldState.fromJson(j).toJson(),
  'DocumentState': (j) => DocumentState.fromJson(j).toJson(),
  'PullRequest': (j) => PullRequest.fromJson(j).toJson(),
  'PullResponse': (j) => PullResponse.fromJson(j).toJson(),
  'PushRequest': (j) => PushRequest.fromJson(j).toJson(),
  'PushResponse': (j) => PushResponse.fromJson(j).toJson(),
  'PresenceState': (j) => PresenceState.fromJson(j).toJson(),
  'PresenceUpdate': (j) => PresenceUpdate.fromJson(j).toJson(),
  'PresenceEvent': (j) => PresenceEvent.fromJson(j).toJson(),
  'PresenceSnapshot': (j) => PresenceSnapshot.fromJson(j).toJson(),
  'WebSocketMessage': (j) => WebSocketMessage.fromJson(j).toJson(),
  'Room': (j) => Room.fromJson(j).toJson(),
  'RoomInfo': (j) => RoomInfo.fromJson(j).toJson(),
  'CursorPosition': (j) => CursorPosition.fromJson(j).toJson(),
  'ParticipantData': (j) => ParticipantData.fromJson(j).toJson(),
};

void main() {
  final cases = (jsonDecode(readFixture('test/fixtures/wire_golden.json')) as List<Object?>)
      .cast<Map<String, Object?>>();

  test('covers every golden type', () {
    expect(cases.map((c) => c['type']).toSet().difference(roundTrip.keys.toSet()), isEmpty);
  });

  for (final c in cases) {
    test('round-trips ${c['name']} exactly as Go encodes it', () {
      final goJson = c['json'];
      final dartJson = jsonDecode(encodeWire(roundTrip[c['type']]!(goJson)));
      expect(jsonEquivalent(goJson, dartJson), isTrue, reason: 'go:   ${jsonEncode(goJson)}\ndart: ${jsonEncode(dartJson)}');
    });
  }

  group('typed decoding', () {
    Map<String, Object?> golden(String name) =>
        cases.firstWhere((c) => c['name'] == name)['json']! as Map<String, Object?>;

    test('a pulled tombstone decodes with CrdtType.none', () {
      final c = ChangeRecord.fromJson(golden('change_tombstone_pulled'));
      expect(c.crdtType, CrdtType.none);
      expect(c.tombstone, isTrue);
      expect(c.field, '_tombstone');
    });
    test('an explicit null value is present, an absent value is null', () {
      expect(ChangeRecord.fromJson(golden('change_lww_null_value')).value, const JsonValue(null));
      expect(ChangeRecord.fromJson(golden('change_counter')).value, isNull);
    });
    test('an empty pull response decodes to no changes', () {
      expect(PullResponse.fromJson(golden('pull_response_empty')).changes, isEmpty);
    });
    test('presence timestamps decode as UTC DateTime', () {
      final s = PresenceState.fromJson(golden('presence_state'));
      expect(s.updatedAt, DateTime.utc(2026, 10, 4, 12, 0, 0, 120));
    });
    test('an unknown crdt_type is rejected', () {
      expect(() => CrdtType.fromWire('graph'), throwsFormatException);
    });
    test('a zero expires_at decodes as null and encodes as Go zero time', () {
      final s = PresenceState.fromJson(golden('presence_state_no_expiry'));
      expect(s.expiresAt, isNull);
      expect(s.toJson()['expires_at'], '0001-01-01T00:00:00Z');
    });
    test('a nanosecond timestamp keeps microseconds, the precision of DateTime', () {
      final s = PresenceState.fromJson({
        ...golden('presence_state'),
        'updated_at': '2026-10-04T12:00:00.123456789Z',
      });
      expect(s.updatedAt, DateTime.utc(2026, 10, 4, 12, 0, 0, 123, 456));
      expect(s.toJson()['updated_at'], '2026-10-04T12:00:00.123456Z');
    });
    test('an offset timestamp decodes to the same UTC instant', () {
      final s = PresenceState.fromJson({...golden('presence_state'), 'updated_at': '2026-10-04T13:00:00.5+01:00'});
      expect(s.updatedAt, DateTime.utc(2026, 10, 4, 12, 0, 0, 500));
    });
    test('an explicit null room metadata is present', () {
      expect(Room.fromJson(golden('room_max_participants')).metadata, const JsonValue(null));
      expect(Room.fromJson(golden('room_minimal')).metadata, isNull);
    });
    test('a pulled sync response keeps the field state of a set', () {
      final c = ChangeRecord.fromJson(golden('change_with_state'));
      expect(c.state!.type, CrdtType.set);
      expect(c.state!.setState!.entries.keys, hasLength(2));
      expect(c.state!.setState!.removed.values, [true]);
    });
    test('a text field decodes fragments, tombstones and attributes', () {
      final f = FieldState.fromJson(golden('field_text')).textState!;
      final frags = f.frags.values.single;
      expect(frags.map((x) => x.content), ['h', 'él', 'lo']);
      expect(frags.map((x) => x.tombstone), [false, true, false]);
      expect(frags.first.attrs['bold']!.value, const JsonValue(true));
      expect(frags.first.parent, TextRef.head);
    });
    test('TextState.clone copies fragments deeply', () {
      final original = FieldState.fromJson(golden('field_text')).textState!;
      final copy = original.clone();
      copy.frags.values.single.first
        ..content = 'changed'
        ..tombstone = true
        ..attrs['italic'] = AttrState(const JsonValue(true), HLC.zero, 'x');
      final first = original.frags.values.single.first;
      expect(first.content, 'h');
      expect(first.tombstone, isFalse);
      expect(first.attrs.keys, ['bold']);
    });
    test('encodeWire escapes HTML characters as Go does', () {
      final c = ChangeRecord.fromJson(golden('change_lww'));
      expect(encodeWire(c.toJson()), contains(r'"value":"Hello \u003cb\u003e"'));
    });
    test('encodeWire writes an HLC ts as a decimal string beyond 2^53', () {
      final c = ChangeRecord.fromJson(golden('change_lww'));
      expect(encodeWire(c.toJson()), contains('"ts":"1712345678901234567"'));
    });
    test('a malformed HLC inside a change is rejected, not coerced', () {
      final bad = {
        ...golden('change_lww'),
        'hlc': {'ts': '12abc', 'c': 0, 'node': 'n'},
      };
      expect(() => ChangeRecord.fromJson(bad), throwsFormatException);
    });
    test('a value of the wrong JSON type is a FormatException', () {
      expect(() => ChangeRecord.fromJson({...golden('change_lww'), 'pk': 5}), throwsFormatException);
      expect(() => ChangeRecord.fromJson({...golden('change_lww'), 'tombstone': 'yes'}), throwsFormatException);
      expect(() => CounterDelta.fromJson({'inc': 1.5}), throwsFormatException);
      expect(() => PullResponse.fromJson({'changes': 3}), throwsFormatException);
      expect(() => ChangeRecord.fromJson('x'), throwsFormatException);
    });
    test('an unknown op or message type is a FormatException', () {
      expect(() => SetOperation.fromJson({'op': 'merge'}), throwsFormatException);
      expect(() => ListOperation.fromJson({'op': 'swap'}), throwsFormatException);
      expect(() => TextOperation.fromJson({'op': 'bold'}), throwsFormatException);
      expect(() => WebSocketMessage.fromJson({'type': 'nope'}), throwsFormatException);
    });
    test('every WsMessageType wire name round-trips', () {
      expect(WsMessageType.values, hasLength(15));
      for (final t in WsMessageType.values) {
        expect(WsMessageType.fromWire(t.wire), t);
      }
    });
    test('a document tombstone carries a non-null tombstone clock', () {
      final live = DocumentState(table: 't', pk: 'p');
      expect(live.tombstoneHlc, HLC.zero);
      expect(live.toJson()['tombstone_hlc'], HLC.zero.toJson());
    });
  });

  group('go json golden', () {
    final goCases = (jsonDecode(readFixture('test/fixtures/go_json_golden.json')) as List<Object?>)
        .cast<Map<String, Object?>>();
    for (final c in goCases) {
      test('goMarshal(${c['input']}) matches Go byte for byte', () {
        expect(goMarshal(jsonDecode(c['input']! as String)), c['output']);
      });
    }
  });
}
