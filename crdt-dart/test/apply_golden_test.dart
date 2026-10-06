import 'dart:convert';

import 'package:grove_crdt/grove_crdt.dart';
import 'package:test/test.dart';

import 'support/fixture.dart';
import 'support/json_equivalent.dart';

typedef Json = Map<String, Object?>;

void main() {
  // Go-generated (tool/conformance_server/cmd/golden). The node platform has no
  // dart:io, so the file is read through readFixture.
  final golden = jsonDecode(readFixture('test/fixtures/apply_golden.json')) as Json;
  final applyCases = (golden['apply']! as List<Object?>).cast<Json>();
  final mergeStateCases = (golden['merge_state']! as List<Object?>).cast<Json>();

  test('has fixtures', () {
    expect(applyCases.length, greaterThanOrEqualTo(10));
    expect(mergeStateCases.length, greaterThanOrEqualTo(4));
  });

  group('applyChange matches Go ApplyChange', () {
    for (final c in applyCases) {
      test(c['name']! as String, () {
        // Go resolves a leaf and a nested path under it by map order, so the
        // fixture leaves the top-level value out of such a case.
        final dropValue = c['nondeterministic_value'] == true;
        FieldState? local;
        var i = 0;
        for (final step in (c['steps']! as List<Object?>).cast<Json>()) {
          final change = ChangeRecord.fromJson(step['change']);
          final changeBefore = encodeWire(change.toJson());
          final previous = local;
          final before = previous == null ? null : encodeWire(previous.toJson());
          if (step['error'] != null) {
            expect(
              () => applyChange(previous, change),
              throwsA(isA<CrdtApplyError>().having((e) => e.message, 'message', step['error'])),
              reason: 'step $i',
            );
          } else {
            final next = applyChange(previous, change);
            local = next;
            final gotJson = next.toJson();
            if (dropValue) gotJson.remove('value');
            final got = jsonDecode(encodeWire(gotJson));
            expect(
              jsonEquivalent(step['result'], got),
              isTrue,
              reason: 'step $i\ngo:   ${jsonEncode(step['result'])}\ndart: ${jsonEncode(got)}',
            );
          }
          // Copy-on-write: applying a change mutates neither its inputs nor
          // the change, whether it succeeds or throws.
          if (previous != null) {
            expect(encodeWire(previous.toJson()), before, reason: 'step $i mutated its input');
          }
          expect(encodeWire(change.toJson()), changeBefore, reason: 'step $i mutated the change');
          i++;
        }
      });
    }
  });

  group('mergeState matches Go MergeEngine.MergeState', () {
    for (final c in mergeStateCases) {
      test(c['name']! as String, () {
        final local = DocumentState.fromJson(c['local']);
        final remote = DocumentState.fromJson(c['remote']);
        if (c['error'] != null) {
          expect(
            () => mergeState(local, remote),
            throwsA(isA<CrdtMergeError>().having((e) => e.message, 'message', c['error'])),
          );
          return;
        }
        final merged = mergeState(local, remote);
        final got = jsonDecode(encodeWire(merged.toJson()));
        expect(
          jsonEquivalent(c['result'], got),
          isTrue,
          reason: 'go:   ${jsonEncode(c['result'])}\ndart: ${jsonEncode(got)}',
        );
        // Inputs are untouched.
        expect(jsonEquivalent(c['local'], jsonDecode(encodeWire(local.toJson()))), isTrue);
        expect(jsonEquivalent(c['remote'], jsonDecode(encodeWire(remote.toJson()))), isTrue);
      });
    }
  });
}
