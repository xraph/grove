import 'package:test/test.dart';

import 'support/json_equivalent.dart';

void main() {
  group('jsonEquivalent', () {
    test('null equals an empty list or map, in either direction', () {
      expect(jsonEquivalent(null, <Object?>[]), isTrue);
      expect(jsonEquivalent(null, <String, Object?>{}), isTrue);
      expect(jsonEquivalent(<Object?>[], null), isTrue);
      expect(jsonEquivalent(<String, Object?>{}, null), isTrue);
      expect(jsonEquivalent(null, null), isTrue);
    });
    test('null never equals a non-empty container or a scalar', () {
      expect(jsonEquivalent(null, [1]), isFalse);
      expect(jsonEquivalent({'a': 1}, null), isFalse);
      expect(jsonEquivalent(null, 0), isFalse);
      expect(jsonEquivalent('', null), isFalse);
      expect(jsonEquivalent(false, null), isFalse);
    });
    test('an empty list does not equal an empty map', () {
      expect(jsonEquivalent(<Object?>[], <String, Object?>{}), isFalse);
      expect(jsonEquivalent({'v': <Object?>[]}, {'v': <String, Object?>{}}), isFalse);
    });
    test('the null rule applies at depth', () {
      expect(jsonEquivalent({'a': null}, {'a': <Object?>[]}), isTrue);
      expect(jsonEquivalent({'a': null}, {'a': [1]}), isFalse);
    });
    test('key presence is exact', () {
      expect(jsonEquivalent({'a': 1}, {'a': 1, 'b': null}), isFalse);
      expect(jsonEquivalent({'a': null}, <String, Object?>{}), isFalse);
    });
    test('lists compare by length and element', () {
      expect(jsonEquivalent([1, 2], [1, 2]), isTrue);
      expect(jsonEquivalent([1, 2], [1]), isFalse);
      expect(jsonEquivalent([1], [2]), isFalse);
    });
    test('1 and 1.0 are the same number', () {
      expect(jsonEquivalent(1, 1.0), isTrue);
    });
  });
}
