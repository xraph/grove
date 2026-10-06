import 'package:grove_crdt/grove_crdt.dart';
import 'package:test/test.dart';

void main() {
  group('goMarshal', () {
    test('escapes HTML characters like Go', () {
      expect(goMarshal('a<b>&c'), r'"a\u003cb\u003e\u0026c"');
    });
    test('escapes U+2028 and U+2029', () {
      expect(goMarshal('\u2028\u2029'), r'"\u2028\u2029"');
    });
    test('uses short escapes for quote, backslash, newline, return and tab', () {
      expect(goMarshal('l\nb\tt"q\\r\r'), r'"l\nb\tt\"q\\r\r"');
    });
    test('escapes other control characters as lowercase u00XX', () {
      expect(goMarshal('\u0001\u001f'), r'"\u0001\u001f"');
    });
    test('leaves DEL and non-ASCII raw', () {
      expect(goMarshal('\u007fhé😀'), '"\u007fhé😀"');
    });
    test('replaces a lone surrogate with U+FFFD', () {
      expect(goMarshal(String.fromCharCode(0xD800)), r'"\ufffd"');
    });
    test('sorts object keys by byte order', () {
      expect(goMarshal({'é': 1, 'e': 2, 'Z': 3}), '{"Z":3,"e":2,"é":1}');
    });
    test('encodes nested values', () {
      expect(
        goMarshal({'b': 1, 'a': [true, false, null], 'c': {'z': '', 'y': 0}}),
        '{"a":[true,false,null],"b":1,"c":{"y":0,"z":""}}',
      );
    });
    test('formats integral doubles without a fraction', () {
      expect(goMarshal(100.0), '100');
      expect(goMarshal(1e20), '100000000000000000000');
    });
    test('formats fractional doubles in shortest form', () {
      expect(goMarshal(0.1), '0.1');
      expect(goMarshal(-3.25), '-3.25');
      expect(goMarshal(0.000001), '0.000001');
    });
    test('switches to exponent form outside [1e-6, 1e21)', () {
      expect(goMarshal(1e21), '1e+21');
      expect(goMarshal(1.5e-7), '1.5e-7');
      expect(goMarshal(1e-7), '1e-7');
    });
    test('emits RawJson verbatim', () {
      expect(goMarshal({'x': const RawJson('{"k":1}')}), '{"x":{"k":1}}');
    });
    test('encodes BigInt as decimal', () {
      expect(goMarshal(BigInt.parse('1712345678901234567')), '1712345678901234567');
    });
    test('rejects NaN', () {
      expect(() => goMarshal(double.nan), throwsUnsupportedError);
    });
    test('rejects infinity', () {
      expect(() => goMarshal(double.infinity), throwsUnsupportedError);
    });
    test('uses short escapes for backspace and form feed', () {
      expect(goMarshal('\b\f'), r'"\b\f"');
    });
    test('keeps the sign of negative zero and pads no exponent', () {
      expect(goMarshal(-0.0), '-0');
      expect(goMarshal(1.5e300), '1.5e+300');
      expect(goMarshal(1e-9), '1e-9');
    });
  });

  test('setElementKey is goMarshal', () {
    expect(setElementKey({'b': 1, 'a': 'x<y'}), r'{"a":"x\u003cy","b":1}');
  });

  test('jsonDeepEquals treats 1 and 1.0 as equal', () {
    expect(jsonDeepEquals({'a': [1]}, {'a': [1.0]}), isTrue);
    expect(jsonDeepEquals({'a': 1}, {'a': 2}), isFalse);
  });

  test('formatRfc3339Nano matches Go time.Time JSON', () {
    expect(formatRfc3339Nano(DateTime.utc(2026, 10, 4, 12)), '2026-10-04T12:00:00Z');
    expect(formatRfc3339Nano(DateTime.utc(2026, 10, 4, 12, 0, 0, 120)), '2026-10-04T12:00:00.12Z');
  });
}
