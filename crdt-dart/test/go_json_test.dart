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
    test('replaces a lone surrogate with a raw U+FFFD', () {
      expect(goMarshal(String.fromCharCode(0xD800)), '"\u{FFFD}"');
    });
    // Go parity: json.Unmarshal turns the escape \ud800 into U+FFFD and
    // json.Marshal then writes that rune raw, not as \ufffd.
    test('writes a lone high or low surrogate as raw U+FFFD like Go', () {
      expect(goMarshal('a\uD800b'), '"a\u{FFFD}b"');
      expect(goMarshal('a\uDC00b'), '"a\u{FFFD}b"');
    });
    test('keeps a paired surrogate as the astral character', () {
      expect(goMarshal('\u{1F600}'), '"\u{1F600}"');
    });
    test('sorts object keys by byte order', () {
      expect(goMarshal({'é': 1, 'e': 2, 'Z': 3}), '{"Z":3,"e":2,"é":1}');
    });
    test('orders a key holding a lone surrogate as U+FFFD, like Go', () {
      // Go parity: encoding/json sorts keys after decoding \ud800 to U+FFFD.
      final keys = <String>['z', '\u00e9', '\ue000', '\uff00', '\uD800', '\uffff', '\u{1F600}'];
      final m = <String, Object?>{for (var i = 0; i < keys.length; i++) keys[i]: i + 1};
      expect(
        goMarshal(m),
        '{"z":1,"\u00e9":2,"\ue000":3,"\uff00":4,"\u{FFFD}":5,"\uffff":6,"\u{1F600}":7}',
      );
    });
    test('rejects keys that collide after surrogate replacement', () {
      // Go keeps one of them silently; a Dart map cannot, so fail loudly.
      expect(() => goMarshal({'\uD800': 1, '\uDC00': 2}), throwsArgumentError);
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

  test('jsonDeepEquals hash agrees with equals for 1, 1.0 and -0.0', () {
    const eq = jsonDeepEquality;
    expect(eq.hash(1), eq.hash(1.0));
    expect(eq.hash(0), eq.hash(-0.0));
    expect(eq.hash([-0.0, 1]), eq.hash([0, 1.0]));
    expect(eq.hash({'a': -0.0}), eq.hash({'a': 0}));
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
