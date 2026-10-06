import 'package:collection/collection.dart';

import 'hlc.dart';

/// A pre-encoded JSON fragment that [goMarshal] emits verbatim.
///
/// Go compacts and HTML-escapes a `json.RawMessage`; this does not. Writing it
/// verbatim is a deliberate exception, so a remove can send another engine's
/// exact key bytes.
final class RawJson {
  /// Wraps already-encoded JSON text.
  const RawJson(this.json);

  /// The JSON text.
  final String json;
}

/// Encodes [value] exactly as Go's `json.Marshal` would encode the
/// equivalent Go value.
String goMarshal(Object? value) {
  final out = StringBuffer();
  _write(out, value);
  return out.toString();
}

/// The OR-set key for [element]: its Go JSON encoding.
String setElementKey(Object? element) => goMarshal(element);

/// [s] as Go holds it after decoding JSON: every unpaired surrogate becomes
/// U+FFFD. Returns [s] itself when it has no unpaired surrogate.
String goString(String s) {
  for (var i = 0; i < s.length; i++) {
    final u = s.codeUnitAt(i);
    if (u < 0xD800 || u > 0xDFFF) continue;
    if (u <= 0xDBFF && i + 1 < s.length) {
      final next = s.codeUnitAt(i + 1);
      if (next >= 0xDC00 && next <= 0xDFFF) {
        i++;
        continue;
      }
    }
    return String.fromCharCodes([
      for (final r in s.runes) r >= 0xD800 && r <= 0xDFFF ? 0xFFFD : r,
    ]);
  }
  return s;
}

/// A deep copy of the JSON value [v] as Go would hold it: every string and
/// object key passes through [goString], and every map and list is new, so
/// the copy shares nothing mutable with [v].
///
/// When two object keys become equal after the replacement, the later one
/// wins, as it does when Go decodes the object. Throws an [ArgumentError] for
/// a value that is not JSON (a non-finite number, a non-string key, or any
/// other object). A [BigInt] and a [RawJson] are kept as they are.
Object? goJsonCopy(Object? v) => switch (v) {
  null || bool() || BigInt() || RawJson() => v,
  final num n =>
    n.isFinite
        ? n
        : throw ArgumentError.value(
            v,
            'value',
            'not JSON: a non-finite number',
          ),
  final String s => goString(s),
  final List<Object?> l => <Object?>[for (final e in l) goJsonCopy(e)],
  final Map<Object?, Object?> m => <String, Object?>{
    for (final e in m.entries) _jsonKey(e.key): goJsonCopy(e.value),
  },
  _ => throw ArgumentError.value(
    v,
    'value',
    'not a JSON value (${v.runtimeType})',
  ),
};

String _jsonKey(Object? k) => k is String
    ? goString(k)
    : throw ArgumentError.value(k, 'key', 'a JSON object key must be a string');

/// Deep JSON equality where `1` and `1.0` are equal.
bool jsonDeepEquals(Object? a, Object? b) => jsonDeepEquality.equals(a, b);

/// The [Equality] behind [jsonDeepEquals], for keying a hash collection by
/// deep JSON value. Its `hash` agrees with `equals` for `1`, `1.0` and `-0.0`.
const Equality<Object?> jsonDeepEquality = _JsonEquality();

final class _JsonEquality implements Equality<Object?> {
  const _JsonEquality();

  @override
  bool equals(Object? a, Object? b) {
    if (a is num && b is num) return a == b;
    if (a is List<Object?> && b is List<Object?>) {
      if (a.length != b.length) return false;
      for (var i = 0; i < a.length; i++) {
        if (!equals(a[i], b[i])) return false;
      }
      return true;
    }
    if (a is Map<String, Object?> && b is Map<String, Object?>) {
      if (a.length != b.length) return false;
      for (final e in a.entries) {
        if (!b.containsKey(e.key) || !equals(e.value, b[e.key])) return false;
      }
      return true;
    }
    return a == b;
  }

  @override
  int hash(Object? e) => switch (e) {
    // equals treats 1, 1.0 and -0.0 as the numbers they equal, so hash by
    // double value, and fold -0.0 into 0.0.
    final num n => n == 0 ? 0.0.hashCode : n.toDouble().hashCode,
    final List<Object?> l => Object.hashAll(l.map(hash)),
    final Map<String, Object?> m => m.entries.fold<int>(
      0,
      (acc, en) => acc + Object.hash(en.key, hash(en.value)),
    ),
    _ => e.hashCode,
  };

  @override
  bool isValidKey(Object? o) => true;
}

/// Go's `time.Time` JSON text (RFC 3339 with trimmed nanoseconds), without
/// the surrounding quotes.
String formatRfc3339Nano(DateTime t) {
  final u = t.toUtc();
  String two(int v) => v.toString().padLeft(2, '0');
  final base =
      '${u.year.toString().padLeft(4, '0')}-${two(u.month)}-${two(u.day)}'
      'T${two(u.hour)}:${two(u.minute)}:${two(u.second)}';
  final micros = u.millisecond * 1000 + u.microsecond;
  if (micros == 0) return '${base}Z';
  final frac = micros
      .toString()
      .padLeft(6, '0')
      .replaceFirst(RegExp(r'0+$'), '');
  return '$base.${frac}Z';
}

const _hex = '0123456789abcdef';

void _write(StringBuffer out, Object? v) {
  switch (v) {
    case null:
      out.write('null');
    case final bool b:
      out.write(b ? 'true' : 'false');
    case final num n:
      out.write(_formatNum(n));
    case final BigInt i:
      out.write(i.toString());
    case final String s:
      _writeString(out, s);
    case final RawJson r:
      out.write(r.json);
    case final List<Object?> l:
      out.write('[');
      for (var i = 0; i < l.length; i++) {
        if (i > 0) out.write(',');
        _write(out, l[i]);
      }
      out.write(']');
    case final Map<Object?, Object?> m:
      // A `{}` literal or `jsonDecode` result is a Map<dynamic, dynamic> at
      // runtime, so the keys are checked one by one.
      final keys = <String>[
        for (final k in m.keys)
          k is String
              ? k
              : throw ArgumentError.value(
                  k,
                  'key',
                  'goMarshal: a JSON object key must be a string, '
                      'got ${k.runtimeType}',
                ),
      ]..sort(compareGoStrings);
      for (var i = 1; i < keys.length; i++) {
        if (compareGoStrings(keys[i - 1], keys[i]) == 0) {
          // Go would decode both keys to the same string and keep one.
          throw ArgumentError.value(
            m,
            'value',
            'goMarshal: keys collide after surrogate replacement',
          );
        }
      }
      out.write('{');
      for (var i = 0; i < keys.length; i++) {
        if (i > 0) out.write(',');
        _writeString(out, keys[i]);
        out.write(':');
        _write(out, m[keys[i]]);
      }
      out.write('}');
    default:
      throw UnsupportedError('goMarshal: cannot encode ${v.runtimeType}');
  }
}

// On the web an integral double is also an `int` (and so is Infinity), so the
// checks that depend on the double's value run before the int/double split.
String _formatNum(num n) {
  if (!n.isFinite) {
    throw UnsupportedError('goMarshal: unsupported value $n');
  }
  if (n == 0 && n.isNegative) return '-0';
  if (n is int) return n.toString();
  return _formatDouble(n as double);
}

String _formatDouble(double d) {
  final abs = d.abs();
  if (abs != 0 && (abs < 1e-6 || abs >= 1e21)) {
    // Dart prints the shortest exponent form without zero padding, which is
    // Go's 'e' form after its e-09 -> e-9 cleanup.
    return d.toString();
  }
  final s = d.toString();
  if (s.contains('e')) return _expandExponent(s);
  return s.endsWith('.0') ? s.substring(0, s.length - 2) : s;
}

// Some runtimes print values in [1e-6, 1e21) with an exponent. Expand to
// fixed notation with the same significant digits.
String _expandExponent(String s) {
  final neg = s.startsWith('-');
  final body = neg ? s.substring(1) : s;
  final parts = body.split('e');
  final exp = int.parse(parts[1]);
  final mantissa = parts[0];
  final dot = mantissa.indexOf('.');
  final digits = mantissa.replaceFirst('.', '');
  final intLen = (dot < 0 ? mantissa.length : dot) + exp;
  String out;
  if (intLen <= 0) {
    out = '0.${'0' * -intLen}$digits';
  } else if (intLen >= digits.length) {
    out = digits + '0' * (intLen - digits.length);
  } else {
    out = '${digits.substring(0, intLen)}.${digits.substring(intLen)}';
  }
  return neg ? '-$out' : out;
}

void _writeString(StringBuffer out, String s) {
  out.write('"');
  final units = s.codeUnits;
  for (var i = 0; i < units.length; i++) {
    final u = units[i];
    if (u >= 0xD800 &&
        u <= 0xDBFF &&
        i + 1 < units.length &&
        units[i + 1] >= 0xDC00 &&
        units[i + 1] <= 0xDFFF) {
      out.writeCharCode(u);
      out.writeCharCode(units[++i]);
      continue;
    }
    if (u >= 0xD800 && u <= 0xDFFF) {
      // Go decodes a lone surrogate escape to U+FFFD and writes that rune raw.
      out.writeCharCode(0xFFFD);
      continue;
    }
    switch (u) {
      case 0x22:
        out.write(r'\"');
      case 0x5C:
        out.write(r'\\');
      case 0x0A:
        out.write(r'\n');
      case 0x0D:
        out.write(r'\r');
      case 0x09:
        out.write(r'\t');
      case 0x08:
        out.write(r'\b');
      case 0x0C:
        out.write(r'\f');
      case 0x3C || 0x3E || 0x26 || 0x2028 || 0x2029:
        out
          ..write(r'\u')
          ..write(_hex[(u >> 12) & 0xF])
          ..write(_hex[(u >> 8) & 0xF])
          ..write(_hex[(u >> 4) & 0xF])
          ..write(_hex[u & 0xF]);
      default:
        if (u < 0x20) {
          out
            ..write(r'\u00')
            ..write(_hex[u >> 4])
            ..write(_hex[u & 0xF]);
        } else {
          out.writeCharCode(u);
        }
    }
  }
  out.write('"');
}
