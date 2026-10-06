import 'package:collection/collection.dart';

import 'hlc.dart';

/// A pre-encoded JSON fragment that [goMarshal] emits verbatim.
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

/// Deep JSON equality where `1` and `1.0` are equal.
bool jsonDeepEquals(Object? a, Object? b) => const _JsonEquality().equals(a, b);

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
  int hash(Object? e) => goMarshal(e).hashCode;

  @override
  bool isValidKey(Object? o) => true;
}

/// Go's `time.Time` JSON text (RFC 3339 with trimmed nanoseconds), without
/// the surrounding quotes.
String formatRfc3339Nano(DateTime t) {
  final u = t.toUtc();
  String two(int v) => v.toString().padLeft(2, '0');
  final base = '${u.year.toString().padLeft(4, '0')}-${two(u.month)}-${two(u.day)}'
      'T${two(u.hour)}:${two(u.minute)}:${two(u.second)}';
  final micros = u.millisecond * 1000 + u.microsecond;
  if (micros == 0) return '${base}Z';
  final frac = micros.toString().padLeft(6, '0').replaceFirst(RegExp(r'0+$'), '');
  return '$base.${frac}Z';
}

const _hex = '0123456789abcdef';

void _write(StringBuffer out, Object? v) {
  switch (v) {
    case null:
      out.write('null');
    case final bool b:
      out.write(b ? 'true' : 'false');
    case final int i:
      out.write(i.toString());
    case final BigInt i:
      out.write(i.toString());
    case final double d:
      out.write(_formatDouble(d));
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
    case final Map<String, Object?> m:
      final keys = m.keys.toList()..sort(compareGoStrings);
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

String _formatDouble(double d) {
  if (d.isNaN || d.isInfinite) {
    throw UnsupportedError('goMarshal: unsupported value $d');
  }
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
    if (u >= 0xD800 && u <= 0xDBFF && i + 1 < units.length && units[i + 1] >= 0xDC00 && units[i + 1] <= 0xDFFF) {
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
