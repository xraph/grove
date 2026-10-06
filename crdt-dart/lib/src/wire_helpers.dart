/// Shared decoding helpers for the wire types. Not part of the public API.
///
/// Decoding follows Go's `encoding/json` into a struct: a missing or `null`
/// key leaves the zero value, and a value of the wrong JSON type is an error
/// (a [FormatException] here, so callers handle one exception type).
library;

/// Decodes a JSON object; `null` decodes as an empty object.
Map<String, Object?> wireObj(Object? j) {
  if (j == null) return const {};
  if (j is Map<String, Object?>) return j;
  if (j is Map) return Map<String, Object?>.from(j);
  throw FormatException('crdt: expected a JSON object, got ${j.runtimeType}');
}

/// Reads a string key; missing or `null` is `''`.
String wireStr(Map<String, Object?> m, String k) {
  final v = m[k];
  if (v == null) return '';
  if (v is String) return v;
  throw FormatException('crdt: "$k" must be a string, got ${v.runtimeType}');
}

/// Converts [v] to an int; missing or `null` is 0, a fractional number throws.
int wireIntValue(Object? v, String what) {
  if (v == null) return 0;
  if (v is int) return v;
  if (v is double && v.isFinite && v == v.truncateToDouble()) return v.toInt();
  throw FormatException('crdt: "$what" must be an integer, got $v');
}

/// Reads an integer key; missing or `null` is 0.
int wireInt(Map<String, Object?> m, String k) => wireIntValue(m[k], k);

/// Reads a numeric key as a double; missing or `null` is 0.
double wireDouble(Map<String, Object?> m, String k) {
  final v = m[k];
  if (v == null) return 0;
  if (v is num) return v.toDouble();
  throw FormatException('crdt: "$k" must be a number, got ${v.runtimeType}');
}

/// Reads a boolean key; missing or `null` is `false`.
bool wireBool(Map<String, Object?> m, String k) {
  final v = m[k];
  if (v == null) return false;
  if (v is bool) return v;
  throw FormatException('crdt: "$k" must be a boolean, got ${v.runtimeType}');
}

/// Decodes a JSON array with [f]; `null` decodes as an empty list.
List<T> wireList<T>(Object? j, T Function(Object?) f) {
  if (j == null) return <T>[];
  if (j is! List) {
    throw FormatException('crdt: expected a JSON array, got ${j.runtimeType}');
  }
  return [for (final e in j) f(e)];
}

/// Decodes a JSON object's values with [f]; `null` decodes as an empty map.
Map<String, T> wireMap<T>(Object? j, T Function(Object?) f) =>
    {for (final e in wireObj(j).entries) e.key: f(e.value)};

/// Looks an enum value up by [name]; an unknown name throws.
T wireEnum<T extends Enum>(List<T> values, String name, String what) {
  for (final v in values) {
    if (v.name == name) return v;
  }
  throw FormatException('crdt: unknown $what "$name"');
}

/// The instant Go's zero `time.Time` marshals as (`0001-01-01T00:00:00Z`).
final DateTime goZeroTime = DateTime.utc(1);

/// Whether [t] is Go's zero time.
bool isGoZeroTime(DateTime t) => t.toUtc() == goZeroTime;

/// Decodes an RFC 3339 string key as a UTC instant. A missing or `null` key is
/// [goZeroTime], as it is for a Go `time.Time` field.
DateTime wireTime(Map<String, Object?> m, String k) {
  final v = m[k];
  if (v == null) return goZeroTime;
  if (v is! String) {
    throw FormatException('crdt: "$k" must be an RFC 3339 string, got $v');
  }
  return DateTime.parse(v).toUtc();
}
