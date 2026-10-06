import 'package:grove_crdt/grove_crdt.dart';

/// Structural equality for golden comparisons. `null` is equivalent to an
/// empty list or an empty map, because Go emits nil non-omitempty slices and
/// maps as `null` and reads `[]`, `{}` and `null` identically. Nothing else is
/// loosened: `[]` is not `{}`, a non-empty container is never `null`, and key
/// presence is exact, which is what pins Go's omitempty behaviour.
bool jsonEquivalent(Object? go, Object? dart) {
  if (go == null || dart == null) {
    final other = go ?? dart;
    return other == null || (other is List && other.isEmpty) || (other is Map && other.isEmpty);
  }
  if (go is Map<String, Object?> && dart is Map<String, Object?>) {
    final keys = {...go.keys, ...dart.keys};
    for (final k in keys) {
      if (go.containsKey(k) != dart.containsKey(k)) return false;
      if (!jsonEquivalent(go[k], dart[k])) return false;
    }
    return true;
  }
  if (go is List<Object?> && dart is List<Object?>) {
    if (go.length != dart.length) return false;
    for (var i = 0; i < go.length; i++) {
      if (!jsonEquivalent(go[i], dart[i])) return false;
    }
    return true;
  }
  return jsonDeepEquals(go, dart);
}
