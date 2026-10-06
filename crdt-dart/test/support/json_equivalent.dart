import 'package:grove_crdt/grove_crdt.dart';

/// Structural equality for golden comparisons. `null` and an empty list or
/// map are equivalent, because Go emits nil non-omitempty containers as
/// `null` and Go reads `[]`, `{}` and `null` identically. Key presence is
/// otherwise exact, which is what pins Go's omitempty behaviour.
bool jsonEquivalent(Object? go, Object? dart) {
  bool emptyish(Object? v) => v == null || (v is List && v.isEmpty) || (v is Map && v.isEmpty);
  if (emptyish(go) && emptyish(dart)) return true;
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
