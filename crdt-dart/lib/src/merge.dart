/// Merge functions for every CRDT type and the field and document merge
/// engine.
///
/// A port of `crdt/merge.go`, `register.go`, `counter.go`, `set.go`,
/// `list.go` and `document.go`. The Go server is the authority, so where
/// `crdt-js/src/merge.ts` differs this module follows Go.
///
/// Every function returns new objects and never mutates its inputs. Go
/// mutates text and document state in place; the store's identity-keyed
/// caches need copy-on-write instead. Untouched substructure (a tag list, a
/// node, a whole field) may be shared between an input and the result, and
/// all of it is immutable except a [TextState], which only ever changes
/// through a clone (see `applyTextOpTo`).
library;

import 'dart:collection';
import 'dart:convert';

import 'go_json.dart';
import 'hlc.dart';
import 'text.dart';
import 'types.dart';

/// Two field states could not merge. Mirrors the Go `MergeField` errors.
final class CrdtMergeError implements Exception {
  /// Creates the error with Go's message text.
  const CrdtMergeError(this.message);

  /// Go's message text.
  final String message;

  @override
  String toString() => message;
}

// --- PN-counter ---

/// Max-per-node merge of two PN-counters. Port of Go `MergeCounter`.
PnCounterState mergeCounter(PnCounterState local, PnCounterState remote) {
  Map<String, int> maxed(Map<String, int> a, Map<String, int> b) {
    final out = <String, int>{...a};
    for (final e in b.entries) {
      final existing = out[e.key];
      if (existing == null || e.value > existing) out[e.key] = e.value;
    }
    return out;
  }

  return PnCounterState(inc: maxed(local.inc, remote.inc), dec: maxed(local.dec, remote.dec));
}

/// Sum of increments minus sum of decrements. Port of Go
/// `PNCounterState.Value`.
int counterValue(PnCounterState s) =>
    s.inc.values.fold(0, (a, b) => a + b) - s.dec.values.fold(0, (a, b) => a + b);

// --- OR-set ---

/// Go `tagKey`: `node:HLC{...}`.
String tagKey(OrSetTag t) => '${t.node}:${hlcString(t.hlc)}';

/// Go `removedKey`: the element-scoped removal key. A multi-element add
/// shares one tag, so a removal keyed by the tag alone would leak across
/// the elements added together.
String removedKey(String elem, OrSetTag t) => '$elem|${tagKey(t)}';

/// Whether [t] is removed for [elem], honouring both the element-scoped key
/// and the legacy tag-only key. Port of Go `tagRemoved`.
bool tagRemoved(OrSetState s, String elem, OrSetTag t) =>
    (s.removed[removedKey(elem, t)] ?? false) || (s.removed[tagKey(t)] ?? false);

List<OrSetTag> _dedupe(List<OrSetTag> tags) {
  final seen = <String>{};
  return [
    for (final t in tags)
      if (seen.add(tagKey(t))) t,
  ];
}

/// Union of entries (tags deduplicated per element) and of removals. Port of
/// Go `MergeSet`.
OrSetState mergeSet(OrSetState local, OrSetState remote) {
  final entries = <String, List<OrSetTag>>{};
  for (final e in local.entries.entries) {
    entries[e.key] = [...e.value];
  }
  for (final e in remote.entries.entries) {
    (entries[e.key] ??= <OrSetTag>[]).addAll(e.value);
  }
  for (final k in entries.keys.toList()) {
    entries[k] = _dedupe(entries[k]!);
  }
  return OrSetState(
    entries: entries,
    removed: {
      for (final e in local.removed.entries)
        if (e.value) e.key: true,
      for (final e in remote.removed.entries)
        if (e.value) e.key: true,
    },
  );
}

/// The keys of the live elements (those with a tag that is not removed), in
/// Go byte order. Port of Go `ORSetState.Elements`.
List<String> setElementKeys(OrSetState s) => [
      for (final e in s.entries.entries)
        if (e.value.any((t) => !tagRemoved(s, e.key, t))) e.key,
    ]..sort(compareGoStrings);

Object? _decodeKey(String key) {
  try {
    return jsonDecode(key);
  } on FormatException {
    return key;
  }
}

/// The live element values in Go byte order of their keys. A key that is not
/// JSON stands for itself (Go keys are always JSON; another engine's may not
/// be).
List<Object?> setElements(OrSetState s) => [for (final k in setElementKeys(s)) _decodeKey(k)];

/// Every entry key whose decoded value deep-equals [element].
///
/// A remove built by the store names these keys, so it reaches entries that
/// another engine wrote under a non-canonical spelling of the same value.
List<String> keysForElement(OrSetState s, Object? element) => [
      for (final k in s.entries.keys)
        if (jsonDeepEquals(_decodeKey(k), element)) k,
    ];

// --- RGA list ---

/// Union of nodes; a tombstone on either side wins. Port of Go `MergeList`.
RgaListState mergeList(RgaListState local, RgaListState remote) {
  final nodes = <String, RgaNode>{...local.nodes};
  for (final e in remote.nodes.entries) {
    final existing = nodes[e.key];
    if (existing == null) {
      nodes[e.key] = e.value;
    } else if (e.value.tombstone && !existing.tombstone) {
      nodes[e.key] = RgaNode(
        id: existing.id,
        nodeId: existing.nodeId,
        parentId: existing.parentId,
        value: existing.value,
        tombstone: true,
      );
    }
  }
  return RgaListState(nodes);
}

/// The visible nodes in RGA order: a pre-order walk from the head, siblings
/// newest first. Port of Go `sortedNodes`, without recursion, so a long
/// parent chain cannot overflow the stack.
List<RgaNode> _walkList(RgaListState s) {
  if (s.nodes.isEmpty) return const [];
  final children = <String, List<RgaNode>>{};
  for (final node in s.nodes.values) {
    (children[hlcString(node.parentId)] ??= <RgaNode>[]).add(node);
  }
  for (final c in children.values) {
    c.sort((a, b) => b.id.compareTo(a.id));
  }
  final out = <RgaNode>[];
  // Two nodes stored under different keys can share an id, which would make
  // a node its own descendant. Go's walk would never return; visit each node
  // once.
  final visited = HashSet<RgaNode>.identity();
  final stack = <RgaNode>[...(children[hlcString(HLC.zero)] ?? const <RgaNode>[]).reversed];
  while (stack.isNotEmpty) {
    final node = stack.removeLast();
    if (!visited.add(node)) continue;
    if (!node.tombstone) out.add(node);
    final kids = children[hlcString(node.id)];
    if (kids != null) stack.addAll(kids.reversed);
  }
  return out;
}

/// A deep copy of a decoded JSON value. Maps and lists are rebuilt, so the
/// copy shares nothing mutable with [v].
Object? _deepCopy(Object? v) => switch (v) {
      final Map<String, Object?> m => <String, Object?>{for (final e in m.entries) e.key: _deepCopy(e.value)},
      final List<Object?> l => <Object?>[for (final e in l) _deepCopy(e)],
      _ => v,
    };

/// The visible values in RGA order. Each value is a deep copy, so changing a
/// returned map or list never changes the list state.
List<Object?> listElements(RgaListState s) => [for (final n in _walkList(s)) _deepCopy(n.value.value)];

/// The visible node ids in RGA order.
List<HLC> listNodeIds(RgaListState s) => [for (final n in _walkList(s)) n.id];

// --- Document ---

/// Path-wise merge. A type mismatch at a path resolves by the higher clock,
/// which discards a merge of the two sides, so regrouping three states can
/// give a different result (see [mergeState]). Port of Go `MergeDocument`.
DocumentCrdtState mergeDocument(DocumentCrdtState local, DocumentCrdtState remote) {
  final out = <String, FieldState>{};
  for (final path in <String>{...local.fields.keys, ...remote.fields.keys}) {
    final l = local.fields[path];
    final r = remote.fields[path];
    try {
      out[path] = mergeField(l, r);
    } on CrdtMergeError {
      // Both sides exist: a one-sided path merges without error.
      out[path] = r!.hlc.isAfter(l!.hlc) ? r : l;
    }
  }
  return DocumentCrdtState(out);
}

/// The nested materialised view of a document. Port of Go `Resolve`.
///
/// The result is the caller's: every map and list in it is new, so changing
/// it cannot change [s], including a leaf object that has no nested path.
///
/// Paths apply in ascending length, so a nested path always wins over a leaf
/// at its prefix. Go documents that intent, but walks a map, so the winner
/// there depends on iteration order. When the leaf is itself an object, the
/// nested paths merge into a copy of it.
Map<String, Object?> documentResolve(DocumentCrdtState s) {
  final result = <String, Object?>{};
  // Maps this call created. A map reached through a field value belongs to
  // the field's state, so descending into one copies it first.
  final owned = HashSet<Map<String, Object?>>.identity()..add(result);
  final paths = s.fields.keys.toList()
    ..sort((a, b) {
      final d = a.length.compareTo(b.length);
      return d != 0 ? d : compareGoStrings(a, b);
    });
  for (final path in paths) {
    final parts = path.split('.');
    var m = result;
    for (var i = 0; i < parts.length - 1; i++) {
      final sub = m[parts[i]];
      final Map<String, Object?> next;
      if (sub is Map<String, Object?> && owned.contains(sub)) {
        next = sub;
      } else {
        next = sub is Map<String, Object?> ? <String, Object?>{...sub} : <String, Object?>{};
        owned.add(next);
        m[parts[i]] = next;
      }
      m = next;
    }
    m[parts.last] = resolveFieldValue(s.fields[path]!);
  }
  return result;
}

/// The application-visible value of a field.
///
/// The result is the caller's: any map or list in it is a deep copy, never a
/// reference into [fs], so changing it cannot change the state.
///
/// Text resolves from its [TextState] when present (Go falls back to `value`,
/// which `applyChange` deliberately leaves unset for text), and a document
/// from its [DocumentCrdtState] when present.
Object? resolveFieldValue(FieldState fs) => switch (fs.type) {
      CrdtType.lww => _deepCopy(fs.value?.value),
      CrdtType.counter => fs.counterState == null ? 0 : counterValue(fs.counterState!),
      CrdtType.set => fs.setState == null ? <Object?>[] : setElements(fs.setState!),
      CrdtType.list => fs.listState == null ? <Object?>[] : listElements(fs.listState!),
      CrdtType.text => fs.textState != null ? textValue(fs.textState!) : _deepCopy(fs.value?.value),
      CrdtType.document => fs.docState != null ? documentResolve(fs.docState!) : _deepCopy(fs.value?.value),
      CrdtType.none => _deepCopy(fs.value?.value),
    };

// --- Field states ---

/// Port of Go `ORSetState.ToFieldState`. An empty set has `value` `[]` where
/// Go writes `null` (its nil slice); readers treat both as empty.
FieldState setFieldState(OrSetState s, HLC hlc, String node) =>
    FieldState(type: CrdtType.set, hlc: hlc, nodeId: node, value: JsonValue(setElements(s)), setState: s);

/// Port of Go `RGAListState.ToFieldState`. An empty list has `value` `[]`
/// where Go writes `null`.
FieldState listFieldState(RgaListState s, HLC hlc, String node) =>
    FieldState(type: CrdtType.list, hlc: hlc, nodeId: node, value: JsonValue(listElements(s)), listState: s);

/// Port of Go `DocumentCRDTState.ToFieldState`.
FieldState documentFieldState(DocumentCrdtState s, HLC hlc, String node) =>
    FieldState(type: CrdtType.document, hlc: hlc, nodeId: node, value: JsonValue(documentResolve(s)), docState: s);

/// Port of Go `TextState.ToFieldState`.
FieldState textFieldState(TextState s, HLC hlc, String node) =>
    FieldState(type: CrdtType.text, hlc: hlc, nodeId: node, value: JsonValue(textValue(s)), textState: s);

/// Merges two states of one field. Port of Go `MergeEngine.MergeField`.
///
/// A `null` side yields the other side itself. Throws a [CrdtMergeError] when
/// the types differ (or are both [CrdtType.none]) and an [ArgumentError] when
/// both sides are `null`.
FieldState mergeField(FieldState? local, FieldState? remote) {
  if (local == null && remote == null) throw ArgumentError('mergeField: both sides are null');
  if (local == null) return remote!;
  if (remote == null) return local;
  if (local.type != remote.type) {
    throw CrdtMergeError('crdt: cannot merge different types: ${local.type.wire} vs ${remote.type.wire}');
  }
  final newer = remote.hlc.isAfter(local.hlc);
  final hlc = newer ? remote.hlc : local.hlc;
  final node = newer ? remote.nodeId : local.nodeId;
  switch (local.type) {
    case CrdtType.lww:
      final w = newer ? remote : local;
      return FieldState(type: CrdtType.lww, hlc: w.hlc, nodeId: w.nodeId, value: w.value);
    case CrdtType.counter:
      return FieldState(
        type: CrdtType.counter,
        hlc: hlc,
        nodeId: node,
        counterState: mergeCounter(local.counterState ?? const PnCounterState(), remote.counterState ?? const PnCounterState()),
      );
    case CrdtType.set:
      return setFieldState(
        mergeSet(local.setState ?? const OrSetState(), remote.setState ?? const OrSetState()),
        hlc,
        node,
      );
    case CrdtType.list:
      return listFieldState(
        mergeList(local.listState ?? const RgaListState(), remote.listState ?? const RgaListState()),
        hlc,
        node,
      );
    case CrdtType.text:
      // Go merges an absent text state as an empty one, which also copies the
      // other side, so the result never aliases an input's mutable state.
      return textFieldState(
        mergeText(local.textState ?? newTextState(), remote.textState ?? newTextState()),
        hlc,
        node,
      );
    case CrdtType.document:
      return documentFieldState(
        mergeDocument(local.docState ?? const DocumentCrdtState(), remote.docState ?? const DocumentCrdtState()),
        hlc,
        node,
      );
    case CrdtType.none:
      throw CrdtMergeError('crdt: unknown type: ${local.type.wire}');
  }
}

HLC _latest(DocumentState s) {
  var latest = HLC.zero;
  for (final fs in s.fields.values) {
    if (fs.hlc.isAfter(latest)) latest = fs.hlc;
  }
  return latest;
}

/// Merges two record states. Fields merge pairwise, and a tombstone wins only
/// when newer than every field write on the other side. Port of Go
/// `MergeEngine.MergeState`.
///
/// A `null` side yields the other side itself. Throws a [CrdtMergeError]
/// naming the field when two fields cannot merge, and an [ArgumentError] when
/// both sides are `null`.
///
/// Commutative, but not associative, exactly as in Go. Two cases break it:
/// when both sides are tombstoned the later tombstone wins and the fields are
/// ignored, so `(a, b), c` can differ from `a, (b, c)`; and a document path
/// whose two sides have different types resolves by the higher clock and
/// discards the merge of the two, so regrouping three states can lose a write.
/// Fold remote states in one consistent order instead of regrouping them.
/// Legacy set removes and document path deletes also depend on delivery order
/// (see [applyChange]).
DocumentState mergeState(DocumentState? local, DocumentState? remote) {
  if (local == null && remote == null) throw ArgumentError('mergeState: both sides are null');
  if (local == null) return remote!;
  if (remote == null) return local;
  final fields = <String, FieldState>{};
  for (final name in <String>{...local.fields.keys, ...remote.fields.keys}) {
    try {
      fields[name] = mergeField(local.fields[name], remote.fields[name]);
    } on CrdtMergeError catch (e) {
      throw CrdtMergeError('field $name: ${e.message}');
    }
  }
  var tombstone = false;
  var tombstoneHlc = HLC.zero;
  if (local.tombstone && remote.tombstone) {
    tombstone = true;
    tombstoneHlc = remote.tombstoneHlc.isAfter(local.tombstoneHlc) ? remote.tombstoneHlc : local.tombstoneHlc;
  } else if (remote.tombstone) {
    if (remote.tombstoneHlc.isAfter(_latest(local))) {
      tombstone = true;
      tombstoneHlc = remote.tombstoneHlc;
    }
  } else if (local.tombstone) {
    if (local.tombstoneHlc.isAfter(_latest(remote))) {
      tombstone = true;
      tombstoneHlc = local.tombstoneHlc;
    }
  }
  return DocumentState(
    table: local.table,
    pk: local.pk,
    fields: fields,
    tombstone: tombstone,
    tombstoneHlc: tombstoneHlc,
  );
}
