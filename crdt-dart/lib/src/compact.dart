import 'hlc.dart';
import 'merge.dart';
import 'types.dart';

// Horizon-based tombstone compaction. Port of crdt-js compact.ts, checked
// against Go crdt/compact.go, which is authoritative.
//
// The horizon is a stability floor the CALLER guarantees: every replica has
// observed all operations with an HLC before the horizon, and no in-flight
// operation references addresses older than it. Passing a horizon that does
// not hold makes replicas diverge.
//
// Every function returns new objects and never mutates its inputs. When
// nothing drops, the identical input object comes back, so a store cache that
// holds it stays valid.

/// Drops tombstoned leaf nodes older than [before], cascading until no more can
/// be dropped. Survivor order is untouched by construction: a node with
/// children is never removed, so no anchor is orphaned.
///
/// Port of Go `RGAListState.Compact`. A zero [before] is a no-op.
({RgaListState state, int dropped}) compactListState(RgaListState s, HLC before) {
  if (before.isZero || s.nodes.isEmpty) return (state: s, dropped: 0);

  var nodes = s.nodes;
  var dropped = 0;
  while (true) {
    // Recompute anchors each round: dropping a leaf may expose its parent.
    final hasChild = <String>{for (final n in nodes.values) hlcString(n.parentId)};
    final survivors = <String, RgaNode>{};
    var droppedThisRound = 0;
    for (final e in nodes.entries) {
      final node = e.value;
      if (node.tombstone && !hasChild.contains(e.key) && before.isAfter(node.id)) {
        droppedThisRound++;
        continue;
      }
      survivors[e.key] = node;
    }
    if (droppedThisRound == 0) break;
    nodes = survivors;
    dropped += droppedThisRound;
  }
  return dropped == 0 ? (state: s, dropped: 0) : (state: RgaListState(nodes), dropped: dropped);
}

/// Drops observed-removed tags older than [before] along with their
/// element-scoped removal markers, and prunes entries left without tags.
///
/// Only the element-scoped marker is consumed. A legacy tag-only marker is
/// shared evidence (a multi-element add shares one tag), so deleting it would
/// resurrect sibling elements that still rely on it. The marker stays.
///
/// Port of Go `ORSetState.Compact`. A zero [before] is a no-op.
///
/// Go parity: Go also deletes an entry whose tag list is already empty and
/// does not count it, so such a state comes back as a new object with
/// `dropped == 0`. States Go itself produces never have an empty entry, so the
/// identity guarantee holds for every state that came off the wire.
({OrSetState state, int dropped}) compactSetState(OrSetState s, HLC before) {
  if (before.isZero) return (state: s, dropped: 0);

  final entries = <String, List<OrSetTag>>{};
  final removed = Map<String, bool>.of(s.removed);
  var dropped = 0;
  var pruned = false;
  for (final e in s.entries.entries) {
    final elem = e.key;
    final kept = <OrSetTag>[];
    for (final tag in e.value) {
      if (tagRemoved(s, elem, tag) && before.isAfter(tag.hlc)) {
        removed.remove(removedKey(elem, tag));
        dropped++;
        continue;
      }
      kept.add(tag);
    }
    if (kept.isEmpty) {
      pruned = true;
    } else {
      entries[elem] = kept;
    }
  }
  return dropped == 0 && !pruned
      ? (state: s, dropped: 0)
      : (state: OrSetState(entries: entries, removed: removed), dropped: dropped);
}

/// Skeletonizes tombstoned fragments whose origin is older than [before]:
/// content and attributes are freed and adjacent skeletons coalesce, but
/// addresses are preserved so every cursor anchor stays resolvable.
///
/// Port of Go `TextState.Compact`. A zero [before] is a no-op. `dropped`
/// counts fragments freed plus fragments coalesced away.
///
/// The fragments of a returned state are copies, because [TextFragment] is
/// mutable.
({TextState state, int dropped}) compactTextState(TextState s, HLC before) {
  if (before.isZero) return (state: s, dropped: 0);

  final frags = <String, List<TextFragment>>{};
  var dropped = 0;
  for (final e in s.frags.entries) {
    final list = [for (final f in e.value) f.clone()];
    for (final f in list) {
      if (f.tombstone && f.content.isNotEmpty && before.isAfter(f.origin)) {
        f.content = '';
        f.attrs = <String, AttrState>{};
        dropped++;
      }
    }
    // Coalesce adjacent skeletons (contiguous, both tombstoned and empty).
    final out = <TextFragment>[];
    for (final f in list) {
      if (out.isNotEmpty) {
        final prev = out.last;
        if (prev.tombstone &&
            prev.content.isEmpty &&
            f.tombstone &&
            f.content.isEmpty &&
            prev.start + prev.length == f.start) {
          prev.length += f.length;
          dropped++;
          continue;
        }
      }
      out.add(f);
    }
    frags[e.key] = out;
  }
  return dropped == 0 ? (state: s, dropped: 0) : (state: TextState(frags), dropped: dropped);
}

/// Compacts every compactable field of [doc]: lists, sets and text.
///
/// Port of Go `State.Compact`. It does not recurse into a nested `docState`,
/// exactly as Go does. A zero [before] is a no-op, and an untouched field keeps
/// its identity.
({DocumentState doc, int dropped}) compactDocument(DocumentState doc, HLC before) {
  if (before.isZero) return (doc: doc, dropped: 0);

  final fields = <String, FieldState>{};
  var dropped = 0;
  for (final e in doc.fields.entries) {
    final fs = e.value;
    switch (fs.type) {
      case CrdtType.list when fs.listState != null:
        final r = compactListState(fs.listState!, before);
        dropped += r.dropped;
        fields[e.key] = r.dropped == 0 ? fs : fs.copyWith(listState: r.state);
      case CrdtType.set when fs.setState != null:
        final r = compactSetState(fs.setState!, before);
        dropped += r.dropped;
        // Go parity: a pruned empty entry changes the state without counting.
        fields[e.key] = identical(r.state, fs.setState) ? fs : fs.copyWith(setState: r.state);
      case CrdtType.text when fs.textState != null:
        final r = compactTextState(fs.textState!, before);
        dropped += r.dropped;
        fields[e.key] = r.dropped == 0 ? fs : fs.copyWith(textState: r.state);
      default:
        fields[e.key] = fs;
    }
  }
  return dropped == 0 && _sameFields(doc.fields, fields)
      ? (doc: doc, dropped: 0)
      : (doc: doc.copyWith(fields: fields), dropped: dropped);
}

bool _sameFields(Map<String, FieldState> a, Map<String, FieldState> b) {
  for (final e in a.entries) {
    if (!identical(e.value, b[e.key])) return false;
  }
  return true;
}
