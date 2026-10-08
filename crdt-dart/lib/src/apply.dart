/// Folding one [ChangeRecord] into a field's state.
///
/// A port of Go `crdt/apply.go`. Where Go returns an error this module throws
/// a [CrdtApplyError] with Go's message text, so a store applying remote
/// changes can reject one change without losing the rest.
library;

import 'go_json.dart';
import 'hlc.dart';
import 'merge.dart';
import 'text.dart';
import 'types.dart';

/// A change could not be applied. Mirrors the Go `ApplyChange` errors.
final class CrdtApplyError implements Exception {
  /// Creates the error with Go's message text and, when one lower layer
  /// raised it, that [cause].
  const CrdtApplyError(this.message, {this.cause});

  /// Go's message text.
  final String message;

  /// The exception or error this one wraps, if any.
  final Object? cause;

  @override
  String toString() => message;
}

/// The OR-set key for an element: verbatim for a [RawJson], else its Go JSON
/// encoding.
String elementKey(Object? element) =>
    element is RawJson ? element.json : setElementKey(element);

/// The clock and node of whichever of [local] and [c] is newer: the merged
/// field's authorship stamp. Port of Go `pickNewer`.
(HLC, String) _pickNewer(FieldState? local, ChangeRecord c) =>
    local != null && local.hlc.isAfter(c.hlc)
    ? (local.hlc, local.nodeId)
    : (c.hlc, c.nodeId);

FieldState _merge(FieldState? local, FieldState remote) {
  try {
    return mergeField(local, remote);
  } on CrdtMergeError catch (e) {
    throw CrdtApplyError(e.message, cause: e);
  }
}

/// Folds one change into a field's state and returns the merged state. Port
/// of Go `ApplyChange`; never mutates [local] or [c].
///
/// [local] is `null` for the first change to a field. The result may share
/// untouched substructure with [local] and [c].
///
/// Two kinds of change depend on delivery order, as they do in Go. A legacy
/// set remove (no tags on the wire) removes only the tags [local] already
/// holds that are older than the remove, so an add delivered after it
/// survives and one delivered before it does not. A document path delete has
/// no memory: it removes what is stored at that moment, so a write delivered
/// after it brings the path back and one delivered before it is removed. A
/// text field's `value` is set only when the last change was a state carrier,
/// so read text through its `textState`, as `resolveFieldValue` does.
///
/// Throws a [CrdtApplyError], with Go's message text, where Go returns an
/// error: a change type that differs from the field's, a missing payload, a
/// malformed document change, or a text insert without content.
FieldState applyChange(FieldState? local, ChangeRecord c) {
  if (local != null && local.type != c.crdtType) {
    throw CrdtApplyError(
      'crdt: cannot apply ${c.crdtType.wire} change onto ${local.type.wire} field',
    );
  }
  final state = c.state;
  if (state != null) {
    if (state.type != c.crdtType) {
      throw CrdtApplyError(
        'crdt: change type ${c.crdtType.wire} carries ${state.type.wire} state',
      );
    }
    return _merge(local, state);
  }
  switch (c.crdtType) {
    case CrdtType.lww:
      return _merge(
        local,
        FieldState(
          type: CrdtType.lww,
          hlc: c.hlc,
          nodeId: c.nodeId,
          value: c.value,
        ),
      );
    case CrdtType.counter:
      final d =
          c.counterDelta ??
          (throw const CrdtApplyError(
            'crdt: counter change missing counter_delta',
          ));
      return _merge(
        local,
        FieldState(
          type: CrdtType.counter,
          hlc: c.hlc,
          nodeId: c.nodeId,
          counterState: PnCounterState(
            inc: {c.nodeId: d.inc},
            dec: {c.nodeId: d.dec},
          ),
        ),
      );
    case CrdtType.set:
      final op =
          c.setOp ??
          (throw const CrdtApplyError('crdt: set change missing set_op'));
      return _merge(local, _setOpState(local, c, op));
    case CrdtType.list:
      final op =
          c.listOp ??
          (throw const CrdtApplyError('crdt: list change missing list_op'));
      return _merge(local, _listOpState(c, op));
    case CrdtType.text:
      final op =
          c.textOp ??
          (throw const CrdtApplyError('crdt: text change missing text_op'));
      final TextState txt;
      try {
        txt = applyTextOpTo(
          local?.textState ?? newTextState(),
          op,
          c.nodeId,
          c.hlc,
        );
      } on StateError catch (e) {
        throw CrdtApplyError(e.message, cause: e);
      }
      // Deliberately not textFieldState: re-materialising the visible text per
      // applied op is O(doc) per keystroke. Readers resolve through the
      // TextState.
      final (hlc, node) = _pickNewer(local, c);
      return FieldState(
        type: CrdtType.text,
        hlc: hlc,
        nodeId: node,
        textState: txt,
      );
    case CrdtType.document:
      return _applyDocument(local, c);
    case CrdtType.none:
      throw CrdtApplyError('crdt: apply unknown type: ${c.crdtType.wire}');
  }
}

String _elementKey(Object? element) {
  try {
    return elementKey(element);
  } on ArgumentError catch (e) {
    // An object whose keys collide once lone surrogates become U+FFFD: Go
    // would never see two keys, so there is no Go message to copy.
    throw CrdtApplyError('crdt: set_op elements: ${e.message}', cause: e);
  }
}

/// The one-op remote OR-set state for a set change. Port of Go `setOpState`.
FieldState _setOpState(FieldState? local, ChangeRecord c, SetOperation op) {
  final entries = <String, List<OrSetTag>>{};
  final removed = <String, bool>{};
  switch (op.op) {
    case SetOpType.add:
      final tag = OrSetTag(c.nodeId, c.hlc);
      for (final el in op.elements) {
        entries[_elementKey(el)] = [tag];
      }
    case SetOpType.remove:
      if (op.tags.isNotEmpty) {
        // Exact observed-remove: the op names the tags it saw, scoped to the
        // elements it removes.
        for (final el in op.elements) {
          final key = _elementKey(el);
          for (final t in op.tags) {
            removed[removedKey(key, t)] = true;
          }
        }
      } else {
        // Legacy remove: every local tag for these elements that is older than
        // the remove. Concurrent or newer adds survive.
        final localSet = local?.setState;
        if (localSet != null) {
          for (final el in op.elements) {
            final key = _elementKey(el);
            for (final t in localSet.entries[key] ?? const <OrSetTag>[]) {
              if (c.hlc.isAfter(t.hlc)) removed[removedKey(key, t)] = true;
            }
          }
        }
      }
  }
  return setFieldState(
    OrSetState(entries: entries, removed: removed),
    c.hlc,
    c.nodeId,
  );
}

/// The one-op remote RGA state for a list change. Port of Go `listOpState`.
FieldState _listOpState(ChangeRecord c, ListOperation op) {
  final nodes = <String, RgaNode>{};
  // A delete or a move names a node this replica may not have seen yet. The
  // tombstone is kept, so a late insert of that node stays deleted.
  RgaNode tomb(HLC id) =>
      RgaNode(id: id, nodeId: c.nodeId, parentId: HLC.zero, tombstone: true);
  switch (op.op) {
    case ListOpType.insert:
      final id = op.nodeId.isZero ? c.hlc : op.nodeId;
      nodes[hlcString(id)] = RgaNode(
        id: id,
        nodeId: c.nodeId,
        parentId: op.parentId,
        value: op.value ?? const JsonValue(null),
      );
    case ListOpType.delete:
      nodes[hlcString(op.nodeId)] = tomb(op.nodeId);
    case ListOpType.move:
      // A move tombstones the old id and re-inserts the value under the new
      // parent, with the op's clock as the new id.
      nodes[hlcString(op.nodeId)] = tomb(op.nodeId);
      nodes[hlcString(c.hlc)] = RgaNode(
        id: c.hlc,
        nodeId: c.nodeId,
        parentId: op.parentId,
        value: op.value ?? const JsonValue(null),
      );
  }
  return listFieldState(RgaListState(nodes), c.hlc, c.nodeId);
}

String _jsonKind(Object? v) => switch (v) {
  null => 'null',
  bool() => 'bool',
  num() => 'number',
  String() => 'string',
  List<Object?>() => 'array',
  _ => 'object',
};

bool _foldEquals(String key, String name) {
  if (key.length != name.length) return false;
  for (var i = 0; i < key.length; i++) {
    var a = key.codeUnitAt(i);
    if (a >= 0x41 && a <= 0x5A) a += 0x20;
    if (a != name.codeUnitAt(i)) return false;
  }
  return true;
}

/// The `{path, value}` payload of a document change, decoded the way Go's
/// `encoding/json` fills `documentPathOp`: keys match case-insensitively, a
/// later key wins, `null` leaves the zero value, and a wrong type is an error.
({String path, bool hasValue, Object? value}) _documentPayload(
  Object? payload,
) {
  if (payload == null) return (path: '', hasValue: false, value: null);
  if (payload is! Map<String, Object?>) {
    throw CrdtApplyError(
      'crdt: document change value: json: cannot unmarshal ${_jsonKind(payload)} into Go value of type crdt.documentPathOp',
    );
  }
  var path = '';
  var hasValue = false;
  Object? value;
  for (final e in payload.entries) {
    if (_foldEquals(e.key, 'path')) {
      final v = e.value;
      if (v == null) continue;
      if (v is! String) {
        throw CrdtApplyError(
          'crdt: document change value: json: cannot unmarshal ${_jsonKind(v)} into Go struct field documentPathOp.path of type string',
        );
      }
      path = v;
    } else if (_foldEquals(e.key, 'value')) {
      hasValue = true;
      value = e.value;
    }
  }
  return (path: path, hasValue: hasValue, value: value);
}

/// Folds a document path write or path delete. Port of Go
/// `applyDocumentChange`.
FieldState _applyDocument(FieldState? local, ChangeRecord c) {
  final raw =
      c.value ??
      (throw const CrdtApplyError('crdt: document change missing value'));
  final op = _documentPayload(raw.value);
  if (op.path.isEmpty) {
    throw const CrdtApplyError('crdt: document change missing path');
  }
  final doc = local?.docState ?? const DocumentCrdtState();
  final (hlc, node) = _pickNewer(local, c);
  if (c.tombstone) {
    // A path delete is last-writer-wins guarded: it removes only what the op
    // could have observed, and takes the path's children with it.
    final existing = doc.fields[op.path];
    if (existing != null && c.hlc.isAfter(existing.hlc)) {
      final prefix = '${op.path}.';
      return documentFieldState(
        DocumentCrdtState({
          for (final e in doc.fields.entries)
            if (e.key != op.path && !e.key.startsWith(prefix)) e.key: e.value,
        }),
        hlc,
        node,
      );
    }
    return documentFieldState(doc, hlc, node);
  }
  final remote = DocumentCrdtState({
    op.path: FieldState(
      type: CrdtType.lww,
      hlc: c.hlc,
      nodeId: c.nodeId,
      value: op.hasValue ? JsonValue(op.value) : null,
    ),
  });
  return documentFieldState(mergeDocument(doc, remote), hlc, node);
}
