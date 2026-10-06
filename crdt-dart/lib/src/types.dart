import 'package:meta/meta.dart';

import 'go_json.dart';
import 'hlc.dart';
import 'wire_helpers.dart';

/// Encodes a wire object with Go's `json.Marshal` value and string rules
/// (number formatting, HTML escaping, lone surrogates). Object keys come out
/// sorted, as Go sorts map keys, but Go writes struct fields in declaration
/// order, so the bytes equal Go's only after both sides are canonicalised
/// (parsed and re-marshalled). Every request body and WebSocket frame goes
/// through this.
String encodeWire(Object? json) => goMarshal(json);

/// A JSON value present on the wire, possibly `null`. A `JsonValue?` field
/// that is itself `null` means the key is absent.
@immutable
final class JsonValue {
  /// Wraps a decoded JSON value.
  const JsonValue(this.value);

  /// The decoded JSON value.
  final Object? value;

  @override
  bool operator ==(Object other) =>
      other is JsonValue && jsonDeepEquals(other.value, value);

  @override
  int get hashCode => goMarshal(value).hashCode;
}

/// The conflict resolution strategy of a field. Mirrors Go `crdt.CRDTType`.
enum CrdtType {
  /// Last-writer-wins register.
  lww('lww'),

  /// PN-counter.
  counter('counter'),

  /// Add-wins observed-remove set.
  set('set'),

  /// RGA list.
  list('list'),

  /// Nested document of path-addressed fields.
  document('document'),

  /// Collaborative text.
  text('text'),

  /// The empty type Go emits on pulled `_tombstone` rows.
  none('');

  const CrdtType(this.wire);

  /// The wire string.
  final String wire;

  /// Decodes a wire string; throws [FormatException] for an unknown one.
  static CrdtType fromWire(Object? s) => switch (s) {
    null || '' => none,
    'lww' => lww,
    'counter' => counter,
    'set' => set,
    'list' => list,
    'document' => document,
    'text' => text,
    _ => throw FormatException('crdt: unknown crdt type $s'),
  };
}

/// OR-set operation kind.
enum SetOpType {
  /// Adds elements.
  add,

  /// Removes elements.
  remove,
}

/// RGA list operation kind.
enum ListOpType {
  /// Inserts a node.
  insert,

  /// Tombstones a node.
  delete,

  /// Tombstones a node and re-inserts its value elsewhere.
  move,
}

/// Text operation kind.
enum TextOpType {
  /// Inserts content.
  insert,

  /// Tombstones spans.
  delete,

  /// Sets attributes on spans.
  format,
}

JsonValue? _present(Map<String, Object?> m, String k) =>
    m.containsKey(k) ? JsonValue(m[k]) : null;

/// A node's cumulative counter totals. Mirrors Go `crdt.CounterDelta`.
@immutable
final class CounterDelta {
  /// Creates a delta snapshot.
  const CounterDelta(this.inc, this.dec);

  /// Total increments by the sending node.
  final int inc;

  /// Total decrements by the sending node.
  final int dec;

  /// Go wire form.
  Map<String, Object?> toJson() => {'inc': inc, 'dec': dec};

  /// Decodes the Go wire form.
  static CounterDelta fromJson(Object? j) {
    final m = wireObj(j);
    return CounterDelta(wireInt(m, 'inc'), wireInt(m, 'dec'));
  }
}

/// One OR-set add tag. Mirrors Go `crdt.Tag`.
@immutable
final class OrSetTag {
  /// Creates a tag.
  const OrSetTag(this.node, this.hlc);

  /// The node that added the element.
  final String node;

  /// The clock of the add.
  final HLC hlc;

  /// Go wire form.
  Map<String, Object?> toJson() => {'node': node, 'hlc': hlc.toJson()};

  /// Decodes the Go wire form.
  static OrSetTag fromJson(Object? j) {
    final m = wireObj(j);
    return OrSetTag(wireStr(m, 'node'), HLC.fromJson(m['hlc']));
  }
}

/// An add or remove on an OR-set. Mirrors Go `crdt.SetOperation`.
@immutable
final class SetOperation {
  /// Creates a set operation.
  const SetOperation(this.op, this.elements, {this.tags = const []});

  /// Add or remove.
  final SetOpType op;

  /// Decoded element values.
  final List<Object?> elements;

  /// Observed add tags an exact remove deletes.
  final List<OrSetTag> tags;

  /// Go wire form.
  Map<String, Object?> toJson() => {
    'op': op.name,
    'elements': elements,
    if (tags.isNotEmpty) 'tags': [for (final t in tags) t.toJson()],
  };

  /// Decodes the Go wire form.
  static SetOperation fromJson(Object? j) {
    final m = wireObj(j);
    return SetOperation(
      wireEnum(SetOpType.values, wireStr(m, 'op'), 'set op'),
      wireList<Object?>(m['elements'], (e) => e),
      tags: wireList(m['tags'], OrSetTag.fromJson),
    );
  }
}

/// An RGA list operation. Mirrors Go `crdt.ListOp`.
@immutable
final class ListOperation {
  /// Creates a list operation.
  ListOperation(this.op, {HLC? nodeId, HLC? parentId, this.value})
    : nodeId = nodeId ?? HLC.zero,
      parentId = parentId ?? HLC.zero;

  /// The operation kind.
  final ListOpType op;

  /// The node the operation targets (zero: use the change HLC on insert).
  final HLC nodeId;

  /// The node an insert or move goes after (zero: list head).
  final HLC parentId;

  /// The inserted value.
  final JsonValue? value;

  /// Go wire form. `node_id` and `parent_id` are always emitted: Go's
  /// `omitempty` never omits a struct.
  Map<String, Object?> toJson() => {
    'op': op.name,
    'node_id': nodeId.toJson(),
    'parent_id': parentId.toJson(),
    if (value != null) 'value': value!.value,
  };

  /// Decodes the Go wire form.
  static ListOperation fromJson(Object? j) {
    final m = wireObj(j);
    return ListOperation(
      wireEnum(ListOpType.values, wireStr(m, 'op'), 'list op'),
      nodeId: HLC.fromJson(m['node_id']),
      parentId: HLC.fromJson(m['parent_id']),
      value: _present(m, 'value'),
    );
  }
}

/// A position in a text CRDT: an offset inside the fragment run that starts at
/// [origin]. Mirrors Go `crdt.TextRef`.
@immutable
final class TextRef {
  /// Creates a reference.
  const TextRef(this.origin, this.offset);

  /// The reference to the head of the text (zero origin, offset 0).
  static final TextRef head = TextRef(HLC.zero, 0);

  /// The origin clock of the run.
  final HLC origin;

  /// The offset within the run, in Unicode code points (runes), as Go counts
  /// them. A Dart string is UTF-16, so an astral character is one unit here
  /// and two UTF-16 code units in [TextFragment.content].
  final int offset;

  /// Go wire form.
  Map<String, Object?> toJson() => {
    'origin': origin.toJson(),
    'offset': offset,
  };

  /// Decodes the Go wire form.
  static TextRef fromJson(Object? j) {
    final m = wireObj(j);
    return TextRef(HLC.fromJson(m['origin']), wireInt(m, 'offset'));
  }

  @override
  bool operator ==(Object other) =>
      other is TextRef && other.origin == origin && other.offset == offset;

  @override
  int get hashCode => Object.hash(origin, offset);
}

/// A span of a fragment run. Mirrors Go `crdt.TextSpan`.
@immutable
final class TextSpan {
  /// Creates a span.
  const TextSpan(this.origin, this.start, this.length);

  /// The origin clock of the run.
  final HLC origin;

  /// The start offset within the run, in runes.
  final int start;

  /// The length in Unicode code points (runes), as Go counts them.
  final int length;

  /// Go wire form.
  Map<String, Object?> toJson() => {
    'origin': origin.toJson(),
    'start': start,
    'length': length,
  };

  /// Decodes the Go wire form.
  static TextSpan fromJson(Object? j) {
    final m = wireObj(j);
    return TextSpan(
      HLC.fromJson(m['origin']),
      wireInt(m, 'start'),
      wireInt(m, 'length'),
    );
  }
}

/// The last-writer-wins state of one text attribute. Mirrors Go
/// `crdt.AttrState`.
@immutable
final class AttrState {
  /// Creates an attribute state.
  const AttrState(this.value, this.hlc, this.nodeId);

  /// The attribute value; `JsonValue(null)` when the attribute is cleared.
  final JsonValue value;

  /// The clock of the write.
  final HLC hlc;

  /// The node that wrote it.
  final String nodeId;

  /// Go wire form.
  Map<String, Object?> toJson() => {
    'value': value.value,
    'hlc': hlc.toJson(),
    'node_id': nodeId,
  };

  /// Decodes the Go wire form.
  static AttrState fromJson(Object? j) {
    final m = wireObj(j);
    return AttrState(
      JsonValue(m['value']),
      HLC.fromJson(m['hlc']),
      wireStr(m, 'node_id'),
    );
  }
}

/// A run of text inserted by one operation, possibly split by later edits.
/// Mirrors Go `crdt.TextFragment`.
///
/// Mutable: the text algorithm edits fragments of a cloned [TextState] in place.
final class TextFragment {
  /// Creates a fragment.
  TextFragment({
    required this.origin,
    required this.start,
    required this.content,
    required this.length,
    TextRef? parent,
    this.tombstone = false,
    Map<String, AttrState>? attrs,
  }) : parent = parent ?? TextRef.head,
       attrs = attrs ?? <String, AttrState>{};

  /// The origin clock of the run this fragment belongs to.
  HLC origin;

  /// The start offset within the run, in runes.
  int start;

  /// The text of the fragment.
  String content;

  /// The length in Unicode code points (runes), as Go counts them, not the
  /// UTF-16 length of [content].
  int length;

  /// Where the run was inserted.
  TextRef parent;

  /// Whether the fragment is deleted.
  bool tombstone;

  /// Attribute states.
  Map<String, AttrState> attrs;

  /// A deep copy.
  TextFragment clone() => TextFragment(
    origin: origin,
    start: start,
    content: content,
    length: length,
    parent: parent,
    tombstone: tombstone,
    attrs: Map<String, AttrState>.of(attrs),
  );

  /// Go wire form.
  Map<String, Object?> toJson() => {
    'origin': origin.toJson(),
    'start': start,
    'content': content,
    'length': length,
    'parent': parent.toJson(),
    if (tombstone) 'tombstone': true,
    if (attrs.isNotEmpty)
      'attrs': {for (final e in attrs.entries) e.key: e.value.toJson()},
  };

  /// Decodes the Go wire form.
  static TextFragment fromJson(Object? j) {
    final m = wireObj(j);
    return TextFragment(
      origin: HLC.fromJson(m['origin']),
      start: wireInt(m, 'start'),
      content: wireStr(m, 'content'),
      length: wireInt(m, 'length'),
      parent: TextRef.fromJson(m['parent']),
      tombstone: wireBool(m, 'tombstone'),
      attrs: wireMap(m['attrs'], AttrState.fromJson),
    );
  }
}

/// The state of a text CRDT: fragments grouped by origin. Mirrors Go
/// `crdt.TextState`.
///
/// Mutable: see [TextFragment].
final class TextState {
  /// Creates a state, empty when [frags] is omitted.
  TextState([Map<String, List<TextFragment>>? frags])
    : frags = frags ?? <String, List<TextFragment>>{};

  /// Fragments keyed by the `HLC.String()` form of their origin.
  Map<String, List<TextFragment>> frags;

  /// A deep copy: every fragment is cloned.
  TextState clone() => TextState({
    for (final e in frags.entries) e.key: [for (final f in e.value) f.clone()],
  });

  /// Go wire form.
  Map<String, Object?> toJson() => {
    'frags': {
      for (final e in frags.entries)
        e.key: [for (final f in e.value) f.toJson()],
    },
  };

  /// Decodes the Go wire form.
  static TextState fromJson(Object? j) {
    final m = wireObj(j);
    return TextState(
      wireMap(m['frags'], (v) => wireList(v, TextFragment.fromJson)),
    );
  }
}

/// A text edit. Mirrors Go `crdt.TextOp`.
@immutable
final class TextOperation {
  /// Creates a text operation.
  TextOperation(
    this.op, {
    TextRef? ref,
    HLC? origin,
    this.content = '',
    this.spans = const [],
    this.attrs = const {},
  }) : ref = ref ?? TextRef.head,
       origin = origin ?? HLC.zero;

  /// The operation kind.
  final TextOpType op;

  /// Where an insert goes (head when absent).
  final TextRef ref;

  /// The origin clock of an insert's run (zero when absent).
  final HLC origin;

  /// The inserted text.
  final String content;

  /// The spans a delete or format touches.
  final List<TextSpan> spans;

  /// The attributes a format sets (a `JsonValue(null)` clears one).
  final Map<String, JsonValue> attrs;

  /// Go wire form. `ref` and `origin` are structs, so they are always emitted.
  Map<String, Object?> toJson() => {
    'op': op.name,
    'ref': ref.toJson(),
    'origin': origin.toJson(),
    if (content.isNotEmpty) 'content': content,
    if (spans.isNotEmpty) 'spans': [for (final s in spans) s.toJson()],
    if (attrs.isNotEmpty)
      'attrs': {for (final e in attrs.entries) e.key: e.value.value},
  };

  /// Decodes the Go wire form.
  static TextOperation fromJson(Object? j) {
    final m = wireObj(j);
    return TextOperation(
      wireEnum(TextOpType.values, wireStr(m, 'op'), 'text op'),
      ref: TextRef.fromJson(m['ref']),
      origin: HLC.fromJson(m['origin']),
      content: wireStr(m, 'content'),
      spans: wireList(m['spans'], TextSpan.fromJson),
      attrs: wireMap(m['attrs'], JsonValue.new),
    );
  }
}

/// The state of a PN-counter: per-node totals. Mirrors Go
/// `crdt.PNCounterState`.
@immutable
final class PnCounterState {
  /// Creates a counter state.
  const PnCounterState({this.inc = const {}, this.dec = const {}});

  /// Increments per node.
  final Map<String, int> inc;

  /// Decrements per node.
  final Map<String, int> dec;

  /// Go wire form.
  Map<String, Object?> toJson() => {'inc': inc, 'dec': dec};

  /// Decodes the Go wire form.
  static PnCounterState fromJson(Object? j) {
    final m = wireObj(j);
    return PnCounterState(
      inc: wireMap(m['inc'], (v) => wireIntValue(v, 'inc')),
      dec: wireMap(m['dec'], (v) => wireIntValue(v, 'dec')),
    );
  }
}

/// The state of an OR-set. Mirrors Go `crdt.ORSetState`.
@immutable
final class OrSetState {
  /// Creates a set state.
  const OrSetState({this.entries = const {}, this.removed = const {}});

  /// Live tags per element key (the element's Go JSON encoding).
  final Map<String, List<OrSetTag>> entries;

  /// Removed `elementKey|tag` identifiers.
  final Map<String, bool> removed;

  /// Go wire form.
  Map<String, Object?> toJson() => {
    'entries': {
      for (final e in entries.entries)
        e.key: [for (final t in e.value) t.toJson()],
    },
    'removed': removed,
  };

  /// Decodes the Go wire form.
  static OrSetState fromJson(Object? j) {
    final m = wireObj(j);
    return OrSetState(
      entries: wireMap(m['entries'], (v) => wireList(v, OrSetTag.fromJson)),
      removed: wireMap(m['removed'], (v) {
        if (v is bool) return v;
        throw FormatException(
          'crdt: "removed" values must be booleans, got $v',
        );
      }),
    );
  }
}

/// One node of an RGA list. Mirrors Go `crdt.RGANode`.
@immutable
final class RgaNode {
  /// Creates a node.
  const RgaNode({
    required this.id,
    required this.nodeId,
    required this.parentId,
    this.value = const JsonValue(null),
    this.tombstone = false,
  });

  /// The node's identity.
  final HLC id;

  /// The node that inserted it.
  final String nodeId;

  /// The node it was inserted after (zero: the head).
  final HLC parentId;

  /// The stored value.
  final JsonValue value;

  /// Whether the node is deleted.
  final bool tombstone;

  /// Go wire form.
  Map<String, Object?> toJson() => {
    'id': id.toJson(),
    'node_id': nodeId,
    'parent_id': parentId.toJson(),
    'value': value.value,
    if (tombstone) 'tombstone': true,
  };

  /// Decodes the Go wire form.
  static RgaNode fromJson(Object? j) {
    final m = wireObj(j);
    return RgaNode(
      id: HLC.fromJson(m['id']),
      nodeId: wireStr(m, 'node_id'),
      parentId: HLC.fromJson(m['parent_id']),
      value: JsonValue(m['value']),
      tombstone: wireBool(m, 'tombstone'),
    );
  }
}

/// The state of an RGA list. Mirrors Go `crdt.RGAListState`.
@immutable
final class RgaListState {
  /// Creates a list state.
  const RgaListState([this.nodes = const {}]);

  /// Nodes keyed by the `HLC.String()` form of their id.
  final Map<String, RgaNode> nodes;

  /// Go wire form.
  Map<String, Object?> toJson() => {
    'nodes': {for (final e in nodes.entries) e.key: e.value.toJson()},
  };

  /// Decodes the Go wire form.
  static RgaListState fromJson(Object? j) =>
      RgaListState(wireMap(wireObj(j)['nodes'], RgaNode.fromJson));
}

/// The state of a nested document CRDT. Mirrors Go `crdt.DocumentCRDTState`.
@immutable
final class DocumentCrdtState {
  /// Creates a document CRDT state.
  const DocumentCrdtState([this.fields = const {}]);

  /// Field states keyed by dotted path.
  final Map<String, FieldState> fields;

  /// Go wire form.
  Map<String, Object?> toJson() => {
    'fields': {for (final e in fields.entries) e.key: e.value.toJson()},
  };

  /// Decodes the Go wire form.
  static DocumentCrdtState fromJson(Object? j) =>
      DocumentCrdtState(wireMap(wireObj(j)['fields'], FieldState.fromJson));
}

/// The full state of one field. Mirrors Go `crdt.FieldState`.
@immutable
final class FieldState {
  /// Creates a field state.
  const FieldState({
    required this.type,
    required this.hlc,
    required this.nodeId,
    this.value,
    this.counterState,
    this.setState,
    this.listState,
    this.docState,
    this.textState,
  });

  /// The conflict resolution strategy.
  final CrdtType type;

  /// The clock of the last write.
  final HLC hlc;

  /// The node of the last write.
  final String nodeId;

  /// The register value, or the materialised value for other types.
  final JsonValue? value;

  /// Counter state, for [CrdtType.counter].
  final PnCounterState? counterState;

  /// Set state, for [CrdtType.set].
  final OrSetState? setState;

  /// List state, for [CrdtType.list].
  final RgaListState? listState;

  /// Document state, for [CrdtType.document].
  final DocumentCrdtState? docState;

  /// Text state, for [CrdtType.text].
  final TextState? textState;

  /// A copy with the given fields replaced. A null argument keeps the field.
  FieldState copyWith({
    CrdtType? type,
    HLC? hlc,
    String? nodeId,
    JsonValue? value,
    PnCounterState? counterState,
    OrSetState? setState,
    RgaListState? listState,
    DocumentCrdtState? docState,
    TextState? textState,
  }) => FieldState(
    type: type ?? this.type,
    hlc: hlc ?? this.hlc,
    nodeId: nodeId ?? this.nodeId,
    value: value ?? this.value,
    counterState: counterState ?? this.counterState,
    setState: setState ?? this.setState,
    listState: listState ?? this.listState,
    docState: docState ?? this.docState,
    textState: textState ?? this.textState,
  );

  /// Go wire form.
  Map<String, Object?> toJson() => {
    'type': type.wire,
    'hlc': hlc.toJson(),
    'node_id': nodeId,
    if (value != null) 'value': value!.value,
    if (counterState != null) 'counter_state': counterState!.toJson(),
    if (setState != null) 'set_state': setState!.toJson(),
    if (listState != null) 'list_state': listState!.toJson(),
    if (docState != null) 'doc_state': docState!.toJson(),
    if (textState != null) 'text_state': textState!.toJson(),
  };

  /// Decodes the Go wire form. A missing `type` decodes as [CrdtType.none].
  static FieldState fromJson(Object? j) {
    final m = wireObj(j);
    return FieldState(
      type: CrdtType.fromWire(m['type']),
      hlc: HLC.fromJson(m['hlc']),
      nodeId: wireStr(m, 'node_id'),
      value: _present(m, 'value'),
      counterState: m['counter_state'] == null
          ? null
          : PnCounterState.fromJson(m['counter_state']),
      setState: m['set_state'] == null
          ? null
          : OrSetState.fromJson(m['set_state']),
      listState: m['list_state'] == null
          ? null
          : RgaListState.fromJson(m['list_state']),
      docState: m['doc_state'] == null
          ? null
          : DocumentCrdtState.fromJson(m['doc_state']),
      textState: m['text_state'] == null
          ? null
          : TextState.fromJson(m['text_state']),
    );
  }
}

/// One change on the wire. Mirrors Go `crdt.ChangeRecord`.
@immutable
final class ChangeRecord {
  /// Creates a change record.
  const ChangeRecord({
    required this.table,
    required this.pk,
    required this.field,
    required this.crdtType,
    required this.hlc,
    required this.nodeId,
    this.value,
    this.tombstone = false,
    this.counterDelta,
    this.setOp,
    this.listOp,
    this.textOp,
    this.state,
  });

  /// The table name.
  final String table;

  /// The primary key.
  final String pk;

  /// The field name (`_tombstone` on a pulled tombstone row).
  final String field;

  /// The conflict resolution strategy ([CrdtType.none] on pulled tombstones).
  final CrdtType crdtType;

  /// The clock of the change.
  final HLC hlc;

  /// The node that made the change.
  final String nodeId;

  /// The new value, when the change carries one.
  final JsonValue? value;

  /// Whether the change deletes the document.
  final bool tombstone;

  /// The counter delta, for counter changes.
  final CounterDelta? counterDelta;

  /// The set operation, for set changes.
  final SetOperation? setOp;

  /// The list operation, for list changes.
  final ListOperation? listOp;

  /// The text operation, for text changes.
  final TextOperation? textOp;

  /// The full field state the server sends on pull.
  final FieldState? state;

  /// A copy with the given fields replaced. A null argument keeps the field.
  ChangeRecord copyWith({
    String? table,
    String? pk,
    String? field,
    CrdtType? crdtType,
    HLC? hlc,
    String? nodeId,
    JsonValue? value,
    bool? tombstone,
    CounterDelta? counterDelta,
    SetOperation? setOp,
    ListOperation? listOp,
    TextOperation? textOp,
    FieldState? state,
  }) => ChangeRecord(
    table: table ?? this.table,
    pk: pk ?? this.pk,
    field: field ?? this.field,
    crdtType: crdtType ?? this.crdtType,
    hlc: hlc ?? this.hlc,
    nodeId: nodeId ?? this.nodeId,
    value: value ?? this.value,
    tombstone: tombstone ?? this.tombstone,
    counterDelta: counterDelta ?? this.counterDelta,
    setOp: setOp ?? this.setOp,
    listOp: listOp ?? this.listOp,
    textOp: textOp ?? this.textOp,
    state: state ?? this.state,
  );

  /// Go wire form.
  Map<String, Object?> toJson() => {
    'table': table,
    'pk': pk,
    'field': field,
    'crdt_type': crdtType.wire,
    'hlc': hlc.toJson(),
    'node_id': nodeId,
    if (value != null) 'value': value!.value,
    if (tombstone) 'tombstone': true,
    if (counterDelta != null) 'counter_delta': counterDelta!.toJson(),
    if (setOp != null) 'set_op': setOp!.toJson(),
    if (listOp != null) 'list_op': listOp!.toJson(),
    if (textOp != null) 'text_op': textOp!.toJson(),
    if (state != null) 'state': state!.toJson(),
  };

  /// Decodes the Go wire form.
  static ChangeRecord fromJson(Object? j) {
    final m = wireObj(j);
    return ChangeRecord(
      table: wireStr(m, 'table'),
      pk: wireStr(m, 'pk'),
      field: wireStr(m, 'field'),
      crdtType: CrdtType.fromWire(m['crdt_type']),
      hlc: HLC.fromJson(m['hlc']),
      nodeId: wireStr(m, 'node_id'),
      value: _present(m, 'value'),
      tombstone: wireBool(m, 'tombstone'),
      counterDelta: m['counter_delta'] == null
          ? null
          : CounterDelta.fromJson(m['counter_delta']),
      setOp: m['set_op'] == null ? null : SetOperation.fromJson(m['set_op']),
      listOp: m['list_op'] == null
          ? null
          : ListOperation.fromJson(m['list_op']),
      textOp: m['text_op'] == null
          ? null
          : TextOperation.fromJson(m['text_op']),
      state: m['state'] == null ? null : FieldState.fromJson(m['state']),
    );
  }
}

/// The state of one document. Mirrors Go `crdt.State`.
@immutable
final class DocumentState {
  /// Creates a document state. [tombstoneHlc] defaults to [HLC.zero], because
  /// Go always emits `tombstone_hlc`.
  DocumentState({
    required this.table,
    required this.pk,
    this.fields = const {},
    this.tombstone = false,
    HLC? tombstoneHlc,
  }) : tombstoneHlc = tombstoneHlc ?? HLC.zero;

  /// The table name.
  final String table;

  /// The primary key.
  final String pk;

  /// Field states keyed by field name.
  final Map<String, FieldState> fields;

  /// Whether the document is deleted.
  final bool tombstone;

  /// The clock of the delete.
  final HLC tombstoneHlc;

  /// A copy with the given fields replaced. A null argument keeps the field.
  DocumentState copyWith({
    String? table,
    String? pk,
    Map<String, FieldState>? fields,
    bool? tombstone,
    HLC? tombstoneHlc,
  }) => DocumentState(
    table: table ?? this.table,
    pk: pk ?? this.pk,
    fields: fields ?? this.fields,
    tombstone: tombstone ?? this.tombstone,
    tombstoneHlc: tombstoneHlc ?? this.tombstoneHlc,
  );

  /// Go wire form.
  Map<String, Object?> toJson() => {
    'table': table,
    'pk': pk,
    'fields': {for (final e in fields.entries) e.key: e.value.toJson()},
    'tombstone': tombstone,
    'tombstone_hlc': tombstoneHlc.toJson(),
  };

  /// Decodes the Go wire form.
  static DocumentState fromJson(Object? j) {
    final m = wireObj(j);
    return DocumentState(
      table: wireStr(m, 'table'),
      pk: wireStr(m, 'pk'),
      fields: wireMap(m['fields'], FieldState.fromJson),
      tombstone: wireBool(m, 'tombstone'),
      tombstoneHlc: HLC.fromJson(m['tombstone_hlc']),
    );
  }
}
