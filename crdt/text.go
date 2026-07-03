package crdt

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

// The text CRDT is a character-level collaborative sequence with formatting
// attributes — grove's YText analog. It adapts Yjs's (client, clock) item
// addressing to grove's HLC world:
//
//   - An ORIGIN is one contiguous insertion span, identified by the HLC of
//     the op that created it. Characters are addressed by rune offset within
//     their origin: TextRef{Origin, Offset} is a stable character address
//     that survives every concurrent edit (the relative-position primitive).
//   - State stores FRAGMENTS of origins. Fragments of one origin cover
//     disjoint [Start, Start+Length) rune ranges and order contiguously by
//     Start; only the head fragment (Start == 0) carries the parent ref, so
//     splitting never re-anchors anything.
//   - Ordering is RGA: at a given parent ref, explicit child origins sort by
//     origin HLC descending; the parent origin's own continuation is the
//     implicit oldest sibling (causality makes explicit children newer), so
//     it always sorts last. Deterministic and convergent.
//   - Sequential typing by one node EXTENDS its own origin instead of
//     minting new ones (the Yjs item-run analog): the op's Origin field
//     names the span it belongs to, decided by the creator, so every
//     replica applies it identically. Extension is creator-only, which
//     keeps offsets single-writer unique.
//   - Deletes and formats resolve their document-order range into explicit
//     address SPANS at creation time; application tombstones / formats
//     those exact addresses everywhere, immune to concurrent inserts
//     inside the range.

// TextRef addresses one character: rune Offset within the insertion span
// identified by Origin. The zero TextRef is the document head.
type TextRef struct {
	Origin HLC `json:"origin"`
	Offset int `json:"offset"`
}

// IsHead reports whether the ref is the document-head sentinel.
func (r TextRef) IsHead() bool { return r.Origin.IsZero() }

// TextSpan names a contiguous address range within one origin.
type TextSpan struct {
	Origin HLC `json:"origin"`
	Start  int `json:"start"`
	Length int `json:"length"`
}

// AttrState is one formatting attribute's LWW register on a fragment.
// A JSON null Value clears the attribute (kept as an LWW tombstone).
type AttrState struct {
	Value  json.RawMessage `json:"value"`
	HLC    HLC             `json:"hlc"`
	NodeID string          `json:"node_id"`
}

// TextFragment is a stored piece of an origin's span.
type TextFragment struct {
	Origin    HLC                  `json:"origin"`
	Start     int                  `json:"start"`
	Content   string               `json:"content"`
	Length    int                  `json:"length"`
	Parent    TextRef              `json:"parent,omitempty"`
	Tombstone bool                 `json:"tombstone,omitempty"`
	Attrs     map[string]AttrState `json:"attrs,omitempty"`
}

// TextState holds the full text CRDT state.
type TextState struct {
	// Frags maps origin key → fragments sorted by Start (disjoint ranges).
	Frags map[string][]*TextFragment `json:"frags"`

	// segs caches the document-order traversal; nil after any mutation.
	segs []textSeg
}

// textSeg is one traversal step: the address range [from, to) of frag.
type textSeg struct {
	frag     *TextFragment
	from, to int
}

// NewTextState creates an empty text CRDT.
func NewTextState() *TextState {
	return &TextState{Frags: make(map[string][]*TextFragment)}
}

// TextOpType identifies a text operation.
type TextOpType string

const (
	TextOpInsert TextOpType = "insert"
	TextOpDelete TextOpType = "delete"
	TextOpFormat TextOpType = "format"
)

// TextOp is a text operation for the sync transport. Inserts carry the
// content, the ref they anchor at and the Origin span they belong to
// (creator-decided: the op's own HLC for a new span, or the extended span's
// origin). Deletes and formats carry the exact address Spans they cover.
type TextOp struct {
	Op      TextOpType                 `json:"op"`
	Ref     TextRef                    `json:"ref,omitempty"`
	Origin  HLC                        `json:"origin,omitempty"`
	Content string                     `json:"content,omitempty"`
	Spans   []TextSpan                 `json:"spans,omitempty"`
	Attrs   map[string]json.RawMessage `json:"attrs,omitempty"`
}

func textOriginKey(h HLC) string { return h.String() }

func textRefKey(r TextRef) string {
	if r.IsHead() {
		return "@head"
	}
	return textOriginKey(r.Origin) + "@" + strconv.Itoa(r.Offset)
}

// --- Local edit API (mutates state AND returns the wire op) ---

// Insert inserts s after the character at ref (or at the document head for
// the zero ref) and returns the op to broadcast. Sequential inserts by the
// same node at its own span's tail extend that span (run coalescing).
func (t *TextState) Insert(ref TextRef, s, nodeID string, clock HLC) (*TextOp, error) {
	if s == "" {
		return nil, fmt.Errorf("crdt: empty text insert")
	}
	op := &TextOp{Op: TextOpInsert, Ref: ref, Content: s, Origin: clock}
	if !ref.IsHead() && ref.Origin.NodeID == nodeID {
		if cover := t.coverage(textOriginKey(ref.Origin)); cover > 0 && ref.Offset == cover-1 {
			op.Origin = ref.Origin
		}
	}
	if err := t.Apply(op, nodeID, clock); err != nil {
		return nil, err
	}
	return op, nil
}

// Delete tombstones length visible characters starting at ref's character
// and returns the op (with the exact spans it resolved) to broadcast.
func (t *TextState) Delete(ref TextRef, length int) (*TextOp, error) {
	spans, err := t.resolveSpans(ref, length)
	if err != nil {
		return nil, err
	}
	op := &TextOp{Op: TextOpDelete, Spans: spans}
	if err := t.Apply(op, "", HLC{}); err != nil {
		return nil, err
	}
	return op, nil
}

// Format applies attribute writes to length visible characters starting at
// ref's character. A JSON null value clears the attribute. Returns the op.
func (t *TextState) Format(ref TextRef, length int, attrs map[string]json.RawMessage, nodeID string, clock HLC) (*TextOp, error) {
	if len(attrs) == 0 {
		return nil, fmt.Errorf("crdt: format without attrs")
	}
	spans, err := t.resolveSpans(ref, length)
	if err != nil {
		return nil, err
	}
	op := &TextOp{Op: TextOpFormat, Spans: spans, Attrs: attrs}
	if err := t.Apply(op, nodeID, clock); err != nil {
		return nil, err
	}
	return op, nil
}

// --- Remote application ---

// Apply folds one TextOp into the state. Inserts place content at
// creator-chosen addresses and are idempotent (duplicate delivery is
// ignored). Delete/format ops touch the exact spans they name and assume
// CAUSAL DELIVERY per origin — they must arrive after the insert whose
// addresses they reference (the same assumption Yjs makes; fabriq's
// seq-ordered log and grove's HLC-ordered pull both satisfy it).
// Arbitrary-order convergence across replicas is provided by MergeText.
func (t *TextState) Apply(op *TextOp, nodeID string, clock HLC) error {
	if op == nil {
		return fmt.Errorf("crdt: apply nil text op")
	}
	switch op.Op {
	case TextOpInsert:
		return t.applyInsert(op, nodeID, clock)
	case TextOpDelete:
		t.applySpans(op.Spans, func(f *TextFragment) { f.Tombstone = true })
		return nil
	case TextOpFormat:
		t.applySpans(op.Spans, func(f *TextFragment) {
			if f.Attrs == nil {
				f.Attrs = make(map[string]AttrState, len(op.Attrs))
			}
			for k, v := range op.Attrs {
				if existing, ok := f.Attrs[k]; ok && !clock.After(existing.HLC) {
					continue
				}
				f.Attrs[k] = AttrState{Value: v, HLC: clock, NodeID: nodeID}
			}
		})
		return nil
	default:
		return fmt.Errorf("crdt: unknown text op %q", op.Op)
	}
}

func (t *TextState) applyInsert(op *TextOp, nodeID string, clock HLC) error {
	if op.Content == "" {
		return fmt.Errorf("crdt: text insert without content")
	}
	origin := op.Origin
	if origin.IsZero() {
		origin = clock
	}
	key := textOriginKey(origin)
	start := 0
	parent := op.Ref
	if origin == op.Ref.Origin {
		// Span extension: content continues the ref'd origin.
		start = op.Ref.Offset + 1
		parent = TextRef{}
	}

	length := len([]rune(op.Content))
	// Idempotence: skip if any part of [start, start+length) already exists.
	for _, f := range t.Frags[key] {
		if f.Start < start+length && start < f.Start+f.Length {
			return nil
		}
	}

	frag := &TextFragment{
		Origin:  origin,
		Start:   start,
		Content: op.Content,
		Length:  length,
		Parent:  parent,
	}
	if len(op.Attrs) > 0 {
		frag.Attrs = make(map[string]AttrState, len(op.Attrs))
		for k, v := range op.Attrs {
			frag.Attrs[k] = AttrState{Value: v, HLC: clock, NodeID: nodeID}
		}
	}

	frags := t.Frags[key]
	// Coalesce with the preceding tail fragment when storage-compatible.
	if n := len(frags); n > 0 && start > 0 {
		tail := frags[n-1]
		if tail.Start+tail.Length == start && !tail.Tombstone &&
			len(tail.Attrs) == 0 && len(frag.Attrs) == 0 && tail.Content != "" {
			tail.Content += op.Content
			tail.Length += length
			t.segs = nil
			return nil
		}
	}
	frags = append(frags, frag)
	sort.Slice(frags, func(i, j int) bool { return frags[i].Start < frags[j].Start })
	t.Frags[key] = frags
	t.segs = nil
	return nil
}

// applySpans splits fragments at every span boundary and applies fn to the
// fragments fully covered by each span.
func (t *TextState) applySpans(spans []TextSpan, fn func(*TextFragment)) {
	for _, span := range spans {
		key := textOriginKey(span.Origin)
		t.splitAt(key, span.Start)
		t.splitAt(key, span.Start+span.Length)
		for _, f := range t.Frags[key] {
			if f.Start >= span.Start && f.Start+f.Length <= span.Start+span.Length {
				fn(f)
			}
		}
	}
	t.segs = nil
}

// splitAt ensures a fragment boundary exists at address `at` in the origin.
func (t *TextState) splitAt(key string, at int) {
	frags := t.Frags[key]
	for i, f := range frags {
		if at <= f.Start || at >= f.Start+f.Length {
			continue
		}
		cut := at - f.Start
		head := *f
		tail := *f
		if f.Content != "" {
			runes := []rune(f.Content)
			head.Content = string(runes[:cut])
			tail.Content = string(runes[cut:])
		}
		head.Length = cut
		tail.Length = f.Length - cut
		tail.Start = at
		tail.Parent = TextRef{}
		if f.Attrs != nil {
			head.Attrs = cloneAttrs(f.Attrs)
			tail.Attrs = cloneAttrs(f.Attrs)
		}
		out := make([]*TextFragment, 0, len(frags)+1)
		out = append(out, frags[:i]...)
		out = append(out, &head, &tail)
		out = append(out, frags[i+1:]...)
		t.Frags[key] = out
		t.segs = nil
		return
	}
}

func cloneAttrs(in map[string]AttrState) map[string]AttrState {
	out := make(map[string]AttrState, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// coverage returns the exclusive upper address bound stored for an origin.
func (t *TextState) coverage(key string) int {
	frags := t.Frags[key]
	if len(frags) == 0 {
		return 0
	}
	last := frags[len(frags)-1]
	return last.Start + last.Length
}

// --- Document-order traversal ---

// walk returns the cached document-order traversal (all addresses,
// including tombstoned ones).
func (t *TextState) walk() []textSeg {
	if t.segs != nil {
		return t.segs
	}

	// children: parent ref key → child origins, newest first.
	children := make(map[string][]HLC)
	heads := make(map[string]bool)
	for key, frags := range t.Frags {
		if len(frags) == 0 || frags[0].Start != 0 {
			continue // origin's head hasn't arrived; unreachable for now
		}
		heads[key] = true
		pk := textRefKey(frags[0].Parent)
		children[pk] = append(children[pk], frags[0].Origin)
	}
	for pk := range children {
		c := children[pk]
		sort.Slice(c, func(i, j int) bool { return c[i].After(c[j]) })
		children[pk] = c
	}

	segs := make([]textSeg, 0, len(t.Frags)*2)
	visited := make(map[string]bool, len(t.Frags))

	var emitOrigin func(origin HLC)
	emitRange := func(f *TextFragment, from, to int) {
		if from < to {
			segs = append(segs, textSeg{frag: f, from: from, to: to})
		}
	}
	emitChildren := func(origin HLC, offset int) {
		for _, child := range children[textRefKey(TextRef{Origin: origin, Offset: offset})] {
			emitOrigin(child)
		}
	}
	emitOrigin = func(origin HLC) {
		key := textOriginKey(origin)
		if visited[key] {
			return
		}
		visited[key] = true

		// Anchor offsets with children, ascending.
		var anchors []int
		prefix := key + "@"
		for pk := range children {
			if strings.HasPrefix(pk, prefix) {
				if off, err := strconv.Atoi(pk[len(prefix):]); err == nil {
					anchors = append(anchors, off)
				}
			}
		}
		sort.Ints(anchors)

		ai := 0
		for _, f := range t.Frags[key] {
			cur := f.Start
			end := f.Start + f.Length
			for ai < len(anchors) && anchors[ai] < end {
				k := anchors[ai]
				if k < cur {
					// Anchor in a hole before this fragment: the anchor
					// character hasn't arrived; emit the children where it
					// will appear.
					emitChildren(origin, k)
					ai++
					continue
				}
				emitRange(f, cur-f.Start, k+1-f.Start)
				emitChildren(origin, k)
				cur = k + 1
				ai++
			}
			emitRange(f, cur-f.Start, end-f.Start)
		}
		// Children anchored past current coverage.
		for ; ai < len(anchors); ai++ {
			emitChildren(origin, anchors[ai])
		}
	}

	for _, root := range children["@head"] {
		emitOrigin(root)
	}
	// Deterministic placement for origins whose anchors are unreachable
	// (parent origin's head missing): append sorted newest-first.
	var orphans []HLC
	for key, frags := range t.Frags {
		if heads[key] && !visited[key] {
			orphans = append(orphans, frags[0].Origin)
		}
	}
	sort.Slice(orphans, func(i, j int) bool { return orphans[i].After(orphans[j]) })
	for _, o := range orphans {
		emitOrigin(o)
	}

	t.segs = segs
	return segs
}

// --- Reads ---

// Value returns the visible text.
func (t *TextState) Value() string {
	var b strings.Builder
	for _, seg := range t.walk() {
		if seg.frag.Tombstone || seg.frag.Content == "" {
			continue
		}
		b.WriteString(substring(seg.frag.Content, seg.from, seg.to))
	}
	return b.String()
}

// Len returns the number of visible characters (runes).
func (t *TextState) Len() int {
	n := 0
	for _, seg := range t.walk() {
		if seg.frag.Tombstone || seg.frag.Content == "" {
			continue
		}
		n += seg.to - seg.from
	}
	return n
}

// TextDelta is one Quill-style segment of the visible text.
type TextDelta struct {
	Insert     string                     `json:"insert"`
	Attributes map[string]json.RawMessage `json:"attributes,omitempty"`
}

// Delta returns the visible text as attribute-run segments, merging
// adjacent segments with identical attributes.
func (t *TextState) Delta() []TextDelta {
	var out []TextDelta
	for _, seg := range t.walk() {
		if seg.frag.Tombstone || seg.frag.Content == "" {
			continue
		}
		attrs := resolvedAttrs(seg.frag.Attrs)
		content := substring(seg.frag.Content, seg.from, seg.to)
		if n := len(out); n > 0 && attrsEqual(out[n-1].Attributes, attrs) {
			out[n-1].Insert += content
			continue
		}
		out = append(out, TextDelta{Insert: content, Attributes: attrs})
	}
	return out
}

// RefAt returns the stable address of the index-th visible character.
func (t *TextState) RefAt(index int) (TextRef, bool) {
	if index < 0 {
		return TextRef{}, false
	}
	n := 0
	for _, seg := range t.walk() {
		if seg.frag.Tombstone || seg.frag.Content == "" {
			continue
		}
		width := seg.to - seg.from
		if index < n+width {
			return TextRef{Origin: seg.frag.Origin, Offset: seg.frag.Start + seg.from + (index - n)}, true
		}
		n += width
	}
	return TextRef{}, false
}

// IndexOf returns the current visible index of the character at ref. A
// tombstoned character collapses to the index it would occupy (the position
// of the next visible character) — cursor semantics.
func (t *TextState) IndexOf(ref TextRef) (int, bool) {
	if ref.IsHead() {
		return 0, true
	}
	n := 0
	for _, seg := range t.walk() {
		covers := seg.frag.Origin == ref.Origin &&
			ref.Offset >= seg.frag.Start+seg.from && ref.Offset < seg.frag.Start+seg.to
		visible := !seg.frag.Tombstone && seg.frag.Content != ""
		if covers {
			if visible {
				return n + (ref.Offset - seg.frag.Start - seg.from), true
			}
			return n, true
		}
		if visible {
			n += seg.to - seg.from
		}
	}
	return 0, false
}

// resolveSpans maps `length` visible characters starting at ref's character
// (which may itself be tombstoned — resolution starts at its collapse
// point) to explicit address spans.
func (t *TextState) resolveSpans(ref TextRef, length int) ([]TextSpan, error) {
	if length <= 0 {
		return nil, fmt.Errorf("crdt: text span length must be positive")
	}
	start, ok := t.IndexOf(ref)
	if !ok {
		return nil, fmt.Errorf("crdt: text ref %s+%d not found", ref.Origin, ref.Offset)
	}
	var spans []TextSpan
	n := 0
	remaining := length
	for _, seg := range t.walk() {
		if remaining == 0 {
			break
		}
		if seg.frag.Tombstone || seg.frag.Content == "" {
			continue
		}
		width := seg.to - seg.from
		if start >= n+width {
			n += width
			continue
		}
		from := 0
		if start > n {
			from = start - n
		}
		take := width - from
		if take > remaining {
			take = remaining
		}
		spans = append(spans, TextSpan{
			Origin: seg.frag.Origin,
			Start:  seg.frag.Start + seg.from + from,
			Length: take,
		})
		remaining -= take
		n += width
	}
	if remaining > 0 {
		return nil, fmt.Errorf("crdt: text span exceeds document (%d chars short)", remaining)
	}
	return mergeAdjacentSpans(spans), nil
}

func mergeAdjacentSpans(spans []TextSpan) []TextSpan {
	if len(spans) < 2 {
		return spans
	}
	out := spans[:1]
	for _, s := range spans[1:] {
		last := &out[len(out)-1]
		if s.Origin == last.Origin && s.Start == last.Start+last.Length {
			last.Length += s.Length
			continue
		}
		out = append(out, s)
	}
	return out
}

func substring(s string, from, to int) string {
	runes := []rune(s)
	if from < 0 {
		from = 0
	}
	if to > len(runes) {
		to = len(runes)
	}
	if from >= to {
		return ""
	}
	return string(runes[from:to])
}

func resolvedAttrs(attrs map[string]AttrState) map[string]json.RawMessage {
	var out map[string]json.RawMessage
	for k, a := range attrs {
		if isJSONNull(a.Value) {
			continue
		}
		if out == nil {
			out = make(map[string]json.RawMessage)
		}
		out[k] = a.Value
	}
	return out
}

func isJSONNull(v json.RawMessage) bool {
	trimmed := strings.TrimSpace(string(v))
	return trimmed == "" || trimmed == "null"
}

func attrsEqual(a, b map[string]json.RawMessage) bool {
	if len(a) != len(b) {
		return false
	}
	return reflect.DeepEqual(a, b)
}

// --- State-based merge ---

// MergeText merges two text states: per-origin union with both sides
// normalized to the finest common fragment partition, then per-fragment
// tombstone-OR and per-attribute LWW. Commutative, associative, idempotent.
// Inputs are never mutated.
func MergeText(local, remote *TextState) *TextState {
	if local == nil {
		return remote
	}
	if remote == nil {
		return local
	}
	merged := NewTextState()
	keys := make(map[string]bool, len(local.Frags)+len(remote.Frags))
	for k := range local.Frags {
		keys[k] = true
	}
	for k := range remote.Frags {
		keys[k] = true
	}
	for k := range keys {
		merged.Frags[k] = mergeOriginFrags(local.Frags[k], remote.Frags[k])
	}
	return merged
}

// mergeOriginFrags merges one origin's fragments from both sides.
func mergeOriginFrags(a, b []*TextFragment) []*TextFragment {
	if len(a) == 0 {
		return copyFrags(b)
	}
	if len(b) == 0 {
		return copyFrags(a)
	}

	// Finest common partition: every boundary from both sides.
	bounds := make(map[int]bool)
	for _, f := range append(append([]*TextFragment{}, a...), b...) {
		bounds[f.Start] = true
		bounds[f.Start+f.Length] = true
	}
	cuts := make([]int, 0, len(bounds))
	for p := range bounds {
		cuts = append(cuts, p)
	}
	sort.Ints(cuts)

	na := normalizeFrags(a, cuts)
	nb := normalizeFrags(b, cuts)

	starts := make([]int, 0, len(na)+len(nb))
	seen := make(map[int]bool)
	for s := range na {
		if !seen[s] {
			seen[s] = true
			starts = append(starts, s)
		}
	}
	for s := range nb {
		if !seen[s] {
			seen[s] = true
			starts = append(starts, s)
		}
	}
	sort.Ints(starts)

	out := make([]*TextFragment, 0, len(starts))
	for _, s := range starts {
		fa, fb := na[s], nb[s]
		switch {
		case fa == nil:
			out = append(out, fb)
		case fb == nil:
			out = append(out, fa)
		default:
			out = append(out, combineFrags(fa, fb))
		}
	}
	return out
}

func copyFrags(frags []*TextFragment) []*TextFragment {
	out := make([]*TextFragment, len(frags))
	for i, f := range frags {
		cp := *f
		if f.Attrs != nil {
			cp.Attrs = cloneAttrs(f.Attrs)
		}
		out[i] = &cp
	}
	return out
}

// normalizeFrags splits copies of frags at every cut point, keyed by Start.
func normalizeFrags(frags []*TextFragment, cuts []int) map[int]*TextFragment {
	out := make(map[int]*TextFragment, len(frags))
	for _, f := range frags {
		start := f.Start
		end := f.Start + f.Length
		prev := start
		for _, c := range cuts {
			if c <= prev || c >= end {
				continue
			}
			out[prev] = subFragment(f, prev, c)
			prev = c
		}
		out[prev] = subFragment(f, prev, end)
	}
	return out
}

// subFragment copies the [from, to) address slice of f.
func subFragment(f *TextFragment, from, to int) *TextFragment {
	cp := *f
	cp.Start = from
	cp.Length = to - from
	if f.Content != "" {
		cp.Content = substring(f.Content, from-f.Start, to-f.Start)
	}
	if from != f.Start {
		cp.Parent = TextRef{}
	}
	if f.Attrs != nil {
		cp.Attrs = cloneAttrs(f.Attrs)
	}
	return &cp
}

// combineFrags merges two aligned fragments (same origin, Start, Length).
func combineFrags(a, b *TextFragment) *TextFragment {
	out := *a
	out.Tombstone = a.Tombstone || b.Tombstone
	if out.Content == "" {
		out.Content = b.Content
	}
	if out.Parent.IsHead() && !b.Parent.IsHead() {
		out.Parent = b.Parent
	}
	if len(a.Attrs) == 0 && len(b.Attrs) == 0 {
		out.Attrs = nil
		return &out
	}
	attrs := make(map[string]AttrState, len(a.Attrs)+len(b.Attrs))
	for k, v := range a.Attrs {
		attrs[k] = v
	}
	for k, v := range b.Attrs {
		if existing, ok := attrs[k]; !ok || v.HLC.After(existing.HLC) {
			attrs[k] = v
		}
	}
	out.Attrs = attrs
	return &out
}

// --- Engine integration ---

// ToFieldState converts to the generic FieldState representation.
func (t *TextState) ToFieldState(clock HLC, nodeID string) *FieldState {
	value, err := json.Marshal(t.Value())
	if err != nil {
		return nil
	}
	return &FieldState{
		Type:      TypeText,
		HLC:       clock,
		NodeID:    nodeID,
		Value:     value,
		TextState: t,
	}
}

// TextFromFieldState reconstructs a TextState from a FieldState.
func TextFromFieldState(fs *FieldState) *TextState {
	if fs == nil || fs.Type != TypeText {
		return nil
	}
	if fs.TextState == nil {
		return NewTextState()
	}
	if fs.TextState.Frags == nil {
		fs.TextState.Frags = make(map[string][]*TextFragment)
	}
	return fs.TextState
}

// SetString reconciles the text toward the given whole string using a
// common prefix/suffix diff — the ORM write path for crdt:"text" fields,
// where the caller only has the new full value. Returns the ops emitted.
func (t *TextState) SetString(s, nodeID string, clock HLC) ([]*TextOp, error) {
	oldRunes := []rune(t.Value())
	newRunes := []rune(s)

	prefix := 0
	for prefix < len(oldRunes) && prefix < len(newRunes) && oldRunes[prefix] == newRunes[prefix] {
		prefix++
	}
	suffix := 0
	for suffix < len(oldRunes)-prefix && suffix < len(newRunes)-prefix &&
		oldRunes[len(oldRunes)-1-suffix] == newRunes[len(newRunes)-1-suffix] {
		suffix++
	}

	var ops []*TextOp
	if del := len(oldRunes) - prefix - suffix; del > 0 {
		ref, ok := t.RefAt(prefix)
		if !ok {
			return nil, fmt.Errorf("crdt: text diff: no char at %d", prefix)
		}
		op, err := t.Delete(ref, del)
		if err != nil {
			return nil, err
		}
		ops = append(ops, op)
	}
	if ins := string(newRunes[prefix : len(newRunes)-suffix]); ins != "" {
		ref := TextRef{}
		if prefix > 0 {
			var ok bool
			ref, ok = t.RefAt(prefix - 1)
			if !ok {
				return nil, fmt.Errorf("crdt: text diff: no char at %d", prefix-1)
			}
		}
		op, err := t.Insert(ref, ins, nodeID, clock)
		if err != nil {
			return nil, err
		}
		ops = append(ops, op)
	}
	return ops, nil
}
