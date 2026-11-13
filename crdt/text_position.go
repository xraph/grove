package crdt

// TextSide selects the side of a stable character address. Before associates
// a position with the character to its right; After with the one to its left.
type TextSide string

const (
	TextBefore TextSide = "before"
	TextAfter  TextSide = "after"
)

// TextPosition is an edit-stable gap between characters, measured in Unicode
// code points, not UTF-16 units. With no Ref, Before means the document end
// and After means its start. Tombstoned anchors collapse to their former gap.
// Keep the referenced text's tombstones for as long as positions are in use.
type TextPosition struct {
	Ref  *TextRef `json:"ref,omitempty"`
	Side TextSide `json:"side"`
}

// TextSelection preserves selection direction using independently anchored
// endpoints. The anchor is where selection started; focus is the moving end.
type TextSelection struct {
	Anchor TextPosition `json:"anchor"`
	Focus  TextPosition `json:"focus"`
}

// PositionAt captures a gap in [0, Len()]. Before moves after text inserted
// at the gap; After stays before it. Invalid offsets or sides return false.
func (t *TextState) PositionAt(index int, side TextSide) (TextPosition, bool) {
	position := TextPosition{Side: side}
	if index < 0 || index > t.Len() || (side != TextBefore && side != TextAfter) {
		return TextPosition{}, false
	}
	if side == TextAfter {
		index--
	}
	if ref, ok := t.RefAt(index); ok {
		position.Ref = &ref
	}
	return position, true
}

// ResolvePosition resolves a captured gap against the current text. An
// unknown address returns false: the referenced edit may not have arrived yet.
func (t *TextState) ResolvePosition(position TextPosition) (int, bool) {
	if position.Side != TextBefore && position.Side != TextAfter {
		return 0, false
	}
	if position.Ref == nil {
		if position.Side == TextBefore {
			return t.Len(), true
		}
		return 0, true
	}
	ref := *position.Ref
	if ref.IsHead() || ref.Offset < 0 {
		return 0, false
	}
	n := 0
	for _, seg := range t.walk() {
		visible := !seg.frag.Tombstone && seg.frag.Content != ""
		if seg.frag.Origin == ref.Origin && ref.Offset >= seg.frag.Start+seg.from && ref.Offset < seg.frag.Start+seg.to {
			if visible {
				n += ref.Offset - seg.frag.Start - seg.from
				if position.Side == TextAfter {
					n++
				}
			}
			return n, true
		}
		if visible {
			n += seg.to - seg.from
		}
	}
	return 0, false
}
