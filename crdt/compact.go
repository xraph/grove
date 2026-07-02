package crdt

// Horizon-based tombstone compaction — the GC pass for long-lived CRDT
// states. The horizon is a stability floor the CALLER guarantees: every
// replica has observed all operations with HLC < horizon, and no in-flight
// operation references addresses older than it. Under that contract:
//
//   - RGA lists drop tombstoned LEAF nodes (no child anchors), cascading to
//     a fixpoint — survivor ordering is untouched by construction.
//   - OR-Sets drop removed tags (and their removal markers) older than the
//     horizon; empty entries are pruned. Add-wins is preserved: only tags
//     already observed-removed are dropped.
//   - Text states never drop addresses — tombstoned fragments are
//     SKELETONIZED (content freed, adjacent skeletons coalesced), keeping
//     every anchor resolvable forever, Yjs GC-marker style.
//
// All Compact methods return how many units they dropped (nodes, tags,
// fragments coalesced away) so callers can meter GC effectiveness. A zero
// horizon is a no-op.

// Compact drops tombstoned leaf nodes older than the horizon, cascading
// until no more can be dropped. Returns the number of nodes removed.
func (l *RGAListState) Compact(before HLC) int {
	if before.IsZero() || len(l.Nodes) == 0 {
		return 0
	}
	dropped := 0
	for {
		// Recompute anchors each round: dropping a leaf may expose its parent.
		hasChild := make(map[string]bool, len(l.Nodes))
		for _, node := range l.Nodes {
			hasChild[rgaNodeKey(node.ParentID)] = true
		}
		droppedThisRound := 0
		for key, node := range l.Nodes {
			if node.Tombstone && !hasChild[key] && before.After(node.ID) {
				delete(l.Nodes, key)
				droppedThisRound++
			}
		}
		if droppedThisRound == 0 {
			break
		}
		dropped += droppedThisRound
	}
	if dropped > 0 {
		l.order = nil
	}
	return dropped
}

// Compact drops observed-removed tags older than the horizon along with
// their removal markers, pruning entries left tagless. Returns the number
// of tags dropped.
func (s *ORSetState) Compact(before HLC) int {
	if before.IsZero() {
		return 0
	}
	dropped := 0
	for elem, tags := range s.Entries {
		kept := tags[:0]
		for _, tag := range tags {
			if s.tagRemoved(elem, tag) && before.After(tag.HLC) {
				delete(s.Removed, removedKey(elem, tag))
				delete(s.Removed, tagKey(tag))
				dropped++
				continue
			}
			kept = append(kept, tag)
		}
		if len(kept) == 0 {
			delete(s.Entries, elem)
		} else {
			s.Entries[elem] = kept
		}
	}
	return dropped
}

// Compact skeletonizes tombstoned fragments whose origin is older than the
// horizon: content is freed and adjacent skeletons coalesce, but addresses
// are preserved so every anchor stays resolvable. Returns the number of
// fragments freed or coalesced away.
func (t *TextState) Compact(before HLC) int {
	if before.IsZero() {
		return 0
	}
	dropped := 0
	for key, frags := range t.Frags {
		changed := false
		for _, f := range frags {
			if f.Tombstone && f.Content != "" && before.After(f.Origin) {
				f.Content = ""
				f.Attrs = nil
				changed = true
				dropped++
			}
		}
		if !changed && len(frags) < 2 {
			continue
		}
		// Coalesce adjacent skeletons (contiguous, both tombstoned+empty).
		out := frags[:0]
		for _, f := range frags {
			if n := len(out); n > 0 {
				prev := out[n-1]
				if prev.Tombstone && prev.Content == "" && f.Tombstone && f.Content == "" &&
					prev.Start+prev.Length == f.Start {
					prev.Length += f.Length
					dropped++
					continue
				}
			}
			out = append(out, f)
		}
		t.Frags[key] = out
	}
	if dropped > 0 {
		t.segs = nil
	}
	return dropped
}

// Compact walks every field of a record state, compacting the types that
// accumulate tombstones. Returns the total units dropped.
func (st *State) Compact(before HLC) int {
	if st == nil || before.IsZero() {
		return 0
	}
	dropped := 0
	for _, fs := range st.Fields {
		switch fs.Type {
		case TypeList:
			if fs.ListState != nil {
				dropped += fs.ListState.Compact(before)
			}
		case TypeSet:
			if fs.SetState != nil {
				dropped += fs.SetState.Compact(before)
			}
		case TypeText:
			if fs.TextState != nil {
				dropped += fs.TextState.Compact(before)
			}
		}
	}
	return dropped
}
