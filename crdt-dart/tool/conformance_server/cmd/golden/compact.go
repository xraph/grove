package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"

	"github.com/xraph/grove/crdt"
)

// compactCase is one Go Compact call: the state before, the horizon, and the
// state and drop count Go produced. Input is marshalled before Compact runs,
// because Compact edits the state it is given.
type compactCase struct {
	Kind    string          `json:"kind"`
	Name    string          `json:"name"`
	Before  crdt.HLC        `json:"before"`
	Input   json.RawMessage `json:"input"`
	Output  json.RawMessage `json:"output"`
	Dropped int             `json:"dropped"`
}

func ch1(ts int64) crdt.HLC { return h(ts, 0, "n1") }

func listNode(id, parent crdt.HLC, value string, tomb bool) *crdt.RGANode {
	return &crdt.RGANode{ID: id, NodeID: id.NodeID, ParentID: parent, Value: json.RawMessage(fmt.Sprintf("%q", value)), Tombstone: tomb}
}

func listOf(nodes ...*crdt.RGANode) *crdt.RGAListState {
	l := crdt.NewRGAListState()
	for _, n := range nodes {
		l.Nodes[n.ID.String()] = n
	}
	return l
}

func listCase(name string, before crdt.HLC, l *crdt.RGAListState) compactCase {
	in := must(json.Marshal(l))
	dropped := l.Compact(before)
	return compactCase{Kind: "list", Name: name, Before: before, Input: in, Output: must(json.Marshal(l)), Dropped: dropped}
}

func setCase(name string, before crdt.HLC, s *crdt.ORSetState) compactCase {
	in := must(json.Marshal(s))
	dropped := s.Compact(before)
	return compactCase{Kind: "set", Name: name, Before: before, Input: in, Output: must(json.Marshal(s)), Dropped: dropped}
}

func textCaseOf(name string, before crdt.HLC, t *crdt.TextState) compactCase {
	in := must(json.Marshal(t))
	dropped := t.Compact(before)
	return compactCase{Kind: "text", Name: name, Before: before, Input: in, Output: must(json.Marshal(t)), Dropped: dropped}
}

func docCase(name string, before crdt.HLC, s *crdt.State) compactCase {
	in := must(json.Marshal(s))
	dropped := s.Compact(before)
	return compactCase{Kind: "document", Name: name, Before: before, Input: in, Output: must(json.Marshal(s)), Dropped: dropped}
}

func frag(origin crdt.HLC, start int, content string, tomb bool) *crdt.TextFragment {
	return &crdt.TextFragment{Origin: origin, Start: start, Content: content, Length: len([]rune(content)), Tombstone: tomb}
}

func textOf(frags ...*crdt.TextFragment) *crdt.TextState {
	t := crdt.NewTextState()
	for _, f := range frags {
		k := f.Origin.String()
		t.Frags[k] = append(t.Frags[k], f)
	}
	return t
}

// skeleton is a tombstoned, content-free fragment covering length runes.
func skeleton(origin crdt.HLC, start, length int) *crdt.TextFragment {
	return &crdt.TextFragment{Origin: origin, Start: start, Length: length, Tombstone: true}
}

func tag(node string, ts int64) crdt.Tag { return crdt.Tag{NodeID: node, HLC: h(ts, 0, node)} }

// randomList builds a tree with random parents and tombstones, so the
// cascade runs for several rounds.
func randomList(seed int64, n int, tombRate float64) *crdt.RGAListState {
	r := rand.New(rand.NewSource(seed))
	l := crdt.NewRGAListState()
	ids := []crdt.HLC{{}}
	for i := 1; i <= n; i++ {
		id := h(int64(i), 0, "n1")
		parent := ids[r.Intn(len(ids))]
		node := listNode(id, parent, fmt.Sprintf("v%d", i), r.Float64() < tombRate)
		l.Nodes[id.String()] = node
		ids = append(ids, id)
	}
	return l
}

func compactCases() []compactCase {
	root := crdt.HLC{}
	var cases []compactCase

	cases = append(cases,
		listCase("list_drops_tombstoned_leaf", ch1(10), listOf(
			listNode(ch1(1), root, "a", false), listNode(ch1(2), ch1(1), "b", true))),
		listCase("list_keeps_tombstone_that_anchors_a_child", ch1(10), listOf(
			listNode(ch1(1), root, "a", true), listNode(ch1(2), ch1(1), "b", false))),
		listCase("list_cascades_to_the_parent", ch1(10), listOf(
			listNode(ch1(1), root, "a", true), listNode(ch1(2), ch1(1), "b", true))),
		listCase("list_cascades_five_deep_below_a_live_root", ch1(10), listOf(
			listNode(ch1(1), root, "a", false),
			listNode(ch1(2), ch1(1), "b", true),
			listNode(ch1(3), ch1(2), "c", true),
			listNode(ch1(4), ch1(3), "d", true),
			listNode(ch1(5), ch1(4), "e", true),
			listNode(ch1(6), ch1(5), "f", true))),
		listCase("list_keeps_tombstone_newer_than_the_horizon", ch1(10), listOf(
			listNode(ch1(20), root, "a", true))),
		listCase("list_keeps_tombstone_at_exactly_the_horizon", ch1(10), listOf(
			listNode(ch1(10), root, "a", true))),
		listCase("list_horizon_tie_breaks_on_node_id", h(10, 0, "b"), listOf(
			listNode(h(10, 0, "a"), root, "x", true), listNode(h(10, 0, "c"), root, "y", true))),
		listCase("list_horizon_tie_breaks_on_counter", h(10, 3, "n1"), listOf(
			listNode(h(10, 2, "n1"), root, "x", true), listNode(h(10, 3, "n1"), root, "y", true), listNode(h(10, 4, "n1"), root, "z", true))),
		listCase("list_zero_horizon_is_a_no_op", crdt.HLC{}, listOf(
			listNode(ch1(1), root, "a", true))),
		listCase("list_empty", ch1(10), crdt.NewRGAListState()),
		listCase("list_live_nodes_untouched", ch1(10), listOf(
			listNode(ch1(1), root, "a", false), listNode(ch1(2), ch1(1), "b", false))),
		listCase("list_random_tree_seed_3", ch1(30), randomList(3, 40, 0.7)),
		listCase("list_random_tree_seed_8_partial_horizon", ch1(20), randomList(8, 40, 0.8)),
	)

	multi := crdt.NewORSetState()
	_ = multi.Add("a", "n1", ch1(1))
	_ = multi.Add("a", "n2", h(2, 0, "n2"))
	_ = multi.Add("b", "n1", ch1(3))
	_ = multi.Remove("a")

	legacy := &crdt.ORSetState{
		Entries: map[string][]crdt.Tag{
			`"x"`: {tag("n1", 1)},
			`"y"`: {tag("n1", 1)},
		},
		// One shared add of x and y. The legacy marker removes both.
		Removed: map[string]bool{tag("n1", 1).NodeID + ":" + h(1, 0, "n1").String(): true},
	}

	both := crdt.NewORSetState()
	_ = both.Add("a", "n1", ch1(1))
	_ = both.Remove("a")
	both.Removed[tag("n1", 1).NodeID+":"+h(1, 0, "n1").String()] = true

	tagless := &crdt.ORSetState{
		Entries: map[string][]crdt.Tag{`"gone"`: {}, `"kept"`: {tag("n1", 1)}},
		Removed: map[string]bool{},
	}

	newer := crdt.NewORSetState()
	_ = newer.Add("a", "n1", ch1(50))
	_ = newer.Remove("a")

	// The same tag twice in one entry, with its element marker. Go deletes the
	// marker on the first copy, so the second copy is no longer removed.
	dup := &crdt.ORSetState{
		Entries: map[string][]crdt.Tag{`"a"`: {tag("n1", 1), tag("n1", 1)}},
		Removed: map[string]bool{`"a"|n1:` + h(1, 0, "n1").String(): true},
	}

	live := crdt.NewORSetState()
	_ = live.Add("a", "n1", ch1(1))

	cases = append(cases,
		setCase("set_drops_removed_tags_and_prunes_tagless_entries", ch1(10), func() *crdt.ORSetState {
			s := crdt.NewORSetState()
			_ = s.Add("a", "n1", ch1(1))
			_ = s.Remove("a")
			return s
		}()),
		setCase("set_multi_tag_element_keeps_nothing_when_all_removed", ch1(10), multi),
		setCase("set_keeps_removed_tag_newer_than_the_horizon", ch1(10), newer),
		setCase("set_keeps_the_legacy_tag_only_marker", ch1(10), legacy),
		setCase("set_element_marker_goes_but_the_legacy_marker_stays", ch1(10), both),
		setCase("set_prunes_an_entry_with_no_tags_without_counting_it", ch1(10), tagless),
		setCase("set_zero_horizon_is_a_no_op", crdt.HLC{}, func() *crdt.ORSetState {
			// Fresh: a removed tag that WOULD drop under a real horizon.
			s := crdt.NewORSetState()
			_ = s.Add("a", "n1", ch1(1))
			_ = s.Remove("a")
			return s
		}()),
		setCase("set_duplicate_tags_in_one_entry_drop_once", ch1(10), dup),
		setCase("set_live_tags_untouched", ch1(10), live),
	)

	o1, o2 := ch1(1), ch1(30)
	attrs := map[string]crdt.AttrState{"bold": {Value: json.RawMessage(`true`), HLC: ch1(5), NodeID: "n1"}}
	withAttrs := frag(o1, 0, "😀gone", true)
	withAttrs.Attrs = attrs
	liveAttrs := frag(o1, 5, "live", false)
	liveAttrs.Attrs = attrs

	cases = append(cases,
		textCaseOf("text_skeletonizes_a_tombstone_and_keeps_the_address", ch1(10), textOf(
			frag(o1, 0, "gone", true), frag(o1, 4, "kept", false))),
		textCaseOf("text_clears_attrs_of_a_skeleton_but_not_of_live_text", ch1(10), textOf(withAttrs, liveAttrs)),
		textCaseOf("text_coalesces_adjacent_skeletons", ch1(10), textOf(
			frag(o1, 0, "ab", true), frag(o1, 2, "😀😀", true), frag(o1, 4, "cd", true), frag(o1, 6, "live", false))),
		textCaseOf("text_does_not_coalesce_skeletons_with_a_gap", ch1(10), textOf(
			frag(o1, 0, "ab", true), frag(o1, 5, "cd", true))),
		textCaseOf("text_does_not_coalesce_across_a_live_fragment", ch1(10), textOf(
			frag(o1, 0, "ab", true), frag(o1, 2, "xy", false), frag(o1, 4, "cd", true))),
		textCaseOf("text_coalesces_skeletons_that_were_already_empty", ch1(10), textOf(
			skeleton(o1, 0, 3), skeleton(o1, 3, 2))),
		textCaseOf("text_keeps_a_tombstone_newer_than_the_horizon", ch1(10), textOf(
			frag(o2, 0, "late", true))),
		textCaseOf("text_keeps_a_tombstone_at_exactly_the_horizon", ch1(10), textOf(
			frag(ch1(10), 0, "edge", true))),
		textCaseOf("text_single_live_fragment_untouched", ch1(10), textOf(frag(o1, 0, "hello", false))),
		textCaseOf("text_only_origins_older_than_the_horizon_compact", ch1(10), textOf(
			frag(o1, 0, "old", true), frag(o2, 0, "new", true))),
		textCaseOf("text_zero_horizon_is_a_no_op", crdt.HLC{}, textOf(frag(o1, 0, "gone", true))),
		textCaseOf("text_empty", ch1(10), crdt.NewTextState()),
	)

	list := listOf(listNode(ch1(1), root, "a", false), listNode(ch1(2), ch1(1), "b", true))
	set := crdt.NewORSetState()
	_ = set.Add("a", "n1", ch1(1))
	_ = set.Remove("a")
	nested := crdt.NewDocumentCRDTState()
	// A separate list: ToFieldState shares the pointer, and Go compacting
	// "items" would otherwise shrink the nested copy too.
	nested.SetFieldState("inner", listOf(
		listNode(ch1(1), root, "a", false), listNode(ch1(2), ch1(1), "b", true)).ToFieldState(ch1(1), "n1"))
	counter := crdt.NewPNCounterState()
	counter.Increment("n1", 4)
	reg := must(crdt.NewLWWRegister("hello", ch1(1), "n1"))

	doc := crdt.NewState("t", "p")
	doc.Fields["items"] = list.ToFieldState(ch1(1), "n1")
	doc.Fields["tags"] = set.ToFieldState(ch1(1), "n1")
	doc.Fields["body"] = textOf(frag(o1, 0, "gone", true), frag(o1, 4, "kept", false)).ToFieldState(ch1(1), "n1")
	doc.Fields["title"] = reg.ToFieldState()
	doc.Fields["views"] = counter.ToFieldState(ch1(1), "n1")
	doc.Fields["nested"] = nested.ToFieldState(ch1(1), "n1")

	flat := crdt.NewState("t", "p")
	flat.Fields["title"] = reg.ToFieldState()
	flat.Fields["views"] = counter.ToFieldState(ch1(1), "n1")

	cases = append(cases,
		docCase("document_compacts_list_set_and_text_and_skips_the_rest", ch1(10), doc),
		docCase("document_zero_horizon_is_a_no_op", crdt.HLC{}, func() *crdt.State {
			// Fresh: a droppable tombstone, which a real horizon would remove.
			d := crdt.NewState("t", "p")
			d.Fields["items"] = listOf(
				listNode(ch1(1), root, "a", false), listNode(ch1(2), ch1(1), "b", true)).ToFieldState(ch1(1), "n1")
			return d
		}()),
		docCase("document_with_nothing_compactable", ch1(10), flat),
	)
	return cases
}

func writeCompactGolden(out string) {
	// Compact JSON: the random trees are large and the fixture is committed.
	must(0, os.WriteFile(filepath.Join(out, "compact_golden.json"), append(must(json.Marshal(compactCases())), '\n'), 0o644))
}
