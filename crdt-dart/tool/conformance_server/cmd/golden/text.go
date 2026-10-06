package main

import (
	"encoding/json"
	"math/rand"
	"os"
	"path/filepath"
	"sort"

	"github.com/xraph/grove/crdt"
)

// textRec is one op as the sync layer carries it: the op, the node that made
// it, and the clock it was stamped with.
type textRec struct {
	NodeID string       `json:"node_id"`
	HLC    crdt.HLC     `json:"hlc"`
	Op     *crdt.TextOp `json:"op"`
}

type textRefIndex struct {
	Ref   crdt.TextRef `json:"ref"`
	Index *int         `json:"index"`
}

// textCase is a replayable text history with everything Go says about the
// result. Every offset, start and length in it counts runes, so the cases put
// astral characters (two UTF-16 units, one rune) before every split point.
type textCase struct {
	Name          string           `json:"name"`
	Records       []textRec        `json:"records"`
	ExpectedValue string           `json:"expected_value"`
	ExpectedLen   int              `json:"expected_len"`
	ExpectedDelta []crdt.TextDelta `json:"expected_delta"`
	RefAt         []crdt.TextRef   `json:"ref_at"`
	IndexOf       []textRefIndex   `json:"index_of"`
	FinalState    *crdt.TextState  `json:"final_state"`
}

// setStringStep is one Go SetString call: the clocks the Dart port must hand
// out, one per emitted op in order, and the ops Go emitted.
type setStringStep struct {
	Value  string         `json:"value"`
	NodeID string         `json:"node_id"`
	Clocks []crdt.HLC     `json:"clocks"`
	Ops    []*crdt.TextOp `json:"ops"`
}

// setStringCase replays Setup, then each step against the live state.
type setStringCase struct {
	Name          string          `json:"name"`
	Setup         []textRec       `json:"setup"`
	Steps         []setStringStep `json:"steps"`
	ExpectedValue string          `json:"expected_value"`
	FinalState    *crdt.TextState `json:"final_state"`
}

type textGolden struct {
	Cases     []textCase      `json:"cases"`
	SetString []setStringCase `json:"set_string"`
}

func refAt(st *crdt.TextState, i int) crdt.TextRef {
	ref, ok := st.RefAt(i)
	if !ok {
		panic("RefAt: no visible character")
	}
	return ref
}

func textHLC(ts int64, node string) crdt.HLC { return h(ts, 0, node) }

func record(recs *[]textRec, node string, ts int64, op *crdt.TextOp) {
	*recs = append(*recs, textRec{NodeID: node, HLC: textHLC(ts, node), Op: op})
}

func replayText(recs []textRec) *crdt.TextState {
	st := crdt.NewTextState()
	for _, r := range recs {
		must(0, st.Apply(r.Op, r.NodeID, r.HLC))
	}
	return st
}

func sameJSON(a, b any) bool {
	return string(must(json.Marshal(a))) == string(must(json.Marshal(b)))
}

func buildTextCase(name string, edit func(st *crdt.TextState, recs *[]textRec)) textCase {
	st := crdt.NewTextState()
	var recs []textRec
	edit(st, &recs)
	replayed := replayText(recs)
	if replayed.Value() != st.Value() || !sameJSON(replayed, st) {
		panic(name + ": replay differs from the live state")
	}
	c := textCase{
		Name:          name,
		Records:       recs,
		ExpectedValue: st.Value(),
		ExpectedLen:   st.Len(),
		ExpectedDelta: st.Delta(),
		FinalState:    st,
	}
	for i := 0; i < st.Len(); i++ {
		ref, ok := st.RefAt(i)
		if !ok {
			panic(name + ": RefAt")
		}
		c.RefAt = append(c.RefAt, ref)
	}
	// IndexOf for every stored address, deleted ones included, in a stable
	// order, so tombstone collapse is pinned too.
	keys := make([]string, 0, len(st.Frags))
	for k := range st.Frags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		for _, f := range st.Frags[k] {
			for off := f.Start; off < f.Start+f.Length; off++ {
				ref := crdt.TextRef{Origin: f.Origin, Offset: off}
				// A null index means Go cannot place the address: its origin's
				// head has not arrived, so the walk never reaches it.
				entry := textRefIndex{Ref: ref}
				if idx, ok := st.IndexOf(ref); ok {
					entry.Index = &idx
				}
				c.IndexOf = append(c.IndexOf, entry)
			}
		}
	}
	return c
}

func insertAt(st *crdt.TextState, recs *[]textRec, index int, s, node string, ts int64) {
	ref := crdt.TextRef{}
	if index > 0 {
		ref = refAt(st, index-1)
	}
	op := must(st.Insert(ref, s, node, textHLC(ts, node)))
	record(recs, node, ts, op)
}

func textCases() []textCase {
	cases := []textCase{
		buildTextCase("astral_insert_split", func(st *crdt.TextState, recs *[]textRec) {
			insertAt(st, recs, 0, "a😀b🌍c", "a", 1)
			insertAt(st, recs, 3, "X", "b", 100)
			insertAt(st, recs, 1, "Y", "c", 101)
			insertAt(st, recs, st.Len(), "😀", "a", 102)
		}),
		buildTextCase("astral_typing_coalesces", func(st *crdt.TextState, recs *[]textRec) {
			ts := int64(1)
			for _, ch := range []string{"😀", "🌍", "a😀", "b"} {
				insertAt(st, recs, st.Len(), ch, "a", ts)
				ts++
			}
			ref := refAt(st, 1)
			record(recs, "a", ts, must(st.Delete(ref, 2)))
		}),
		buildTextCase("astral_delete_format_anchor", func(st *crdt.TextState, recs *[]textRec) {
			insertAt(st, recs, 0, "x😀😀y🌍z", "a", 1)
			deadAnchor := refAt(st, 2)
			record(recs, "a", 2, must(st.Delete(refAt(st, 1), 3)))
			op := must(st.Insert(deadAnchor, "Q", "b", textHLC(200, "b")))
			record(recs, "b", 200, op)
			format := must(st.Format(refAt(st, 1), 2, map[string]json.RawMessage{"italic": json.RawMessage(`true`)}, "c", textHLC(300, "c")))
			record(recs, "c", 300, format)
			unset := must(st.Format(refAt(st, 0), 1, map[string]json.RawMessage{"italic": json.RawMessage(`null`)}, "c", textHLC(301, "c")))
			record(recs, "c", 301, unset)
		}),
		buildTextCase("astral_concurrent_after_emoji", func(st *crdt.TextState, recs *[]textRec) {
			insertAt(st, recs, 0, "😀😀", "a", 1)
			ref := refAt(st, 0)
			record(recs, "n1", 100, must(st.Insert(ref, "X", "n1", textHLC(100, "n1"))))
			op2 := &crdt.TextOp{Op: crdt.TextOpInsert, Ref: ref, Content: "Y", Origin: textHLC(101, "n2")}
			must(0, st.Apply(op2, "n2", textHLC(101, "n2")))
			record(recs, "n2", 101, op2)
		}),
	}
	return cases
}

// walkCase is a compact walk-order fixture: inserts only, in delivery order,
// with the visible text and the address of every visible character as Go
// reports them. A record is
// [ts, c, node, refTs, refC, refNode, refOffset, originTs, originC, originNode, content]
// and a ref is [ts, c, node, offset]. Intermediate state is not recorded.
type walkCase struct {
	Name      string  `json:"name"`
	Records   [][]any `json:"records"`
	Value     string  `json:"value"`
	RefAt     [][]any `json:"ref_at"`
	Unreached int     `json:"unreached"`
}

// shuffledWalkCase builds a tree with a chain of chainLen origins, each
// anchored in the previous one, then branching inserts at random positions
// (so branching happens at several depths), and delivers the inserts in
// shuffled order with some dropped. The walk meets holes, anchors past
// coverage and orphan origins. Nodes "n～" and "n😀" order differently in
// UTF-16 and in Go's byte order, and clocks tie often, so sibling order
// depends on the Go string comparison.
func shuffledWalkCase(name string, seed int64, chainLen, branches int, keep float64) walkCase {
	rng := rand.New(rand.NewSource(seed))
	nodes := []string{"a", "b", "n～", "n😀", "c"}
	alpha := []string{"x", "é", "😀", "中", "𝄞"}
	src := crdt.NewTextState()
	var all []textRec
	var last crdt.TextRef
	ts := int64(0)
	for i := 0; i < chainLen+branches; i++ {
		ts++
		// Consecutive chain links use different nodes, so none extends the
		// previous origin's span.
		node := nodes[i%len(nodes)]
		if i >= chainLen {
			node = nodes[rng.Intn(len(nodes))]
		}
		clock := crdt.HLC{Timestamp: ts / 2, Counter: uint32(rng.Intn(2)), NodeID: node}
		if _, ok := src.Frags[clock.String()]; ok {
			continue
		}
		ref := crdt.TextRef{}
		if i < chainLen {
			ref = last
		} else if l := src.Len(); l > 0 {
			if idx := rng.Intn(l + 1); idx > 0 {
				ref = refAt(src, idx-1)
			}
		}
		text := alpha[rng.Intn(len(alpha))]
		if i >= chainLen && rng.Intn(3) == 0 {
			text += alpha[rng.Intn(len(alpha))]
		}
		op := must(src.Insert(ref, text, node, clock))
		all = append(all, textRec{NodeID: node, HLC: clock, Op: op})
		idx, _ := src.IndexOf(crdt.TextRef{Origin: op.Origin, Offset: 0})
		last = refAt(src, idx)
	}
	dst := crdt.NewTextState()
	c := walkCase{Name: name}
	for _, p := range rng.Perm(len(all)) {
		if rng.Float64() > keep {
			continue
		}
		r := all[p]
		must(0, dst.Apply(r.Op, r.NodeID, r.HLC))
		c.Records = append(c.Records, []any{
			r.HLC.Timestamp, r.HLC.Counter, r.HLC.NodeID,
			r.Op.Ref.Origin.Timestamp, r.Op.Ref.Origin.Counter, r.Op.Ref.Origin.NodeID, r.Op.Ref.Offset,
			r.Op.Origin.Timestamp, r.Op.Origin.Counter, r.Op.Origin.NodeID, r.Op.Content,
		})
	}
	c.Value = dst.Value()
	for i := 0; i < dst.Len(); i++ {
		r := refAt(dst, i)
		c.RefAt = append(c.RefAt, []any{r.Origin.Timestamp, r.Origin.Counter, r.Origin.NodeID, r.Offset})
	}
	return c
}

func setStringCases() []setStringCase {
	build := func(name string, setup string, values []string) setStringCase {
		st := crdt.NewTextState()
		var recs []textRec
		insertAt(st, &recs, 0, setup, "a", 1)
		c := setStringCase{Name: name, Setup: recs}
		ts := int64(10)
		for _, v := range values {
			clock := textHLC(ts, "a")
			ops := must(st.SetString(v, "a", clock))
			step := setStringStep{Value: v, NodeID: "a", Ops: ops}
			for _, op := range ops {
				if op.Op == crdt.TextOpDelete {
					step.Clocks = append(step.Clocks, textHLC(ts-1, "a"))
				} else {
					step.Clocks = append(step.Clocks, clock)
				}
			}
			c.Steps = append(c.Steps, step)
			if st.Value() != v {
				panic(name + ": SetString did not reach " + v)
			}
			ts += 10
		}
		c.ExpectedValue = st.Value()
		c.FinalState = st
		return c
	}
	return []setStringCase{
		build("set_string_astral_insert", "a😀b", []string{"a😀😀b", "a😀😀b!", "😀a😀😀b!"}),
		build("set_string_astral_replace", "a😀😀b", []string{"a🌍b", "a🌍", "🌍", "", "😀"}),
		build("set_string_mixed", "héllo🌍 world", []string{"héllo🌍 there 😀", "héllo 😀"}),
	}
}

func writeTextGolden(out string) {
	write(filepath.Join(out, "text_astral_golden.json"), textGolden{Cases: textCases(), SetString: setStringCases()})
	// Compact on purpose: the walk cases are large trees and the fixture is
	// committed.
	walks := []walkCase{
		shuffledWalkCase("walk_shuffled_complete", 7, 70, 90, 1.0),
		shuffledWalkCase("walk_shuffled_with_holes", 11, 70, 110, 0.75),
	}
	must(0, os.WriteFile(filepath.Join(out, "text_walk_golden.json"), append(must(json.Marshal(walks)), '\n'), 0o644))
}
