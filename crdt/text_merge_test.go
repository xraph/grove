package crdt

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"
)

func TestMergeText_NilSides(t *testing.T) {
	st, _ := typeText(t, "a", 1, "hi")
	if got := MergeText(nil, st); got.Value() != "hi" {
		t.Fatalf("nil local: %q", got.Value())
	}
	if got := MergeText(st, nil); got.Value() != "hi" {
		t.Fatalf("nil remote: %q", got.Value())
	}
}

func TestMergeText_SplitVsUnsplit(t *testing.T) {
	// Both replicas share "hello"; one deletes "ll" (splitting the origin),
	// the other formats the whole word. Merge must reconcile boundaries.
	base, ops := typeText(t, "a", 1, "hello")
	r1 := replay(t, ops, metasFor("a", len(ops)))
	r2 := replay(t, ops, metasFor("a", len(ops)))
	_ = base

	ref1, _ := r1.RefAt(2)
	if _, err := r1.Delete(ref1, 2); err != nil {
		t.Fatal(err)
	}
	ref2, _ := r2.RefAt(0)
	bold := map[string]json.RawMessage{"bold": json.RawMessage(`true`)}
	if _, err := r2.Format(ref2, 5, bold, "b", HLC{Timestamp: 60, NodeID: "b"}); err != nil {
		t.Fatal(err)
	}

	m1 := MergeText(r1, r2)
	m2 := MergeText(r2, r1)
	if m1.Value() != "heo" || m2.Value() != "heo" {
		t.Fatalf("merge values: %q / %q", m1.Value(), m2.Value())
	}
	d1, d2 := m1.Delta(), m2.Delta()
	if fmt.Sprint(d1) != fmt.Sprint(d2) {
		t.Fatalf("delta divergence:\n%v\n%v", d1, d2)
	}
	// Surviving chars keep the formatting.
	if len(d1) != 1 || string(d1[0].Attributes["bold"]) != "true" {
		t.Fatalf("delta: %+v", d1)
	}
}

func TestMergeText_TombstoneOR(t *testing.T) {
	_, ops := typeText(t, "a", 1, "abc")
	r1 := replay(t, ops, metasFor("a", len(ops)))
	r2 := replay(t, ops, metasFor("a", len(ops)))

	ref, _ := r1.RefAt(0)
	if _, err := r1.Delete(ref, 1); err != nil {
		t.Fatal(err)
	}
	// r2 keeps everything. Deleted char stays deleted after merge, both ways.
	if got := MergeText(r1, r2).Value(); got != "bc" {
		t.Fatalf("merge: %q", got)
	}
	if got := MergeText(r2, r1).Value(); got != "bc" {
		t.Fatalf("merge reversed: %q", got)
	}
}

func TestMergeText_Idempotent(t *testing.T) {
	st, _ := typeText(t, "a", 1, "hello")
	ref, _ := st.RefAt(1)
	if _, err := st.Delete(ref, 2); err != nil {
		t.Fatal(err)
	}
	merged := MergeText(st, st)
	if merged.Value() != st.Value() {
		t.Fatalf("merge(a,a): %q != %q", merged.Value(), st.Value())
	}
}

// randomTextOps drives n random ops on the state from the given node,
// returning the ops with their metas.
func randomTextOps(t *testing.T, rng *rand.Rand, st *TextState, node string, baseTS int64, n int) ([]*TextOp, []opMeta) {
	t.Helper()
	var ops []*TextOp
	var metas []opMeta
	alphabet := "abcdefghij"
	for i := 0; i < n; i++ {
		clock := HLC{Timestamp: baseTS + int64(i), NodeID: node}
		l := st.Len()
		roll := rng.Intn(10)
		switch {
		case l == 0 || roll < 5: // insert
			ref := TextRef{}
			if l > 0 {
				idx := rng.Intn(l + 1)
				if idx > 0 {
					var ok bool
					ref, ok = st.RefAt(idx - 1)
					if !ok {
						t.Fatalf("RefAt(%d)", idx-1)
					}
				}
			}
			s := string(alphabet[rng.Intn(len(alphabet))]) + string(alphabet[rng.Intn(len(alphabet))])
			op, err := st.Insert(ref, s, node, clock)
			if err != nil {
				t.Fatalf("insert: %v", err)
			}
			ops = append(ops, op)
			metas = append(metas, opMeta{node: node, hlc: clock})
		case roll < 8: // delete
			idx := rng.Intn(l)
			maxLen := l - idx
			dl := 1 + rng.Intn(minInt(3, maxLen))
			ref, _ := st.RefAt(idx)
			op, err := st.Delete(ref, dl)
			if err != nil {
				t.Fatalf("delete: %v", err)
			}
			ops = append(ops, op)
			metas = append(metas, opMeta{node: node, hlc: clock})
		default: // format
			idx := rng.Intn(l)
			fl := 1 + rng.Intn(minInt(4, l-idx))
			ref, _ := st.RefAt(idx)
			attrs := map[string]json.RawMessage{"bold": json.RawMessage(`true`)}
			if rng.Intn(2) == 0 {
				attrs = map[string]json.RawMessage{"bold": json.RawMessage(`null`)}
			}
			op, err := st.Format(ref, fl, attrs, node, clock)
			if err != nil {
				t.Fatalf("format: %v", err)
			}
			ops = append(ops, op)
			metas = append(metas, opMeta{node: node, hlc: clock})
		}
	}
	return ops, metas
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func fingerprint(st *TextState) string {
	return fmt.Sprintf("%s|%v", st.Value(), st.Delta())
}

func TestText_ConvergenceRandomOps(t *testing.T) {
	// Three replicas edit independently from a shared base; every pairwise
	// merge order and grouping must converge to identical visible state.
	for seed := int64(0); seed < 20; seed++ {
		rng := rand.New(rand.NewSource(seed))

		base, baseOps := typeText(t, "base", 1, "the quick brown fox")
		baseMetas := metasFor("base", len(baseOps))
		_ = base

		replicas := make([]*TextState, 3)
		for i := range replicas {
			replicas[i] = replay(t, baseOps, baseMetas)
		}
		for i, r := range replicas {
			node := fmt.Sprintf("n%d", i)
			randomTextOps(t, rng, r, node, 1000*(int64(i)+1), 8)
		}

		// merge orders: ((0,1),2), ((1,2),0), ((2,0),1), and reversals.
		m1 := MergeText(MergeText(replicas[0], replicas[1]), replicas[2])
		m2 := MergeText(replicas[0], MergeText(replicas[1], replicas[2]))
		m3 := MergeText(MergeText(replicas[2], replicas[0]), replicas[1])
		m4 := MergeText(replicas[2], MergeText(replicas[1], replicas[0]))

		f1 := fingerprint(m1)
		for i, m := range []*TextState{m2, m3, m4} {
			if got := fingerprint(m); got != f1 {
				t.Fatalf("seed %d: merge order %d diverged:\n%s\n%s", seed, i+2, f1, got)
			}
		}
		// Idempotence on the merged result.
		if got := fingerprint(MergeText(m1, m1)); got != f1 {
			t.Fatalf("seed %d: idempotence violated", seed)
		}
		// Ops replayed onto the merge change nothing (already folded).
		if got := fingerprint(MergeText(m1, replicas[0])); got != f1 {
			t.Fatalf("seed %d: re-merging a source changed state", seed)
		}
	}
}
