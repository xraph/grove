package crdt

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestApplyChange_TextOps(t *testing.T) {
	st := NewTextState()
	insOp, err := st.Insert(TextRef{}, "hello", "a", HLC{Timestamp: 10, NodeID: "a"})
	if err != nil {
		t.Fatal(err)
	}

	var fs *FieldState
	fs = mustApply(t, nil, &ChangeRecord{
		CRDTType: TypeText, HLC: applyHLC(10, 0, "a"), NodeID: "a", TextOp: insOp,
	})
	txt := TextFromFieldState(fs)
	if txt.Value() != "hello" {
		t.Fatalf("value = %q", txt.Value())
	}

	ref, _ := txt.RefAt(4)
	worldSt := NewTextState()
	_ = worldSt
	// Build the op against a replica so spans resolve identically.
	replica := TextFromFieldState(fs)
	insOp2, err := replica.Insert(ref, " world", "b", HLC{Timestamp: 20, NodeID: "b"})
	if err != nil {
		t.Fatal(err)
	}
	fs = mustApply(t, fs, &ChangeRecord{
		CRDTType: TypeText, HLC: applyHLC(20, 0, "b"), NodeID: "b", TextOp: insOp2,
	})
	if got := TextFromFieldState(fs).Value(); got != "hello world" {
		t.Fatalf("value = %q", got)
	}
	// Resolved LWW value rides ChangeRecord-free reads too.
	var plain string
	if err := json.Unmarshal(fs.Value, &plain); err != nil || plain != "hello world" {
		t.Fatalf("fs.Value = %s (%v)", fs.Value, err)
	}
}

func TestMergeField_Text(t *testing.T) {
	a, _ := typeText(t, "a", 1, "shared")
	bOps := metasFor("a", 1, 1)
	_ = bOps
	b := NewTextState()
	if _, err := b.Insert(TextRef{}, "other", "b", HLC{Timestamp: 100, NodeID: "b"}); err != nil {
		t.Fatal(err)
	}
	engine := NewMergeEngine()
	merged, err := engine.MergeField(
		a.ToFieldState(HLC{Timestamp: 6, NodeID: "a"}, "a"),
		b.ToFieldState(HLC{Timestamp: 100, NodeID: "b"}, "b"),
	)
	if err != nil {
		t.Fatal(err)
	}
	got := TextFromFieldState(merged).Value()
	// Both head spans survive; newer origin sorts first.
	if got != "othershared" {
		t.Fatalf("merged = %q", got)
	}
}

func TestValidation_TextType(t *testing.T) {
	if !ValidCRDTType("text") {
		t.Fatal("text must be a valid CRDT type")
	}
}

func TestText_SetString_Diff(t *testing.T) {
	st := NewTextState()
	ops, err := st.SetString("hello world", "a", HLC{Timestamp: 10, NodeID: "a"})
	if err != nil {
		t.Fatal(err)
	}
	if len(ops) != 1 || ops[0].Op != TextOpInsert {
		t.Fatalf("initial write ops: %+v", ops)
	}
	// Replace the middle word only: expect one delete + one insert.
	ops, err = st.SetString("hello brave world", "a", HLC{Timestamp: 11, NodeID: "a"})
	if err != nil {
		t.Fatal(err)
	}
	if st.Value() != "hello brave world" {
		t.Fatalf("value = %q", st.Value())
	}
	if len(ops) != 1 || ops[0].Op != TextOpInsert {
		t.Fatalf("insert-only diff expected: %+v", ops)
	}
	ops, err = st.SetString("hello world", "a", HLC{Timestamp: 12, NodeID: "a"})
	if err != nil {
		t.Fatal(err)
	}
	if st.Value() != "hello world" {
		t.Fatalf("value = %q", st.Value())
	}
	if len(ops) != 1 || ops[0].Op != TextOpDelete {
		t.Fatalf("delete-only diff expected: %+v", ops)
	}
}

// --- Golden parity fixtures (consumed by crdt-js vitest) ---

type goldenCase struct {
	Name          string       `json:"name"`
	Records       []goldenRec  `json:"records"`
	ExpectedValue string       `json:"expected_value"`
	ExpectedDelta []TextDelta  `json:"expected_delta"`
	FinalState    *ChangeState `json:"final_state,omitempty"`
}

// ChangeState wraps the state JSON so the fixture is self-describing.
type ChangeState struct {
	Text *TextState `json:"text"`
}

type goldenRec struct {
	NodeID string  `json:"node_id"`
	HLC    HLC     `json:"hlc"`
	Op     *TextOp `json:"op"`
}

// TestText_GoldenFixtures asserts cross-replica behavior AND writes the
// fixture file crdt-js's tests replay to prove byte-identical convergence.
func TestText_GoldenFixtures(t *testing.T) {
	build := func(name string, edit func(st *TextState, recs *[]goldenRec)) goldenCase {
		st := NewTextState()
		var recs []goldenRec
		edit(st, &recs)
		// Replay onto a fresh state must converge identically.
		replayed := NewTextState()
		for _, r := range recs {
			if err := replayed.Apply(r.Op, r.NodeID, r.HLC); err != nil {
				t.Fatalf("%s: replay: %v", name, err)
			}
		}
		if replayed.Value() != st.Value() {
			t.Fatalf("%s: replay %q != %q", name, replayed.Value(), st.Value())
		}
		return goldenCase{
			Name:          name,
			Records:       recs,
			ExpectedValue: st.Value(),
			ExpectedDelta: st.Delta(),
			FinalState:    &ChangeState{Text: st},
		}
	}

	record := func(recs *[]goldenRec, node string, ts int64, op *TextOp) {
		*recs = append(*recs, goldenRec{NodeID: node, HLC: HLC{Timestamp: ts, NodeID: node}, Op: op})
	}

	cases := []goldenCase{
		build("typing_coalesces", func(st *TextState, recs *[]goldenRec) {
			ts := int64(1)
			for _, ch := range []string{"h", "e", "l", "l", "o"} {
				ref := TextRef{}
				if l := st.Len(); l > 0 {
					ref, _ = st.RefAt(l - 1)
				}
				op, err := st.Insert(ref, ch, "a", HLC{Timestamp: ts, NodeID: "a"})
				if err != nil {
					t.Fatal(err)
				}
				record(recs, "a", ts, op)
				ts++
			}
		}),
		build("concurrent_insert_same_point", func(st *TextState, recs *[]goldenRec) {
			op, err := st.Insert(TextRef{}, "ab", "a", HLC{Timestamp: 1, NodeID: "a"})
			if err != nil {
				t.Fatal(err)
			}
			record(recs, "a", 1, op)
			ref, _ := st.RefAt(0)
			op1, err := st.Insert(ref, "X", "n1", HLC{Timestamp: 100, NodeID: "n1"})
			if err != nil {
				t.Fatal(err)
			}
			record(recs, "n1", 100, op1)
			op2 := &TextOp{Op: TextOpInsert, Ref: ref, Content: "Y", Origin: HLC{Timestamp: 101, NodeID: "n2"}}
			if err := st.Apply(op2, "n2", HLC{Timestamp: 101, NodeID: "n2"}); err != nil {
				t.Fatal(err)
			}
			record(recs, "n2", 101, op2)
		}),
		build("delete_and_format", func(st *TextState, recs *[]goldenRec) {
			op, err := st.Insert(TextRef{}, "hello world", "a", HLC{Timestamp: 1, NodeID: "a"})
			if err != nil {
				t.Fatal(err)
			}
			record(recs, "a", 1, op)
			ref, _ := st.RefAt(5)
			del, err := st.Delete(ref, 6)
			if err != nil {
				t.Fatal(err)
			}
			record(recs, "a", 2, del)
			fref, _ := st.RefAt(0)
			format, err := st.Format(fref, 5, map[string]json.RawMessage{"bold": json.RawMessage(`true`)}, "b", HLC{Timestamp: 50, NodeID: "b"})
			if err != nil {
				t.Fatal(err)
			}
			record(recs, "b", 50, format)
		}),
		build("unicode", func(st *TextState, recs *[]goldenRec) {
			op, err := st.Insert(TextRef{}, "héllo 🌍", "a", HLC{Timestamp: 1, NodeID: "a"})
			if err != nil {
				t.Fatal(err)
			}
			record(recs, "a", 1, op)
			ref, _ := st.RefAt(5)
			op2, err := st.Insert(ref, "brave ", "b", HLC{Timestamp: 100, NodeID: "b"})
			if err != nil {
				t.Fatal(err)
			}
			record(recs, "b", 100, op2)
		}),
	}

	if err := os.MkdirAll("testdata", 0o755); err != nil {
		t.Fatal(err)
	}
	raw, err := json.MarshalIndent(cases, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join("testdata", "text_golden.json"), append(raw, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}
