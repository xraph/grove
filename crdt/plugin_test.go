package crdt

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/grove/hook"
)

// fixedClock always returns the same HLC, so a test can predict stamps.
type fixedClock struct{ h HLC }

func (c *fixedClock) Now() HLC     { return c.h }
func (c *fixedClock) Update(_ HLC) {}

func newMemPlugin(t *testing.T) (*Plugin, *memShadow) {
	t.Helper()
	db := newMemShadow()
	p := New(WithNodeID("node-a"), WithClock(&fixedClock{h: HLC{Timestamp: 1000, Counter: 1, NodeID: "node-a"}}))
	p.SetExecutor(db)

	return p, db
}

func crdtQC(op hook.Operation, pk any, fields map[string]string) *hook.QueryContext {
	vals := map[string]any{"_crdt_fields": fields}
	if pk != nil {
		vals["_crdt_pk"] = pk
	}

	return &hook.QueryContext{Table: "notes", Operation: op, Values: vals}
}

// --- Plugin construction and accessors ---

func TestPlugin_OptionsAndAccessors(t *testing.T) {
	hk := &recordingSyncHook{}
	p := New(
		WithNodeID("n1"),
		WithTombstoneTTL(time.Hour),
		WithMaxClockDrift(2*time.Second),
		WithTables("notes", "tasks"),
		WithSyncHook(hk),
	)

	assert.Equal(t, "crdt", p.Name())
	assert.Equal(t, "n1", p.NodeID())
	assert.Equal(t, time.Hour, p.tombstoneTTL)
	assert.Equal(t, 2*time.Second, p.maxClockDrift)
	assert.Equal(t, []string{"notes", "tasks"}, p.tables)
	require.NotNil(t, p.Clock(), "a node ID alone gets a default hybrid clock")
	assert.Equal(t, "n1", p.Clock().Now().NodeID)
	assert.NotNil(t, p.MergeEngine())
	assert.Equal(t, 1, p.SyncHooks().Len())
	assert.Nil(t, p.MetadataStore(), "no store until SetExecutor")

	p.SetExecutor(newMemShadow())
	assert.NotNil(t, p.MetadataStore())
}

func TestPlugin_InitRequiresNodeID(t *testing.T) {
	err := New().Init(t.Context(), nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "node ID is required")
}

func TestPlugin_InitFillsMissingClock(t *testing.T) {
	p := New()
	p.nodeID = "late"
	require.Nil(t, p.Clock())

	require.NoError(t, p.Init(t.Context(), nil))
	require.NotNil(t, p.Clock())
	assert.Equal(t, "late", p.Clock().Now().NodeID)
}

func TestPlugin_MethodsNeedExecutor(t *testing.T) {
	p := New(WithNodeID("n1"))

	_, err := p.Inspect(t.Context(), "notes", "1")
	require.ErrorContains(t, err, "no executor set")
	require.ErrorContains(t, p.CleanupTombstones(t.Context(), "notes"), "no executor set")
	require.ErrorContains(t, p.EnsureShadowTable(t.Context(), "notes"), "no executor set")

	// The cleanup loop refuses to start without an executor or tables.
	p.RunTombstoneCleanup(t.Context(), time.Millisecond, "notes")
	p.SetExecutor(newMemShadow())
	p.RunTombstoneCleanup(t.Context(), time.Millisecond)
}

func TestPlugin_EnsureShadowTable(t *testing.T) {
	var queries []string
	p := New(WithNodeID("n1"))
	p.SetExecutor(&mockExecutor{execFn: func(_ context.Context, q string, _ ...any) (ExecResult, error) {
		queries = append(queries, q)

		return &mockExecResult{}, nil
	}})

	require.NoError(t, p.EnsureShadowTable(t.Context(), "notes"))
	require.Len(t, queries, 2)
	assert.Equal(t, ShadowTableDDL("notes"), queries[0])
	assert.Equal(t, ShadowTableSyncIndex("notes"), queries[1])
	assert.Contains(t, queries[1], "idx__notes_crdt_sync ON _notes_crdt (hlc_ts, hlc_counter)")
}

func TestPlugin_EnsureShadowTableErrors(t *testing.T) {
	boom := errors.New("permission denied")

	for _, failOn := range []string{"CREATE TABLE", "CREATE INDEX"} {
		p := New(WithNodeID("n1"))
		p.SetExecutor(&mockExecutor{execFn: func(_ context.Context, q string, _ ...any) (ExecResult, error) {
			if strings.HasPrefix(q, failOn) {
				return nil, boom
			}

			return &mockExecResult{}, nil
		}})

		err := p.EnsureShadowTable(t.Context(), "notes")
		require.ErrorIs(t, err, boom, failOn)
	}
}

func TestPlugin_CleanupTombstonesUsesTTL(t *testing.T) {
	var (
		gotQuery  string
		gotCutoff int64
	)
	p := New(WithNodeID("n1"), WithTombstoneTTL(time.Hour))
	p.SetExecutor(&mockExecutor{execFn: func(_ context.Context, q string, args ...any) (ExecResult, error) {
		gotQuery = q
		gotCutoff = args[0].(int64)

		return &mockExecResult{}, nil
	}})

	before := time.Now().Add(-time.Hour).UnixNano()
	require.NoError(t, p.CleanupTombstones(t.Context(), "notes"))
	after := time.Now().Add(-time.Hour).UnixNano()

	assert.Equal(t, "DELETE FROM _notes_crdt WHERE tombstone = TRUE AND hlc_ts < $1", gotQuery)
	assert.GreaterOrEqual(t, gotCutoff, before)
	assert.LessOrEqual(t, gotCutoff, after)
}

func TestPlugin_CleanupTombstonesWrapsError(t *testing.T) {
	boom := errors.New("locked")
	p := New(WithNodeID("n1"))
	p.SetExecutor(&mockExecutor{execFn: func(context.Context, string, ...any) (ExecResult, error) {
		return nil, boom
	}})

	err := p.CleanupTombstones(t.Context(), "notes")
	require.ErrorIs(t, err, boom)
	assert.Contains(t, err.Error(), "clean tombstones")
}

func TestPlugin_RunTombstoneCleanupCleansEachTable(t *testing.T) {
	cleaned := make(chan string, 16)
	p := New(WithNodeID("n1"))
	p.SetExecutor(&mockExecutor{execFn: func(_ context.Context, q string, _ ...any) (ExecResult, error) {
		cleaned <- q
		if strings.Contains(q, "_tasks_crdt") {
			// A failing table must not stop the loop.
			return nil, errors.New("transient")
		}

		return &mockExecResult{}, nil
	}})

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	p.RunTombstoneCleanup(ctx, time.Millisecond, "notes", "tasks")

	seen := map[string]bool{}
	deadline := time.After(5 * time.Second)
	for !seen["notes"] || !seen["tasks"] {
		select {
		case q := <-cleaned:
			switch {
			case strings.Contains(q, "_notes_crdt"):
				seen["notes"] = true
			case strings.Contains(q, "_tasks_crdt"):
				seen["tasks"] = true
			}
		case <-deadline:
			t.Fatalf("cleanup loop did not reach both tables: %v", seen)
		}
	}
}

// --- AfterMutation ---

func TestPlugin_AfterMutationSkipsWithoutMetadata(t *testing.T) {
	db := newMemShadow()

	// No executor yet.
	p := New(WithNodeID("n1"))
	require.NoError(t, p.AfterMutation(t.Context(), crdtQC(hook.OpInsert, "1", map[string]string{"title": "lww"}), nil, nil))

	p.SetExecutor(db)
	// No CRDT fields, an unknown CRDT type, nil Values, and no primary key
	// are all ignored without a write.
	require.NoError(t, p.AfterMutation(t.Context(), &hook.QueryContext{Table: "notes", Operation: hook.OpInsert}, nil, nil))
	require.NoError(t, p.AfterMutation(t.Context(), crdtQC(hook.OpInsert, "1", map[string]string{"title": "bogus"}), nil, nil))
	require.NoError(t, p.AfterMutation(t.Context(), crdtQC(hook.OpInsert, nil, map[string]string{"title": "lww"}), nil, nil))
	// A select is not a mutation the plugin records.
	require.NoError(t, p.AfterMutation(t.Context(), crdtQC(hook.OpSelect, "1", map[string]string{"title": "lww"}), nil, nil))

	assert.Zero(t, db.writeCount())
}

func TestPlugin_BeforeQueryAllows(t *testing.T) {
	res, err := New(WithNodeID("n1")).BeforeQuery(t.Context(), &hook.QueryContext{})
	require.NoError(t, err)
	assert.Equal(t, hook.Allow, res.Decision)
}

func TestPlugin_AfterMutationWritesLWW(t *testing.T) {
	p, db := newMemPlugin(t)

	qc := crdtQC(hook.OpInsert, 42, map[string]string{"title": "lww"})
	require.NoError(t, p.AfterMutation(t.Context(), qc, map[string]any{"title": "Hello"}, nil))

	state, err := p.Inspect(t.Context(), "notes", "42")
	require.NoError(t, err)
	fs := state.Fields["title"]
	require.NotNil(t, fs)
	assert.Equal(t, TypeLWW, fs.Type)
	assert.JSONEq(t, `"Hello"`, string(fs.Value))
	assert.Equal(t, int64(1000), fs.HLC.Timestamp)
	assert.Equal(t, "node-a", fs.NodeID)

	r := db.row("42", "title", "node-a")
	require.NotNil(t, r)
	assert.Equal(t, int64(1000), r.ts)
}

func TestPlugin_AfterMutationStructDataViaJSON(t *testing.T) {
	p, _ := newMemPlugin(t)
	type note struct {
		Title string `json:"title"`
	}

	qc := crdtQC(hook.OpUpdate, "s1", map[string]string{"title": "lww"})
	require.NoError(t, p.AfterMutation(t.Context(), qc, note{Title: "From struct"}, nil))

	state, err := p.Inspect(t.Context(), "notes", "s1")
	require.NoError(t, err)
	assert.JSONEq(t, `"From struct"`, string(state.Fields["title"].Value))
}

func TestPlugin_AfterMutationUnserialisableDataWritesNull(t *testing.T) {
	p, _ := newMemPlugin(t)

	// A channel cannot be marshalled, so no field values are extracted and
	// the LWW register records a null.
	qc := crdtQC(hook.OpUpdate, "c1", map[string]string{"title": "lww"})
	require.NoError(t, p.AfterMutation(t.Context(), qc, make(chan int), nil))

	state, err := p.Inspect(t.Context(), "notes", "c1")
	require.NoError(t, err)
	assert.JSONEq(t, `null`, string(state.Fields["title"].Value))
}

func TestPlugin_AfterMutationCounterTracksDelta(t *testing.T) {
	p, _ := newMemPlugin(t)
	fields := map[string]string{"views": "counter"}

	for _, v := range []any{5, int64(8), float64(3), json.Number("3")} {
		require.NoError(t, p.AfterMutation(t.Context(), crdtQC(hook.OpUpdate, "c", fields), map[string]any{"views": v}, nil))

		state, err := p.Inspect(t.Context(), "notes", "c")
		require.NoError(t, err)
		want := toInt64(v)
		assert.Equal(t, want, CounterFromFieldState(state.Fields["views"]).Value(), "after writing %v", v)
	}

	// A write without the field keeps the current total.
	require.NoError(t, p.AfterMutation(t.Context(), crdtQC(hook.OpUpdate, "c", fields), map[string]any{}, nil))
	state, err := p.Inspect(t.Context(), "notes", "c")
	require.NoError(t, err)
	assert.Equal(t, int64(3), CounterFromFieldState(state.Fields["views"]).Value())
}

func TestPlugin_AfterMutationSetAddsElements(t *testing.T) {
	p, _ := newMemPlugin(t)
	fields := map[string]string{"tags": "set"}

	require.NoError(t, p.AfterMutation(t.Context(), crdtQC(hook.OpInsert, "s", fields), map[string]any{"tags": []string{"a", "b"}}, nil))
	require.NoError(t, p.AfterMutation(t.Context(), crdtQC(hook.OpUpdate, "s", fields), map[string]any{"tags": []any{"c"}}, nil))

	state, err := p.Inspect(t.Context(), "notes", "s")
	require.NoError(t, err)
	set := SetFromFieldState(state.Fields["tags"])
	for _, want := range []string{"a", "b", "c"} {
		ok, err := set.Contains(want)
		require.NoError(t, err)
		assert.True(t, ok, "set should contain %q", want)
	}

	// An unsupported value type adds nothing.
	assert.Nil(t, toSlice(42))
}

func TestPlugin_AfterMutationTextReconcilesString(t *testing.T) {
	p, _ := newMemPlugin(t)
	fields := map[string]string{"body": "text"}

	require.NoError(t, p.AfterMutation(t.Context(), crdtQC(hook.OpInsert, "t", fields), map[string]any{"body": "hello"}, nil))
	require.NoError(t, p.AfterMutation(t.Context(), crdtQC(hook.OpUpdate, "t", fields), map[string]any{"body": "hello world"}, nil))

	state, err := p.Inspect(t.Context(), "notes", "t")
	require.NoError(t, err)
	txt := TextFromFieldState(state.Fields["body"])
	require.NotNil(t, txt)
	assert.Equal(t, "hello world", txt.Value())
}

func TestPlugin_AfterMutationTextRejectsNonString(t *testing.T) {
	p, db := newMemPlugin(t)

	err := p.AfterMutation(t.Context(), crdtQC(hook.OpUpdate, "t", map[string]string{"body": "text"}), map[string]any{"body": 12}, nil)

	require.ErrorContains(t, err, "text field body requires a string value, got int")
	assert.Zero(t, db.writeCount(), "a rejected text write must not wipe the stored text")
}

func TestPlugin_AfterMutationReadErrorsAreWrapped(t *testing.T) {
	boom := errors.New("read failed")

	for typ, want := range map[string]string{
		"counter": "read counter state",
		"set":     "read set state",
		"text":    "read text state",
	} {
		p := New(WithNodeID("n1"))
		p.SetExecutor(&mockExecutor{queryFn: func(context.Context, string, ...any) (Rows, error) {
			return nil, boom
		}})

		err := p.AfterMutation(t.Context(), crdtQC(hook.OpUpdate, "x", map[string]string{"f": typ}), map[string]any{"f": "v"}, nil)
		require.ErrorIs(t, err, boom, typ)
		assert.Contains(t, err.Error(), want)
	}
}

func TestPlugin_AfterMutationLWWMarshalError(t *testing.T) {
	p, _ := newMemPlugin(t)

	err := p.AfterMutation(t.Context(), crdtQC(hook.OpUpdate, "x", map[string]string{"f": "lww"}), map[string]any{"f": make(chan int)}, nil)
	require.ErrorContains(t, err, "crdt: marshal f")
}

func TestPlugin_AfterMutationDeleteWritesTombstone(t *testing.T) {
	p, db := newMemPlugin(t)
	fields := map[string]string{"title": "lww"}

	require.NoError(t, p.AfterMutation(t.Context(), crdtQC(hook.OpInsert, "d", fields), map[string]any{"title": "x"}, nil))
	require.NoError(t, p.AfterMutation(t.Context(), crdtQC(hook.OpDelete, "d", fields), nil, nil))

	state, err := p.Inspect(t.Context(), "notes", "d")
	require.NoError(t, err)
	assert.True(t, state.Tombstone)
	assert.Equal(t, int64(1000), state.TombstoneHLC.Timestamp)
	assert.Equal(t, "node-a", state.TombstoneHLC.NodeID)
	assert.NotNil(t, db.row("d", "_tombstone", "node-a"))
}

func TestToInt64(t *testing.T) {
	assert.Equal(t, int64(4), toInt64(int32(4)))
	assert.Equal(t, int64(0), toInt64(json.Number("not-a-number")))
	assert.Equal(t, int64(0), toInt64("7"))
}

// --- WriteFieldStatesAtomic ---

// txShadow is a memShadow that also supports transactions.
type txShadow struct {
	*memShadow
	beginErr  error
	commitErr error
	failWrite bool
	mu        sync.Mutex
	commits   int
	rollbacks int
}

type txShadowHandle struct{ s *txShadow }

func (h *txShadowHandle) ExecContext(ctx context.Context, q string, args ...any) (ExecResult, error) {
	if h.s.failWrite {
		return nil, errors.New("write in tx failed")
	}

	return h.s.ExecContext(ctx, q, args...)
}

func (h *txShadowHandle) QueryContext(ctx context.Context, q string, args ...any) (Rows, error) {
	return h.s.QueryContext(ctx, q, args...)
}

func (h *txShadowHandle) Commit() error {
	h.s.mu.Lock()
	defer h.s.mu.Unlock()
	h.s.commits++

	return h.s.commitErr
}

func (h *txShadowHandle) Rollback() error {
	h.s.mu.Lock()
	defer h.s.mu.Unlock()
	h.s.rollbacks++

	return nil
}

func (s *txShadow) BeginTx(context.Context) (TxHandle, error) {
	if s.beginErr != nil {
		return nil, s.beginErr
	}

	return &txShadowHandle{s: s}, nil
}

func twoFields() map[string]*FieldState {
	h := HLC{Timestamp: 10, Counter: 0, NodeID: "n"}

	return map[string]*FieldState{
		"a": {Type: TypeLWW, HLC: h, NodeID: "n", Value: json.RawMessage(`1`)},
		"b": {Type: TypeLWW, HLC: h, NodeID: "n", Value: json.RawMessage(`2`)},
	}
}

func TestWriteFieldStatesAtomic_CommitsInTx(t *testing.T) {
	db := &txShadow{memShadow: newMemShadow()}
	ms := NewMetadataStore(db)

	require.NoError(t, ms.WriteFieldStatesAtomic(t.Context(), "notes", "p", twoFields()))

	assert.Equal(t, 1, db.commits)
	assert.Zero(t, db.rollbacks)
	state, err := ms.ReadState(t.Context(), "notes", "p")
	require.NoError(t, err)
	assert.Len(t, state.Fields, 2)
}

func TestWriteFieldStatesAtomic_RollsBackOnWriteError(t *testing.T) {
	db := &txShadow{memShadow: newMemShadow(), failWrite: true}
	ms := NewMetadataStore(db)

	err := ms.WriteFieldStatesAtomic(t.Context(), "notes", "p", twoFields())

	require.ErrorContains(t, err, "write in tx failed")
	assert.Equal(t, 1, db.rollbacks)
	assert.Zero(t, db.commits)
}

func TestWriteFieldStatesAtomic_BeginAndCommitErrors(t *testing.T) {
	beginErr := errors.New("no conn")
	ms := NewMetadataStore(&txShadow{memShadow: newMemShadow(), beginErr: beginErr})
	err := ms.WriteFieldStatesAtomic(t.Context(), "notes", "p", twoFields())
	require.ErrorIs(t, err, beginErr)
	assert.Contains(t, err.Error(), "begin tx")

	commitErr := errors.New("serialization failure")
	ms = NewMetadataStore(&txShadow{memShadow: newMemShadow(), commitErr: commitErr})
	err = ms.WriteFieldStatesAtomic(t.Context(), "notes", "p", twoFields())
	require.ErrorIs(t, err, commitErr)
	assert.Contains(t, err.Error(), "commit field states")
}

func TestWriteFieldStatesAtomic_NoTxFallsBackToSingleWrites(t *testing.T) {
	db := newMemShadow()
	ms := NewMetadataStore(db)

	require.NoError(t, ms.WriteFieldStatesAtomic(t.Context(), "notes", "p", twoFields()))
	assert.Equal(t, 2, db.writeCount())

	// A single field skips the transaction even when one is available.
	tx := &txShadow{memShadow: newMemShadow()}
	single := map[string]*FieldState{"a": twoFields()["a"]}
	require.NoError(t, NewMetadataStore(tx).WriteFieldStatesAtomic(t.Context(), "notes", "p", single))
	assert.Zero(t, tx.commits)
	assert.Equal(t, 1, tx.writeCount())
}

func TestWriteFieldStatesAtomic_FallbackWriteErrors(t *testing.T) {
	boom := errors.New("disk full")
	failing := &mockExecutor{execFn: func(context.Context, string, ...any) (ExecResult, error) {
		return nil, boom
	}}
	ms := NewMetadataStore(failing)

	require.ErrorIs(t, ms.WriteFieldStatesAtomic(t.Context(), "notes", "p", twoFields()), boom)
	require.ErrorIs(t, ms.WriteFieldStatesAtomic(t.Context(), "notes", "p", map[string]*FieldState{"a": twoFields()["a"]}), boom)
	require.ErrorIs(t, ms.WriteTombstone(t.Context(), "notes", "p", HLC{Timestamp: 1}, "n"), boom)
}

// --- MetadataStore reads ---

func TestMetadataStore_ReadStateMergesNodesAndTombstones(t *testing.T) {
	db := newMemShadow()
	ms := NewMetadataStore(db)
	ctx := t.Context()

	older := &FieldState{Type: TypeLWW, HLC: HLC{Timestamp: 10, NodeID: "a"}, NodeID: "a", Value: json.RawMessage(`"old"`)}
	newer := &FieldState{Type: TypeLWW, HLC: HLC{Timestamp: 20, NodeID: "b"}, NodeID: "b", Value: json.RawMessage(`"new"`)}
	require.NoError(t, ms.WriteFieldState(ctx, "notes", "r", "title", older))
	require.NoError(t, ms.WriteFieldState(ctx, "notes", "r", "title", newer))
	require.NoError(t, ms.WriteTombstone(ctx, "notes", "r", HLC{Timestamp: 30, NodeID: "a"}, "a"))
	require.NoError(t, ms.WriteTombstone(ctx, "notes", "r", HLC{Timestamp: 25, NodeID: "b"}, "b"))

	state, err := ms.ReadState(ctx, "notes", "r")
	require.NoError(t, err)
	assert.JSONEq(t, `"new"`, string(state.Fields["title"].Value), "the later write wins across nodes")
	assert.True(t, state.Tombstone)
	assert.Equal(t, int64(30), state.TombstoneHLC.Timestamp, "the latest delete clock is kept")

	deleted, at, err := ms.readTombstone(ctx, "notes", "r")
	require.NoError(t, err)
	assert.True(t, deleted)
	assert.Equal(t, int64(30), at.Timestamp)

	maxC, err := ms.maxCursor(ctx, "notes")
	require.NoError(t, err)
	assert.Equal(t, int64(30), maxC.Timestamp)
}

func TestMetadataStore_ReadErrors(t *testing.T) {
	boom := errors.New("gone")
	ctx := t.Context()

	queryFails := NewMetadataStore(&mockExecutor{queryFn: func(context.Context, string, ...any) (Rows, error) {
		return nil, boom
	}})
	_, err := queryFails.ReadState(ctx, "notes", "r")
	require.ErrorIs(t, err, boom)
	_, _, err = queryFails.readTombstone(ctx, "notes", "r")
	require.ErrorIs(t, err, boom)
	_, err = queryFails.maxCursor(ctx, "notes")
	require.ErrorIs(t, err, boom)
	_, err = queryFails.ReadChangesSince(ctx, "notes", HLC{})
	require.ErrorIs(t, err, boom)

	scanErr := errors.New("bad column")
	scanFails := NewMetadataStore(&mockExecutor{queryFn: func(context.Context, string, ...any) (Rows, error) {
		return &mockRows{rows: []mockRow{{}}, scanErr: scanErr}, nil
	}})
	_, err = scanFails.ReadState(ctx, "notes", "r")
	require.ErrorIs(t, err, scanErr)
	_, _, err = scanFails.readTombstone(ctx, "notes", "r")
	require.ErrorIs(t, err, scanErr)
	_, err = scanFails.ReadChangesSince(ctx, "notes", HLC{})
	require.ErrorIs(t, err, scanErr)

	badState := NewMetadataStore(&mockExecutor{queryFn: func(context.Context, string, ...any) (Rows, error) {
		return &mockRows{rows: []mockRow{{pkHash: "r", fieldName: "title", nodeID: "a", crdtState: json.RawMessage(`{not json`)}}}, nil
	}})
	_, err = badState.ReadState(ctx, "notes", "r")
	require.ErrorContains(t, err, "unmarshal field state for title")
}

func TestMetadataStore_ReadChangesSinceCarriesState(t *testing.T) {
	db := newMemShadow()
	ms := NewMetadataStore(db)
	ctx := t.Context()

	counter := NewPNCounterState()
	counter.Increment("a", 5)
	counter.Decrement("a", 2)
	require.NoError(t, ms.WriteFieldState(ctx, "notes", "r", "views", counter.ToFieldState(HLC{Timestamp: 10, NodeID: "a"}, "a")))
	set := NewORSetState()
	require.NoError(t, set.Add("x", "a", HLC{Timestamp: 11, NodeID: "a"}))
	require.NoError(t, ms.WriteFieldState(ctx, "notes", "r", "tags", set.ToFieldState(HLC{Timestamp: 11, NodeID: "a"}, "a")))
	require.NoError(t, ms.WriteTombstone(ctx, "notes", "q", HLC{Timestamp: 12, NodeID: "a"}, "a"))

	changes, err := ms.ReadChangesSince(ctx, "notes", HLC{})
	require.NoError(t, err)
	require.Len(t, changes, 3)

	assert.Equal(t, TypeCounter, changes[0].CRDTType)
	require.NotNil(t, changes[0].CounterDelta)
	assert.Equal(t, CounterDelta{Increment: 5, Decrement: 2}, *changes[0].CounterDelta)
	require.NotNil(t, changes[0].State)

	assert.Equal(t, TypeSet, changes[1].CRDTType)
	require.NotNil(t, changes[1].State)

	assert.True(t, changes[2].Tombstone)
	assert.Equal(t, "q", changes[2].PK)
	assert.Equal(t, int64(12), changes[2].HLC.Timestamp)

	// A limit pages, and resuming after the last row returns the rest.
	page, err := ms.ReadChangesSince(ctx, "notes", HLC{}, 1)
	require.NoError(t, err)
	require.Len(t, page, 1)
	rest, err := ms.ReadChangesSince(ctx, "notes", HLC{Timestamp: 10})
	require.NoError(t, err)
	assert.Len(t, rest, 2)
}

func TestExtractCounterDeltaNil(t *testing.T) {
	assert.Nil(t, extractCounterDelta(nil, "a"))
}

func TestRowChange_UndecodableAndEmptyState(t *testing.T) {
	cr := rowChange("notes", &MetadataRow{PKHash: "p", FieldName: "f", HLCTS: 5, NodeID: "n"})
	assert.Equal(t, int64(5), cr.HLC.Timestamp)
	assert.Empty(t, cr.CRDTType)

	cr = rowChange("notes", &MetadataRow{PKHash: "p", FieldName: "f", HLCTS: 6, NodeID: "n", CRDTState: json.RawMessage(`{bad`)})
	assert.Equal(t, int64(6), cr.HLC.Timestamp, "an undecodable state falls back to the cursor clock")
	assert.Nil(t, cr.Value)
}

// --- Small helpers and DDL ---

func TestShadowDDL(t *testing.T) {
	assert.Equal(t, "_notes_crdt", ShadowTableName("notes"))
	assert.Contains(t, ShadowTableDDL("notes"), "CREATE TABLE IF NOT EXISTS _notes_crdt")
	assert.Contains(t, ShadowTableDDL("notes"), "PRIMARY KEY (pk_hash, field_name, node_id)")
	assert.Equal(t, "DROP TABLE IF EXISTS _notes_crdt", DropShadowTableDDL("notes"))
}

func TestLWWRegister_RoundTrip(t *testing.T) {
	clock := HLC{Timestamp: 9, Counter: 2, NodeID: "n"}
	reg, err := NewLWWRegister(map[string]int{"x": 1}, clock, "n")
	require.NoError(t, err)

	var got map[string]int
	require.NoError(t, reg.Decode(&got))
	assert.Equal(t, map[string]int{"x": 1}, got)

	back := LWWFromFieldState(reg.ToFieldState())
	require.NotNil(t, back)
	assert.Equal(t, clock, back.Clock)
	assert.Equal(t, "n", back.NodeID)
	assert.Nil(t, LWWFromFieldState(&FieldState{Type: TypeCounter}))

	_, err = NewLWWRegister(make(chan int), clock, "n")
	require.Error(t, err)

	var wrong int
	require.Error(t, reg.Decode(&wrong), "decoding an object into an int fails")
}

func TestSyncReport_String(t *testing.T) {
	r := &SyncReport{Pulled: 3, Pushed: 2, Merged: 1, Conflicts: 0}
	assert.Equal(t, "pulled=3 pushed=2 merged=1 conflicts=0", r.String())
}
