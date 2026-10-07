package crdt

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- In-memory shadow tables ---

// memShadow is an Executor that keeps shadow rows in memory and answers the
// queries MetadataStore issues the way SQL would: the upserts keyed by
// (pk_hash, field_name, node_id), the per-record read (with a cursor cut
// when the query has one), the changes-since cursor with its ORDER BY and
// LIMIT, and the max-cursor lookup.
type memShadow struct {
	mu     sync.Mutex
	tables map[string]map[string]*memRow
	seq    int
	writes int
}

type memRow struct {
	pk, field, node string
	ts              int64
	counter         uint32
	tombstone       bool
	state           json.RawMessage
	seq             int
}

func newMemShadow() *memShadow {
	return &memShadow{tables: make(map[string]map[string]*memRow)}
}

var memTableRe = regexp.MustCompile(`(?:FROM|INTO) _([A-Za-z0-9]+)_crdt`)

func (m *memShadow) table(query string) map[string]*memRow {
	match := memTableRe.FindStringSubmatch(query)
	if match == nil {
		panic("memShadow: no table in query: " + query)
	}
	t, ok := m.tables[match[1]]
	if !ok {
		t = make(map[string]*memRow)
		m.tables[match[1]] = t
	}
	return t
}

func (m *memShadow) upsert(t map[string]*memRow, r *memRow) {
	key := r.pk + "\x00" + r.field + "\x00" + r.node
	m.seq++
	r.seq = m.seq
	if old, ok := t[key]; ok {
		r.seq = old.seq
	}
	t[key] = r
	m.writes++
}

func (m *memShadow) ExecContext(_ context.Context, query string, args ...any) (ExecResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !strings.Contains(query, "INSERT INTO") {
		return &mockExecResult{}, nil
	}
	t := m.table(query)
	if strings.Contains(query, "'_tombstone'") {
		r := &memRow{
			pk: args[0].(string), field: "_tombstone",
			ts: args[1].(int64), counter: args[2].(uint32), node: args[3].(string),
			tombstone: true,
		}
		if len(args) > 4 {
			r.state = append(json.RawMessage(nil), args[4].([]byte)...)
		}
		m.upsert(t, r)
	} else {
		m.upsert(t, &memRow{
			pk: args[0].(string), field: args[1].(string),
			ts: args[2].(int64), counter: args[3].(uint32), node: args[4].(string),
			tombstone: args[5].(bool), state: append(json.RawMessage(nil), args[6].([]byte)...),
		})
	}
	return &mockExecResult{affected: 1}, nil
}

func (m *memShadow) sorted(t map[string]*memRow) []*memRow {
	rows := make([]*memRow, 0, len(t))
	for _, r := range t {
		rows = append(rows, r)
	}
	sort.Slice(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]
		if a.ts != b.ts {
			return a.ts < b.ts
		}
		if a.counter != b.counter {
			return a.counter < b.counter
		}
		return a.seq < b.seq
	})
	return rows
}

func (m *memShadow) QueryContext(_ context.Context, query string, args ...any) (Rows, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t := m.table(query)
	switch {
	case strings.Contains(query, "hlc_ts > $1"):
		rows := m.sorted(t)
		ts, counter, limit := args[0].(int64), args[1].(uint32), args[2].(int)
		var out [][]any
		for _, r := range rows {
			if r.ts > ts || (r.ts == ts && r.counter > counter) {
				out = append(out, r.full())
			}
			if len(out) == limit {
				break
			}
		}
		return &memRows{vals: out}, nil
	case strings.Contains(query, "ORDER BY hlc_ts DESC, hlc_counter DESC LIMIT 1"):
		var last *memRow
		for _, r := range t {
			if last == nil || r.ts > last.ts || (r.ts == last.ts && r.counter > last.counter) {
				last = r
			}
		}
		if last == nil {
			return &memRows{}, nil
		}
		return &memRows{vals: [][]any{{last.ts, last.counter}}}, nil
	case strings.Contains(query, "WHERE pk_hash = $1"):
		// A per-record read, cut at a cursor position when the query
		// has one.
		cut := strings.Contains(query, "hlc_ts < $2")
		var out [][]any
		for _, r := range t {
			if r.pk != args[0].(string) {
				continue
			}
			if cut {
				ts, counter := args[1].(int64), args[2].(uint32)
				if r.ts > ts || (r.ts == ts && r.counter > counter) {
					continue
				}
			}
			out = append(out, r.full())
		}
		return &memRows{vals: out}, nil
	}
	return nil, fmt.Errorf("memShadow: unhandled query: %s", query)
}

func (r *memRow) full() []any {
	return []any{r.pk, r.field, r.ts, r.counter, r.node, r.tombstone, r.state}
}

// row returns the stored notes row for pk/field/node, or nil.
func (m *memShadow) row(pk, field, node string) *memRow {
	m.mu.Lock()
	defer m.mu.Unlock()
	r := m.tables["notes"][pk+"\x00"+field+"\x00"+node]
	if r == nil {
		return nil
	}
	cp := *r
	return &cp
}

// allCursors returns every stored cursor position across all tables.
func (m *memShadow) allCursors() []HLC {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []HLC
	for _, t := range m.tables {
		for _, r := range t {
			out = append(out, HLC{Timestamp: r.ts, Counter: r.counter})
		}
	}
	return out
}

func (m *memShadow) writeCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.writes
}

type memRows struct {
	vals [][]any
	i    int
}

func (r *memRows) Next() bool {
	if r.i >= len(r.vals) {
		return false
	}
	r.i++
	return true
}

func (r *memRows) Scan(dest ...any) error {
	row := r.vals[r.i-1]
	for i, d := range dest {
		switch p := d.(type) {
		case *string:
			*p = row[i].(string)
		case *int64:
			*p = row[i].(int64)
		case *uint32:
			*p = row[i].(uint32)
		case *bool:
			*p = row[i].(bool)
		case *json.RawMessage:
			if row[i] == nil {
				*p = nil
			} else {
				*p = row[i].(json.RawMessage)
			}
		default:
			return fmt.Errorf("memRows: unsupported scan dest %T", d)
		}
	}
	return nil
}

func (r *memRows) Close() error { return nil }
func (r *memRows) Err() error   { return nil }

// --- Test server and replicas ---

var lateBase = time.Unix(1_700_000_000, 0).UnixNano()

// lateServer is a sync controller over memShadow with a settable wall clock.
type lateServer struct {
	db   *memShadow
	now  *atomic.Int64
	ctrl *SyncController
}

func newLateServer(t *testing.T, db *memShadow) *lateServer {
	t.Helper()
	now := &atomic.Int64{}
	now.Store(lateBase)
	clock := NewHybridClock("server", WithNowFunc(func() time.Time { return time.Unix(0, now.Load()) }))
	p := New(WithNodeID("server"), WithClock(clock))
	p.SetExecutor(db)
	return &lateServer{db: db, now: now, ctrl: NewSyncController(p)}
}

func (s *lateServer) push(t *testing.T, changes ...ChangeRecord) {
	t.Helper()
	_, err := s.ctrl.HandlePush(context.Background(), &PushRequest{Changes: changes, NodeID: changes[0].NodeID})
	require.NoError(t, err)
}

// replica is a client that folds pulled changes through ApplyChange, as
// the Go, crdt-js and Dart clients do, with sticky max-clock tombstones.
type replica struct {
	node   string
	fields map[string]*FieldState
	tombs  map[string]HLC
	cursor HLC
	// dartCursor resumes from the highest change HLC of a page (falling
	// back to LatestHLC when that makes no progress), pulling from just
	// below it, as grove_crdt's SyncEngine does. Otherwise the replica
	// resumes from LatestHLC, as crdt-js and the Go Syncer do.
	dartCursor bool
	received   []ChangeRecord
}

func newReplica(node string) *replica {
	return &replica{node: node, fields: map[string]*FieldState{}, tombs: map[string]HLC{}}
}

func fieldKey(table, pk, field string) string { return table + "/" + pk + "/" + field }

func (r *replica) apply(t *testing.T, ch ChangeRecord) {
	t.Helper()
	isDocPathDelete := ch.CRDTType == TypeDocument && len(ch.Value) > 0
	if ch.Tombstone && !isDocPathDelete {
		key := ch.Table + "/" + ch.PK
		if at, ok := r.tombs[key]; !ok || ch.HLC.After(at) {
			r.tombs[key] = ch.HLC
		}
		return
	}
	key := fieldKey(ch.Table, ch.PK, ch.Field)
	merged, err := ApplyChange(nil, r.fields[key], &ch)
	require.NoError(t, err)
	r.fields[key] = merged
}

// local applies a change the replica made itself and returns it for a push.
func (r *replica) local(t *testing.T, ch ChangeRecord) ChangeRecord {
	t.Helper()
	ch.NodeID = r.node
	ch.HLC.NodeID = r.node
	r.apply(t, ch)
	return ch
}

func below(h HLC) HLC {
	if h.Counter > 0 {
		return HLC{Timestamp: h.Timestamp, Counter: h.Counter - 1}
	}
	return HLC{Timestamp: h.Timestamp - 1, Counter: ^uint32(0)}
}

// pull drains the server: it pulls until a pull makes no progress and
// returns the changes received.
func (r *replica) pull(t *testing.T, s *lateServer, tables ...string) []ChangeRecord {
	t.Helper()
	var got []ChangeRecord
	for range 100 {
		since := r.cursor
		if r.dartCursor && !r.cursor.IsZero() {
			since = below(r.cursor)
		}
		resp, err := s.ctrl.HandlePull(context.Background(), &PullRequest{Tables: tables, Since: since, NodeID: r.node})
		require.NoError(t, err)
		for _, ch := range resp.Changes {
			r.apply(t, ch)
		}
		got = append(got, resp.Changes...)

		var next *HLC
		if r.dartCursor && len(resp.Changes) > 0 {
			pageMax := resp.Changes[0].HLC
			for _, ch := range resp.Changes {
				if cursorAfter(ch.HLC, pageMax) {
					pageMax = ch.HLC
				}
			}
			if cursorAfter(pageMax, r.cursor) {
				next = &pageMax
			}
		}
		if next == nil && !resp.LatestHLC.IsZero() && cursorAfter(resp.LatestHLC, r.cursor) {
			next = &resp.LatestHLC
		}
		if next == nil {
			r.received = append(r.received, got...)
			return got
		}
		r.cursor = *next
	}
	t.Fatal("pull never stopped making progress")
	return nil
}

// value resolves a field to a comparable Go value.
func (r *replica) value(table, pk, field string) any {
	return resolveForTest(r.fields[fieldKey(table, pk, field)])
}

func resolveForTest(fs *FieldState) any {
	if fs == nil {
		return nil
	}
	switch fs.Type {
	case TypeText:
		if txt := TextFromFieldState(fs); txt != nil {
			return txt.Value()
		}
		return ""
	case TypeDocument:
		if doc := DocumentFromFieldState(fs); doc != nil {
			return doc.Resolve()
		}
		return nil
	case TypeSet, TypeList:
		var out []string
		for _, el := range resolveFieldValue(fs).([]json.RawMessage) {
			out = append(out, string(el))
		}
		if fs.Type == TypeSet {
			sort.Strings(out)
		}
		return out
	default:
		return resolveFieldValue(fs)
	}
}

func serverValue(t *testing.T, s *lateServer, table, pk, field string) any {
	t.Helper()
	state, err := s.ctrl.metadata.ReadState(context.Background(), table, pk)
	require.NoError(t, err)
	return resolveForTest(state.Fields[field])
}

func at(ts int64, node string) HLC { return HLC{Timestamp: ts, NodeID: node} }

// movePastT has device B write an unrelated row stamped T and pull, so its
// cursor sits at or past T.
func movePastT(t *testing.T, s *lateServer, b *replica, ts int64) {
	t.Helper()
	s.push(t, b.local(t, ChangeRecord{
		Table: "notes", PK: "other", Field: "title", CRDTType: TypeLWW,
		HLC: at(ts, b.node), Value: json.RawMessage(`"b"`),
	}))
	b.pull(t, s, "notes")
	require.False(t, cursorAfter(HLC{Timestamp: ts}, b.cursor), "B's cursor must be at or past T")
}

// --- Late-stamp scenarios ---

func TestLateStamp_ReachesPeerPastTheStamp(t *testing.T) {
	T := lateBase
	late := T - int64(5*time.Second)

	textOrigin := at(late, "dev-a")
	cases := []struct {
		name   string
		field  string
		change ChangeRecord
		want   any
	}{
		{
			name: "counter increment", field: "views",
			change: ChangeRecord{CRDTType: TypeCounter, CounterDelta: &CounterDelta{Increment: 3}},
			want:   int64(3),
		},
		{
			name: "set add", field: "tags",
			change: ChangeRecord{CRDTType: TypeSet, SetOp: &SetOperation{Op: SetOpAdd, Elements: json.RawMessage(`["x"]`)}},
			want:   []string{`"x"`},
		},
		{
			name: "list insert", field: "items",
			change: ChangeRecord{CRDTType: TypeList, ListOp: &ListOp{Op: ListOpInsert, Value: json.RawMessage(`"i1"`)}},
			want:   []string{`"i1"`},
		},
		{
			name: "text insert", field: "body",
			change: ChangeRecord{CRDTType: TypeText, TextOp: &TextOp{Op: TextOpInsert, Origin: textOrigin, Content: "hello"}},
			want:   "hello",
		},
		{
			name: "document field", field: "meta",
			change: ChangeRecord{CRDTType: TypeDocument, Value: json.RawMessage(`{"path":"a.b","value":1}`)},
			want:   map[string]any{"a": map[string]any{"b": float64(1)}},
		},
	}

	for _, tc := range cases {
		for _, dart := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/dartCursor=%v", tc.name, dart), func(t *testing.T) {
				s := newLateServer(t, newMemShadow())
				a, b := newReplica("dev-a"), newReplica("dev-b")
				b.dartCursor = dart

				movePastT(t, s, b, T)

				ch := tc.change
				ch.Table, ch.PK, ch.Field, ch.HLC = "notes", "n1", tc.field, at(late, "")
				s.push(t, a.local(t, ch))

				got := b.pull(t, s, "notes")
				require.NotEmpty(t, got, "B's next pull must return the late change")
				assert.Equal(t, tc.want, b.value("notes", "n1", tc.field))
				assert.Equal(t, tc.want, serverValue(t, s, "notes", "n1", tc.field))
				for _, c := range got {
					if c.PK == "n1" {
						assert.Equal(t, late, c.HLC.Timestamp, "a restamped row keeps its own clock on the wire")
					}
				}
			})
		}
	}
}

// The Dart convergence case: both devices increment one counter, B (later
// clock) syncs first, then A pushes its earlier stamp into B's merged row.
func TestLateStamp_CounterMergedIntoAnotherNodesRow(t *testing.T) {
	for _, dart := range []bool{false, true} {
		t.Run(fmt.Sprintf("dartCursor=%v", dart), func(t *testing.T) {
			s := newLateServer(t, newMemShadow())
			a, b := newReplica("dev-a"), newReplica("dev-b")
			a.dartCursor, b.dartCursor = dart, dart
			T := lateBase

			aInc := a.local(t, ChangeRecord{Table: "notes", PK: "n1", Field: "views", CRDTType: TypeCounter, HLC: at(T, ""), CounterDelta: &CounterDelta{Increment: 3}})
			bInc := b.local(t, ChangeRecord{Table: "notes", PK: "n1", Field: "views", CRDTType: TypeCounter, HLC: at(T+int64(time.Second), ""), CounterDelta: &CounterDelta{Increment: 7}})

			b.pull(t, s, "notes")
			s.push(t, bInc)
			b.pull(t, s, "notes") // B's cursor is now past A's stamp.
			a.pull(t, s, "notes")
			s.push(t, aInc)
			b.pull(t, s, "notes")
			a.pull(t, s, "notes")

			assert.Equal(t, int64(10), a.value("notes", "n1", "views"))
			assert.Equal(t, int64(10), b.value("notes", "n1", "views"))
			assert.Equal(t, int64(10), serverValue(t, s, "notes", "n1", "views"))
		})
	}
}

func TestLateStamp_LWWLoserDoesNotRestamp(t *testing.T) {
	s := newLateServer(t, newMemShadow())
	a, b := newReplica("dev-a"), newReplica("dev-b")
	T := lateBase

	s.push(t, b.local(t, ChangeRecord{Table: "notes", PK: "n1", Field: "title", CRDTType: TypeLWW, HLC: at(T, ""), Value: json.RawMessage(`"b new"`)}))
	b.pull(t, s, "notes")
	before := s.db.row("n1", "title", "dev-b")
	require.NotNil(t, before)
	writes := s.db.writeCount()

	resp, err := s.ctrl.HandlePush(context.Background(), &PushRequest{
		Changes: []ChangeRecord{a.local(t, ChangeRecord{Table: "notes", PK: "n1", Field: "title", CRDTType: TypeLWW, HLC: at(T-int64(5*time.Second), ""), Value: json.RawMessage(`"a old"`)})},
		NodeID:  "dev-a",
	})
	require.NoError(t, err)
	assert.Equal(t, 1, resp.Merged, "a losing change is still accepted")

	assert.Equal(t, writes, s.db.writeCount(), "a losing LWW value writes nothing")
	assert.Equal(t, before, s.db.row("n1", "title", "dev-b"), "the winning row keeps its cursor position")
	assert.Nil(t, s.db.row("n1", "title", "dev-a"))

	assert.Empty(t, b.pull(t, s, "notes"), "nothing changed, so B pulls nothing")
	assert.Equal(t, "b new", b.value("notes", "n1", "title"))
	a.pull(t, s, "notes")
	assert.Equal(t, "b new", a.value("notes", "n1", "title"))
	assert.Equal(t, "b new", serverValue(t, s, "notes", "n1", "title"))
}

func TestLateStamp_LWWWinnerIsRedeliveredAndWinsEverywhere(t *testing.T) {
	T := lateBase
	sec := int64(time.Second)

	t.Run("late winner reaches a peer past its stamp", func(t *testing.T) {
		s := newLateServer(t, newMemShadow())
		a, b, c := newReplica("dev-a"), newReplica("dev-b"), newReplica("dev-c")

		s.push(t, c.local(t, ChangeRecord{Table: "notes", PK: "n1", Field: "title", CRDTType: TypeLWW, HLC: at(T-10*sec, ""), Value: json.RawMessage(`"orig"`)}))
		movePastT(t, s, b, T)
		require.Equal(t, "orig", b.value("notes", "n1", "title"))

		s.push(t, a.local(t, ChangeRecord{Table: "notes", PK: "n1", Field: "title", CRDTType: TypeLWW, HLC: at(T-5*sec, ""), Value: json.RawMessage(`"a wins"`)}))
		got := b.pull(t, s, "notes")
		require.Len(t, got, 1)
		assert.Equal(t, at(T-5*sec, "dev-a"), got[0].HLC, "the winner keeps its own clock and node")
		assert.Equal(t, "dev-a", got[0].NodeID)

		c.pull(t, s, "notes")
		a.pull(t, s, "notes")
		for _, r := range []*replica{a, b, c} {
			assert.Equal(t, "a wins", r.value("notes", "n1", "title"), r.node)
		}
		assert.Equal(t, "a wins", serverValue(t, s, "notes", "n1", "title"))
	})

	t.Run("a newer local write still beats the redelivered winner", func(t *testing.T) {
		s := newLateServer(t, newMemShadow())
		a, b := newReplica("dev-a"), newReplica("dev-b")

		movePastT(t, s, b, T)
		// B edits offline, later than A's late edit but not yet pushed.
		bLocal := b.local(t, ChangeRecord{Table: "notes", PK: "n1", Field: "title", CRDTType: TypeLWW, HLC: at(T-2*sec, ""), Value: json.RawMessage(`"b later"`)})

		s.push(t, a.local(t, ChangeRecord{Table: "notes", PK: "n1", Field: "title", CRDTType: TypeLWW, HLC: at(T-5*sec, ""), Value: json.RawMessage(`"a earlier"`)}))
		b.pull(t, s, "notes")
		assert.Equal(t, "b later", b.value("notes", "n1", "title"), "the redelivered value is no newer than it was")

		s.push(t, bLocal)
		a.pull(t, s, "notes")
		b.pull(t, s, "notes")
		assert.Equal(t, "b later", a.value("notes", "n1", "title"))
		assert.Equal(t, "b later", b.value("notes", "n1", "title"))
		assert.Equal(t, "b later", serverValue(t, s, "notes", "n1", "title"))
	})
}

func TestLateStamp_Tombstones(t *testing.T) {
	T := lateBase
	sec := int64(time.Second)
	tomb := func(ts int64, node string) ChangeRecord {
		return ChangeRecord{Table: "notes", PK: "n1", Field: "_tombstone", Tombstone: true, HLC: at(ts, node), NodeID: node}
	}

	s := newLateServer(t, newMemShadow())
	b := newReplica("dev-b")
	movePastT(t, s, b, T)

	// A late delete reaches B even though B pulled past its clock.
	s.push(t, tomb(T-5*sec, "dev-a"))
	got := b.pull(t, s, "notes")
	require.Len(t, got, 1)
	assert.True(t, got[0].Tombstone)
	assert.Equal(t, at(T-5*sec, "dev-a"), got[0].HLC, "the tombstone keeps its delete clock")

	// An older delete changes nothing: no write, nothing redelivered.
	first := s.db.row("n1", "_tombstone", "dev-a")
	require.NotNil(t, first)
	writes := s.db.writeCount()
	s.push(t, tomb(T-8*sec, "dev-c"))
	s.push(t, tomb(T-5*sec, "dev-a")) // a retry of the same delete
	assert.Equal(t, writes, s.db.writeCount())
	assert.Equal(t, first, s.db.row("n1", "_tombstone", "dev-a"), "the delete row keeps its position")
	assert.Empty(t, b.pull(t, s, "notes"))

	// A newer delete moves the clock and is redelivered.
	s.push(t, tomb(T-3*sec, "dev-c"))
	got = b.pull(t, s, "notes")
	require.Len(t, got, 1)
	assert.Equal(t, at(T-3*sec, "dev-c"), b.tombs["notes/n1"])

	state, err := s.ctrl.metadata.ReadState(context.Background(), "notes", "n1")
	require.NoError(t, err)
	assert.True(t, state.Tombstone)
	assert.Equal(t, at(T-3*sec, "dev-c"), state.TombstoneHLC, "the latest delete clock wins across nodes")
}

func TestLateStamp_CursorStaysMonotonic(t *testing.T) {
	db := newMemShadow()
	s := newLateServer(t, db)
	sec := int64(time.Second)
	b := newReplica("dev-b")

	var issued []HLC
	check := func(label string) {
		t.Helper()
		resp, err := s.ctrl.HandlePull(context.Background(), &PullRequest{Tables: []string{"notes", "tasks"}, NodeID: "probe"})
		require.NoError(t, err)
		seen := map[HLC]bool{}
		for _, c := range db.allCursors() {
			assert.False(t, seen[c], "%s: two rows share cursor position %v", label, c)
			seen[c] = true
		}
		issued = append(issued, resp.LatestHLC)
		for i := 1; i < len(issued); i++ {
			assert.False(t, cursorAfter(issued[i-1], issued[i]), "%s: the cursor went backwards", label)
		}
	}

	// A frozen and then backwards-moving wall clock, late and future stamps.
	stamps := []int64{0, -5 * sec, -5 * sec, 3 * sec, -60 * sec, 4 * sec, -1}
	for i, off := range stamps {
		if i == 4 {
			s.now.Add(-30 * sec) // the server's wall clock steps back
		}
		table := []string{"notes", "tasks"}[i%2]
		before := maxOf(db.allCursors())
		s.push(t, ChangeRecord{
			Table: table, PK: fmt.Sprintf("p%d", i%3), Field: "views", CRDTType: TypeCounter, NodeID: "dev-a",
			HLC: at(lateBase+off, "dev-a"), CounterDelta: &CounterDelta{Increment: int64(i + 1)},
		})
		after := maxOf(db.allCursors())
		assert.True(t, cursorAfter(after, before), "push %d must land past every stored position", i)
		check(fmt.Sprintf("push %d", i))
	}

	// A change stamped beyond the clock's drift bound: the clock clamps it,
	// but its row still lands past its own clock and every later row past it.
	far := lateBase + int64(time.Hour)
	s.push(t, ChangeRecord{Table: "notes", PK: "far", Field: "title", CRDTType: TypeLWW, NodeID: "dev-z", HLC: at(far, "dev-z"), Value: json.RawMessage(`"future"`)})
	r := db.row("far", "title", "dev-z")
	require.NotNil(t, r)
	assert.True(t, cursorAfter(HLC{Timestamp: r.ts, Counter: r.counter}, at(far, "")), "a row sorts past its own clock")
	b.pull(t, s, "notes", "tasks")
	s.push(t, ChangeRecord{Table: "tasks", PK: "after", Field: "views", CRDTType: TypeCounter, NodeID: "dev-a", HLC: at(lateBase, "dev-a"), CounterDelta: &CounterDelta{Increment: 1}})
	got := b.pull(t, s, "notes", "tasks")
	require.Len(t, got, 1, "a late write after a far-future one still reaches a peer past both")
	check("far future")

	// A restarted server (fresh clock and allocator, wall clock behind the
	// stored rows) still lands past every stored position of the table.
	restarted := newLateServer(t, db)
	restarted.now.Store(lateBase - int64(time.Minute))
	before := maxOf(db.allCursors())
	restarted.push(t, ChangeRecord{Table: "tasks", PK: "after", Field: "views", CRDTType: TypeCounter, NodeID: "dev-a", HLC: at(lateBase-int64(time.Minute), "dev-a"), CounterDelta: &CounterDelta{Increment: 6}})
	assert.True(t, cursorAfter(maxOf(db.allCursors()), before), "a restarted server must not reuse an old position")
	got = b.pull(t, restarted, "notes", "tasks")
	require.Len(t, got, 1)
	assert.Equal(t, int64(6), b.value("tasks", "after", "views"))
	assert.Equal(t, int64(6), serverValue(t, restarted, "tasks", "after", "views"))
}

func maxOf(hs []HLC) HLC {
	var m HLC
	for _, h := range hs {
		if cursorAfter(h, m) {
			m = h
		}
	}
	return m
}

// A multi-table pull cuts its window at the first truncated page (#35). A
// restamped row sorts past that cut, so it must arrive in a later window
// and the cursor must never pass it first.
func TestLateStamp_MultiTableWindowNeverSkipsRestampedRow(t *testing.T) {
	for _, dart := range []bool{false, true} {
		t.Run(fmt.Sprintf("dartCursor=%v", dart), func(t *testing.T) {
			db := newMemShadow()
			s := newLateServer(t, db)
			sec := int64(time.Second)

			// A backlog in "big" larger than one page, written before the
			// late change, plus one row in "small".
			backlog := make([]ChangeRecord, 0, DefaultChangesLimit+500)
			for i := range DefaultChangesLimit + 500 {
				backlog = append(backlog, ChangeRecord{
					Table: "big", PK: fmt.Sprintf("pk-%d", i), Field: "title", CRDTType: TypeLWW, NodeID: "dev-c",
					HLC: at(lateBase-100*sec+int64(i), "dev-c"), Value: json.RawMessage(`"v"`),
				})
			}
			s.push(t, backlog...)
			s.push(t, ChangeRecord{Table: "small", PK: "s1", Field: "title", CRDTType: TypeLWW, NodeID: "dev-c", HLC: at(lateBase, "dev-c"), Value: json.RawMessage(`"s"`)})

			b := newReplica("dev-b")
			b.dartCursor = dart
			resp, err := s.ctrl.HandlePull(context.Background(), &PullRequest{Tables: []string{"big", "small"}, NodeID: "dev-b"})
			require.NoError(t, err)
			require.Len(t, resp.Changes, DefaultChangesLimit, "the first window stops at big's page end")
			for _, ch := range resp.Changes {
				b.apply(t, ch)
			}
			b.cursor = resp.LatestHLC

			// A late counter increment into "small", stamped before the
			// whole backlog, while B is midway through it.
			s.push(t, ChangeRecord{Table: "small", PK: "s1", Field: "views", CRDTType: TypeCounter, NodeID: "dev-a", HLC: at(lateBase-200*sec, "dev-a"), CounterDelta: &CounterDelta{Increment: 4}})

			b.pull(t, s, "big", "small")
			seen := map[string]bool{}
			for _, ch := range append(resp.Changes, b.received...) {
				seen[ch.Table+"/"+ch.PK+"/"+ch.Field] = true
			}
			assert.Len(t, seen, DefaultChangesLimit+500+2, "every backlog row, the small row and the late change arrive")
			assert.Equal(t, int64(4), b.value("small", "s1", "views"))
		})
	}
}

func TestLateStamp_StreamDeliversRestampedRow(t *testing.T) {
	db := newMemShadow()
	now := &atomic.Int64{}
	now.Store(lateBase)
	clock := NewHybridClock("server", WithNowFunc(func() time.Time { return time.Unix(0, now.Load()) }))
	p := New(WithNodeID("server"), WithClock(clock))
	p.SetExecutor(db)
	s := &lateServer{db: db, now: now, ctrl: NewSyncController(p, WithStreamPollInterval(5*time.Millisecond))}

	b := newReplica("dev-b")
	movePastT(t, s, b, lateBase)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := s.ctrl.StreamChangesSince(ctx, []string{"notes"}, b.cursor)
	require.NoError(t, err)

	s.push(t, ChangeRecord{Table: "notes", PK: "n1", Field: "tags", CRDTType: TypeSet, NodeID: "dev-a", HLC: at(lateBase-int64(5*time.Second), "dev-a"), SetOp: &SetOperation{Op: SetOpAdd, Elements: json.RawMessage(`["x"]`)}})

	var got []ChangeRecord
	deadline := time.After(5 * time.Second)
	for len(got) == 0 {
		select {
		case batch := <-ch:
			got = append(got, batch...)
		case <-deadline:
			t.Fatal("the stream never delivered the restamped row")
		}
	}
	require.Len(t, got, 1)
	assert.Equal(t, "n1", got[0].PK)
	assert.Equal(t, lateBase-int64(5*time.Second), got[0].HLC.Timestamp)

	// The stream resumes past the restamped row, not from its old clock.
	select {
	case batch := <-ch:
		t.Fatalf("the stream redelivered %d changes", len(batch))
	case <-time.After(50 * time.Millisecond):
	}
}

// Rows written before restamping keep the cursor columns equal to the
// state's clock; tombstones have no state at all. Both read back as before.
func TestLateStamp_LegacyRowsReadAsBefore(t *testing.T) {
	db := newMemShadow()
	s := newLateServer(t, db)
	lww, _ := json.Marshal(&FieldState{Type: TypeLWW, HLC: at(lateBase, "dev-a"), NodeID: "dev-a", Value: json.RawMessage(`"x"`)})
	db.tables["notes"] = map[string]*memRow{
		"n1\x00title\x00dev-a":      {pk: "n1", field: "title", node: "dev-a", ts: lateBase, state: lww, seq: 1},
		"n2\x00_tombstone\x00dev-b": {pk: "n2", field: "_tombstone", node: "dev-b", ts: lateBase + 1, tombstone: true, seq: 2},
	}

	resp, err := s.ctrl.HandlePull(context.Background(), &PullRequest{Tables: []string{"notes"}, NodeID: "probe"})
	require.NoError(t, err)
	require.Len(t, resp.Changes, 2)
	assert.Equal(t, at(lateBase, "dev-a"), resp.Changes[0].HLC)
	assert.Equal(t, at(lateBase+1, "dev-b"), resp.Changes[1].HLC)
	assert.True(t, resp.Changes[1].Tombstone)
	assert.Equal(t, CRDTType(""), resp.Changes[1].CRDTType)
	assert.Equal(t, at(lateBase+1, "dev-b"), resp.LatestHLC)

	state, err := s.ctrl.metadata.ReadState(context.Background(), "notes", "n2")
	require.NoError(t, err)
	assert.Equal(t, at(lateBase+1, "dev-b"), state.TombstoneHLC)
}

// pos is a stored row's cursor position.
func (r *memRow) pos() HLC { return HLC{Timestamp: r.ts, Counter: r.counter} }

// ReadStateAt cuts on each row's own clock, not its cursor position. A
// restamped row sits at a position past its clock, and memShadow honours a
// position cut, so cutting on positions would drop the row (with every
// field state it holds) from each time before the restamp.
func TestLateStamp_ReadStateAtCutsOnOwnClock(t *testing.T) {
	db := newMemShadow()
	s := newLateServer(t, db)
	ctx := context.Background()
	T := lateBase
	sec := int64(time.Second)

	s.now.Store(T + 2*sec)
	s.push(t, ChangeRecord{Table: "notes", PK: "n1", Field: "views", CRDTType: TypeCounter, NodeID: "dev-b",
		HLC: at(T+sec, "dev-b"), CounterDelta: &CounterDelta{Increment: 7}})
	s.now.Store(T + 10*sec)
	s.push(t, ChangeRecord{Table: "notes", PK: "n1", Field: "views", CRDTType: TypeCounter, NodeID: "dev-a",
		HLC: at(T, "dev-a"), CounterDelta: &CounterDelta{Increment: 3}})
	s.now.Store(T + 20*sec)
	s.push(t, ChangeRecord{Table: "notes", PK: "n1", Field: "_tombstone", Tombstone: true, NodeID: "dev-c",
		HLC: at(T+3*sec, "dev-c")})

	cut := HLC{Timestamp: T + 5*sec}
	views := db.row("n1", "views", "dev-b")
	require.NotNil(t, views)
	require.True(t, cursorAfter(views.pos(), cut), "the merged counter row sits past the cut")
	tomb := db.row("n1", "_tombstone", "dev-c")
	require.NotNil(t, tomb)
	require.True(t, cursorAfter(tomb.pos(), cut), "the late delete sits past the cut")

	st, err := s.ctrl.metadata.ReadStateAt(ctx, "notes", "n1", cut)
	require.NoError(t, err)
	assert.Equal(t, int64(10), resolveForTest(st.Fields["views"]), "a row whose clock is before the cut is in it")
	assert.True(t, st.Tombstone)
	assert.Equal(t, at(T+3*sec, "dev-c"), st.TombstoneHLC)

	st, err = s.ctrl.metadata.ReadStateAt(ctx, "notes", "n1", HLC{Timestamp: T + 2*sec})
	require.NoError(t, err)
	assert.Equal(t, int64(10), resolveForTest(st.Fields["views"]))
	assert.False(t, st.Tombstone, "a delete whose clock is past the cut is not in it")

	st, err = s.ctrl.metadata.ReadStateAt(ctx, "notes", "n1", HLC{Timestamp: T + sec/2})
	require.NoError(t, err)
	assert.Empty(t, st.Fields, "a row whose clock is past the cut is not in it")
	assert.False(t, st.Tombstone)
}
