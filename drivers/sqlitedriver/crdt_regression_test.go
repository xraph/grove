package sqlitedriver

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/xraph/grove"
	"github.com/xraph/grove/crdt"
	"github.com/xraph/grove/hook"
)

type crdtRegressionRow struct {
	grove.BaseModel `grove:"table:regression_notes"`
	ID              int64  `grove:"id,pk"`
	Title           string `grove:"title,crdt:lww" json:"other_name"`
	Body            string `grove:"body,crdt:lww"`
}

type crdtRegressionExecutor struct{ db *SqliteDB }

func (e crdtRegressionExecutor) ExecContext(ctx context.Context, query string, args ...any) (crdt.ExecResult, error) {
	return e.db.Exec(ctx, strings.ReplaceAll(query, "$", "?"), args...)
}
func (e crdtRegressionExecutor) QueryContext(ctx context.Context, query string, args ...any) (crdt.Rows, error) {
	return e.db.Query(ctx, strings.ReplaceAll(query, "$", "?"), args...)
}

func newCRDTDatabase(t *testing.T) (*SqliteDB, *crdt.Plugin) {
	t.Helper()
	db := New()
	require.NoError(t, db.Open(t.Context(), filepath.Join(t.TempDir(), "crdt.db")))
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	_, err := db.NewCreateTable((*crdtRegressionRow)(nil)).Exec(t.Context())
	require.NoError(t, err)
	p := crdt.New(crdt.WithNodeID("local"))
	p.SetExecutor(crdtRegressionExecutor{db: db})
	require.NoError(t, p.EnsureShadowTable(t.Context(), "regression_notes"))
	engine := hook.NewEngine()
	engine.AddHook(p, hook.Scope{Tables: []string{"regression_notes"}})
	db.SetHooks(engine)
	return db, p
}

func TestRegressionTaggedORMInsertProducesCRDTState(t *testing.T) {
	db, p := newCRDTDatabase(t)
	_, err := db.NewInsert(&crdtRegressionRow{ID: 1, Title: "must sync"}).Exec(t.Context())
	require.NoError(t, err)
	state, err := p.Inspect(t.Context(), "regression_notes", "1")
	require.NoError(t, err)
	require.JSONEq(t, `"must sync"`, string(state.Fields["title"].Value))
	require.NotContains(t, state.Fields, "other_name")
}

func TestRegressionORMPartialUpdateAndDelete(t *testing.T) {
	db, p := newCRDTDatabase(t)
	_, err := db.NewInsert(&crdtRegressionRow{ID: 1, Title: "first", Body: "keep"}).Exec(t.Context())
	require.NoError(t, err)
	for _, omitZero := range []bool{false, true} {
		row := &crdtRegressionRow{ID: 1, Title: "updated"}
		q := db.NewUpdate(row).WherePK()
		if omitZero {
			q.OmitZero()
		} else {
			q.Column("title")
		}
		_, err = q.Exec(t.Context())
		require.NoError(t, err)
		state, inspectErr := p.Inspect(t.Context(), "regression_notes", "1")
		require.NoError(t, inspectErr)
		require.JSONEq(t, `"updated"`, string(state.Fields["title"].Value))
		require.JSONEq(t, `"keep"`, string(state.Fields["body"].Value))
		var title, body string
		require.NoError(t, db.db.QueryRowContext(t.Context(), "SELECT title, body FROM regression_notes WHERE id = 1").Scan(&title, &body))
		require.Equal(t, "updated", title)
		require.Equal(t, "keep", body)
	}
	_, err = db.NewDelete(&crdtRegressionRow{ID: 1}).WherePK().Exec(t.Context())
	require.NoError(t, err)
	state, err := p.Inspect(t.Context(), "regression_notes", "1")
	require.NoError(t, err)
	require.True(t, state.Tombstone)
}

func TestRegressionORMZeroRowUpdateDoesNotCreateMetadata(t *testing.T) {
	db, p := newCRDTDatabase(t)
	_, err := db.NewUpdate(&crdtRegressionRow{ID: 99, Title: "absent"}).WherePK().Exec(t.Context())
	require.NoError(t, err)
	state, err := p.Inspect(t.Context(), "regression_notes", "99")
	require.NoError(t, err)
	require.True(t, state == nil || len(state.Fields) == 0)
}

func TestRegressionORMBulkInsertTracksEveryRecord(t *testing.T) {
	db, p := newCRDTDatabase(t)
	_, err := db.NewInsert(&[]crdtRegressionRow{{ID: 1, Title: "one"}, {ID: 2, Title: "two"}}).Exec(t.Context())
	require.NoError(t, err)
	for pk, title := range map[string]string{"1": `"one"`, "2": `"two"`} {
		state, inspectErr := p.Inspect(t.Context(), "regression_notes", pk)
		require.NoError(t, inspectErr)
		require.JSONEq(t, title, string(state.Fields["title"].Value))
	}
}

func TestRegressionORMExpressionsFailBeforeWriting(t *testing.T) {
	db, p := newCRDTDatabase(t)
	_, err := db.NewInsert(&crdtRegressionRow{ID: 1, Title: "keep"}).Exec(t.Context())
	require.NoError(t, err)
	_, err = db.NewUpdate(&crdtRegressionRow{ID: 1}).Set("title = ?", "expression").WherePK().Exec(t.Context())
	require.ErrorContains(t, err, "expression mutations")
	var title string
	require.NoError(t, db.db.QueryRowContext(t.Context(), "SELECT title FROM regression_notes WHERE id = 1").Scan(&title))
	require.Equal(t, "keep", title)
	state, err := p.Inspect(t.Context(), "regression_notes", "1")
	require.NoError(t, err)
	require.JSONEq(t, `"keep"`, string(state.Fields["title"].Value))
}

type crdtGeneratedRow struct {
	grove.BaseModel `grove:"table:generated_notes"`
	ID              int64  `grove:"id,pk,autoincrement"`
	Title           string `grove:"title,crdt:lww"`
}

func TestRegressionORMGeneratedKeyTracksInsertedRecord(t *testing.T) {
	db, p := newCRDTDatabase(t)
	_, err := db.NewCreateTable((*crdtGeneratedRow)(nil)).Exec(t.Context())
	require.NoError(t, err)
	require.NoError(t, p.EnsureShadowTable(t.Context(), "generated_notes"))
	engine := hook.NewEngine()
	engine.AddHook(p, hook.Scope{Tables: []string{"generated_notes"}})
	db.SetHooks(engine)
	result, err := db.NewInsert(&crdtGeneratedRow{Title: "generated"}).Exec(t.Context())
	require.NoError(t, err)
	id, err := result.LastInsertId()
	require.NoError(t, err)
	require.Positive(t, id)
	state, err := p.Inspect(t.Context(), "generated_notes", fmt.Sprint(id))
	require.NoError(t, err)
	require.JSONEq(t, `"generated"`, string(state.Fields["title"].Value))
}
