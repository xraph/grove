package sqlitedriver

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/xraph/grove"
	"github.com/xraph/grove/crdt"
	"github.com/xraph/grove/hook"
)

type crdtReviewRow struct {
	grove.BaseModel `grove:"table:review_notes"`
	ID              int64  `grove:"id,pk"`
	Title           string `grove:"title,crdt:lww"`
}

type crdtReviewExecutor struct{ db *SqliteDB }

func (e crdtReviewExecutor) ExecContext(ctx context.Context, query string, args ...any) (crdt.ExecResult, error) {
	return e.db.Exec(ctx, strings.ReplaceAll(query, "$", "?"), args...)
}
func (e crdtReviewExecutor) QueryContext(ctx context.Context, query string, args ...any) (crdt.Rows, error) {
	return e.db.Query(ctx, strings.ReplaceAll(query, "$", "?"), args...)
}

// This opt-in review probe asserts the required behavior and currently fails.
// Run with GROVE_CRDT_REVIEW=1 until the ORM hook integration is fixed.
func TestReviewTaggedORMInsertProducesCRDTState(t *testing.T) {
	if os.Getenv("GROVE_CRDT_REVIEW") != "1" {
		t.Skip("set GROVE_CRDT_REVIEW=1 to run known-failing review reproductions")
	}
	db := New()
	require.NoError(t, db.Open(t.Context(), filepath.Join(t.TempDir(), "review.db")))
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	_, err := db.NewCreateTable((*crdtReviewRow)(nil)).Exec(t.Context())
	require.NoError(t, err)
	p := crdt.New(crdt.WithNodeID("local"))
	p.SetExecutor(crdtReviewExecutor{db: db})
	require.NoError(t, p.EnsureShadowTable(t.Context(), "review_notes"))
	engine := hook.NewEngine()
	engine.AddHook(p, hook.Scope{Tables: []string{"review_notes"}})
	db.SetHooks(engine)
	_, err = db.NewInsert(&crdtReviewRow{ID: 1, Title: "must sync"}).Exec(t.Context())
	require.NoError(t, err)
	var count int
	require.NoError(t, db.db.QueryRowContext(t.Context(), "SELECT count(*) FROM _review_notes_crdt").Scan(&count))
	require.Positive(t, count, "the model tag and registered hook must record an inserted field")
}
