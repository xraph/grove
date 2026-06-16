package pgdriver

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/grove"
)

// nzItem exercises the nullzero-on-insert gate. project_id mirrors the real
// twinos pattern: a nullable foreign-key column whose empty Go string must
// insert SQL NULL (not ”), since ” fails the FK to projects(id).
type nzItem struct {
	grove.BaseModel `grove:"table:nz_items,alias:nz"`
	ID              string `grove:"id,pk"`
	ProjectID       string `grove:"project_id,nullzero"`  // nullable + nullzero -> empty binds NULL
	WorkspaceID     string `grove:"workspace_id,notnull"` // notnull, no nullzero -> empty binds ""
	Tag             string `grove:"tag"`                  // plain -> empty binds ""
	Seq             int    `grove:"seq,nullzero,notnull"` // notnull wins over nullzero -> zero binds 0
}

// An empty nullzero+nullable field binds SQL NULL; notnull and plain fields keep
// their zero literal so behavior is unchanged for them.
func TestInsertQuery_NullZeroNullableBindsNil(t *testing.T) {
	db := newTestDB()
	m := &nzItem{ID: "x1", WorkspaceID: "ws1"} // ProjectID/Tag empty, Seq 0

	_, args, err := db.NewInsert(m).Build()
	require.NoError(t, err)

	// Column order follows struct order: id, project_id, workspace_id, tag, seq.
	require.Len(t, args, 5)
	assert.Equal(t, "x1", args[0])
	assert.Nil(t, args[1], "empty nullzero nullable project_id must bind SQL NULL, not ''")
	assert.Equal(t, "ws1", args[2])
	assert.Equal(t, "", args[3], "plain field keeps empty string")
	assert.Equal(t, 0, args[4], "notnull+nullzero must keep the zero value, never NULL")
}

// A populated nullzero field binds its value normally.
func TestInsertQuery_NullZeroPopulatedBindsValue(t *testing.T) {
	db := newTestDB()
	m := &nzItem{ID: "x2", ProjectID: "proj_123", WorkspaceID: "ws1"}

	_, args, err := db.NewInsert(m).Build()
	require.NoError(t, err)
	require.Len(t, args, 5)
	assert.Equal(t, "proj_123", args[1], "populated project_id binds its value, not NULL")
}
