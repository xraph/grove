package sqlitedriver

import (
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/xraph/grove"
	"github.com/xraph/grove/observability/telemetrytest"
)

type relationReadParent struct {
	grove.BaseModel `grove:"table:relation_parents"`
	N               int64               `grove:"n,pk"`
	First           *relationReadChild  `grove:"rel:has-one,join:n=n"`
	Owner           *relationReadChild  `grove:"rel:belongs-to,join:n=n"`
	Children        []relationReadChild `grove:"rel:has-many,join:n=n"`
}

type relationReadChild struct {
	grove.BaseModel `grove:"table:relation_children"`
	N               int64 `grove:"n,pk"`
}

func TestRelationsAvoidDuplicateAndUnboundedReads(t *testing.T) {
	for _, name := range []string{"First", "Owner", "Children"} {
		t.Run(name, func(t *testing.T) {
			var queries []string
			db := New()
			db.db = telemetrytest.SQLDB(t, func(query string) error {
				queries = append(queries, query)
				if strings.Contains(query, "fail_unique") {
					return errors.New("relation read failed")
				}
				if strings.Contains(query, "no_rows") {
					return sql.ErrNoRows
				}
				return nil
			})
			getQueries := func() []string { return queries }

			var parent relationReadParent
			q := db.NewSelect(&parent).Relation(name).Relation(name)
			require.NoError(t, q.Scan(t.Context()))
			executed := getQueries()
			require.Len(t, executed, 2, "one base query and one relation query")
			if name == "Children" {
				require.NotContains(t, executed[1], "LIMIT")
				require.Len(t, parent.Children, 1)
			} else {
				require.Contains(t, executed[1], "LIMIT 1")
				if name == "First" {
					require.NotNil(t, parent.First)
				} else {
					require.NotNil(t, parent.Owner)
				}
			}
		})
	}
}

func TestSingleRelationsPreserveDatabaseErrors(t *testing.T) {
	for _, name := range []string{"First", "Owner"} {
		for _, column := range []string{"no_rows", "fail_unique"} {
			t.Run(name+"/"+column, func(t *testing.T) {
				var queries []string
				db := New()
				db.db = telemetrytest.SQLDB(t, func(query string) error {
					queries = append(queries, query)
					if strings.Contains(query, "fail_unique") {
						return errors.New("relation read failed")
					}
					if strings.Contains(query, "no_rows") {
						return sql.ErrNoRows
					}
					return nil
				})
				getQueries := func() []string { return queries }

				var parent relationReadParent
				q := db.NewSelect(&parent).Relation(name)
				for _, rel := range q.table.Relations {
					if rel.Field.GoName == name {
						rel.JoinColumn = column
					}
				}
				err := q.Scan(t.Context())
				if column == "no_rows" {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
				}
				require.Len(t, getQueries(), 2)
				require.Zero(t, db.db.Stats().InUse)
				require.Nil(t, parent.First)
				require.Nil(t, parent.Owner)
			})
		}
	}
}
