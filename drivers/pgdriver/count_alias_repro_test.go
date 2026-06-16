package pgdriver

import (
	"strings"
	"testing"
	"time"

	"github.com/xraph/grove"
)

type fnRepro struct {
	grove.BaseModel `grove:"table:fns,alias:fd"`
	ID              string     `grove:"id,pk"`
	WorkspaceID     string     `grove:"workspace_id"`
	Version         int        `grove:"version"`
	DeletedAt       *time.Time `grove:"deleted_at,soft_delete"`
}

// A subquery built as NewSelect(model).TableExpr("fns AS sub") must qualify the
// auto soft-delete filter with the TableExpr alias ("sub"), not the model alias
// ("fd") — otherwise the generated SQL references a FROM entry that doesn't
// exist and Postgres 42P01s.
func TestSoftDelete_tableExprOverridesAlias(t *testing.T) {
	db := newTestDB()

	sub := db.NewSelect((*fnRepro)(nil)).
		TableExpr("fns AS sub").
		ColumnExpr("sub.id").
		ColumnExpr("MAX(sub.version) AS max_version").
		GroupExpr("sub.id")

	sql, _, err := sub.Build()
	if err != nil {
		t.Fatalf("Build err: %v", err)
	}
	t.Logf("SUBQUERY SQL: %s", sql)

	if !strings.Contains(sql, `"sub"."deleted_at" IS NULL`) {
		t.Errorf("soft-delete not qualified with the TableExpr alias 'sub'; got: %s", sql)
	}
	if strings.Contains(sql, `"fd"."deleted_at"`) {
		t.Errorf("soft-delete wrongly qualified with the model alias 'fd'; got: %s", sql)
	}
}

func TestAliasFromTableExpr(t *testing.T) {
	cases := []struct {
		in    string
		alias string
		ok    bool
	}{
		{"function_definitions AS sub", "sub", true},
		{"fns sub", "sub", true},
		{`"fns" AS "sub"`, "sub", true},
		{"function_definitions", "", true},
		{"(SELECT 1) AS latest", "", false},
		{"a JOIN b ON a.id=b.id", "", false},
		{"a, b", "", false},
		{"", "", false},
	}
	for _, c := range cases {
		alias, ok := aliasFromTableExpr(c.in)
		if ok != c.ok || alias != c.alias {
			t.Errorf("aliasFromTableExpr(%q) = (%q,%v); want (%q,%v)", c.in, alias, ok, c.alias, c.ok)
		}
	}
}
