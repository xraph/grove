package tursodriver

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/grove"
	"github.com/xraph/grove/schema"
)

// ---------------------------------------------------------------------------
// Test models
// ---------------------------------------------------------------------------

// TestUser has an alias and a soft_delete field.
type TestUser struct {
	grove.BaseModel `grove:"table:users,alias:u"`
	ID              int64     `grove:"id,pk,autoincrement"`
	Name            string    `grove:"name,notnull"`
	Email           string    `grove:"email,notnull,unique"`
	Role            string    `grove:"role,notnull,default:'user'"`
	CreatedAt       time.Time `grove:"created_at,nullzero,notnull"`
	UpdatedAt       time.Time `grove:"updated_at,nullzero"`
	DeletedAt       time.Time `grove:"deleted_at,soft_delete,nullzero"`
}

// TestPost has no alias and no soft_delete field.
type TestPost struct {
	grove.BaseModel `grove:"table:posts"`
	ID              int64  `grove:"id,pk,autoincrement"`
	Title           string `grove:"title,notnull"`
	UserID          int64  `grove:"user_id,notnull"`
}

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

func newTestDB() *TursoDB {
	return &TursoDB{dialect: &TursoDialect{}, registry: schema.NewRegistry()}
}

// =========================================================================
// SELECT QUERY TESTS
// =========================================================================

func TestSelectQuery_BasicAllFields(t *testing.T) {
	db := newTestDB()

	sql, args, err := db.NewSelect((*TestUser)(nil)).Build()
	require.NoError(t, err)

	// Should list all fields with the alias prefix, include FROM with alias,
	// and auto-add the soft delete filter.
	expected := `SELECT "u"."id", "u"."name", "u"."email", "u"."role", "u"."created_at", "u"."updated_at", "u"."deleted_at" FROM "users" AS "u" WHERE "u"."deleted_at" IS NULL`
	assert.Equal(t, expected, sql)
	assert.Empty(t, args)
}

func TestSelectQuery_SpecificColumns(t *testing.T) {
	db := newTestDB()

	sql, args, err := db.NewSelect((*TestUser)(nil)).
		Column(`"u"."name"`, `"u"."email"`).
		Build()
	require.NoError(t, err)

	expected := `SELECT "u"."name", "u"."email" FROM "users" AS "u" WHERE "u"."deleted_at" IS NULL`
	assert.Equal(t, expected, sql)
	assert.Empty(t, args)
}

func TestSelectQuery_WhereClause(t *testing.T) {
	db := newTestDB()

	sql, args, err := db.NewSelect((*TestUser)(nil)).
		Where(`"u"."role" = 'admin'`).
		Build()
	require.NoError(t, err)

	expected := `SELECT "u"."id", "u"."name", "u"."email", "u"."role", "u"."created_at", "u"."updated_at", "u"."deleted_at" FROM "users" AS "u" WHERE "u"."role" = 'admin' AND "u"."deleted_at" IS NULL`
	assert.Equal(t, expected, sql)
	assert.Empty(t, args)
}

func TestSelectQuery_MultipleWhereAnd(t *testing.T) {
	db := newTestDB()

	sql, args, err := db.NewSelect((*TestUser)(nil)).
		Where(`"u"."role" = 'admin'`).
		Where(`"u"."name" = 'Alice'`).
		Build()
	require.NoError(t, err)

	expected := `SELECT "u"."id", "u"."name", "u"."email", "u"."role", "u"."created_at", "u"."updated_at", "u"."deleted_at" FROM "users" AS "u" WHERE "u"."role" = 'admin' AND "u"."name" = 'Alice' AND "u"."deleted_at" IS NULL`
	assert.Equal(t, expected, sql)
	assert.Empty(t, args)
}

func TestSelectQuery_WhereOr(t *testing.T) {
	db := newTestDB()

	sql, args, err := db.NewSelect((*TestUser)(nil)).
		Where(`"u"."role" = 'admin'`).
		WhereOr(`"u"."role" = 'superadmin'`).
		Build()
	require.NoError(t, err)

	expected := `SELECT "u"."id", "u"."name", "u"."email", "u"."role", "u"."created_at", "u"."updated_at", "u"."deleted_at" FROM "users" AS "u" WHERE "u"."role" = 'admin' OR "u"."role" = 'superadmin' AND "u"."deleted_at" IS NULL`
	assert.Equal(t, expected, sql)
	assert.Empty(t, args)
}

func TestSelectQuery_WherePK(t *testing.T) {
	db := newTestDB()
	user := &TestUser{ID: 42}

	sql, args, err := db.NewSelect(user).WherePK().Build()
	require.NoError(t, err)

	expected := `SELECT "u"."id", "u"."name", "u"."email", "u"."role", "u"."created_at", "u"."updated_at", "u"."deleted_at" FROM "users" AS "u" WHERE "u"."id" = ? AND "u"."deleted_at" IS NULL`
	assert.Equal(t, expected, sql)
	require.Len(t, args, 1)
	assert.Equal(t, int64(42), args[0])
}

func TestSelectQuery_Join(t *testing.T) {
	db := newTestDB()

	sql, args, err := db.NewSelect((*TestUser)(nil)).
		Join("JOIN", `"posts" AS "p"`, `"p"."user_id" = "u"."id"`).
		Build()
	require.NoError(t, err)

	expected := `SELECT "u"."id", "u"."name", "u"."email", "u"."role", "u"."created_at", "u"."updated_at", "u"."deleted_at" FROM "users" AS "u" JOIN "posts" AS "p" ON "p"."user_id" = "u"."id" WHERE "u"."deleted_at" IS NULL`
	assert.Equal(t, expected, sql)
	assert.Empty(t, args)
}

func TestSelectQuery_LeftJoin(t *testing.T) {
	db := newTestDB()

	sql, args, err := db.NewSelect((*TestUser)(nil)).
		Join("LEFT JOIN", `"posts" AS "p"`, `"p"."user_id" = "u"."id"`).
		Build()
	require.NoError(t, err)

	assert.Contains(t, sql, "LEFT JOIN")
	assert.Contains(t, sql, `"posts" AS "p"`)
	assert.Contains(t, sql, `ON "p"."user_id" = "u"."id"`)
	assert.Empty(t, args)
}

func TestSelectQuery_OrderByGroupByHaving(t *testing.T) {
	db := newTestDB()

	sql, args, err := db.NewSelect((*TestUser)(nil)).
		Column(`"u"."role"`, `COUNT(*) AS "cnt"`).
		GroupExpr(`"u"."role"`).
		Having(`COUNT(*) > 5`).
		OrderExpr(`"cnt" DESC`).
		Build()
	require.NoError(t, err)

	expected := `SELECT "u"."role", COUNT(*) AS "cnt" FROM "users" AS "u" WHERE "u"."deleted_at" IS NULL GROUP BY "u"."role" HAVING COUNT(*) > 5 ORDER BY "cnt" DESC`
	assert.Equal(t, expected, sql)
	assert.Empty(t, args)
}

func TestSelectQuery_LimitOffset(t *testing.T) {
	db := newTestDB()

	sql, args, err := db.NewSelect((*TestUser)(nil)).
		Limit(10).
		Offset(20).
		Build()
	require.NoError(t, err)

	assert.Contains(t, sql, "LIMIT 10")
	assert.Contains(t, sql, "OFFSET 20")
	assert.Empty(t, args)
}

func TestSelectQuery_BuildCount(t *testing.T) {
	db := newTestDB()

	sql, args, err := db.NewSelect((*TestUser)(nil)).
		Where(`"u"."role" = 'admin'`).
		BuildCount()
	require.NoError(t, err)

	expected := `SELECT COUNT(*) FROM "users" AS "u" WHERE "u"."role" = 'admin' AND "u"."deleted_at" IS NULL`
	assert.Equal(t, expected, sql)
	assert.Empty(t, args)
}

func TestSelectQuery_BuildCountNoWhere(t *testing.T) {
	db := newTestDB()

	sql, args, err := db.NewSelect((*TestUser)(nil)).BuildCount()
	require.NoError(t, err)

	expected := `SELECT COUNT(*) FROM "users" AS "u" WHERE "u"."deleted_at" IS NULL`
	assert.Equal(t, expected, sql)
	assert.Empty(t, args)
}

// ---------------------------------------------------------------------------
// Soft delete behaviour in SELECT
// ---------------------------------------------------------------------------

func TestSelectQuery_SoftDeleteAutoFilter(t *testing.T) {
	db := newTestDB()

	// TestUser has soft_delete on deleted_at and alias "u".
	sql, _, err := db.NewSelect((*TestUser)(nil)).Build()
	require.NoError(t, err)

	assert.Contains(t, sql, `"u"."deleted_at" IS NULL`,
		"soft delete filter should be auto-added with alias prefix for TestUser")
}

func TestSelectQuery_SoftDeleteWithDeleted(t *testing.T) {
	db := newTestDB()

	sql, _, err := db.NewSelect((*TestUser)(nil)).WithDeleted().Build()
	require.NoError(t, err)

	assert.NotContains(t, sql, `"deleted_at" IS NULL`,
		"WithDeleted() should bypass the soft delete filter")
}

func TestSelectQuery_NoSoftDeleteOnTestPost(t *testing.T) {
	db := newTestDB()

	// TestPost has no soft_delete field, so no filter should be added.
	sql, _, err := db.NewSelect((*TestPost)(nil)).Build()
	require.NoError(t, err)

	assert.NotContains(t, sql, "deleted_at",
		"TestPost without soft_delete should not get a deleted_at filter")
	assert.NotContains(t, sql, "WHERE",
		"TestPost with no WHERE clauses should not have WHERE")
}

func TestSelectQuery_PostBasicAllFields(t *testing.T) {
	db := newTestDB()

	sql, args, err := db.NewSelect((*TestPost)(nil)).Build()
	require.NoError(t, err)

	// TestPost has no alias, so columns should not be prefixed.
	expected := `SELECT "id", "title", "user_id" FROM "posts"`
	assert.Equal(t, expected, sql)
	assert.Empty(t, args)
}

func TestSelectQuery_ColumnExpr(t *testing.T) {
	db := newTestDB()

	sql, args, err := db.NewSelect((*TestUser)(nil)).
		ColumnExpr("COUNT(*) AS cnt").
		Build()
	require.NoError(t, err)

	assert.Contains(t, sql, "SELECT COUNT(*) AS cnt FROM")
	assert.Empty(t, args)
}

func TestSelectQuery_WhereWithArgs(t *testing.T) {
	db := newTestDB()

	sql, args, err := db.NewSelect((*TestPost)(nil)).
		Where(`"id" = ?`, int64(99)).
		Build()
	require.NoError(t, err)

	assert.Contains(t, sql, `WHERE "id" = ?`)
	require.Len(t, args, 1)
	assert.Equal(t, int64(99), args[0])
}

func TestSelectQuery_CombinedFeatures(t *testing.T) {
	db := newTestDB()

	sql, args, err := db.NewSelect((*TestUser)(nil)).
		Column(`"u"."id"`, `"u"."name"`).
		Where(`"u"."role" = 'admin'`).
		OrderExpr(`"u"."name" ASC`).
		Limit(5).
		Offset(10).
		Build()
	require.NoError(t, err)

	assert.Contains(t, sql, `SELECT "u"."id", "u"."name"`)
	assert.Contains(t, sql, `FROM "users" AS "u"`)
	assert.Contains(t, sql, `WHERE "u"."role" = 'admin'`)
	assert.Contains(t, sql, `"u"."deleted_at" IS NULL`)
	assert.Contains(t, sql, `ORDER BY "u"."name" ASC`)
	assert.Contains(t, sql, "LIMIT 5")
	assert.Contains(t, sql, "OFFSET 10")
	assert.Empty(t, args)
}

// =========================================================================
// INSERT QUERY TESTS
// =========================================================================

func TestInsertQuery_BasicFromModel(t *testing.T) {
	db := newTestDB()
	user := &TestUser{
		Name:  "Alice",
		Email: "alice@example.com",
		Role:  "admin",
	}

	sql, args, err := db.NewInsert(user).Build()
	require.NoError(t, err)

	// Autoincrement field "id" should be excluded.
	assert.Contains(t, sql, `INSERT INTO "users"`)
	assert.Contains(t, sql, `"name"`)
	assert.Contains(t, sql, `"email"`)
	assert.Contains(t, sql, `"role"`)
	assert.NotContains(t, sql, `"id"`,
		"autoincrement field should be excluded from INSERT")
	assert.Contains(t, sql, "VALUES (?, ?, ?, ?, ?, ?)")
	require.Len(t, args, 6) // name, email, role, created_at, updated_at, deleted_at
	assert.Equal(t, "Alice", args[0])
	assert.Equal(t, "alice@example.com", args[1])
	assert.Equal(t, "admin", args[2])
}

func TestInsertQuery_ExplicitColumns(t *testing.T) {
	db := newTestDB()
	user := &TestUser{
		Name:  "Bob",
		Email: "bob@example.com",
	}

	sql, args, err := db.NewInsert(user).
		Column("name", "email").
		Build()
	require.NoError(t, err)

	assert.Contains(t, sql, `INSERT INTO "users" ("name", "email")`)
	assert.Contains(t, sql, "VALUES (?, ?)")
	require.Len(t, args, 2)
	assert.Equal(t, "Bob", args[0])
	assert.Equal(t, "bob@example.com", args[1])
}

func TestInsertQuery_ExplicitValues(t *testing.T) {
	db := newTestDB()
	user := &TestUser{}

	sql, args, err := db.NewInsert(user).
		Column("name", "email").
		Value("Charlie", "charlie@example.com").
		Build()
	require.NoError(t, err)

	assert.Contains(t, sql, `INSERT INTO "users" ("name", "email")`)
	assert.Contains(t, sql, "VALUES (?, ?)")
	require.Len(t, args, 2)
	assert.Equal(t, "Charlie", args[0])
	assert.Equal(t, "charlie@example.com", args[1])
}

func TestInsertQuery_OnConflictDoNothing(t *testing.T) {
	db := newTestDB()
	user := &TestUser{
		Name:  "Alice",
		Email: "alice@example.com",
		Role:  "user",
	}

	sql, _, err := db.NewInsert(user).
		OnConflict("(email) DO NOTHING").
		Build()
	require.NoError(t, err)

	assert.Contains(t, sql, "ON CONFLICT (email) DO NOTHING")
}

func TestInsertQuery_OnConflictUpsert(t *testing.T) {
	db := newTestDB()
	user := &TestUser{
		Name:  "Alice",
		Email: "alice@example.com",
		Role:  "admin",
	}

	sql, args, err := db.NewInsert(user).
		OnConflict("(email) DO UPDATE").
		Set(`"name" = EXCLUDED."name"`).
		Set(`"role" = EXCLUDED."role"`).
		Build()
	require.NoError(t, err)

	assert.Contains(t, sql, "ON CONFLICT (email) DO UPDATE")
	assert.Contains(t, sql, `SET "name" = EXCLUDED."name", "role" = EXCLUDED."role"`)
	require.NotEmpty(t, args)
}

func TestInsertQuery_Returning(t *testing.T) {
	db := newTestDB()
	user := &TestUser{
		Name:  "Alice",
		Email: "alice@example.com",
		Role:  "user",
	}

	sql, _, err := db.NewInsert(user).
		Returning("id", "created_at").
		Build()
	require.NoError(t, err)

	assert.Contains(t, sql, `RETURNING "id", "created_at"`)
}

func TestInsertQuery_BulkInsert(t *testing.T) {
	db := newTestDB()
	posts := &[]TestPost{
		{Title: "First", UserID: 1},
		{Title: "Second", UserID: 2},
		{Title: "Third", UserID: 1},
	}

	sql, args, err := db.NewInsert(posts).Build()
	require.NoError(t, err)

	assert.Contains(t, sql, `INSERT INTO "posts"`)
	assert.Contains(t, sql, `"title"`)
	assert.Contains(t, sql, `"user_id"`)
	// 3 rows x 2 fields = 6 args (id is autoincrement, excluded).
	require.Len(t, args, 6)
	// Check placeholder structure: (?, ?), (?, ?), (?, ?)
	assert.Contains(t, sql, "(?, ?), (?, ?), (?, ?)")
	assert.Equal(t, "First", args[0])
	assert.Equal(t, int64(1), args[1])
	assert.Equal(t, "Second", args[2])
	assert.Equal(t, int64(2), args[3])
	assert.Equal(t, "Third", args[4])
	assert.Equal(t, int64(1), args[5])
}

func TestInsertQuery_PostBasic(t *testing.T) {
	db := newTestDB()
	post := &TestPost{Title: "Hello World", UserID: 5}

	sql, args, err := db.NewInsert(post).Build()
	require.NoError(t, err)

	assert.Contains(t, sql, `INSERT INTO "posts" ("title", "user_id")`)
	assert.Contains(t, sql, "VALUES (?, ?)")
	require.Len(t, args, 2)
	assert.Equal(t, "Hello World", args[0])
	assert.Equal(t, int64(5), args[1])
}

// =========================================================================
// UPDATE QUERY TESTS
// =========================================================================

func TestUpdateQuery_SetExpressions(t *testing.T) {
	db := newTestDB()
	user := &TestUser{ID: 1}

	sql, args, err := db.NewUpdate(user).
		Set(`"name" = ?`, "UpdatedAlice").
		Set(`"role" = ?`, "moderator").
		Where(`"id" = ?`, int64(1)).
		Build()
	require.NoError(t, err)

	expected := `UPDATE "users" SET "name" = ?, "role" = ? WHERE "id" = ?`
	assert.Equal(t, expected, sql)
	require.Len(t, args, 3)
	assert.Equal(t, "UpdatedAlice", args[0])
	assert.Equal(t, "moderator", args[1])
	assert.Equal(t, int64(1), args[2])
}

func TestUpdateQuery_FromModelAllFields(t *testing.T) {
	db := newTestDB()
	now := time.Date(2025, 1, 15, 12, 0, 0, 0, time.UTC)
	user := &TestUser{
		ID:        10,
		Name:      "Alice",
		Email:     "alice@example.com",
		Role:      "admin",
		CreatedAt: now,
		UpdatedAt: now,
	}

	sql, args, err := db.NewUpdate(user).
		Where(`"id" = ?`, int64(10)).
		Build()
	require.NoError(t, err)

	// Should include all updatable fields (exclude id/pk and autoincrement).
	assert.Contains(t, sql, `UPDATE "users" SET`)
	assert.Contains(t, sql, `"name" = ?`)
	assert.Contains(t, sql, `"email" = ?`)
	assert.Contains(t, sql, `"role" = ?`)
	assert.Contains(t, sql, `"created_at" = ?`)
	assert.Contains(t, sql, `"updated_at" = ?`)
	assert.Contains(t, sql, `"deleted_at" = ?`)
	assert.Contains(t, sql, `WHERE "id" = ?`)
	require.Len(t, args, 7)
	assert.Equal(t, "Alice", args[0])
	assert.Equal(t, "alice@example.com", args[1])
	assert.Equal(t, "admin", args[2])
	assert.Equal(t, int64(10), args[6])
}

func TestUpdateQuery_ColumnSpecific(t *testing.T) {
	db := newTestDB()
	user := &TestUser{
		ID:   7,
		Name: "Bob",
		Role: "editor",
	}

	sql, args, err := db.NewUpdate(user).
		Column("name", "role").
		Where(`"id" = ?`, int64(7)).
		Build()
	require.NoError(t, err)

	assert.Contains(t, sql, `UPDATE "users" SET "name" = ?, "role" = ?`)
	assert.Contains(t, sql, `WHERE "id" = ?`)
	require.Len(t, args, 3)
	assert.Equal(t, "Bob", args[0])
	assert.Equal(t, "editor", args[1])
	assert.Equal(t, int64(7), args[2])
}

func TestUpdateQuery_OmitZero(t *testing.T) {
	db := newTestDB()
	user := &TestUser{
		ID:   5,
		Name: "Carol",
		// Email, Role, CreatedAt, UpdatedAt, DeletedAt are zero values.
	}

	sql, args, err := db.NewUpdate(user).
		OmitZero().
		Where(`"id" = ?`, int64(5)).
		Build()
	require.NoError(t, err)

	// Only non-zero fields should appear in SET.
	assert.Contains(t, sql, `"name" = ?`)
	assert.NotContains(t, sql, `"email"`,
		"zero-value email should be omitted with OmitZero")
	assert.NotContains(t, sql, `"created_at"`,
		"zero-value created_at should be omitted with OmitZero")
	require.True(t, len(args) >= 2, "should have at least name arg and where arg")
	assert.Equal(t, "Carol", args[0])
}

func TestUpdateQuery_WherePK(t *testing.T) {
	db := newTestDB()
	user := &TestUser{
		ID:    99,
		Name:  "Diana",
		Email: "diana@example.com",
		Role:  "user",
	}

	sql, args, err := db.NewUpdate(user).
		Column("name").
		WherePK().
		Build()
	require.NoError(t, err)

	assert.Contains(t, sql, `UPDATE "users" SET "name" = ?`)
	assert.Contains(t, sql, `WHERE "id" = ?`)
	require.Len(t, args, 2)
	assert.Equal(t, "Diana", args[0])
	assert.Equal(t, int64(99), args[1])
}

func TestUpdateQuery_Returning(t *testing.T) {
	db := newTestDB()
	user := &TestUser{
		ID:   1,
		Name: "Eve",
	}

	sql, _, err := db.NewUpdate(user).
		Set(`"name" = ?`, "Eve Updated").
		Where(`"id" = ?`, int64(1)).
		Returning("id", "name", "updated_at").
		Build()
	require.NoError(t, err)

	assert.Contains(t, sql, `RETURNING "id", "name", "updated_at"`)
}

func TestUpdateQuery_PostBasic(t *testing.T) {
	db := newTestDB()
	post := &TestPost{
		ID:     3,
		Title:  "Updated Title",
		UserID: 10,
	}

	sql, args, err := db.NewUpdate(post).
		Column("title").
		WherePK().
		Build()
	require.NoError(t, err)

	assert.Contains(t, sql, `UPDATE "posts" SET "title" = ?`)
	assert.Contains(t, sql, `WHERE "id" = ?`)
	require.Len(t, args, 2)
	assert.Equal(t, "Updated Title", args[0])
	assert.Equal(t, int64(3), args[1])
}

// =========================================================================
// DELETE QUERY TESTS
// =========================================================================

func TestDeleteQuery_HardDeleteWithWhere(t *testing.T) {
	db := newTestDB()
	// TestPost has no soft_delete, so delete is always hard.
	post := &TestPost{}

	sql, args, err := db.NewDelete(post).
		Where(`"user_id" = ?`, int64(42)).
		Build()
	require.NoError(t, err)

	expected := `DELETE FROM "posts" WHERE "user_id" = ?`
	assert.Equal(t, expected, sql)
	require.Len(t, args, 1)
	assert.Equal(t, int64(42), args[0])
}

func TestDeleteQuery_HardDeleteWithWherePK(t *testing.T) {
	db := newTestDB()
	post := &TestPost{ID: 55}

	sql, args, err := db.NewDelete(post).
		WherePK().
		Build()
	require.NoError(t, err)

	expected := `DELETE FROM "posts" WHERE "id" = ?`
	assert.Equal(t, expected, sql)
	require.Len(t, args, 1)
	assert.Equal(t, int64(55), args[0])
}

func TestDeleteQuery_SoftDeleteAutoBehavior(t *testing.T) {
	db := newTestDB()
	user := &TestUser{ID: 10}

	sql, args, err := db.NewDelete(user).
		WherePK().
		Build()
	require.NoError(t, err)

	// Should generate UPDATE ... SET "deleted_at" = DATETIME('now') instead of DELETE.
	assert.Contains(t, sql, `UPDATE "users" SET "deleted_at" = DATETIME('now')`)
	assert.Contains(t, sql, `WHERE "id" = ?`)
	assert.NotContains(t, sql, "DELETE",
		"soft delete should not generate a DELETE statement")
	require.Len(t, args, 1)
	assert.Equal(t, int64(10), args[0])
}

func TestDeleteQuery_SoftDeleteWithWhereClause(t *testing.T) {
	db := newTestDB()
	user := &TestUser{}

	sql, args, err := db.NewDelete(user).
		Where(`"role" = ?`, "inactive").
		Build()
	require.NoError(t, err)

	assert.Contains(t, sql, `UPDATE "users" SET "deleted_at" = DATETIME('now')`)
	assert.Contains(t, sql, `WHERE "role" = ?`)
	require.Len(t, args, 1)
	assert.Equal(t, "inactive", args[0])
}

func TestDeleteQuery_ForceDeleteBypassesSoftDelete(t *testing.T) {
	db := newTestDB()
	user := &TestUser{ID: 10}

	sql, args, err := db.NewDelete(user).
		WherePK().
		ForceDelete().
		Build()
	require.NoError(t, err)

	// ForceDelete should generate a real DELETE FROM.
	expected := `DELETE FROM "users" WHERE "id" = ?`
	assert.Equal(t, expected, sql)
	assert.NotContains(t, sql, "UPDATE",
		"ForceDelete should bypass soft delete")
	require.Len(t, args, 1)
	assert.Equal(t, int64(10), args[0])
}

func TestDeleteQuery_Returning(t *testing.T) {
	db := newTestDB()
	post := &TestPost{ID: 77}

	sql, args, err := db.NewDelete(post).
		WherePK().
		Returning("id", "title").
		Build()
	require.NoError(t, err)

	assert.Contains(t, sql, `DELETE FROM "posts"`)
	assert.Contains(t, sql, `RETURNING "id", "title"`)
	require.Len(t, args, 1)
	assert.Equal(t, int64(77), args[0])
}

func TestDeleteQuery_SoftDeleteReturning(t *testing.T) {
	db := newTestDB()
	user := &TestUser{ID: 33}

	sql, args, err := db.NewDelete(user).
		WherePK().
		Returning("id", "deleted_at").
		Build()
	require.NoError(t, err)

	// Soft delete with RETURNING should generate UPDATE ... RETURNING.
	assert.Contains(t, sql, `UPDATE "users" SET "deleted_at" = DATETIME('now')`)
	assert.Contains(t, sql, `RETURNING "id", "deleted_at"`)
	require.Len(t, args, 1)
	assert.Equal(t, int64(33), args[0])
}

func TestDeleteQuery_PostNoSoftDeleteGeneratesRealDelete(t *testing.T) {
	db := newTestDB()
	post := &TestPost{ID: 1}

	sql, _, err := db.NewDelete(post).
		WherePK().
		Build()
	require.NoError(t, err)

	assert.Contains(t, sql, "DELETE FROM")
	assert.NotContains(t, sql, "UPDATE",
		"TestPost without soft_delete should use real DELETE")
}

func TestDeleteQuery_MultipleWhereConditions(t *testing.T) {
	db := newTestDB()
	post := &TestPost{}

	sql, args, err := db.NewDelete(post).
		Where(`"user_id" = ?`, int64(5)).
		Where(`"title" = ?`, "Old Post").
		Build()
	require.NoError(t, err)

	expected := `DELETE FROM "posts" WHERE "user_id" = ? AND "title" = ?`
	assert.Equal(t, expected, sql)
	require.Len(t, args, 2)
	assert.Equal(t, int64(5), args[0])
	assert.Equal(t, "Old Post", args[1])
}

// =========================================================================
// Table-driven tests
// =========================================================================

func TestSelectQuery_Variants(t *testing.T) {
	db := newTestDB()

	tests := []struct {
		name     string
		build    func() (string, []any, error)
		wantSQL  string
		wantArgs []any
	}{
		{
			name: "Post select all",
			build: func() (string, []any, error) {
				return db.NewSelect((*TestPost)(nil)).Build()
			},
			wantSQL:  `SELECT "id", "title", "user_id" FROM "posts"`,
			wantArgs: nil,
		},
		{
			name: "User select with WithDeleted",
			build: func() (string, []any, error) {
				return db.NewSelect((*TestUser)(nil)).WithDeleted().Build()
			},
			wantSQL:  `SELECT "u"."id", "u"."name", "u"."email", "u"."role", "u"."created_at", "u"."updated_at", "u"."deleted_at" FROM "users" AS "u"`,
			wantArgs: nil,
		},
		{
			name: "Post select with limit",
			build: func() (string, []any, error) {
				return db.NewSelect((*TestPost)(nil)).Limit(25).Build()
			},
			wantSQL:  `SELECT "id", "title", "user_id" FROM "posts" LIMIT 25`,
			wantArgs: nil,
		},
		{
			name: "Post WherePK",
			build: func() (string, []any, error) {
				return db.NewSelect(&TestPost{ID: 7}).WherePK().Build()
			},
			wantSQL:  `SELECT "id", "title", "user_id" FROM "posts" WHERE "id" = ?`,
			wantArgs: []any{int64(7)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql, args, err := tt.build()
			require.NoError(t, err)
			assert.Equal(t, tt.wantSQL, sql)
			if tt.wantArgs == nil {
				assert.Empty(t, args)
			} else {
				assert.Equal(t, tt.wantArgs, args)
			}
		})
	}
}

func TestInsertQuery_Variants(t *testing.T) {
	db := newTestDB()

	tests := []struct {
		name        string
		build       func() (string, []any, error)
		wantContain []string
		wantArgLen  int
	}{
		{
			name: "Insert with ON CONFLICT DO NOTHING",
			build: func() (string, []any, error) {
				return db.NewInsert(&TestPost{Title: "test", UserID: 1}).
					OnConflict("DO NOTHING").
					Build()
			},
			wantContain: []string{
				`INSERT INTO "posts"`,
				"ON CONFLICT DO NOTHING",
			},
			wantArgLen: 2,
		},
		{
			name: "Insert with RETURNING all",
			build: func() (string, []any, error) {
				return db.NewInsert(&TestPost{Title: "test", UserID: 1}).
					Returning("id").
					Build()
			},
			wantContain: []string{
				`INSERT INTO "posts"`,
				`RETURNING "id"`,
			},
			wantArgLen: 2,
		},
		{
			name: "Insert multiple explicit values",
			build: func() (string, []any, error) {
				return db.NewInsert(&TestPost{}).
					Column("title", "user_id").
					Value("Post A", int64(1)).
					Value("Post B", int64(2)).
					Build()
			},
			wantContain: []string{
				`INSERT INTO "posts" ("title", "user_id")`,
				"VALUES (?, ?), (?, ?)",
			},
			wantArgLen: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql, args, err := tt.build()
			require.NoError(t, err)
			for _, s := range tt.wantContain {
				assert.Contains(t, sql, s)
			}
			assert.Len(t, args, tt.wantArgLen)
		})
	}
}

func TestUpdateQuery_Variants(t *testing.T) {
	db := newTestDB()

	tests := []struct {
		name     string
		build    func() (string, []any, error)
		wantSQL  string
		wantArgs []any
	}{
		{
			name: "Single SET with WHERE",
			build: func() (string, []any, error) {
				return db.NewUpdate(&TestPost{ID: 1}).
					Set(`"title" = ?`, "New Title").
					Where(`"id" = ?`, int64(1)).
					Build()
			},
			wantSQL:  `UPDATE "posts" SET "title" = ? WHERE "id" = ?`,
			wantArgs: []any{"New Title", int64(1)},
		},
		{
			name: "Multiple SET expressions",
			build: func() (string, []any, error) {
				return db.NewUpdate(&TestPost{ID: 1}).
					Set(`"title" = ?`, "Updated").
					Set(`"user_id" = ?`, int64(99)).
					Where(`"id" = ?`, int64(1)).
					Build()
			},
			wantSQL:  `UPDATE "posts" SET "title" = ?, "user_id" = ? WHERE "id" = ?`,
			wantArgs: []any{"Updated", int64(99), int64(1)},
		},
		{
			name: "Update with Returning",
			build: func() (string, []any, error) {
				return db.NewUpdate(&TestPost{ID: 1}).
					Set(`"title" = ?`, "Final").
					Where(`"id" = ?`, int64(1)).
					Returning("id", "title").
					Build()
			},
			wantSQL:  `UPDATE "posts" SET "title" = ? WHERE "id" = ? RETURNING "id", "title"`,
			wantArgs: []any{"Final", int64(1)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql, args, err := tt.build()
			require.NoError(t, err)
			assert.Equal(t, tt.wantSQL, sql)
			assert.Equal(t, tt.wantArgs, args)
		})
	}
}

func TestDeleteQuery_Variants(t *testing.T) {
	db := newTestDB()

	tests := []struct {
		name        string
		build       func() (string, []any, error)
		wantSQL     string
		wantArgs    []any
		wantContain []string
		notContain  []string
	}{
		{
			name: "Hard delete post by PK",
			build: func() (string, []any, error) {
				return db.NewDelete(&TestPost{ID: 1}).WherePK().Build()
			},
			wantSQL:  `DELETE FROM "posts" WHERE "id" = ?`,
			wantArgs: []any{int64(1)},
		},
		{
			name: "Soft delete user by PK",
			build: func() (string, []any, error) {
				return db.NewDelete(&TestUser{ID: 5}).WherePK().Build()
			},
			wantContain: []string{
				`UPDATE "users" SET "deleted_at" = DATETIME('now')`,
				`WHERE "id" = ?`,
			},
			notContain: []string{"DELETE"},
			wantArgs:   []any{int64(5)},
		},
		{
			name: "Force delete user by PK",
			build: func() (string, []any, error) {
				return db.NewDelete(&TestUser{ID: 5}).WherePK().ForceDelete().Build()
			},
			wantSQL:  `DELETE FROM "users" WHERE "id" = ?`,
			wantArgs: []any{int64(5)},
		},
		{
			name: "Delete post with returning",
			build: func() (string, []any, error) {
				return db.NewDelete(&TestPost{ID: 9}).
					WherePK().
					Returning("id").
					Build()
			},
			wantSQL:  `DELETE FROM "posts" WHERE "id" = ? RETURNING "id"`,
			wantArgs: []any{int64(9)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql, args, err := tt.build()
			require.NoError(t, err)

			if tt.wantSQL != "" {
				assert.Equal(t, tt.wantSQL, sql)
			}
			for _, s := range tt.wantContain {
				assert.Contains(t, sql, s)
			}
			for _, s := range tt.notContain {
				assert.NotContains(t, sql, s)
			}
			assert.Equal(t, tt.wantArgs, args)
		})
	}
}

// =========================================================================
// GROVE ADAPTER METHOD TESTS
// =========================================================================

func TestTursoDB_GroveSelect(t *testing.T) {
	db := newTestDB()
	result := db.GroveSelect((*TestUser)(nil))
	assert.NotNil(t, result, "GroveSelect should return non-nil")
}

func TestTursoDB_GroveInsert(t *testing.T) {
	db := newTestDB()
	result := db.GroveInsert(&TestPost{Title: "Test", UserID: 1})
	assert.NotNil(t, result, "GroveInsert should return non-nil")
}

func TestTursoDB_GroveUpdate(t *testing.T) {
	db := newTestDB()
	result := db.GroveUpdate(&TestUser{ID: 1, Name: "Alice"})
	assert.NotNil(t, result, "GroveUpdate should return non-nil")
}

func TestTursoDB_GroveDelete(t *testing.T) {
	db := newTestDB()
	result := db.GroveDelete(&TestPost{ID: 1})
	assert.NotNil(t, result, "GroveDelete should return non-nil")
}

// Verify that GroveSelect returns the same type as NewSelect.
func TestTursoDB_GroveSelect_MatchesNewSelect(t *testing.T) {
	db := newTestDB()

	groveResult := db.GroveSelect((*TestPost)(nil))
	directResult := db.NewSelect((*TestPost)(nil))

	// Both should be *SelectQuery.
	_, groveOk := groveResult.(*SelectQuery)
	assert.True(t, groveOk, "GroveSelect should return *SelectQuery")

	// Both should produce equivalent SQL.
	groveSQ := groveResult.(*SelectQuery)
	directSQ := directResult

	groveSQL, groveArgs, err1 := groveSQ.Build()
	directSQL, directArgs, err2 := directSQ.Build()

	require.NoError(t, err1)
	require.NoError(t, err2)
	assert.Equal(t, directSQL, groveSQL, "GroveSelect and NewSelect should produce identical SQL")
	assert.Equal(t, directArgs, groveArgs)
}

// Verify that GroveInsert returns the same type as NewInsert.
func TestTursoDB_GroveInsert_MatchesNewInsert(t *testing.T) {
	db := newTestDB()
	post := &TestPost{Title: "X", UserID: 5}

	groveResult := db.GroveInsert(post)
	_, ok := groveResult.(*InsertQuery)
	assert.True(t, ok, "GroveInsert should return *InsertQuery")
}

// Verify that GroveUpdate returns the same type as NewUpdate.
func TestTursoDB_GroveUpdate_MatchesNewUpdate(t *testing.T) {
	db := newTestDB()
	user := &TestUser{ID: 1, Name: "Alice"}

	groveResult := db.GroveUpdate(user)
	_, ok := groveResult.(*UpdateQuery)
	assert.True(t, ok, "GroveUpdate should return *UpdateQuery")
}

// Verify that GroveDelete returns the same type as NewDelete.
func TestTursoDB_GroveDelete_MatchesNewDelete(t *testing.T) {
	db := newTestDB()
	post := &TestPost{ID: 1}

	groveResult := db.GroveDelete(post)
	_, ok := groveResult.(*DeleteQuery)
	assert.True(t, ok, "GroveDelete should return *DeleteQuery")
}
