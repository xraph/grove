package mongodriver

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/xraph/grove"
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

// TestArticle is a simpler model for insert testing.
type TestArticle struct {
	grove.BaseModel `grove:"table:articles"`
	Title           string `grove:"title,notnull"`
	Body            string `grove:"body"`
	AuthorID        int64  `grove:"author_id,notnull"`
}

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

func newTestMongoDB() *MongoDB {
	return &MongoDB{}
}

// =========================================================================
// FIND QUERY TESTS
// =========================================================================

func TestFindQuery_BasicFromModel(t *testing.T) {
	db := newTestMongoDB()

	q := db.NewFind((*TestUser)(nil))
	assert.NotNil(t, q)
	assert.Equal(t, "users", q.GetCollection())
	assert.Empty(t, q.GetFilter())
}

func TestFindQuery_PostCollectionName(t *testing.T) {
	db := newTestMongoDB()

	q := db.NewFind((*TestPost)(nil))
	assert.Equal(t, "posts", q.GetCollection())
}

func TestFindQuery_Filter(t *testing.T) {
	db := newTestMongoDB()

	q := db.NewFind((*TestUser)(nil)).
		Filter(bson.M{"role": "admin"})

	assert.Equal(t, bson.M{"role": "admin"}, q.GetFilter())
}

func TestFindQuery_FilterMerge(t *testing.T) {
	db := newTestMongoDB()

	q := db.NewFind((*TestUser)(nil)).
		Filter(bson.M{"role": "admin"}).
		Filter(bson.M{"name": "Alice"})

	expected := bson.M{"role": "admin", "name": "Alice"}
	assert.Equal(t, expected, q.GetFilter())
}

func TestFindQuery_Sort(t *testing.T) {
	db := newTestMongoDB()

	sortDoc := bson.D{{Key: "name", Value: 1}, {Key: "created_at", Value: -1}}
	q := db.NewFind((*TestUser)(nil)).Sort(sortDoc)

	assert.Equal(t, sortDoc, q.GetSort())
}

func TestFindQuery_Projection(t *testing.T) {
	db := newTestMongoDB()

	proj := bson.M{"name": 1, "email": 1, "_id": 0}
	q := db.NewFind((*TestUser)(nil)).Project(proj)

	assert.Equal(t, proj, q.GetProjection())
}

func TestFindQuery_Limit(t *testing.T) {
	db := newTestMongoDB()

	q := db.NewFind((*TestUser)(nil)).Limit(10)
	assert.Equal(t, int64(10), q.GetLimit())
}

func TestFindQuery_Skip(t *testing.T) {
	db := newTestMongoDB()

	q := db.NewFind((*TestUser)(nil)).Skip(20)
	assert.Equal(t, int64(20), q.GetSkip())
}

func TestFindQuery_CombinedFeatures(t *testing.T) {
	db := newTestMongoDB()

	q := db.NewFind((*TestUser)(nil)).
		Filter(bson.M{"role": "admin"}).
		Sort(bson.D{{Key: "name", Value: 1}}).
		Project(bson.M{"name": 1, "email": 1}).
		Limit(5).
		Skip(10)

	assert.Equal(t, bson.M{"role": "admin"}, q.GetFilter())
	assert.Equal(t, bson.D{{Key: "name", Value: 1}}, q.GetSort())
	assert.Equal(t, bson.M{"name": 1, "email": 1}, q.GetProjection())
	assert.Equal(t, int64(5), q.GetLimit())
	assert.Equal(t, int64(10), q.GetSkip())
}

func TestFindQuery_CollectionOverride(t *testing.T) {
	db := newTestMongoDB()

	q := db.NewFind((*TestUser)(nil)).Collection("custom_users")
	assert.Equal(t, "custom_users", q.GetCollection())
}

func TestFindQuery_ComplexFilter(t *testing.T) {
	db := newTestMongoDB()

	q := db.NewFind((*TestUser)(nil)).
		Filter(bson.M{
			"$or": bson.A{
				bson.M{"role": "admin"},
				bson.M{"role": "superadmin"},
			},
		})

	filter := q.GetFilter()
	assert.Contains(t, filter, "$or")
}

// =========================================================================
// INSERT QUERY TESTS
// =========================================================================

func TestInsertQuery_SingleDocument(t *testing.T) {
	db := newTestMongoDB()

	article := &TestArticle{
		Title:    "Hello World",
		Body:     "This is a test",
		AuthorID: 42,
	}

	q := db.NewInsert(article)
	assert.Equal(t, "articles", q.GetCollection())

	doc, err := q.BuildDoc()
	require.NoError(t, err)

	assert.Equal(t, "Hello World", doc["title"])
	assert.Equal(t, "This is a test", doc["body"])
	assert.Equal(t, int64(42), doc["author_id"])
}

func TestInsertQuery_SingleDocAutoIncrementExcluded(t *testing.T) {
	db := newTestMongoDB()

	post := &TestPost{
		Title:  "Test Post",
		UserID: 5,
	}

	q := db.NewInsert(post)
	doc, err := q.BuildDoc()
	require.NoError(t, err)

	// ID is autoincrement, should be excluded.
	_, hasID := doc["id"]
	assert.False(t, hasID, "autoincrement field 'id' should be excluded from insert")
	_, hasMongoID := doc["_id"]
	assert.False(t, hasMongoID, "zero-value PK '_id' should be excluded from insert")

	assert.Equal(t, "Test Post", doc["title"])
	assert.Equal(t, int64(5), doc["user_id"])
}

func TestInsertQuery_BulkInsert(t *testing.T) {
	db := newTestMongoDB()

	articles := &[]TestArticle{
		{Title: "First", Body: "Body 1", AuthorID: 1},
		{Title: "Second", Body: "Body 2", AuthorID: 2},
		{Title: "Third", Body: "Body 3", AuthorID: 1},
	}

	q := db.NewInsert(articles)
	docs, err := q.BuildDocs()
	require.NoError(t, err)
	require.Len(t, docs, 3)

	assert.Equal(t, "First", docs[0]["title"])
	assert.Equal(t, "Second", docs[1]["title"])
	assert.Equal(t, "Third", docs[2]["title"])
	assert.Equal(t, int64(1), docs[0]["author_id"])
	assert.Equal(t, int64(2), docs[1]["author_id"])
	assert.Equal(t, int64(1), docs[2]["author_id"])
}

func TestInsertQuery_CollectionOverride(t *testing.T) {
	db := newTestMongoDB()

	article := &TestArticle{Title: "Test", Body: "Body", AuthorID: 1}
	q := db.NewInsert(article).Collection("custom_articles")
	assert.Equal(t, "custom_articles", q.GetCollection())
}

// =========================================================================
// UPDATE QUERY TESTS
// =========================================================================

func TestUpdateQuery_SetField(t *testing.T) {
	db := newTestMongoDB()

	q := db.NewUpdate((*TestUser)(nil)).
		Filter(bson.M{"_id": "abc123"}).
		Set("name", "UpdatedAlice").
		Set("role", "moderator")

	assert.Equal(t, "users", q.GetCollection())
	assert.Equal(t, bson.M{"_id": "abc123"}, q.GetFilter())

	updateDoc := q.GetUpdate()
	setDoc, ok := updateDoc["$set"].(bson.M)
	require.True(t, ok, "update should contain $set")
	assert.Equal(t, "UpdatedAlice", setDoc["name"])
	assert.Equal(t, "moderator", setDoc["role"])
}

func TestUpdateQuery_FilterMerge(t *testing.T) {
	db := newTestMongoDB()

	q := db.NewUpdate((*TestUser)(nil)).
		Filter(bson.M{"role": "admin"}).
		Filter(bson.M{"active": true}).
		Set("role", "superadmin")

	expected := bson.M{"role": "admin", "active": true}
	assert.Equal(t, expected, q.GetFilter())
}

func TestUpdateQuery_SetUpdate(t *testing.T) {
	db := newTestMongoDB()

	q := db.NewUpdate((*TestUser)(nil)).
		Filter(bson.M{"_id": "abc123"}).
		SetUpdate(bson.M{
			"$inc":  bson.M{"login_count": 1},
			"$push": bson.M{"tags": "active"},
		})

	updateDoc := q.GetUpdate()
	assert.Contains(t, updateDoc, "$inc")
	assert.Contains(t, updateDoc, "$push")
}

func TestUpdateQuery_Upsert(t *testing.T) {
	db := newTestMongoDB()

	q := db.NewUpdate((*TestUser)(nil)).
		Filter(bson.M{"email": "alice@example.com"}).
		Set("name", "Alice").
		Upsert()

	assert.True(t, q.IsUpsert())
}

func TestUpdateQuery_Many(t *testing.T) {
	db := newTestMongoDB()

	q := db.NewUpdate((*TestUser)(nil)).
		Filter(bson.M{"role": "inactive"}).
		Set("role", "archived").
		Many()

	assert.True(t, q.IsMany())
}

func TestUpdateQuery_CollectionOverride(t *testing.T) {
	db := newTestMongoDB()

	q := db.NewUpdate((*TestUser)(nil)).Collection("custom_users")
	assert.Equal(t, "custom_users", q.GetCollection())
}

// =========================================================================
// DELETE QUERY TESTS
// =========================================================================

func TestDeleteQuery_BasicFilter(t *testing.T) {
	db := newTestMongoDB()

	q := db.NewDelete((*TestPost)(nil)).
		Filter(bson.M{"user_id": int64(42)})

	assert.Equal(t, "posts", q.GetCollection())
	assert.Equal(t, bson.M{"user_id": int64(42)}, q.GetFilter())
	assert.False(t, q.IsMany())
}

func TestDeleteQuery_Many(t *testing.T) {
	db := newTestMongoDB()

	q := db.NewDelete((*TestPost)(nil)).
		Filter(bson.M{"user_id": int64(42)}).
		Many()

	assert.True(t, q.IsMany())
}

func TestDeleteQuery_FilterMerge(t *testing.T) {
	db := newTestMongoDB()

	q := db.NewDelete((*TestPost)(nil)).
		Filter(bson.M{"user_id": int64(5)}).
		Filter(bson.M{"title": "Old Post"})

	expected := bson.M{"user_id": int64(5), "title": "Old Post"}
	assert.Equal(t, expected, q.GetFilter())
}

func TestDeleteQuery_CollectionOverride(t *testing.T) {
	db := newTestMongoDB()

	q := db.NewDelete((*TestPost)(nil)).Collection("custom_posts")
	assert.Equal(t, "custom_posts", q.GetCollection())
}

// =========================================================================
// AGGREGATE QUERY TESTS
// =========================================================================

func TestAggregateQuery_Match(t *testing.T) {
	db := newTestMongoDB()

	q := db.NewAggregate("users").
		Match(bson.M{"role": "admin"})

	pipeline := q.GetPipeline()
	require.Len(t, pipeline, 1)

	stage, ok := pipeline[0].(bson.M)
	require.True(t, ok)
	assert.Contains(t, stage, "$match")
}

func TestAggregateQuery_Group(t *testing.T) {
	db := newTestMongoDB()

	q := db.NewAggregate("users").
		Group(bson.M{
			"_id":   "$role",
			"count": bson.M{"$sum": 1},
		})

	pipeline := q.GetPipeline()
	require.Len(t, pipeline, 1)

	stage, ok := pipeline[0].(bson.M)
	require.True(t, ok)
	assert.Contains(t, stage, "$group")
}

func TestAggregateQuery_Sort(t *testing.T) {
	db := newTestMongoDB()

	q := db.NewAggregate("users").
		Sort(bson.D{{Key: "count", Value: -1}})

	pipeline := q.GetPipeline()
	require.Len(t, pipeline, 1)

	stage, ok := pipeline[0].(bson.M)
	require.True(t, ok)
	assert.Contains(t, stage, "$sort")
}

func TestAggregateQuery_Project(t *testing.T) {
	db := newTestMongoDB()

	q := db.NewAggregate("users").
		Project(bson.M{"name": 1, "_id": 0})

	pipeline := q.GetPipeline()
	require.Len(t, pipeline, 1)

	stage, ok := pipeline[0].(bson.M)
	require.True(t, ok)
	assert.Contains(t, stage, "$project")
}

func TestAggregateQuery_Unwind(t *testing.T) {
	db := newTestMongoDB()

	q := db.NewAggregate("posts").
		Unwind("$tags")

	pipeline := q.GetPipeline()
	require.Len(t, pipeline, 1)

	stage, ok := pipeline[0].(bson.M)
	require.True(t, ok)
	assert.Contains(t, stage, "$unwind")
	assert.Equal(t, "$tags", stage["$unwind"])
}

func TestAggregateQuery_Lookup(t *testing.T) {
	db := newTestMongoDB()

	q := db.NewAggregate("posts").
		Lookup(bson.M{
			"from":         "users",
			"localField":   "user_id",
			"foreignField": "_id",
			"as":           "author",
		})

	pipeline := q.GetPipeline()
	require.Len(t, pipeline, 1)

	stage, ok := pipeline[0].(bson.M)
	require.True(t, ok)
	assert.Contains(t, stage, "$lookup")
}

func TestAggregateQuery_LimitAndSkip(t *testing.T) {
	db := newTestMongoDB()

	q := db.NewAggregate("users").
		Skip(10).
		Limit(5)

	pipeline := q.GetPipeline()
	require.Len(t, pipeline, 2)

	skipStage, ok := pipeline[0].(bson.M)
	require.True(t, ok)
	assert.Equal(t, int64(10), skipStage["$skip"])

	limitStage, ok := pipeline[1].(bson.M)
	require.True(t, ok)
	assert.Equal(t, int64(5), limitStage["$limit"])
}

func TestAggregateQuery_FullPipeline(t *testing.T) {
	db := newTestMongoDB()

	q := db.NewAggregate("orders").
		Match(bson.M{"status": "completed"}).
		Group(bson.M{
			"_id":   "$customer_id",
			"total": bson.M{"$sum": "$amount"},
		}).
		Sort(bson.D{{Key: "total", Value: -1}}).
		Limit(10)

	pipeline := q.GetPipeline()
	require.Len(t, pipeline, 4)

	// Verify stage order.
	assert.Contains(t, pipeline[0].(bson.M), "$match")
	assert.Contains(t, pipeline[1].(bson.M), "$group")
	assert.Contains(t, pipeline[2].(bson.M), "$sort")
	assert.Contains(t, pipeline[3].(bson.M), "$limit")
}

func TestAggregateQuery_CustomStage(t *testing.T) {
	db := newTestMongoDB()

	q := db.NewAggregate("users").
		Stage(bson.M{
			"$addFields": bson.M{
				"fullName": bson.M{"$concat": bson.A{"$firstName", " ", "$lastName"}},
			},
		})

	pipeline := q.GetPipeline()
	require.Len(t, pipeline, 1)

	stage, ok := pipeline[0].(bson.M)
	require.True(t, ok)
	assert.Contains(t, stage, "$addFields")
}

func TestAggregateQuery_CollectionName(t *testing.T) {
	db := newTestMongoDB()

	q := db.NewAggregate("my_collection")
	assert.Equal(t, "my_collection", q.GetCollection())
}

// =========================================================================
// GROVE ADAPTER METHOD TESTS
// =========================================================================

func TestMongoDB_GroveSelect(t *testing.T) {
	db := newTestMongoDB()
	result := db.GroveSelect((*TestUser)(nil))
	assert.NotNil(t, result, "GroveSelect should return non-nil")
}

func TestMongoDB_GroveInsert(t *testing.T) {
	db := newTestMongoDB()
	result := db.GroveInsert(&TestPost{Title: "Test", UserID: 1})
	assert.NotNil(t, result, "GroveInsert should return non-nil")
}

func TestMongoDB_GroveUpdate(t *testing.T) {
	db := newTestMongoDB()
	result := db.GroveUpdate(&TestUser{ID: 1, Name: "Alice"})
	assert.NotNil(t, result, "GroveUpdate should return non-nil")
}

func TestMongoDB_GroveDelete(t *testing.T) {
	db := newTestMongoDB()
	result := db.GroveDelete(&TestPost{ID: 1})
	assert.NotNil(t, result, "GroveDelete should return non-nil")
}

// Verify that GroveSelect returns the same type as NewFind.
func TestMongoDB_GroveSelect_MatchesNewFind(t *testing.T) {
	db := newTestMongoDB()

	groveResult := db.GroveSelect((*TestPost)(nil))
	directResult := db.NewFind((*TestPost)(nil))

	_, groveOk := groveResult.(*FindQuery)
	assert.True(t, groveOk, "GroveSelect should return *FindQuery")

	groveFQ := groveResult.(*FindQuery)
	assert.Equal(t, directResult.GetCollection(), groveFQ.GetCollection(),
		"GroveSelect and NewFind should produce equivalent collection names")
}

// Verify that GroveInsert returns the same type as NewInsert.
func TestMongoDB_GroveInsert_MatchesNewInsert(t *testing.T) {
	db := newTestMongoDB()
	post := &TestPost{Title: "X", UserID: 5}

	groveResult := db.GroveInsert(post)
	_, ok := groveResult.(*InsertQuery)
	assert.True(t, ok, "GroveInsert should return *InsertQuery")
}

// Verify that GroveUpdate returns the same type as NewUpdate.
func TestMongoDB_GroveUpdate_MatchesNewUpdate(t *testing.T) {
	db := newTestMongoDB()
	user := &TestUser{ID: 1, Name: "Alice"}

	groveResult := db.GroveUpdate(user)
	_, ok := groveResult.(*UpdateQuery)
	assert.True(t, ok, "GroveUpdate should return *UpdateQuery")
}

// Verify that GroveDelete returns the same type as NewDelete.
func TestMongoDB_GroveDelete_MatchesNewDelete(t *testing.T) {
	db := newTestMongoDB()
	post := &TestPost{ID: 1}

	groveResult := db.GroveDelete(post)
	_, ok := groveResult.(*DeleteQuery)
	assert.True(t, ok, "GroveDelete should return *DeleteQuery")
}

// =========================================================================
// MODEL RESOLUTION TESTS
// =========================================================================

func TestResolveTable_StructPointer(t *testing.T) {
	table, err := resolveTable((*TestUser)(nil))
	require.NoError(t, err)
	assert.Equal(t, "users", table.Name)
}

func TestResolveTable_SlicePointer(t *testing.T) {
	table, err := resolveTable(&[]TestPost{})
	require.NoError(t, err)
	assert.Equal(t, "posts", table.Name)
}

func TestResolveTable_NilModel(t *testing.T) {
	_, err := resolveTable(nil)
	assert.Error(t, err)
}

// =========================================================================
// STRUCT TO MAP CONVERSION TESTS
// =========================================================================

func TestStructToMapInsert_ExcludesAutoIncrement(t *testing.T) {
	table, err := resolveTable((*TestPost)(nil))
	require.NoError(t, err)

	post := &TestPost{ID: 0, Title: "Test", UserID: 5}
	doc, err := structToMapInsert(post, table)
	require.NoError(t, err)

	// ID is autoincrement, should be excluded.
	_, hasID := doc["id"]
	assert.False(t, hasID)
	_, hasMongoID := doc["_id"]
	assert.False(t, hasMongoID)

	assert.Equal(t, "Test", doc["title"])
	assert.Equal(t, int64(5), doc["user_id"])
}

func TestStructToUpdateMap_ExcludesPK(t *testing.T) {
	table, err := resolveTable((*TestPost)(nil))
	require.NoError(t, err)

	post := &TestPost{ID: 10, Title: "Updated", UserID: 7}
	doc, err := structToUpdateMap(post, table)
	require.NoError(t, err)

	// PK (id) should be excluded from update map.
	_, hasID := doc["id"]
	assert.False(t, hasID)

	assert.Equal(t, "Updated", doc["title"])
	assert.Equal(t, int64(7), doc["user_id"])
}

// =========================================================================
// FIND QUERY TABLE-DRIVEN TESTS
// =========================================================================

func TestFindQuery_Variants(t *testing.T) {
	db := newTestMongoDB()

	tests := []struct {
		name       string
		build      func() *FindQuery
		wantColl   string
		wantFilter bson.M
		wantLimit  int64
		wantSkip   int64
	}{
		{
			name: "Post find all",
			build: func() *FindQuery {
				return db.NewFind((*TestPost)(nil))
			},
			wantColl:   "posts",
			wantFilter: bson.M{},
			wantLimit:  0,
			wantSkip:   0,
		},
		{
			name: "User find with filter and limit",
			build: func() *FindQuery {
				return db.NewFind((*TestUser)(nil)).
					Filter(bson.M{"role": "admin"}).
					Limit(25)
			},
			wantColl:   "users",
			wantFilter: bson.M{"role": "admin"},
			wantLimit:  25,
			wantSkip:   0,
		},
		{
			name: "Post find with skip",
			build: func() *FindQuery {
				return db.NewFind((*TestPost)(nil)).
					Filter(bson.M{"user_id": int64(7)}).
					Skip(10)
			},
			wantColl:   "posts",
			wantFilter: bson.M{"user_id": int64(7)},
			wantLimit:  0,
			wantSkip:   10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := tt.build()
			assert.Equal(t, tt.wantColl, q.GetCollection())
			assert.Equal(t, tt.wantFilter, q.GetFilter())
			assert.Equal(t, tt.wantLimit, q.GetLimit())
			assert.Equal(t, tt.wantSkip, q.GetSkip())
		})
	}
}

// =========================================================================
// UPDATE QUERY TABLE-DRIVEN TESTS
// =========================================================================

func TestUpdateQuery_Variants(t *testing.T) {
	db := newTestMongoDB()

	tests := []struct {
		name       string
		build      func() *UpdateQuery
		wantColl   string
		wantFilter bson.M
		wantUpsert bool
		wantMany   bool
	}{
		{
			name: "Simple set with filter",
			build: func() *UpdateQuery {
				return db.NewUpdate((*TestUser)(nil)).
					Filter(bson.M{"_id": "abc"}).
					Set("name", "Bob")
			},
			wantColl:   "users",
			wantFilter: bson.M{"_id": "abc"},
			wantUpsert: false,
			wantMany:   false,
		},
		{
			name: "Upsert",
			build: func() *UpdateQuery {
				return db.NewUpdate((*TestUser)(nil)).
					Filter(bson.M{"email": "bob@test.com"}).
					Set("name", "Bob").
					Upsert()
			},
			wantColl:   "users",
			wantFilter: bson.M{"email": "bob@test.com"},
			wantUpsert: true,
			wantMany:   false,
		},
		{
			name: "Update many",
			build: func() *UpdateQuery {
				return db.NewUpdate((*TestPost)(nil)).
					Filter(bson.M{"user_id": int64(5)}).
					Set("title", "Archived").
					Many()
			},
			wantColl:   "posts",
			wantFilter: bson.M{"user_id": int64(5)},
			wantUpsert: false,
			wantMany:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := tt.build()
			assert.Equal(t, tt.wantColl, q.GetCollection())
			assert.Equal(t, tt.wantFilter, q.GetFilter())
			assert.Equal(t, tt.wantUpsert, q.IsUpsert())
			assert.Equal(t, tt.wantMany, q.IsMany())
		})
	}
}

// =========================================================================
// DELETE QUERY TABLE-DRIVEN TESTS
// =========================================================================

func TestDeleteQuery_Variants(t *testing.T) {
	db := newTestMongoDB()

	tests := []struct {
		name       string
		build      func() *DeleteQuery
		wantColl   string
		wantFilter bson.M
		wantMany   bool
	}{
		{
			name: "Delete one by ID",
			build: func() *DeleteQuery {
				return db.NewDelete((*TestPost)(nil)).
					Filter(bson.M{"_id": "xyz"})
			},
			wantColl:   "posts",
			wantFilter: bson.M{"_id": "xyz"},
			wantMany:   false,
		},
		{
			name: "Delete many by filter",
			build: func() *DeleteQuery {
				return db.NewDelete((*TestPost)(nil)).
					Filter(bson.M{"user_id": int64(42)}).
					Many()
			},
			wantColl:   "posts",
			wantFilter: bson.M{"user_id": int64(42)},
			wantMany:   true,
		},
		{
			name: "Delete user by role",
			build: func() *DeleteQuery {
				return db.NewDelete((*TestUser)(nil)).
					Filter(bson.M{"role": "banned"}).
					Many()
			},
			wantColl:   "users",
			wantFilter: bson.M{"role": "banned"},
			wantMany:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := tt.build()
			assert.Equal(t, tt.wantColl, q.GetCollection())
			assert.Equal(t, tt.wantFilter, q.GetFilter())
			assert.Equal(t, tt.wantMany, q.IsMany())
		})
	}
}
