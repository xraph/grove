package esdriver

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

// TestEvent has a non-autoincrement string PK for docID extraction testing.
type TestEvent struct {
	grove.BaseModel `grove:"table:events"`
	EventID         string `grove:"event_id,pk"`
	Type            string `grove:"type,notnull"`
}

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

func newTestElasticDB() *ElasticDB {
	return &ElasticDB{}
}

// =========================================================================
// SEARCH QUERY TESTS
// =========================================================================

func TestSearchQuery_BasicFromModel(t *testing.T) {
	db := newTestElasticDB()

	q := db.NewSearch((*TestUser)(nil))
	assert.NotNil(t, q)
	assert.Equal(t, "users", q.GetIndex())
	assert.Nil(t, q.GetQuery())
}

func TestSearchQuery_PostIndexName(t *testing.T) {
	db := newTestElasticDB()

	q := db.NewSearch((*TestPost)(nil))
	assert.Equal(t, "posts", q.GetIndex())
}

func TestSearchQuery_Match(t *testing.T) {
	db := newTestElasticDB()

	q := db.NewSearch((*TestUser)(nil)).
		Match("name", "alice")

	query := q.GetQuery()
	assert.Contains(t, query, "match")
}

func TestSearchQuery_MatchPhrase(t *testing.T) {
	db := newTestElasticDB()

	q := db.NewSearch((*TestUser)(nil)).
		MatchPhrase("name", "alice smith")

	query := q.GetQuery()
	assert.Contains(t, query, "match_phrase")
}

func TestSearchQuery_Term(t *testing.T) {
	db := newTestElasticDB()

	q := db.NewSearch((*TestUser)(nil)).
		Term("role", "admin")

	query := q.GetQuery()
	assert.Contains(t, query, "term")
}

func TestSearchQuery_Terms(t *testing.T) {
	db := newTestElasticDB()

	q := db.NewSearch((*TestUser)(nil)).
		Terms("role", "admin", "moderator")

	query := q.GetQuery()
	assert.Contains(t, query, "terms")
}

func TestSearchQuery_Range(t *testing.T) {
	db := newTestElasticDB()

	q := db.NewSearch((*TestUser)(nil)).
		Range("created_at", RangeOpts{GTE: "2024-01-01", LT: "2025-01-01"})

	query := q.GetQuery()
	assert.Contains(t, query, "range")
}

func TestSearchQuery_Exists(t *testing.T) {
	db := newTestElasticDB()

	q := db.NewSearch((*TestUser)(nil)).
		Exists("email")

	query := q.GetQuery()
	assert.Contains(t, query, "exists")
}

func TestSearchQuery_RawQuery(t *testing.T) {
	db := newTestElasticDB()

	rawQ := M{"wildcard": M{"name": M{"value": "ali*"}}}
	q := db.NewSearch((*TestUser)(nil)).
		RawQuery(rawQ)

	assert.Equal(t, rawQ, q.GetQuery())
}

func TestSearchQuery_Bool(t *testing.T) {
	db := newTestElasticDB()

	q := db.NewSearch((*TestUser)(nil)).
		Bool(func(b *BoolQuery) {
			b.Must(M{"match": M{"name": "alice"}})
			b.Filter(M{"term": M{"role": "admin"}})
		})

	query := q.GetQuery()
	assert.Contains(t, query, "bool")
}

func TestSearchQuery_BoolComplex(t *testing.T) {
	db := newTestElasticDB()

	q := db.NewSearch((*TestUser)(nil)).
		Bool(func(b *BoolQuery) {
			b.Must(M{"match": M{"name": "alice"}})
			b.Should(M{"term": M{"role": "admin"}})
			b.Should(M{"term": M{"role": "moderator"}})
			b.MustNot(M{"term": M{"role": "banned"}})
			b.MinimumShouldMatch(1)
		})

	query := q.GetQuery()
	boolQ, ok := query["bool"].(M)
	require.True(t, ok, "query should contain bool")
	assert.Contains(t, boolQ, "must")
	assert.Contains(t, boolQ, "should")
	assert.Contains(t, boolQ, "must_not")
	assert.Contains(t, boolQ, "minimum_should_match")
}

func TestSearchQuery_Sort(t *testing.T) {
	db := newTestElasticDB()

	q := db.NewSearch((*TestUser)(nil)).
		Sort("name", "asc").
		Sort("created_at", "desc")

	sort := q.GetSort()
	require.Len(t, sort, 2)
	assert.Equal(t, M{"name": M{"order": "asc"}}, sort[0])
	assert.Equal(t, M{"created_at": M{"order": "desc"}}, sort[1])
}

func TestSearchQuery_FromAndSize(t *testing.T) {
	db := newTestElasticDB()

	q := db.NewSearch((*TestUser)(nil)).
		From(20).
		Size(10)

	assert.Equal(t, 20, q.GetFrom())
	assert.Equal(t, 10, q.GetSize())
}

func TestSearchQuery_Source(t *testing.T) {
	db := newTestElasticDB()

	q := db.NewSearch((*TestUser)(nil)).
		Source("name", "email")

	body, err := q.BuildBody()
	require.NoError(t, err)
	assert.Contains(t, body, "_source")
}

func TestSearchQuery_ExcludeSource(t *testing.T) {
	db := newTestElasticDB()

	q := db.NewSearch((*TestUser)(nil)).
		ExcludeSource("deleted_at", "updated_at")

	body, err := q.BuildBody()
	require.NoError(t, err)
	assert.Contains(t, body, "_source")
}

func TestSearchQuery_SearchAfter(t *testing.T) {
	db := newTestElasticDB()

	q := db.NewSearch((*TestUser)(nil)).
		Sort("created_at", "desc").
		SearchAfter("2024-01-01T00:00:00Z", "abc123")

	body, err := q.BuildBody()
	require.NoError(t, err)
	assert.Contains(t, body, "search_after")
}

func TestSearchQuery_TrackTotalHits(t *testing.T) {
	db := newTestElasticDB()

	q := db.NewSearch((*TestUser)(nil)).
		TrackTotalHits()

	body, err := q.BuildBody()
	require.NoError(t, err)
	assert.Contains(t, body, "track_total_hits")
	assert.Equal(t, true, body["track_total_hits"])
}

func TestSearchQuery_IndexOverride(t *testing.T) {
	db := newTestElasticDB()

	q := db.NewSearch((*TestUser)(nil)).Index("custom_users")
	assert.Equal(t, "custom_users", q.GetIndex())
}

func TestSearchQuery_CombinedFeatures(t *testing.T) {
	db := newTestElasticDB()

	q := db.NewSearch((*TestUser)(nil)).
		Match("name", "alice").
		Sort("created_at", "desc").
		From(10).
		Size(5).
		TrackTotalHits()

	assert.Equal(t, "users", q.GetIndex())
	assert.NotNil(t, q.GetQuery())
	assert.Len(t, q.GetSort(), 1)
	assert.Equal(t, 10, q.GetFrom())
	assert.Equal(t, 5, q.GetSize())

	body, err := q.BuildBody()
	require.NoError(t, err)
	assert.Contains(t, body, "query")
	assert.Contains(t, body, "sort")
	assert.Contains(t, body, "from")
	assert.Contains(t, body, "size")
	assert.Contains(t, body, "track_total_hits")
}

func TestSearchQuery_BuildBody_Empty(t *testing.T) {
	db := newTestElasticDB()

	q := db.NewSearch((*TestUser)(nil))
	body, err := q.BuildBody()
	require.NoError(t, err)
	// Empty query should still produce a valid body.
	assert.NotNil(t, body)
}

func TestSearchQuery_MultipleQueries_AutoBool(t *testing.T) {
	db := newTestElasticDB()

	q := db.NewSearch((*TestUser)(nil)).
		Match("name", "alice").
		Term("role", "admin")

	query := q.GetQuery()
	// Multiple clauses should be wrapped in a bool.must query.
	assert.Contains(t, query, "bool")
}

func TestSearchQuery_Highlight(t *testing.T) {
	db := newTestElasticDB()

	h := M{"fields": M{"name": M{}}}
	q := db.NewSearch((*TestUser)(nil)).
		Highlight(h)

	body, err := q.BuildBody()
	require.NoError(t, err)
	assert.Contains(t, body, "highlight")
}

func TestSearchQuery_Aggs(t *testing.T) {
	db := newTestElasticDB()

	q := db.NewSearch((*TestUser)(nil)).
		Aggs(M{"roles": M{"terms": M{"field": "role"}}})

	body, err := q.BuildBody()
	require.NoError(t, err)
	assert.Contains(t, body, "aggs")
}

// =========================================================================
// INSERT QUERY TESTS
// =========================================================================

func TestInsertQuery_SingleDocument(t *testing.T) {
	db := newTestElasticDB()

	article := &TestArticle{
		Title:    "Hello World",
		Body:     "This is a test",
		AuthorID: 42,
	}

	q := db.NewInsert(article)
	assert.Equal(t, "articles", q.GetIndex())

	doc, err := q.BuildDoc()
	require.NoError(t, err)

	assert.Equal(t, "Hello World", doc["title"])
	assert.Equal(t, "This is a test", doc["body"])
	assert.Equal(t, int64(42), doc["author_id"])
}

func TestInsertQuery_AutoIncrementExcluded(t *testing.T) {
	db := newTestElasticDB()

	post := &TestPost{
		Title:  "Test Post",
		UserID: 5,
	}

	q := db.NewInsert(post)
	doc, err := q.BuildDoc()
	require.NoError(t, err)

	// ID is autoincrement and should be excluded from the document body.
	_, hasID := doc["id"]
	assert.False(t, hasID, "autoincrement field 'id' should be excluded from insert")

	assert.Equal(t, "Test Post", doc["title"])
	assert.Equal(t, int64(5), doc["user_id"])
}

func TestInsertQuery_BulkInsert(t *testing.T) {
	db := newTestElasticDB()

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

func TestInsertQuery_IndexOverride(t *testing.T) {
	db := newTestElasticDB()

	article := &TestArticle{Title: "Test", Body: "Body", AuthorID: 1}
	q := db.NewInsert(article).Index("custom_articles")
	assert.Equal(t, "custom_articles", q.GetIndex())
}

func TestInsertQuery_DocumentID(t *testing.T) {
	db := newTestElasticDB()

	article := &TestArticle{Title: "Test", Body: "Body", AuthorID: 1}
	q := db.NewInsert(article).DocumentID("my-custom-id")

	assert.Equal(t, "my-custom-id", q.docID)
}

// =========================================================================
// UPDATE QUERY TESTS
// =========================================================================

func TestUpdateQuery_SetField(t *testing.T) {
	db := newTestElasticDB()

	q := db.NewUpdate((*TestUser)(nil)).
		Filter(M{"term": M{"_id": "abc123"}}).
		Set("name", "UpdatedAlice").
		Set("role", "moderator")

	assert.Equal(t, "users", q.GetIndex())
	assert.NotNil(t, q.GetFilter())

	doc := q.GetDoc()
	assert.Equal(t, "UpdatedAlice", doc["name"])
	assert.Equal(t, "moderator", doc["role"])
}

func TestUpdateQuery_FilterMerge(t *testing.T) {
	db := newTestElasticDB()

	q := db.NewUpdate((*TestUser)(nil)).
		Filter(M{"term": M{"role": "admin"}}).
		Filter(M{"range": M{"age": M{"gte": 18}}}).
		Set("role", "superadmin")

	filter := q.GetFilter()
	assert.Contains(t, filter, "term")
	assert.Contains(t, filter, "range")
}

func TestUpdateQuery_SetDoc(t *testing.T) {
	db := newTestElasticDB()

	q := db.NewUpdate((*TestUser)(nil)).
		DocumentID("abc123").
		SetDoc(M{"name": "Alice", "role": "admin"})

	doc := q.GetDoc()
	assert.Equal(t, "Alice", doc["name"])
	assert.Equal(t, "admin", doc["role"])
}

func TestUpdateQuery_SetScript(t *testing.T) {
	db := newTestElasticDB()

	q := db.NewUpdate((*TestUser)(nil)).
		DocumentID("abc123").
		SetScript(Script{
			Source: "ctx._source.login_count += params.inc",
			Lang:   "painless",
			Params: M{"inc": 1},
		})

	assert.NotNil(t, q.script)
	assert.Equal(t, "ctx._source.login_count += params.inc", q.script.Source)
	assert.Equal(t, "painless", q.script.Lang)
}

func TestUpdateQuery_Upsert(t *testing.T) {
	db := newTestElasticDB()

	q := db.NewUpdate((*TestUser)(nil)).
		DocumentID("abc123").
		Set("name", "Alice").
		Upsert()

	assert.True(t, q.IsUpsert())
}

func TestUpdateQuery_Many(t *testing.T) {
	db := newTestElasticDB()

	q := db.NewUpdate((*TestUser)(nil)).
		Filter(M{"term": M{"role": "inactive"}}).
		Set("role", "archived").
		Many()

	assert.True(t, q.IsMany())
}

func TestUpdateQuery_IndexOverride(t *testing.T) {
	db := newTestElasticDB()

	q := db.NewUpdate((*TestUser)(nil)).Index("custom_users")
	assert.Equal(t, "custom_users", q.GetIndex())
}

// =========================================================================
// DELETE QUERY TESTS
// =========================================================================

func TestDeleteQuery_BasicFilter(t *testing.T) {
	db := newTestElasticDB()

	q := db.NewDelete((*TestPost)(nil)).
		Filter(M{"term": M{"user_id": int64(42)}})

	assert.Equal(t, "posts", q.GetIndex())
	assert.NotNil(t, q.GetFilter())
	assert.False(t, q.IsMany())
}

func TestDeleteQuery_ByDocumentID(t *testing.T) {
	db := newTestElasticDB()

	q := db.NewDelete((*TestPost)(nil)).
		DocumentID("xyz789")

	assert.Equal(t, "xyz789", q.docID)
}

func TestDeleteQuery_Many(t *testing.T) {
	db := newTestElasticDB()

	q := db.NewDelete((*TestPost)(nil)).
		Filter(M{"term": M{"user_id": int64(42)}}).
		Many()

	assert.True(t, q.IsMany())
}

func TestDeleteQuery_FilterMerge(t *testing.T) {
	db := newTestElasticDB()

	q := db.NewDelete((*TestPost)(nil)).
		Filter(M{"term": M{"user_id": int64(5)}}).
		Filter(M{"term": M{"title": "Old Post"}})

	filter := q.GetFilter()
	assert.Contains(t, filter, "term")
}

func TestDeleteQuery_IndexOverride(t *testing.T) {
	db := newTestElasticDB()

	q := db.NewDelete((*TestPost)(nil)).Index("custom_posts")
	assert.Equal(t, "custom_posts", q.GetIndex())
}

// =========================================================================
// AGGREGATE QUERY TESTS
// =========================================================================

func TestAggregateQuery_Terms(t *testing.T) {
	db := newTestElasticDB()

	q := db.NewAggregate("users").
		Terms("role_breakdown", "role")

	aggs := q.GetAggs()
	assert.Contains(t, aggs, "role_breakdown")
}

func TestAggregateQuery_DateHistogram(t *testing.T) {
	db := newTestElasticDB()

	q := db.NewAggregate("orders").
		DateHistogram("orders_over_time", "created_at", "1d")

	aggs := q.GetAggs()
	assert.Contains(t, aggs, "orders_over_time")
}

func TestAggregateQuery_MetricAggs(t *testing.T) {
	db := newTestElasticDB()

	q := db.NewAggregate("orders").
		Avg("avg_amount", "amount").
		Sum("total_amount", "amount").
		Min("min_amount", "amount").
		Max("max_amount", "amount").
		Cardinality("unique_customers", "customer_id")

	aggs := q.GetAggs()
	assert.Contains(t, aggs, "avg_amount")
	assert.Contains(t, aggs, "total_amount")
	assert.Contains(t, aggs, "min_amount")
	assert.Contains(t, aggs, "max_amount")
	assert.Contains(t, aggs, "unique_customers")
}

func TestAggregateQuery_SubAgg(t *testing.T) {
	db := newTestElasticDB()

	q := db.NewAggregate("orders").
		Terms("by_customer", "customer_id").
		SubAgg("by_customer", M{
			"total_spent": M{"sum": M{"field": "amount"}},
		})

	aggs := q.GetAggs()
	assert.Contains(t, aggs, "by_customer")
	byCustomer, ok := aggs["by_customer"].(M)
	require.True(t, ok)
	assert.Contains(t, byCustomer, "aggs")
}

func TestAggregateQuery_WithQuery(t *testing.T) {
	db := newTestElasticDB()

	q := db.NewAggregate("orders").
		Query(M{"term": M{"status": "completed"}}).
		Terms("by_customer", "customer_id")

	assert.NotNil(t, q.GetQuery())
	assert.NotNil(t, q.GetAggs())
}

func TestAggregateQuery_RawAggs(t *testing.T) {
	db := newTestElasticDB()

	rawAggs := M{
		"price_ranges": M{
			"range": M{
				"field":  "price",
				"ranges": []M{{}, {"from": 50}, {"from": 100}},
			},
		},
	}

	q := db.NewAggregate("products").
		RawAggs(rawAggs)

	aggs := q.GetAggs()
	assert.Contains(t, aggs, "price_ranges")
}

func TestAggregateQuery_BuildBody(t *testing.T) {
	db := newTestElasticDB()

	q := db.NewAggregate("orders").
		Query(M{"term": M{"status": "completed"}}).
		Terms("by_customer", "customer_id").
		Size(0)

	body := q.BuildBody()
	assert.Contains(t, body, "query")
	assert.Contains(t, body, "aggs")
	assert.Equal(t, 0, body["size"])
}

func TestAggregateQuery_IndexName(t *testing.T) {
	db := newTestElasticDB()

	q := db.NewAggregate("my_index")
	assert.Equal(t, "my_index", q.GetIndex())
}

// =========================================================================
// BULK QUERY TESTS
// =========================================================================

func TestBulkQuery_MixedOperations(t *testing.T) {
	db := newTestElasticDB()

	q := db.NewBulk().
		Index("users", "1", M{"name": "Alice"}).
		Create("users", "2", M{"name": "Bob"}).
		Update("users", "3", M{"doc": M{"name": "Charlie Updated"}}).
		Delete("users", "4")

	assert.Len(t, q.actions, 4)
	assert.Equal(t, "index", q.actions[0].op)
	assert.Equal(t, "create", q.actions[1].op)
	assert.Equal(t, "update", q.actions[2].op)
	assert.Equal(t, "delete", q.actions[3].op)
}

func TestBulkQuery_Refresh(t *testing.T) {
	db := newTestElasticDB()

	q := db.NewBulk().
		Index("users", "1", M{"name": "Alice"}).
		Refresh("wait_for")

	assert.Equal(t, "wait_for", q.refresh)
}

// =========================================================================
// GROVE ADAPTER METHOD TESTS
// =========================================================================

func TestElasticDB_GroveSelect(t *testing.T) {
	db := newTestElasticDB()
	result := db.GroveSelect((*TestUser)(nil))
	assert.NotNil(t, result, "GroveSelect should return non-nil")
}

func TestElasticDB_GroveInsert(t *testing.T) {
	db := newTestElasticDB()
	result := db.GroveInsert(&TestPost{Title: "Test", UserID: 1})
	assert.NotNil(t, result, "GroveInsert should return non-nil")
}

func TestElasticDB_GroveUpdate(t *testing.T) {
	db := newTestElasticDB()
	result := db.GroveUpdate(&TestUser{ID: 1, Name: "Alice"})
	assert.NotNil(t, result, "GroveUpdate should return non-nil")
}

func TestElasticDB_GroveDelete(t *testing.T) {
	db := newTestElasticDB()
	result := db.GroveDelete(&TestPost{ID: 1})
	assert.NotNil(t, result, "GroveDelete should return non-nil")
}

func TestElasticDB_GroveSelect_MatchesNewSearch(t *testing.T) {
	db := newTestElasticDB()

	groveResult := db.GroveSelect((*TestPost)(nil))
	directResult := db.NewSearch((*TestPost)(nil))

	_, groveOk := groveResult.(*SearchQuery)
	assert.True(t, groveOk, "GroveSelect should return *SearchQuery")

	groveSQ := groveResult.(*SearchQuery)
	assert.Equal(t, directResult.GetIndex(), groveSQ.GetIndex(),
		"GroveSelect and NewSearch should produce equivalent index names")
}

func TestElasticDB_GroveInsert_MatchesNewInsert(t *testing.T) {
	db := newTestElasticDB()
	post := &TestPost{Title: "X", UserID: 5}

	groveResult := db.GroveInsert(post)
	_, ok := groveResult.(*InsertQuery)
	assert.True(t, ok, "GroveInsert should return *InsertQuery")
}

func TestElasticDB_GroveUpdate_MatchesNewUpdate(t *testing.T) {
	db := newTestElasticDB()
	user := &TestUser{ID: 1, Name: "Alice"}

	groveResult := db.GroveUpdate(user)
	_, ok := groveResult.(*UpdateQuery)
	assert.True(t, ok, "GroveUpdate should return *UpdateQuery")
}

func TestElasticDB_GroveDelete_MatchesNewDelete(t *testing.T) {
	db := newTestElasticDB()
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

func TestIndexName(t *testing.T) {
	table, err := resolveTable((*TestUser)(nil))
	require.NoError(t, err)
	assert.Equal(t, "users", indexName(table))
}

// =========================================================================
// STRUCT TO DOC CONVERSION TESTS
// =========================================================================

func TestStructToDocInsert_ExcludesAutoIncrement(t *testing.T) {
	table, err := resolveTable((*TestPost)(nil))
	require.NoError(t, err)

	post := &TestPost{ID: 0, Title: "Test", UserID: 5}
	doc, docID, err := structToDocInsert(post, table)
	require.NoError(t, err)

	// ID is autoincrement and zero, should be excluded.
	_, hasID := doc["id"]
	assert.False(t, hasID, "autoincrement field 'id' should be excluded from insert doc")

	assert.Empty(t, docID, "zero autoincrement PK should not produce a docID")
	assert.Equal(t, "Test", doc["title"])
	assert.Equal(t, int64(5), doc["user_id"])
}

func TestStructToDocInsert_NonZeroPK(t *testing.T) {
	table, err := resolveTable((*TestEvent)(nil))
	require.NoError(t, err)

	event := &TestEvent{EventID: "evt-42", Type: "click"}
	doc, docID, err := structToDocInsert(event, table)
	require.NoError(t, err)

	// Non-zero, non-autoincrement PK should be extracted as docID.
	assert.Equal(t, "evt-42", docID)
	// PK should NOT be in the document body (ES stores _id outside _source).
	_, hasID := doc["event_id"]
	assert.False(t, hasID)
	assert.Equal(t, "click", doc["type"])
}

func TestStructToDocUpdate_ExcludesPK(t *testing.T) {
	table, err := resolveTable((*TestPost)(nil))
	require.NoError(t, err)

	post := &TestPost{ID: 10, Title: "Updated", UserID: 7}
	doc, err := structToDocUpdate(post, table)
	require.NoError(t, err)

	// PK (id) should be excluded from update map.
	_, hasID := doc["id"]
	assert.False(t, hasID)

	assert.Equal(t, "Updated", doc["title"])
	assert.Equal(t, int64(7), doc["user_id"])
}

// =========================================================================
// SEARCH QUERY TABLE-DRIVEN TESTS
// =========================================================================

func TestSearchQuery_Variants(t *testing.T) {
	db := newTestElasticDB()

	tests := []struct {
		name      string
		build     func() *SearchQuery
		wantIndex string
		wantSize  int
		wantFrom  int
	}{
		{
			name: "Post search all",
			build: func() *SearchQuery {
				return db.NewSearch((*TestPost)(nil))
			},
			wantIndex: "posts",
			wantSize:  -1, // ES default
			wantFrom:  0,
		},
		{
			name: "User search with size",
			build: func() *SearchQuery {
				return db.NewSearch((*TestUser)(nil)).
					Match("role", "admin").
					Size(25)
			},
			wantIndex: "users",
			wantSize:  25,
			wantFrom:  0,
		},
		{
			name: "Post search with from",
			build: func() *SearchQuery {
				return db.NewSearch((*TestPost)(nil)).
					Term("user_id", int64(7)).
					From(10)
			},
			wantIndex: "posts",
			wantSize:  -1, // ES default
			wantFrom:  10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := tt.build()
			assert.Equal(t, tt.wantIndex, q.GetIndex())
			assert.Equal(t, tt.wantSize, q.GetSize())
			assert.Equal(t, tt.wantFrom, q.GetFrom())
		})
	}
}

// =========================================================================
// UPDATE QUERY TABLE-DRIVEN TESTS
// =========================================================================

func TestUpdateQuery_Variants(t *testing.T) {
	db := newTestElasticDB()

	tests := []struct {
		name       string
		build      func() *UpdateQuery
		wantIndex  string
		wantUpsert bool
		wantMany   bool
	}{
		{
			name: "Simple set with doc ID",
			build: func() *UpdateQuery {
				return db.NewUpdate((*TestUser)(nil)).
					DocumentID("abc").
					Set("name", "Bob")
			},
			wantIndex:  "users",
			wantUpsert: false,
			wantMany:   false,
		},
		{
			name: "Upsert",
			build: func() *UpdateQuery {
				return db.NewUpdate((*TestUser)(nil)).
					DocumentID("abc").
					Set("name", "Bob").
					Upsert()
			},
			wantIndex:  "users",
			wantUpsert: true,
			wantMany:   false,
		},
		{
			name: "Update many",
			build: func() *UpdateQuery {
				return db.NewUpdate((*TestPost)(nil)).
					Filter(M{"term": M{"user_id": int64(5)}}).
					Set("title", "Archived").
					Many()
			},
			wantIndex:  "posts",
			wantUpsert: false,
			wantMany:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := tt.build()
			assert.Equal(t, tt.wantIndex, q.GetIndex())
			assert.Equal(t, tt.wantUpsert, q.IsUpsert())
			assert.Equal(t, tt.wantMany, q.IsMany())
		})
	}
}

// =========================================================================
// DELETE QUERY TABLE-DRIVEN TESTS
// =========================================================================

func TestDeleteQuery_Variants(t *testing.T) {
	db := newTestElasticDB()

	tests := []struct {
		name      string
		build     func() *DeleteQuery
		wantIndex string
		wantMany  bool
	}{
		{
			name: "Delete one by ID",
			build: func() *DeleteQuery {
				return db.NewDelete((*TestPost)(nil)).
					DocumentID("xyz")
			},
			wantIndex: "posts",
			wantMany:  false,
		},
		{
			name: "Delete many by filter",
			build: func() *DeleteQuery {
				return db.NewDelete((*TestPost)(nil)).
					Filter(M{"term": M{"user_id": int64(42)}}).
					Many()
			},
			wantIndex: "posts",
			wantMany:  true,
		},
		{
			name: "Delete users by role",
			build: func() *DeleteQuery {
				return db.NewDelete((*TestUser)(nil)).
					Filter(M{"term": M{"role": "banned"}}).
					Many()
			},
			wantIndex: "users",
			wantMany:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := tt.build()
			assert.Equal(t, tt.wantIndex, q.GetIndex())
			assert.Equal(t, tt.wantMany, q.IsMany())
		})
	}
}

// =========================================================================
// BOOL QUERY BUILDER TESTS
// =========================================================================

func TestBoolQuery_Build_MustOnly(t *testing.T) {
	b := &BoolQuery{}
	b.Must(M{"match": M{"name": "alice"}})
	b.Must(M{"term": M{"role": "admin"}})

	result := b.Build()
	boolM, ok := result["bool"].(M)
	require.True(t, ok)
	must, ok := boolM["must"].([]any)
	require.True(t, ok)
	assert.Len(t, must, 2)
}

func TestBoolQuery_Build_AllClauses(t *testing.T) {
	b := &BoolQuery{}
	b.Must(M{"match": M{"name": "alice"}})
	b.Should(M{"term": M{"role": "admin"}})
	b.MustNot(M{"term": M{"status": "banned"}})
	b.Filter(M{"range": M{"age": M{"gte": 18}}})
	b.MinimumShouldMatch(1)

	result := b.Build()
	boolM, ok := result["bool"].(M)
	require.True(t, ok)

	assert.Contains(t, boolM, "must")
	assert.Contains(t, boolM, "should")
	assert.Contains(t, boolM, "must_not")
	assert.Contains(t, boolM, "filter")
	assert.Equal(t, 1, boolM["minimum_should_match"])
}

func TestBoolQuery_Build_Empty(t *testing.T) {
	b := &BoolQuery{}
	result := b.Build()
	boolM, ok := result["bool"].(M)
	require.True(t, ok)
	// Empty bool query should still produce a valid structure.
	assert.NotNil(t, boolM)
}

// =========================================================================
// RANGE OPTS TESTS
// =========================================================================

func TestRangeOpts_AllFields(t *testing.T) {
	db := newTestElasticDB()

	q := db.NewSearch((*TestUser)(nil)).
		Range("created_at", RangeOpts{
			GTE:    "2024-01-01",
			LT:     "2025-01-01",
			Format: "yyyy-MM-dd",
		})

	body, err := q.BuildBody()
	require.NoError(t, err)
	assert.Contains(t, body, "query")
}

// =========================================================================
// PARSE DURATION TESTS
// =========================================================================

func TestParseDuration(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want time.Duration
	}{
		{"one minute", "1m", time.Minute},
		{"five minutes", "5m", 5 * time.Minute},
		{"thirty seconds", "30s", 30 * time.Second},
		{"empty defaults to 1m", "", time.Minute},
		{"invalid defaults to 1m", "invalid", time.Minute},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseDuration(tt.in)
			assert.Equal(t, tt.want, got)
		})
	}
}
