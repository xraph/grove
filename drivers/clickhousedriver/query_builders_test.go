package clickhousedriver

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

// TestEvent has an alias.
type TestEvent struct {
	grove.BaseModel `grove:"table:events,alias:e"`
	ID              int64     `grove:"id,pk"`
	EventType       string    `grove:"event_type,notnull"`
	UserID          int64     `grove:"user_id,notnull"`
	Payload         string    `grove:"payload"`
	CreatedAt       time.Time `grove:"created_at,notnull"`
}

// TestMetric has no alias.
type TestMetric struct {
	grove.BaseModel `grove:"table:metrics"`
	ID              int64   `grove:"id,pk"`
	Name            string  `grove:"name,notnull"`
	Value           float64 `grove:"value,notnull"`
	Timestamp       int64   `grove:"timestamp,notnull"`
}

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

func newTestDB() *ClickHouseDB {
	return &ClickHouseDB{dialect: &ClickHouseDialect{}, registry: schema.NewRegistry()}
}

// =========================================================================
// SELECT QUERY TESTS
// =========================================================================

func TestSelectQuery_BasicAllFields(t *testing.T) {
	db := newTestDB()

	sql, args, err := db.NewSelect((*TestEvent)(nil)).Build()
	require.NoError(t, err)

	expected := "SELECT `e`.`id`, `e`.`event_type`, `e`.`user_id`, `e`.`payload`, `e`.`created_at` FROM `events` AS `e`"
	assert.Equal(t, expected, sql)
	assert.Empty(t, args)
}

func TestSelectQuery_SpecificColumns(t *testing.T) {
	db := newTestDB()

	sql, args, err := db.NewSelect((*TestEvent)(nil)).
		Column("`e`.`event_type`", "`e`.`user_id`").
		Build()
	require.NoError(t, err)

	expected := "SELECT `e`.`event_type`, `e`.`user_id` FROM `events` AS `e`"
	assert.Equal(t, expected, sql)
	assert.Empty(t, args)
}

func TestSelectQuery_WhereClause(t *testing.T) {
	db := newTestDB()

	sql, args, err := db.NewSelect((*TestEvent)(nil)).
		Where("`e`.`event_type` = 'click'").
		Build()
	require.NoError(t, err)

	expected := "SELECT `e`.`id`, `e`.`event_type`, `e`.`user_id`, `e`.`payload`, `e`.`created_at` FROM `events` AS `e` WHERE `e`.`event_type` = 'click'"
	assert.Equal(t, expected, sql)
	assert.Empty(t, args)
}

func TestSelectQuery_WhereWithArgs(t *testing.T) {
	db := newTestDB()

	sql, args, err := db.NewSelect((*TestMetric)(nil)).
		Where("`id` = ?", int64(99)).
		Build()
	require.NoError(t, err)

	assert.Contains(t, sql, "WHERE `id` = ?")
	require.Len(t, args, 1)
	assert.Equal(t, int64(99), args[0])
}

func TestSelectQuery_WherePK(t *testing.T) {
	db := newTestDB()
	event := &TestEvent{ID: 42}

	sql, args, err := db.NewSelect(event).WherePK().Build()
	require.NoError(t, err)

	expected := "SELECT `e`.`id`, `e`.`event_type`, `e`.`user_id`, `e`.`payload`, `e`.`created_at` FROM `events` AS `e` WHERE `e`.`id` = ?"
	assert.Equal(t, expected, sql)
	require.Len(t, args, 1)
	assert.Equal(t, int64(42), args[0])
}

func TestSelectQuery_Final(t *testing.T) {
	db := newTestDB()

	sql, args, err := db.NewSelect((*TestEvent)(nil)).
		Final().
		Build()
	require.NoError(t, err)

	assert.Contains(t, sql, "FROM `events` AS `e` FINAL")
	assert.Empty(t, args)
}

func TestSelectQuery_Sample(t *testing.T) {
	db := newTestDB()

	sql, args, err := db.NewSelect((*TestEvent)(nil)).
		Sample(0.1).
		Build()
	require.NoError(t, err)

	assert.Contains(t, sql, "SAMPLE 0.1")
	assert.Empty(t, args)
}

func TestSelectQuery_Prewhere(t *testing.T) {
	db := newTestDB()

	sql, args, err := db.NewSelect((*TestEvent)(nil)).
		Prewhere("`e`.`event_type` = ?", "click").
		Where("`e`.`user_id` = ?", int64(5)).
		Build()
	require.NoError(t, err)

	assert.Contains(t, sql, "PREWHERE `e`.`event_type` = ?")
	assert.Contains(t, sql, "WHERE `e`.`user_id` = ?")
	require.Len(t, args, 2)
	assert.Equal(t, "click", args[0])
	assert.Equal(t, int64(5), args[1])
}

func TestSelectQuery_FinalSamplePrewhere(t *testing.T) {
	db := newTestDB()

	sql, args, err := db.NewSelect((*TestEvent)(nil)).
		Final().
		Sample(0.5).
		Prewhere("`e`.`event_type` = ?", "view").
		Where("`e`.`user_id` > ?", int64(100)).
		Build()
	require.NoError(t, err)

	// Verify order: FROM table FINAL SAMPLE PREWHERE WHERE
	assert.Contains(t, sql, "FROM `events` AS `e` FINAL SAMPLE 0.5 PREWHERE `e`.`event_type` = ? WHERE `e`.`user_id` > ?")
	require.Len(t, args, 2)
	assert.Equal(t, "view", args[0])
	assert.Equal(t, int64(100), args[1])
}

func TestSelectQuery_Join(t *testing.T) {
	db := newTestDB()

	sql, args, err := db.NewSelect((*TestEvent)(nil)).
		Join("JOIN", "`metrics` AS `m`", "`m`.`id` = `e`.`id`").
		Build()
	require.NoError(t, err)

	assert.Contains(t, sql, "JOIN `metrics` AS `m` ON `m`.`id` = `e`.`id`")
	assert.Empty(t, args)
}

func TestSelectQuery_OrderByGroupByHaving(t *testing.T) {
	db := newTestDB()

	sql, args, err := db.NewSelect((*TestEvent)(nil)).
		Column("`e`.`event_type`", "COUNT(*) AS `cnt`").
		GroupExpr("`e`.`event_type`").
		Having("COUNT(*) > 5").
		OrderExpr("`cnt` DESC").
		Build()
	require.NoError(t, err)

	expected := "SELECT `e`.`event_type`, COUNT(*) AS `cnt` FROM `events` AS `e` GROUP BY `e`.`event_type` HAVING COUNT(*) > 5 ORDER BY `cnt` DESC"
	assert.Equal(t, expected, sql)
	assert.Empty(t, args)
}

func TestSelectQuery_LimitOffset(t *testing.T) {
	db := newTestDB()

	sql, args, err := db.NewSelect((*TestEvent)(nil)).
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

	sql, args, err := db.NewSelect((*TestEvent)(nil)).
		Where("`e`.`event_type` = 'click'").
		BuildCount()
	require.NoError(t, err)

	expected := "SELECT COUNT(*) FROM `events` AS `e` WHERE `e`.`event_type` = 'click'"
	assert.Equal(t, expected, sql)
	assert.Empty(t, args)
}

func TestSelectQuery_MetricBasicAllFields(t *testing.T) {
	db := newTestDB()

	sql, args, err := db.NewSelect((*TestMetric)(nil)).Build()
	require.NoError(t, err)

	// TestMetric has no alias, so columns should not be prefixed.
	expected := "SELECT `id`, `name`, `value`, `timestamp` FROM `metrics`"
	assert.Equal(t, expected, sql)
	assert.Empty(t, args)
}

func TestSelectQuery_ColumnExpr(t *testing.T) {
	db := newTestDB()

	sql, args, err := db.NewSelect((*TestEvent)(nil)).
		ColumnExpr("COUNT(*) AS cnt").
		Build()
	require.NoError(t, err)

	assert.Contains(t, sql, "SELECT COUNT(*) AS cnt FROM")
	assert.Empty(t, args)
}

// =========================================================================
// INSERT QUERY TESTS
// =========================================================================

func TestInsertQuery_BasicFromModel(t *testing.T) {
	db := newTestDB()
	event := &TestEvent{
		ID:        1,
		EventType: "click",
		UserID:    42,
		Payload:   `{"page": "/home"}`,
	}

	sql, args, err := db.NewInsert(event).Build()
	require.NoError(t, err)

	assert.Contains(t, sql, "INSERT INTO `events`")
	assert.Contains(t, sql, "`id`")
	assert.Contains(t, sql, "`event_type`")
	assert.Contains(t, sql, "`user_id`")
	assert.Contains(t, sql, "`payload`")
	assert.Contains(t, sql, "VALUES (?, ?, ?, ?, ?)")
	require.Len(t, args, 5) // id, event_type, user_id, payload, created_at
	assert.Equal(t, int64(1), args[0])
	assert.Equal(t, "click", args[1])
	assert.Equal(t, int64(42), args[2])
}

func TestInsertQuery_ExplicitColumns(t *testing.T) {
	db := newTestDB()
	event := &TestEvent{
		EventType: "view",
		UserID:    10,
	}

	sql, args, err := db.NewInsert(event).
		Column("event_type", "user_id").
		Build()
	require.NoError(t, err)

	assert.Contains(t, sql, "INSERT INTO `events` (`event_type`, `user_id`)")
	assert.Contains(t, sql, "VALUES (?, ?)")
	require.Len(t, args, 2)
	assert.Equal(t, "view", args[0])
	assert.Equal(t, int64(10), args[1])
}

func TestInsertQuery_ExplicitValues(t *testing.T) {
	db := newTestDB()
	metric := &TestMetric{}

	sql, args, err := db.NewInsert(metric).
		Column("name", "value").
		Value("cpu_usage", 85.5).
		Build()
	require.NoError(t, err)

	assert.Contains(t, sql, "INSERT INTO `metrics` (`name`, `value`)")
	assert.Contains(t, sql, "VALUES (?, ?)")
	require.Len(t, args, 2)
	assert.Equal(t, "cpu_usage", args[0])
	assert.Equal(t, 85.5, args[1])
}

func TestInsertQuery_BulkInsert(t *testing.T) {
	db := newTestDB()
	metrics := &[]TestMetric{
		{ID: 1, Name: "cpu", Value: 50.0, Timestamp: 1000},
		{ID: 2, Name: "mem", Value: 75.0, Timestamp: 1001},
		{ID: 3, Name: "disk", Value: 90.0, Timestamp: 1002},
	}

	sql, args, err := db.NewInsert(metrics).Build()
	require.NoError(t, err)

	assert.Contains(t, sql, "INSERT INTO `metrics`")
	// 3 rows x 4 fields = 12 args.
	require.Len(t, args, 12)
	assert.Contains(t, sql, "(?, ?, ?, ?), (?, ?, ?, ?), (?, ?, ?, ?)")
}

func TestInsertQuery_MetricBasic(t *testing.T) {
	db := newTestDB()
	metric := &TestMetric{ID: 1, Name: "latency", Value: 42.5, Timestamp: 12345}

	sql, args, err := db.NewInsert(metric).Build()
	require.NoError(t, err)

	assert.Contains(t, sql, "INSERT INTO `metrics` (`id`, `name`, `value`, `timestamp`)")
	assert.Contains(t, sql, "VALUES (?, ?, ?, ?)")
	require.Len(t, args, 4)
	assert.Equal(t, int64(1), args[0])
	assert.Equal(t, "latency", args[1])
	assert.Equal(t, 42.5, args[2])
	assert.Equal(t, int64(12345), args[3])
}

// =========================================================================
// UPDATE QUERY TESTS (ALTER TABLE ... UPDATE)
// =========================================================================

func TestUpdateQuery_SetExpressions(t *testing.T) {
	db := newTestDB()
	event := &TestEvent{ID: 1}

	sql, args, err := db.NewUpdate(event).
		Set("`event_type` = ?", "updated_click").
		Where("`id` = ?", int64(1)).
		Build()
	require.NoError(t, err)

	expected := "ALTER TABLE `events` UPDATE `event_type` = ? WHERE `id` = ?"
	assert.Equal(t, expected, sql)
	require.Len(t, args, 2)
	assert.Equal(t, "updated_click", args[0])
	assert.Equal(t, int64(1), args[1])
}

func TestUpdateQuery_FromModelAllFields(t *testing.T) {
	db := newTestDB()
	now := time.Date(2025, 1, 15, 12, 0, 0, 0, time.UTC)
	event := &TestEvent{
		ID:        10,
		EventType: "view",
		UserID:    42,
		Payload:   `{"page": "/about"}`,
		CreatedAt: now,
	}

	sql, args, err := db.NewUpdate(event).
		Where("`id` = ?", int64(10)).
		Build()
	require.NoError(t, err)

	// Should use ALTER TABLE ... UPDATE syntax.
	assert.Contains(t, sql, "ALTER TABLE `events` UPDATE")
	assert.Contains(t, sql, "`event_type` = ?")
	assert.Contains(t, sql, "`user_id` = ?")
	assert.Contains(t, sql, "`payload` = ?")
	assert.Contains(t, sql, "`created_at` = ?")
	assert.Contains(t, sql, "WHERE `id` = ?")
	require.Len(t, args, 5) // event_type, user_id, payload, created_at, where_id
	assert.Equal(t, "view", args[0])
	assert.Equal(t, int64(42), args[1])
}

func TestUpdateQuery_ColumnSpecific(t *testing.T) {
	db := newTestDB()
	event := &TestEvent{
		ID:        7,
		EventType: "hover",
		Payload:   "new-payload",
	}

	sql, args, err := db.NewUpdate(event).
		Column("event_type", "payload").
		Where("`id` = ?", int64(7)).
		Build()
	require.NoError(t, err)

	assert.Contains(t, sql, "ALTER TABLE `events` UPDATE `event_type` = ?, `payload` = ?")
	assert.Contains(t, sql, "WHERE `id` = ?")
	require.Len(t, args, 3)
	assert.Equal(t, "hover", args[0])
	assert.Equal(t, "new-payload", args[1])
	assert.Equal(t, int64(7), args[2])
}

func TestUpdateQuery_OmitZero(t *testing.T) {
	db := newTestDB()
	event := &TestEvent{
		ID:        5,
		EventType: "scroll",
		// UserID, Payload, CreatedAt are zero values.
	}

	sql, args, err := db.NewUpdate(event).
		OmitZero().
		Where("`id` = ?", int64(5)).
		Build()
	require.NoError(t, err)

	assert.Contains(t, sql, "`event_type` = ?")
	assert.NotContains(t, sql, "`payload`",
		"zero-value payload should be omitted with OmitZero")
	require.True(t, len(args) >= 2, "should have at least event_type arg and where arg")
	assert.Equal(t, "scroll", args[0])
}

func TestUpdateQuery_WherePK(t *testing.T) {
	db := newTestDB()
	event := &TestEvent{
		ID:        99,
		EventType: "click",
		UserID:    42,
	}

	sql, args, err := db.NewUpdate(event).
		Column("event_type").
		WherePK().
		Build()
	require.NoError(t, err)

	assert.Contains(t, sql, "ALTER TABLE `events` UPDATE `event_type` = ?")
	assert.Contains(t, sql, "WHERE `id` = ?")
	require.Len(t, args, 2)
	assert.Equal(t, "click", args[0])
	assert.Equal(t, int64(99), args[1])
}

// =========================================================================
// DELETE QUERY TESTS (ALTER TABLE ... DELETE)
// =========================================================================

func TestDeleteQuery_WithWhere(t *testing.T) {
	db := newTestDB()
	metric := &TestMetric{}

	sql, args, err := db.NewDelete(metric).
		Where("`timestamp` < ?", int64(1000)).
		Build()
	require.NoError(t, err)

	expected := "ALTER TABLE `metrics` DELETE WHERE `timestamp` < ?"
	assert.Equal(t, expected, sql)
	require.Len(t, args, 1)
	assert.Equal(t, int64(1000), args[0])
}

func TestDeleteQuery_WithWherePK(t *testing.T) {
	db := newTestDB()
	metric := &TestMetric{ID: 55}

	sql, args, err := db.NewDelete(metric).
		WherePK().
		Build()
	require.NoError(t, err)

	expected := "ALTER TABLE `metrics` DELETE WHERE `id` = ?"
	assert.Equal(t, expected, sql)
	require.Len(t, args, 1)
	assert.Equal(t, int64(55), args[0])
}

func TestDeleteQuery_MultipleWhereConditions(t *testing.T) {
	db := newTestDB()
	metric := &TestMetric{}

	sql, args, err := db.NewDelete(metric).
		Where("`name` = ?", "cpu").
		Where("`timestamp` < ?", int64(500)).
		Build()
	require.NoError(t, err)

	expected := "ALTER TABLE `metrics` DELETE WHERE `name` = ? AND `timestamp` < ?"
	assert.Equal(t, expected, sql)
	require.Len(t, args, 2)
	assert.Equal(t, "cpu", args[0])
	assert.Equal(t, int64(500), args[1])
}

// =========================================================================
// ALTER TABLE QUERY TESTS (ClickHouse-specific)
// =========================================================================

func TestAlterUpdateQuery_Basic(t *testing.T) {
	db := newTestDB()
	event := &TestEvent{}

	sql, args, err := db.NewAlterUpdate(event).
		Set("`event_type` = ?", "modified").
		Where("`id` = ?", int64(1)).
		Build()
	require.NoError(t, err)

	expected := "ALTER TABLE `events` UPDATE `event_type` = ? WHERE `id` = ?"
	assert.Equal(t, expected, sql)
	require.Len(t, args, 2)
	assert.Equal(t, "modified", args[0])
	assert.Equal(t, int64(1), args[1])
}

func TestAlterUpdateQuery_MultipleSets(t *testing.T) {
	db := newTestDB()
	event := &TestEvent{}

	sql, args, err := db.NewAlterUpdate(event).
		Set("`event_type` = ?", "updated").
		Set("`payload` = ?", "new-payload").
		Where("`user_id` = ?", int64(42)).
		Build()
	require.NoError(t, err)

	assert.Contains(t, sql, "ALTER TABLE `events` UPDATE `event_type` = ?, `payload` = ? WHERE `user_id` = ?")
	require.Len(t, args, 3)
}

func TestAlterUpdateQuery_NoWhere_Error(t *testing.T) {
	db := newTestDB()
	event := &TestEvent{}

	_, _, err := db.NewAlterUpdate(event).
		Set("`event_type` = ?", "modified").
		Build()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "WHERE clause")
}

func TestAlterUpdateQuery_NoSet_Error(t *testing.T) {
	db := newTestDB()
	event := &TestEvent{}

	_, _, err := db.NewAlterUpdate(event).
		Where("`id` = ?", int64(1)).
		Build()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "SET clause")
}

func TestAlterDeleteQuery_Basic(t *testing.T) {
	db := newTestDB()
	event := &TestEvent{}

	sql, args, err := db.NewAlterDelete(event).
		Where("`id` = ?", int64(1)).
		Build()
	require.NoError(t, err)

	expected := "ALTER TABLE `events` DELETE WHERE `id` = ?"
	assert.Equal(t, expected, sql)
	require.Len(t, args, 1)
	assert.Equal(t, int64(1), args[0])
}

func TestAlterDeleteQuery_NoWhere_Error(t *testing.T) {
	db := newTestDB()
	event := &TestEvent{}

	_, _, err := db.NewAlterDelete(event).Build()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "WHERE clause")
}

// =========================================================================
// CREATE TABLE QUERY TESTS
// =========================================================================

func TestCreateTableQuery_Basic(t *testing.T) {
	db := newTestDB()

	sql, _, err := db.NewCreateTable((*TestEvent)(nil)).
		OrderBy("id").
		Build()
	require.NoError(t, err)

	assert.Contains(t, sql, "CREATE TABLE `events`")
	assert.Contains(t, sql, "`id` Int64")
	assert.Contains(t, sql, "`event_type` String")
	assert.Contains(t, sql, "`user_id` Int64")
	assert.Contains(t, sql, "ENGINE = MergeTree()")
	assert.Contains(t, sql, "ORDER BY (`id`)")
}

func TestCreateTableQuery_IfNotExists(t *testing.T) {
	db := newTestDB()

	sql, _, err := db.NewCreateTable((*TestMetric)(nil)).
		IfNotExists().
		OrderBy("id").
		Build()
	require.NoError(t, err)

	assert.Contains(t, sql, "CREATE TABLE IF NOT EXISTS `metrics`")
}

func TestCreateTableQuery_CustomEngine(t *testing.T) {
	db := newTestDB()

	sql, _, err := db.NewCreateTable((*TestEvent)(nil)).
		Engine("ReplacingMergeTree(created_at)").
		OrderBy("id", "event_type").
		Build()
	require.NoError(t, err)

	assert.Contains(t, sql, "ENGINE = ReplacingMergeTree(created_at)")
	assert.Contains(t, sql, "ORDER BY (`id`, `event_type`)")
}

func TestCreateTableQuery_PartitionByTTLSettings(t *testing.T) {
	db := newTestDB()

	sql, _, err := db.NewCreateTable((*TestEvent)(nil)).
		OrderBy("id").
		PartitionBy("toYYYYMM(created_at)").
		TTL("created_at + INTERVAL 30 DAY").
		Settings("index_granularity = 8192").
		Build()
	require.NoError(t, err)

	assert.Contains(t, sql, "PARTITION BY toYYYYMM(created_at)")
	assert.Contains(t, sql, "TTL created_at + INTERVAL 30 DAY")
	assert.Contains(t, sql, "SETTINGS index_granularity = 8192")
}

func TestDropTableQuery_Basic(t *testing.T) {
	db := newTestDB()

	sql, _, err := db.NewDropTable((*TestEvent)(nil)).Build()
	require.NoError(t, err)

	assert.Equal(t, "DROP TABLE `events`", sql)
}

func TestDropTableQuery_IfExists(t *testing.T) {
	db := newTestDB()

	sql, _, err := db.NewDropTable((*TestMetric)(nil)).IfExists().Build()
	require.NoError(t, err)

	assert.Equal(t, "DROP TABLE IF EXISTS `metrics`", sql)
}

// =========================================================================
// GROVE ADAPTER METHOD TESTS
// =========================================================================

func TestClickHouseDB_GroveSelect(t *testing.T) {
	db := newTestDB()
	result := db.GroveSelect((*TestEvent)(nil))
	assert.NotNil(t, result, "GroveSelect should return non-nil")
}

func TestClickHouseDB_GroveInsert(t *testing.T) {
	db := newTestDB()
	result := db.GroveInsert(&TestMetric{ID: 1, Name: "Test", Value: 1.0, Timestamp: 1})
	assert.NotNil(t, result, "GroveInsert should return non-nil")
}

func TestClickHouseDB_GroveUpdate(t *testing.T) {
	db := newTestDB()
	result := db.GroveUpdate(&TestEvent{ID: 1, EventType: "click"})
	assert.NotNil(t, result, "GroveUpdate should return non-nil")
}

func TestClickHouseDB_GroveDelete(t *testing.T) {
	db := newTestDB()
	result := db.GroveDelete(&TestMetric{ID: 1})
	assert.NotNil(t, result, "GroveDelete should return non-nil")
}

func TestClickHouseDB_GroveSelect_MatchesNewSelect(t *testing.T) {
	db := newTestDB()

	groveResult := db.GroveSelect((*TestMetric)(nil))
	directResult := db.NewSelect((*TestMetric)(nil))

	_, groveOk := groveResult.(*SelectQuery)
	assert.True(t, groveOk, "GroveSelect should return *SelectQuery")

	groveSQ := groveResult.(*SelectQuery)
	directSQ := directResult

	groveSQL, groveArgs, err1 := groveSQ.Build()
	directSQL, directArgs, err2 := directSQ.Build()

	require.NoError(t, err1)
	require.NoError(t, err2)
	assert.Equal(t, directSQL, groveSQL, "GroveSelect and NewSelect should produce identical SQL")
	assert.Equal(t, directArgs, groveArgs)
}

func TestClickHouseDB_GroveInsert_MatchesNewInsert(t *testing.T) {
	db := newTestDB()
	metric := &TestMetric{ID: 1, Name: "X", Value: 5.0, Timestamp: 100}

	groveResult := db.GroveInsert(metric)
	_, ok := groveResult.(*InsertQuery)
	assert.True(t, ok, "GroveInsert should return *InsertQuery")
}

func TestClickHouseDB_GroveUpdate_MatchesNewUpdate(t *testing.T) {
	db := newTestDB()
	event := &TestEvent{ID: 1, EventType: "click"}

	groveResult := db.GroveUpdate(event)
	_, ok := groveResult.(*UpdateQuery)
	assert.True(t, ok, "GroveUpdate should return *UpdateQuery")
}

func TestClickHouseDB_GroveDelete_MatchesNewDelete(t *testing.T) {
	db := newTestDB()
	metric := &TestMetric{ID: 1}

	groveResult := db.GroveDelete(metric)
	_, ok := groveResult.(*DeleteQuery)
	assert.True(t, ok, "GroveDelete should return *DeleteQuery")
}

// =========================================================================
// TYPE HELPER TESTS
// =========================================================================

func TestLowCardinality(t *testing.T) {
	assert.Equal(t, "LowCardinality(String)", LowCardinality("String"))
}

func TestArrayType(t *testing.T) {
	assert.Equal(t, "Array(String)", ArrayType("String"))
}

func TestNullableType(t *testing.T) {
	assert.Equal(t, "Nullable(String)", NullableType("String"))
}
