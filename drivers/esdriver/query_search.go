package esdriver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v8/esapi"

	"github.com/xraph/grove/schema"
)

// SearchQuery builds and executes Elasticsearch search operations.
// Use ElasticDB.NewSearch() to create one.
type SearchQuery struct {
	db          *ElasticDB
	index       string
	table       *schema.Table
	model       any
	query       M     // root "query" object
	sort        []M   // array of sort clauses
	source      any   // _source inclusion/exclusion
	from        int   // offset
	size        int   // limit (default -1 = use ES default of 10)
	aggs        M     // aggregations
	highlight   M     // highlight configuration
	searchAfter []any // for keyset pagination
	trackTotal  bool  // track_total_hits
	err         error
}

// NewSearch creates a new SearchQuery. model can be:
//   - *[]User (slice pointer for multi-row)
//   - *User (struct pointer for single row)
//   - (*User)(nil) (nil pointer for index reference without binding)
func (db *ElasticDB) NewSearch(model ...any) *SearchQuery {
	q := &SearchQuery{
		db:   db,
		size: -1, // use ES default
	}

	if len(model) > 0 && model[0] != nil {
		q.model = model[0]
		table, err := resolveTable(model[0])
		if err != nil {
			q.err = err
		} else {
			q.table = table
			q.index = indexName(table)
		}
	}

	return q
}

// Index overrides the index name derived from the model.
func (q *SearchQuery) Index(name string) *SearchQuery {
	q.index = name
	return q
}

// Match sets a match query on a field.
func (q *SearchQuery) Match(field string, value any) *SearchQuery {
	q.setQuery(M{"match": M{field: value}})
	return q
}

// MatchPhrase sets a match_phrase query on a field.
func (q *SearchQuery) MatchPhrase(field, phrase string) *SearchQuery {
	q.setQuery(M{"match_phrase": M{field: phrase}})
	return q
}

// Term sets an exact term query on a field.
func (q *SearchQuery) Term(field string, value any) *SearchQuery {
	q.setQuery(M{"term": M{field: value}})
	return q
}

// Terms sets a terms query (IN equivalent) on a field.
func (q *SearchQuery) Terms(field string, values ...any) *SearchQuery {
	q.setQuery(M{"terms": M{field: values}})
	return q
}

// Range sets a range query on a field.
func (q *SearchQuery) Range(field string, opts RangeOpts) *SearchQuery {
	rangeClause := M{}
	if opts.GT != nil {
		rangeClause["gt"] = opts.GT
	}
	if opts.GTE != nil {
		rangeClause["gte"] = opts.GTE
	}
	if opts.LT != nil {
		rangeClause["lt"] = opts.LT
	}
	if opts.LTE != nil {
		rangeClause["lte"] = opts.LTE
	}
	if opts.Format != "" {
		rangeClause["format"] = opts.Format
	}
	q.setQuery(M{"range": M{field: rangeClause}})
	return q
}

// Bool provides access to a bool query builder. The callback receives a
// BoolQuery that can accumulate must, should, must_not, and filter clauses.
func (q *SearchQuery) Bool(fn func(b *BoolQuery)) *SearchQuery {
	b := &BoolQuery{}
	fn(b)
	q.query = b.Build()
	return q
}

// Exists checks for the existence of a field.
func (q *SearchQuery) Exists(field string) *SearchQuery {
	q.setQuery(M{"exists": M{"field": field}})
	return q
}

// RawQuery sets the entire query body from a raw map (escape hatch).
func (q *SearchQuery) RawQuery(query M) *SearchQuery {
	q.query = query
	return q
}

// Sort adds a sort clause. order should be "asc" or "desc".
func (q *SearchQuery) Sort(field, order string) *SearchQuery {
	q.sort = append(q.sort, M{field: M{"order": order}})
	return q
}

// SortBy sets multiple sort clauses at once, replacing any existing ones.
func (q *SearchQuery) SortBy(sorts ...M) *SearchQuery {
	q.sort = sorts
	return q
}

// Source sets _source inclusion (which fields to include in results).
func (q *SearchQuery) Source(fields ...string) *SearchQuery {
	q.source = M{"includes": fields}
	return q
}

// ExcludeSource sets _source exclusion (which fields to exclude).
func (q *SearchQuery) ExcludeSource(fields ...string) *SearchQuery {
	q.source = M{"excludes": fields}
	return q
}

// From sets the offset for pagination.
func (q *SearchQuery) From(n int) *SearchQuery {
	q.from = n
	return q
}

// Size sets the maximum number of results to return.
func (q *SearchQuery) Size(n int) *SearchQuery {
	q.size = n
	return q
}

// Highlight sets the highlight configuration.
func (q *SearchQuery) Highlight(h M) *SearchQuery {
	q.highlight = h
	return q
}

// TrackTotalHits enables exact total hit counting.
func (q *SearchQuery) TrackTotalHits() *SearchQuery {
	q.trackTotal = true
	return q
}

// SearchAfter sets the search_after values for keyset pagination.
func (q *SearchQuery) SearchAfter(values ...any) *SearchQuery {
	q.searchAfter = values
	return q
}

// Aggs sets aggregations on the search query.
func (q *SearchQuery) Aggs(aggs M) *SearchQuery {
	q.aggs = aggs
	return q
}

// ---------------------------------------------------------------------------
// Testing helpers
// ---------------------------------------------------------------------------

// BuildBody returns the full request body as a map for testing/inspection.
func (q *SearchQuery) BuildBody() (M, error) {
	if q.err != nil {
		return nil, q.err
	}
	return q.buildBody(), nil
}

// GetIndex returns the current index name.
func (q *SearchQuery) GetIndex() string {
	return q.index
}

// GetQuery returns the current query object.
func (q *SearchQuery) GetQuery() M {
	return q.query
}

// GetSort returns the current sort clauses.
func (q *SearchQuery) GetSort() []M {
	return q.sort
}

// GetSize returns the current size.
func (q *SearchQuery) GetSize() int {
	return q.size
}

// GetFrom returns the current from offset.
func (q *SearchQuery) GetFrom() int {
	return q.from
}

// ---------------------------------------------------------------------------
// Execution
// ---------------------------------------------------------------------------

// Scan executes the search query and decodes results into the model.
// For slice pointers, it decodes all matching documents.
// For struct pointers, it decodes the first matching document.
func (q *SearchQuery) Scan(ctx context.Context) error {
	if q.err != nil {
		return q.err
	}

	target := q.model
	if target == nil {
		return fmt.Errorf("esdriver: Scan requires a model; pass a model to NewSearch")
	}

	result, err := q.execute(ctx)
	if err != nil {
		return err
	}

	targetType := reflect.TypeOf(target)
	if targetType.Kind() == reflect.Ptr {
		innerType := targetType.Elem()
		if innerType.Kind() == reflect.Slice {
			return q.scanMany(result, target)
		}
	}

	return q.scanOne(result, target)
}

// ScanHits executes the search query and returns the raw SearchResult
// with scores, highlights, and other metadata.
func (q *SearchQuery) ScanHits(ctx context.Context) (*SearchResult, error) {
	if q.err != nil {
		return nil, q.err
	}
	return q.execute(ctx)
}

// Count executes a count query and returns the number of matching documents.
func (q *SearchQuery) Count(ctx context.Context) (int64, error) {
	if q.err != nil {
		return 0, q.err
	}

	idx := q.db.resolveIndex(q.index)
	if idx == "" {
		return 0, fmt.Errorf("esdriver: no index specified")
	}

	var body bytes.Buffer
	if q.query != nil {
		if err := json.NewEncoder(&body).Encode(M{"query": q.query}); err != nil {
			return 0, fmt.Errorf("esdriver: marshal count body: %w", err)
		}
	}

	res, err := q.db.client.Count(
		q.db.client.Count.WithContext(ctx),
		q.db.client.Count.WithIndex(idx),
		q.db.client.Count.WithBody(&body),
	)
	if err != nil {
		return 0, fmt.Errorf("esdriver: count: %w", err)
	}

	var countResp countResponse
	if err := decodeResponse(res, &countResp); err != nil {
		return 0, err
	}
	return countResp.Count, nil
}

// Scroll creates a scroll cursor for deep pagination.
func (q *SearchQuery) Scroll(ctx context.Context, keepAlive string) (*EsCursor, error) {
	if q.err != nil {
		return nil, q.err
	}

	idx := q.db.resolveIndex(q.index)
	if idx == "" {
		return nil, fmt.Errorf("esdriver: no index specified")
	}

	bodyMap := q.buildBody()
	data, err := json.Marshal(bodyMap)
	if err != nil {
		return nil, fmt.Errorf("esdriver: marshal scroll body: %w", err)
	}

	res, err := q.db.client.Search(
		q.db.client.Search.WithContext(ctx),
		q.db.client.Search.WithIndex(idx),
		q.db.client.Search.WithBody(bytes.NewReader(data)),
		q.db.client.Search.WithScroll(parseDuration(keepAlive)),
	)
	if err != nil {
		return nil, fmt.Errorf("esdriver: scroll search: %w", err)
	}

	var result SearchResult
	if err := decodeResponse(res, &result); err != nil {
		return nil, err
	}

	size := q.size
	if size < 0 {
		size = 10
	}

	return &EsCursor{
		db:       q.db,
		scrollID: result.ScrollID,
		hits:     result.Hits.Hits,
		size:     size,
	}, nil
}

// ---------------------------------------------------------------------------
// Internal
// ---------------------------------------------------------------------------

func (q *SearchQuery) execute(ctx context.Context) (*SearchResult, error) {
	idx := q.db.resolveIndex(q.index)
	if idx == "" {
		return nil, fmt.Errorf("esdriver: no index specified")
	}

	bodyMap := q.buildBody()
	data, err := json.Marshal(bodyMap)
	if err != nil {
		return nil, fmt.Errorf("esdriver: marshal search body: %w", err)
	}

	opts := []func(*esapi.SearchRequest){
		q.db.client.Search.WithContext(ctx),
		q.db.client.Search.WithIndex(idx),
		q.db.client.Search.WithBody(bytes.NewReader(data)),
	}
	if q.trackTotal {
		opts = append(opts, q.db.client.Search.WithTrackTotalHits(true))
	}

	res, err := q.db.client.Search(opts...)
	if err != nil {
		return nil, fmt.Errorf("esdriver: search: %w", err)
	}

	var result SearchResult
	if err := decodeResponse(res, &result); err != nil {
		return nil, err
	}

	return &result, nil
}

func (q *SearchQuery) buildBody() M {
	body := M{}

	if q.query != nil {
		body["query"] = q.query
	}
	if len(q.sort) > 0 {
		body["sort"] = q.sort
	}
	if q.source != nil {
		body["_source"] = q.source
	}
	if q.from > 0 {
		body["from"] = q.from
	}
	if q.size >= 0 {
		body["size"] = q.size
	}
	if q.highlight != nil {
		body["highlight"] = q.highlight
	}
	if len(q.searchAfter) > 0 {
		body["search_after"] = q.searchAfter
	}
	if q.aggs != nil {
		body["aggs"] = q.aggs
	}
	if q.trackTotal {
		body["track_total_hits"] = true
	}

	return body
}

func (q *SearchQuery) scanMany(result *SearchResult, dest any) error {
	destVal := reflect.ValueOf(dest)
	if destVal.Kind() != reflect.Ptr || destVal.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("esdriver: scanMany requires a pointer to a slice")
	}

	sliceVal := destVal.Elem()
	elemType := sliceVal.Type().Elem()

	isPtr := elemType.Kind() == reflect.Ptr
	if isPtr {
		elemType = elemType.Elem()
	}

	for _, hit := range result.Hits.Hits {
		elem := reflect.New(elemType)
		if err := json.Unmarshal(hit.Source, elem.Interface()); err != nil {
			return fmt.Errorf("esdriver: decode hit: %w", err)
		}

		// Populate PK field from _id.
		q.setPKFromID(elem.Elem(), hit.ID)

		if isPtr {
			sliceVal = reflect.Append(sliceVal, elem)
		} else {
			sliceVal = reflect.Append(sliceVal, elem.Elem())
		}
	}

	destVal.Elem().Set(sliceVal)
	return nil
}

func (q *SearchQuery) scanOne(result *SearchResult, dest any) error {
	if len(result.Hits.Hits) == 0 {
		return fmt.Errorf("esdriver: no documents found")
	}

	hit := result.Hits.Hits[0]
	if err := json.Unmarshal(hit.Source, dest); err != nil {
		return fmt.Errorf("esdriver: decode hit: %w", err)
	}

	// Populate PK field from _id.
	val := reflect.ValueOf(dest)
	for val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	q.setPKFromID(val, hit.ID)

	return nil
}

// setPKFromID sets the PK field on a struct value from the ES _id.
func (q *SearchQuery) setPKFromID(val reflect.Value, id string) {
	if q.table == nil || len(q.table.PKFields) == 0 {
		return
	}

	pk := q.table.PKFields[0]
	fv := val
	for _, idx := range pk.GoIndex {
		fv = fv.Field(idx)
	}

	if !fv.CanSet() {
		return
	}

	// Set the PK from the string _id based on the field's Go type.
	switch fv.Kind() {
	case reflect.String:
		fv.SetString(id)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		var n int64
		if _, err := fmt.Sscanf(id, "%d", &n); err == nil {
			fv.SetInt(n)
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		var n uint64
		if _, err := fmt.Sscanf(id, "%d", &n); err == nil {
			fv.SetUint(n)
		}
	}
}

// setQuery wraps a single query clause into a bool.must if there's already
// a query set, otherwise sets it directly.
func (q *SearchQuery) setQuery(clause M) {
	if q.query == nil {
		q.query = clause
		return
	}

	// Wrap existing + new into a bool must.
	existing := q.query
	if boolQ, ok := existing["bool"]; ok {
		// Already a bool query — append to must.
		boolMap, ok := boolQ.(M)
		if ok {
			must, _ := boolMap["must"].([]any) //nolint:errcheck // safe: nil slice works fine with append
			must = append(must, clause)
			boolMap["must"] = must
			return
		}
	}

	// Convert to bool query with both clauses in must.
	q.query = M{
		"bool": M{
			"must": []any{existing, clause},
		},
	}
}

// parseDuration converts a string like "1m", "5m", "30s" to time.Duration.
func parseDuration(s string) time.Duration {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Minute // default 1m
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return time.Minute
	}
	return d
}

// ---------------------------------------------------------------------------
// BoolQuery
// ---------------------------------------------------------------------------

// BoolQuery builds an Elasticsearch bool query with must, should,
// must_not, and filter clauses.
type BoolQuery struct {
	must               []any
	should             []any
	mustNot            []any
	filter             []any
	minimumShouldMatch *int
}

// Must adds a clause to the must array (AND semantics, contributes to score).
func (b *BoolQuery) Must(clause M) *BoolQuery {
	b.must = append(b.must, clause)
	return b
}

// Should adds a clause to the should array (OR semantics, boosts score).
func (b *BoolQuery) Should(clause M) *BoolQuery {
	b.should = append(b.should, clause)
	return b
}

// MustNot adds a clause to the must_not array (NOT semantics).
func (b *BoolQuery) MustNot(clause M) *BoolQuery {
	b.mustNot = append(b.mustNot, clause)
	return b
}

// Filter adds a clause to the filter array (AND semantics, no scoring).
func (b *BoolQuery) Filter(clause M) *BoolQuery {
	b.filter = append(b.filter, clause)
	return b
}

// MinimumShouldMatch sets the minimum number of should clauses that must match.
func (b *BoolQuery) MinimumShouldMatch(n int) *BoolQuery {
	b.minimumShouldMatch = &n
	return b
}

// Build returns the bool query as a map.
func (b *BoolQuery) Build() M {
	boolClause := M{}
	if len(b.must) > 0 {
		boolClause["must"] = b.must
	}
	if len(b.should) > 0 {
		boolClause["should"] = b.should
	}
	if len(b.mustNot) > 0 {
		boolClause["must_not"] = b.mustNot
	}
	if len(b.filter) > 0 {
		boolClause["filter"] = b.filter
	}
	if b.minimumShouldMatch != nil {
		boolClause["minimum_should_match"] = *b.minimumShouldMatch
	}
	return M{"bool": boolClause}
}
