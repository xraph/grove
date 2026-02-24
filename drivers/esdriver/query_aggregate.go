package esdriver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
)

// AggregateQuery builds and executes Elasticsearch aggregation queries.
// Use ElasticDB.NewAggregate() to create one.
type AggregateQuery struct {
	db    *ElasticDB
	index string
	query M   // optional filter query
	aggs  M   // aggregation definitions
	size  int // usually 0 for pure aggregations
	err   error
}

// NewAggregate creates a new aggregation query for the given index.
func (db *ElasticDB) NewAggregate(index string) *AggregateQuery {
	return &AggregateQuery{
		db:    db,
		index: index,
		aggs:  M{},
		size:  0,
	}
}

// Query sets a filter query to restrict the documents being aggregated.
func (q *AggregateQuery) Query(query M) *AggregateQuery {
	q.query = query
	return q
}

// Terms adds a terms aggregation.
func (q *AggregateQuery) Terms(name, field string) *AggregateQuery {
	q.aggs[name] = M{"terms": M{"field": field}}
	return q
}

// DateHistogram adds a date_histogram aggregation.
func (q *AggregateQuery) DateHistogram(name, field, interval string) *AggregateQuery {
	q.aggs[name] = M{"date_histogram": M{"field": field, "calendar_interval": interval}}
	return q
}

// Avg adds an avg metric aggregation.
func (q *AggregateQuery) Avg(name, field string) *AggregateQuery {
	q.aggs[name] = M{"avg": M{"field": field}}
	return q
}

// Sum adds a sum metric aggregation.
func (q *AggregateQuery) Sum(name, field string) *AggregateQuery {
	q.aggs[name] = M{"sum": M{"field": field}}
	return q
}

// Min adds a min metric aggregation.
func (q *AggregateQuery) Min(name, field string) *AggregateQuery {
	q.aggs[name] = M{"min": M{"field": field}}
	return q
}

// Max adds a max metric aggregation.
func (q *AggregateQuery) Max(name, field string) *AggregateQuery {
	q.aggs[name] = M{"max": M{"field": field}}
	return q
}

// Cardinality adds a cardinality (approximate distinct count) aggregation.
func (q *AggregateQuery) Cardinality(name, field string) *AggregateQuery {
	q.aggs[name] = M{"cardinality": M{"field": field}}
	return q
}

// SubAgg adds a sub-aggregation to an existing parent aggregation.
func (q *AggregateQuery) SubAgg(parent string, child M) *AggregateQuery {
	if parentAgg, exists := q.aggs[parent]; exists {
		parentMap, isM := parentAgg.(M)
		if !isM {
			return q
		}
		if _, hasAggs := parentMap["aggs"]; !hasAggs {
			parentMap["aggs"] = M{}
		}
		subAggs, isM := parentMap["aggs"].(M)
		if !isM {
			return q
		}
		for k, v := range child {
			subAggs[k] = v
		}
	}
	return q
}

// RawAggs sets the entire aggregations body from a raw map.
func (q *AggregateQuery) RawAggs(aggs M) *AggregateQuery {
	q.aggs = aggs
	return q
}

// Size sets the number of hits to return alongside aggregations.
// Default is 0 (aggregations only).
func (q *AggregateQuery) Size(n int) *AggregateQuery {
	q.size = n
	return q
}

// GetIndex returns the index name. Useful for testing.
func (q *AggregateQuery) GetIndex() string {
	return q.index
}

// GetAggs returns the current aggregation definitions. Useful for testing.
func (q *AggregateQuery) GetAggs() M {
	return q.aggs
}

// GetQuery returns the current query filter. Useful for testing.
func (q *AggregateQuery) GetQuery() M {
	return q.query
}

// BuildBody returns the full request body as a map for testing/inspection.
func (q *AggregateQuery) BuildBody() M {
	body := M{
		"size": q.size,
	}
	if q.query != nil {
		body["query"] = q.query
	}
	if len(q.aggs) > 0 {
		body["aggs"] = q.aggs
	}
	return body
}

// Scan executes the aggregation and decodes results into dest.
// dest should be a pointer to the structure receiving aggregation results.
func (q *AggregateQuery) Scan(ctx context.Context, dest any) error {
	if q.err != nil {
		return q.err
	}

	result, err := q.execute(ctx)
	if err != nil {
		return err
	}

	return json.Unmarshal(result.Aggs, dest)
}

// ScanRaw executes the aggregation and returns the raw SearchResult.
func (q *AggregateQuery) ScanRaw(ctx context.Context) (*SearchResult, error) {
	if q.err != nil {
		return nil, q.err
	}
	return q.execute(ctx)
}

func (q *AggregateQuery) execute(ctx context.Context) (*SearchResult, error) {
	idx := q.db.resolveIndex(q.index)
	if idx == "" {
		return nil, fmt.Errorf("esdriver: no index specified")
	}

	body := q.BuildBody()
	data, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("esdriver: marshal aggregate body: %w", err)
	}

	res, err := q.db.client.Search(
		q.db.client.Search.WithContext(ctx),
		q.db.client.Search.WithIndex(idx),
		q.db.client.Search.WithBody(bytes.NewReader(data)),
	)
	if err != nil {
		return nil, fmt.Errorf("esdriver: aggregate search: %w", err)
	}

	var result SearchResult
	if err := decodeResponse(res, &result); err != nil {
		return nil, err
	}

	return &result, nil
}
