package esdriver

import "encoding/json"

// M is an unordered map for building Elasticsearch JSON request bodies.
// Analogous to mongodriver.M (bson.M).
type M = map[string]any

// SearchResult holds the parsed response from the ES _search API.
type SearchResult struct {
	Took     int64           `json:"took"`
	TimedOut bool            `json:"timed_out"`
	Hits     HitsResult      `json:"hits"`
	Aggs     json.RawMessage `json:"aggregations,omitempty"`
	ScrollID string          `json:"_scroll_id,omitempty"`
}

// HitsResult contains the search hits and metadata.
type HitsResult struct {
	Total    TotalHits `json:"total"`
	MaxScore *float64  `json:"max_score"`
	Hits     []Hit     `json:"hits"`
}

// TotalHits holds the total count and its relation (exact or lower bound).
type TotalHits struct {
	Value    int64  `json:"value"`
	Relation string `json:"relation"` // "eq" or "gte"
}

// Hit represents a single search result document.
type Hit struct {
	Index     string          `json:"_index"`
	ID        string          `json:"_id"`
	Score     *float64        `json:"_score"`
	Source    json.RawMessage `json:"_source"`
	Sort      []any           `json:"sort,omitempty"`
	Highlight M               `json:"highlight,omitempty"`
}

// AggregateResult holds parsed aggregation results.
type AggregateResult struct {
	Raw json.RawMessage `json:"-"`
}

// BulkResult holds the response from the ES _bulk API.
type BulkResult struct {
	Took   int64      `json:"took"`
	Errors bool       `json:"errors"`
	Items  []BulkItem `json:"items"`
}

// BulkItem holds the result for a single bulk action.
type BulkItem struct {
	Index  *BulkItemResult `json:"index,omitempty"`
	Create *BulkItemResult `json:"create,omitempty"`
	Update *BulkItemResult `json:"update,omitempty"`
	Delete *BulkItemResult `json:"delete,omitempty"`
}

// BulkItemResult holds details of a single bulk action outcome.
type BulkItemResult struct {
	Index   string `json:"_index"`
	ID      string `json:"_id"`
	Version int64  `json:"_version"`
	Status  int    `json:"status"`
	Error   *M     `json:"error,omitempty"`
}

// RangeOpts configures a range query clause.
type RangeOpts struct {
	GT     any    // Greater than
	GTE    any    // Greater than or equal
	LT     any    // Less than
	LTE    any    // Less than or equal
	Format string // Date format (e.g. "yyyy-MM-dd")
}

// Script represents an Elasticsearch script for scripted updates.
type Script struct {
	Source string
	Lang   string
	Params M
}

// esErrorResponse is used to decode Elasticsearch error responses.
type esErrorResponse struct {
	Error struct {
		Type   string `json:"type"`
		Reason string `json:"reason"`
	} `json:"error"`
	Status int `json:"status"`
}

// indexResponse is used to decode Elasticsearch index/update/delete responses.
type indexResponse struct {
	Index   string `json:"_index"`
	ID      string `json:"_id"`
	Version int64  `json:"_version"`
	Result  string `json:"result"` // "created", "updated", "deleted", "noop"
}

// countResponse is used to decode the ES _count API response.
type countResponse struct {
	Count int64 `json:"count"`
}

// deleteByQueryResponse is used to decode delete_by_query / update_by_query responses.
type byQueryResponse struct {
	Deleted int64 `json:"deleted,omitempty"`
	Updated int64 `json:"updated,omitempty"`
	Total   int64 `json:"total"`
}
