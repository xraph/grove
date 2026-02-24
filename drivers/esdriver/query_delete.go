package esdriver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/elastic/go-elasticsearch/v8/esapi"

	"github.com/xraph/grove/schema"
)

// DeleteQuery builds and executes Elasticsearch delete operations.
// Use ElasticDB.NewDelete() to create one.
type DeleteQuery struct {
	db      *ElasticDB
	index   string
	table   *schema.Table
	model   any
	docID   string // document _id for single-document delete
	filter  M      // query filter for delete-by-query
	many    bool
	refresh string
	err     error
}

// NewDelete creates a new DeleteQuery.
func (db *ElasticDB) NewDelete(model any) *DeleteQuery {
	q := &DeleteQuery{
		db:    db,
		model: model,
	}

	table, err := resolveTable(model)
	if err != nil {
		q.err = err
		return q
	}
	q.table = table
	q.index = indexName(table)
	return q
}

// Index overrides the index name derived from the model.
func (q *DeleteQuery) Index(name string) *DeleteQuery {
	q.index = name
	return q
}

// DocumentID sets the document _id for a single-document delete.
func (q *DeleteQuery) DocumentID(id string) *DeleteQuery {
	q.docID = id
	return q
}

// Filter sets the query filter for delete-by-query operations.
func (q *DeleteQuery) Filter(f M) *DeleteQuery {
	if q.filter == nil {
		q.filter = M{}
	}
	for k, v := range f {
		q.filter[k] = v
	}
	return q
}

// Many configures the query to delete all matching documents (delete-by-query).
func (q *DeleteQuery) Many() *DeleteQuery {
	q.many = true
	return q
}

// Refresh overrides the default refresh policy for this operation.
func (q *DeleteQuery) Refresh(r string) *DeleteQuery {
	q.refresh = r
	return q
}

// GetFilter returns the current filter. Useful for testing.
func (q *DeleteQuery) GetFilter() M {
	return q.filter
}

// GetIndex returns the index name. Useful for testing.
func (q *DeleteQuery) GetIndex() string {
	return q.index
}

// IsMany returns whether the query targets multiple documents. Useful for testing.
func (q *DeleteQuery) IsMany() bool {
	return q.many
}

// Exec executes the delete operation.
func (q *DeleteQuery) Exec(ctx context.Context) (*EsResult, error) {
	if q.err != nil {
		return nil, q.err
	}

	idx := q.db.resolveIndex(q.index)
	if idx == "" {
		return nil, fmt.Errorf("esdriver: no index specified")
	}

	if q.many || (q.docID == "" && q.filter != nil) {
		return q.deleteByQuery(ctx, idx)
	}
	return q.deleteOne(ctx, idx)
}

func (q *DeleteQuery) deleteOne(ctx context.Context, idx string) (*EsResult, error) {
	if q.docID == "" {
		return nil, fmt.Errorf("esdriver: DocumentID required for single-document delete; use Many() for delete-by-query")
	}

	esOpts := []func(*esDeleteRequest){
		q.db.client.Delete.WithContext(ctx),
	}
	if r := q.db.refreshOption(q.refresh); r != "" {
		esOpts = append(esOpts, q.db.client.Delete.WithRefresh(r))
	}

	res, err := q.db.client.Delete(idx, q.docID, esOpts...)
	if err != nil {
		return nil, fmt.Errorf("esdriver: delete: %w", err)
	}

	var resp indexResponse
	if err := decodeResponse(res, &resp); err != nil {
		return nil, err
	}

	return &EsResult{
		documentID: resp.ID,
		version:    resp.Version,
		action:     resp.Result,
	}, nil
}

func (q *DeleteQuery) deleteByQuery(ctx context.Context, idx string) (*EsResult, error) {
	body := M{}
	if q.filter != nil {
		body["query"] = q.filter
	} else {
		body["query"] = M{"match_all": M{}}
	}

	data, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("esdriver: marshal delete-by-query body: %w", err)
	}

	esOpts := []func(*esDeleteByQueryRequest){
		q.db.client.DeleteByQuery.WithContext(ctx),
	}
	if r := q.db.refreshOption(q.refresh); r != "" {
		esOpts = append(esOpts, q.db.client.DeleteByQuery.WithRefresh(true))
	}

	res, err := q.db.client.DeleteByQuery([]string{idx}, bytes.NewReader(data), esOpts...)
	if err != nil {
		return nil, fmt.Errorf("esdriver: delete-by-query: %w", err)
	}

	var resp byQueryResponse
	if err := decodeResponse(res, &resp); err != nil {
		return nil, err
	}

	return &EsResult{
		action:   "deleted",
		affected: resp.Deleted,
	}, nil
}

type esDeleteRequest = esapi.DeleteRequest
type esDeleteByQueryRequest = esapi.DeleteByQueryRequest
