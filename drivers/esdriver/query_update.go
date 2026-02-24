package esdriver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/elastic/go-elasticsearch/v8/esapi"

	"github.com/xraph/grove/schema"
)

// UpdateQuery builds and executes Elasticsearch update operations.
// Use ElasticDB.NewUpdate() to create one.
type UpdateQuery struct {
	db      *ElasticDB
	index   string
	table   *schema.Table
	model   any
	docID   string // document _id for single-document update
	filter  M      // query filter for update-by-query
	doc     M      // partial document for doc update
	script  *Script
	upsert  bool
	many    bool
	refresh string
	err     error
}

// NewUpdate creates a new UpdateQuery.
func (db *ElasticDB) NewUpdate(model any) *UpdateQuery {
	q := &UpdateQuery{
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
func (q *UpdateQuery) Index(name string) *UpdateQuery {
	q.index = name
	return q
}

// DocumentID sets the document _id for a single-document update.
func (q *UpdateQuery) DocumentID(id string) *UpdateQuery {
	q.docID = id
	return q
}

// Filter sets the query filter for update-by-query operations.
func (q *UpdateQuery) Filter(f M) *UpdateQuery {
	if q.filter == nil {
		q.filter = M{}
	}
	for k, v := range f {
		q.filter[k] = v
	}
	return q
}

// Set adds a field to the partial document update.
func (q *UpdateQuery) Set(field string, value any) *UpdateQuery {
	if q.doc == nil {
		q.doc = M{}
	}
	q.doc[field] = value
	return q
}

// SetDoc sets the entire partial document for update.
func (q *UpdateQuery) SetDoc(doc M) *UpdateQuery {
	q.doc = doc
	return q
}

// SetScript sets a script for scripted updates.
func (q *UpdateQuery) SetScript(s Script) *UpdateQuery {
	q.script = &s
	return q
}

// Upsert enables upsert behavior: if no document matches, insert a new one.
func (q *UpdateQuery) Upsert() *UpdateQuery {
	q.upsert = true
	return q
}

// Many configures the query to update all matching documents (update-by-query).
func (q *UpdateQuery) Many() *UpdateQuery {
	q.many = true
	return q
}

// Refresh overrides the default refresh policy for this operation.
func (q *UpdateQuery) Refresh(r string) *UpdateQuery {
	q.refresh = r
	return q
}

// GetFilter returns the current filter. Useful for testing.
func (q *UpdateQuery) GetFilter() M {
	return q.filter
}

// GetDoc returns the current partial document. Useful for testing.
func (q *UpdateQuery) GetDoc() M {
	return q.doc
}

// GetIndex returns the index name. Useful for testing.
func (q *UpdateQuery) GetIndex() string {
	return q.index
}

// IsUpsert returns whether upsert is enabled. Useful for testing.
func (q *UpdateQuery) IsUpsert() bool {
	return q.upsert
}

// IsMany returns whether the query targets multiple documents. Useful for testing.
func (q *UpdateQuery) IsMany() bool {
	return q.many
}

// Exec executes the update operation.
func (q *UpdateQuery) Exec(ctx context.Context) (*EsResult, error) {
	if q.err != nil {
		return nil, q.err
	}

	idx := q.db.resolveIndex(q.index)
	if idx == "" {
		return nil, fmt.Errorf("esdriver: no index specified")
	}

	// Build the update doc from model fields if no explicit doc or script set.
	if q.doc == nil && q.script == nil {
		doc, err := structToDocUpdate(q.model, q.table)
		if err != nil {
			return nil, err
		}
		q.doc = doc
	}

	if q.many || (q.docID == "" && q.filter != nil) {
		return q.updateByQuery(ctx, idx)
	}
	return q.updateOne(ctx, idx)
}

func (q *UpdateQuery) updateOne(ctx context.Context, idx string) (*EsResult, error) {
	if q.docID == "" {
		return nil, fmt.Errorf("esdriver: DocumentID required for single-document update; use Many() for update-by-query")
	}

	body := M{}
	if q.script != nil {
		scriptMap := M{"source": q.script.Source}
		if q.script.Lang != "" {
			scriptMap["lang"] = q.script.Lang
		}
		if q.script.Params != nil {
			scriptMap["params"] = q.script.Params
		}
		body["script"] = scriptMap
	} else if q.doc != nil {
		body["doc"] = q.doc
	}

	if q.upsert && q.doc != nil {
		body["doc_as_upsert"] = true
	}

	data, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("esdriver: marshal update body: %w", err)
	}

	esOpts := []func(*esUpdateRequest){
		q.db.client.Update.WithContext(ctx),
	}
	if r := q.db.refreshOption(q.refresh); r != "" {
		esOpts = append(esOpts, q.db.client.Update.WithRefresh(r))
	}

	res, err := q.db.client.Update(idx, q.docID, bytes.NewReader(data), esOpts...)
	if err != nil {
		return nil, fmt.Errorf("esdriver: update: %w", err)
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

func (q *UpdateQuery) updateByQuery(ctx context.Context, idx string) (*EsResult, error) {
	body := M{}

	if q.filter != nil {
		body["query"] = q.filter
	} else {
		body["query"] = M{"match_all": M{}}
	}

	if q.script != nil {
		scriptMap := M{"source": q.script.Source}
		if q.script.Lang != "" {
			scriptMap["lang"] = q.script.Lang
		}
		if q.script.Params != nil {
			scriptMap["params"] = q.script.Params
		}
		body["script"] = scriptMap
	} else if q.doc != nil {
		// update-by-query requires a script; build one from the doc fields.
		parts := make([]string, 0, len(q.doc))
		params := M{}
		for k, v := range q.doc {
			paramKey := "p_" + k
			parts = append(parts, fmt.Sprintf("ctx._source['%s'] = params.%s", k, paramKey))
			params[paramKey] = v
		}
		body["script"] = M{
			"source": joinParts(parts),
			"lang":   "painless",
			"params": params,
		}
	}

	data, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("esdriver: marshal update-by-query body: %w", err)
	}

	esOpts := []func(*esUpdateByQueryRequest){
		q.db.client.UpdateByQuery.WithContext(ctx),
		q.db.client.UpdateByQuery.WithBody(bytes.NewReader(data)),
	}
	if r := q.db.refreshOption(q.refresh); r != "" {
		esOpts = append(esOpts, q.db.client.UpdateByQuery.WithRefresh(true))
	}

	res, err := q.db.client.UpdateByQuery([]string{idx}, esOpts...)
	if err != nil {
		return nil, fmt.Errorf("esdriver: update-by-query: %w", err)
	}

	var resp byQueryResponse
	if err := decodeResponse(res, &resp); err != nil {
		return nil, err
	}

	return &EsResult{
		action:   "updated",
		affected: resp.Updated,
	}, nil
}

func joinParts(parts []string) string {
	result := ""
	for i, p := range parts {
		if i > 0 {
			result += "; "
		}
		result += p
	}
	return result
}

type esUpdateRequest = esapi.UpdateRequest
type esUpdateByQueryRequest = esapi.UpdateByQueryRequest
