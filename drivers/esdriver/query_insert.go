package esdriver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/elastic/go-elasticsearch/v8/esapi"

	"github.com/xraph/grove/schema"
)

// InsertQuery builds and executes Elasticsearch index (insert) operations.
// Use ElasticDB.NewInsert() to create one.
type InsertQuery struct {
	db       *ElasticDB
	index    string
	table    *schema.Table
	model    any
	docID    string // explicit _id
	routing  string // ES routing
	pipeline string // ingest pipeline
	refresh  string // override default refresh policy
	err      error
}

// NewInsert creates a new InsertQuery.
// model can be a struct pointer or a pointer to a slice (for bulk insert).
func (db *ElasticDB) NewInsert(model any) *InsertQuery {
	q := &InsertQuery{
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
func (q *InsertQuery) Index(name string) *InsertQuery {
	q.index = name
	return q
}

// DocumentID sets an explicit document _id.
func (q *InsertQuery) DocumentID(id string) *InsertQuery {
	q.docID = id
	return q
}

// Routing sets the ES routing value.
func (q *InsertQuery) Routing(r string) *InsertQuery {
	q.routing = r
	return q
}

// Pipeline sets the ingest pipeline name.
func (q *InsertQuery) Pipeline(p string) *InsertQuery {
	q.pipeline = p
	return q
}

// Refresh overrides the default refresh policy for this operation.
func (q *InsertQuery) Refresh(r string) *InsertQuery {
	q.refresh = r
	return q
}

// GetIndex returns the index name. Useful for testing.
func (q *InsertQuery) GetIndex() string {
	return q.index
}

// BuildDoc converts the model to a document map for insertion.
// Exported for testing purposes.
func (q *InsertQuery) BuildDoc() (M, error) {
	if q.err != nil {
		return nil, q.err
	}
	doc, _, err := structToDocInsert(q.model, q.table)
	return doc, err
}

// BuildDocs converts a slice model to document maps for insertion.
// Exported for testing purposes.
func (q *InsertQuery) BuildDocs() ([]M, error) {
	if q.err != nil {
		return nil, q.err
	}

	val := reflect.ValueOf(q.model)
	for val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return nil, fmt.Errorf("esdriver: nil model pointer")
		}
		val = val.Elem()
	}

	if val.Kind() != reflect.Slice {
		doc, _, err := structToDocInsert(q.model, q.table)
		if err != nil {
			return nil, err
		}
		return []M{doc}, nil
	}

	n := val.Len()
	docs := make([]M, n)
	for i := range n {
		elem := val.Index(i).Interface()
		doc, _, err := structToDocInsert(elem, q.table)
		if err != nil {
			return nil, err
		}
		docs[i] = doc
	}
	return docs, nil
}

// Exec executes the insert operation.
// For single documents, uses the Index API.
// For slices, uses the Bulk API.
func (q *InsertQuery) Exec(ctx context.Context) (*EsResult, error) {
	if q.err != nil {
		return nil, q.err
	}

	idx := q.db.resolveIndex(q.index)
	if idx == "" {
		return nil, fmt.Errorf("esdriver: no index specified")
	}

	val := reflect.ValueOf(q.model)
	for val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return nil, fmt.Errorf("esdriver: nil model pointer")
		}
		val = val.Elem()
	}

	if val.Kind() == reflect.Slice {
		return q.insertMany(ctx, idx, val)
	}

	return q.insertOne(ctx, idx)
}

func (q *InsertQuery) insertOne(ctx context.Context, idx string) (*EsResult, error) {
	doc, docID, err := structToDocInsert(q.model, q.table)
	if err != nil {
		return nil, err
	}

	// Use explicit docID if set on the query.
	if q.docID != "" {
		docID = q.docID
	}

	data, err := json.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("esdriver: marshal document: %w", err)
	}

	opts := []func(*indexRequestOptions){
		withIndexContext(ctx),
	}
	if docID != "" {
		opts = append(opts, withDocumentID(docID))
	}
	if r := q.db.refreshOption(q.refresh); r != "" {
		opts = append(opts, withRefresh(r))
	}
	if q.routing != "" {
		opts = append(opts, withRouting(q.routing))
	}
	if q.pipeline != "" {
		opts = append(opts, withPipeline(q.pipeline))
	}

	res, err := q.db.indexDocument(idx, bytes.NewReader(data), opts...)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (q *InsertQuery) insertMany(ctx context.Context, idx string, sliceVal reflect.Value) (*EsResult, error) {
	n := sliceVal.Len()
	if n == 0 {
		return nil, fmt.Errorf("esdriver: empty slice for bulk insert")
	}

	var buf bytes.Buffer
	for i := range n {
		elem := sliceVal.Index(i).Interface()
		doc, docID, err := structToDocInsert(elem, q.table)
		if err != nil {
			return nil, fmt.Errorf("esdriver: insert many: element %d: %w", i, err)
		}

		// Action line.
		indexMeta := M{"_index": idx}
		if docID != "" {
			indexMeta["_id"] = docID
		}
		if q.routing != "" {
			indexMeta["routing"] = q.routing
		}
		if q.pipeline != "" {
			indexMeta["pipeline"] = q.pipeline
		}
		action := M{"index": indexMeta}

		actionLine, err := json.Marshal(action)
		if err != nil {
			return nil, fmt.Errorf("esdriver: insert many: marshal action %d: %w", i, err)
		}
		buf.Write(actionLine)
		buf.WriteByte('\n')

		docLine, err := json.Marshal(doc)
		if err != nil {
			return nil, fmt.Errorf("esdriver: insert many: marshal doc %d: %w", i, err)
		}
		buf.Write(docLine)
		buf.WriteByte('\n')
	}

	opts := []func(*bulkRequestOptions){
		withBulkContext(ctx),
	}
	if r := q.db.refreshOption(q.refresh); r != "" {
		opts = append(opts, withBulkRefresh(r))
	}

	bulkRes, err := q.db.bulkRequest(&buf, opts...)
	if err != nil {
		return nil, err
	}

	if bulkRes.Errors {
		return nil, q.bulkErrors(bulkRes)
	}

	return &EsResult{
		action:   "created",
		affected: int64(len(bulkRes.Items)),
	}, nil
}

func (q *InsertQuery) bulkErrors(result *BulkResult) error {
	var errs []string
	for i, item := range result.Items {
		var r *BulkItemResult
		switch {
		case item.Index != nil:
			r = item.Index
		case item.Create != nil:
			r = item.Create
		}
		if r != nil && r.Error != nil {
			errs = append(errs, fmt.Sprintf("item %d: %v", i, *r.Error))
		}
	}
	return fmt.Errorf("esdriver: bulk errors: %s", strings.Join(errs, "; "))
}

// ---------------------------------------------------------------------------
// Index API helpers
// ---------------------------------------------------------------------------

type indexRequestOptions struct {
	ctx      context.Context
	docID    string
	refresh  string
	routing  string
	pipeline string
}

func withIndexContext(ctx context.Context) func(*indexRequestOptions) {
	return func(o *indexRequestOptions) { o.ctx = ctx }
}
func withDocumentID(id string) func(*indexRequestOptions) {
	return func(o *indexRequestOptions) { o.docID = id }
}
func withRefresh(r string) func(*indexRequestOptions) {
	return func(o *indexRequestOptions) { o.refresh = r }
}
func withRouting(r string) func(*indexRequestOptions) {
	return func(o *indexRequestOptions) { o.routing = r }
}
func withPipeline(p string) func(*indexRequestOptions) {
	return func(o *indexRequestOptions) { o.pipeline = p }
}

func (db *ElasticDB) indexDocument(index string, body *bytes.Reader, opts ...func(*indexRequestOptions)) (*EsResult, error) {
	o := &indexRequestOptions{ctx: context.Background()}
	for _, fn := range opts {
		fn(o)
	}

	reqOpts := []indexRequestFn{
		withIndexReqContext(o.ctx),
	}
	if o.docID != "" {
		reqOpts = append(reqOpts, withIndexReqDocID(o.docID))
	}
	if o.refresh != "" {
		reqOpts = append(reqOpts, withIndexReqRefresh(o.refresh))
	}
	if o.routing != "" {
		reqOpts = append(reqOpts, withIndexReqRouting(o.routing))
	}
	if o.pipeline != "" {
		reqOpts = append(reqOpts, withIndexReqPipeline(o.pipeline))
	}

	res, err := db.doIndex(index, body, reqOpts...)
	if err != nil {
		return nil, fmt.Errorf("esdriver: index: %w", err)
	}

	var resp indexResponse
	if err := decodeResponse(res.Response, &resp); err != nil {
		return nil, err
	}

	return &EsResult{
		documentID: resp.ID,
		version:    resp.Version,
		action:     resp.Result,
	}, nil
}

type indexRequestFn func(*indexReqOpts)
type indexReqOpts struct {
	ctx      context.Context
	docID    string
	refresh  string
	routing  string
	pipeline string
}

func withIndexReqContext(ctx context.Context) indexRequestFn {
	return func(o *indexReqOpts) { o.ctx = ctx }
}
func withIndexReqDocID(id string) indexRequestFn {
	return func(o *indexReqOpts) { o.docID = id }
}
func withIndexReqRefresh(r string) indexRequestFn {
	return func(o *indexReqOpts) { o.refresh = r }
}
func withIndexReqRouting(r string) indexRequestFn {
	return func(o *indexReqOpts) { o.routing = r }
}
func withIndexReqPipeline(p string) indexRequestFn {
	return func(o *indexReqOpts) { o.pipeline = p }
}

func (db *ElasticDB) doIndex(index string, body *bytes.Reader, opts ...indexRequestFn) (*indexAPIResponse, error) {
	o := &indexReqOpts{ctx: context.Background()}
	for _, fn := range opts {
		fn(o)
	}

	esOpts := []func(*esIndexRequest){}
	if o.ctx != nil {
		esOpts = append(esOpts, db.client.Index.WithContext(o.ctx))
	}
	if o.docID != "" {
		esOpts = append(esOpts, db.client.Index.WithDocumentID(o.docID))
	}
	if o.refresh != "" {
		esOpts = append(esOpts, db.client.Index.WithRefresh(o.refresh))
	}
	if o.routing != "" {
		esOpts = append(esOpts, db.client.Index.WithRouting(o.routing))
	}
	if o.pipeline != "" {
		esOpts = append(esOpts, db.client.Index.WithPipeline(o.pipeline))
	}

	res, err := db.client.Index(index, body, esOpts...)
	if err != nil {
		return nil, err
	}

	return &indexAPIResponse{Response: res}, nil
}

type indexAPIResponse struct {
	*esapi.Response
}

type esIndexRequest = esapi.IndexRequest

// ---------------------------------------------------------------------------
// Bulk API helpers
// ---------------------------------------------------------------------------

type bulkRequestOptions struct {
	ctx     context.Context
	refresh string
}

func withBulkContext(ctx context.Context) func(*bulkRequestOptions) {
	return func(o *bulkRequestOptions) { o.ctx = ctx }
}
func withBulkRefresh(r string) func(*bulkRequestOptions) {
	return func(o *bulkRequestOptions) { o.refresh = r }
}

func (db *ElasticDB) bulkRequest(body *bytes.Buffer, opts ...func(*bulkRequestOptions)) (*BulkResult, error) {
	o := &bulkRequestOptions{ctx: context.Background()}
	for _, fn := range opts {
		fn(o)
	}

	esOpts := []func(*esapi.BulkRequest){
		db.client.Bulk.WithContext(o.ctx),
	}
	if o.refresh != "" {
		esOpts = append(esOpts, db.client.Bulk.WithRefresh(o.refresh))
	}

	res, err := db.client.Bulk(body, esOpts...)
	if err != nil {
		return nil, fmt.Errorf("esdriver: bulk: %w", err)
	}

	var result BulkResult
	if err := decodeResponse(res, &result); err != nil {
		return nil, err
	}

	return &result, nil
}
