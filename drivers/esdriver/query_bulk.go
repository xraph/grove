package esdriver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// BulkQuery builds and executes mixed Elasticsearch bulk operations
// (index, create, update, delete in a single request).
// Use ElasticDB.NewBulk() to create one.
type BulkQuery struct {
	db      *ElasticDB
	actions []BulkAction
	refresh string
	err     error
}

type BulkAction struct {
	op      string // "index", "create", "update", "delete"
	index   string
	docID   string
	doc     any
	routing string
}

// NewBulk creates a new BulkQuery.
func (db *ElasticDB) NewBulk() *BulkQuery {
	return &BulkQuery{db: db}
}

// Index adds an index action (insert or overwrite).
func (q *BulkQuery) Index(index, docID string, doc any) *BulkQuery {
	q.actions = append(q.actions, BulkAction{op: "index", index: index, docID: docID, doc: doc})
	return q
}

// Create adds a create action (insert only, fail if exists).
func (q *BulkQuery) Create(index, docID string, doc any) *BulkQuery {
	q.actions = append(q.actions, BulkAction{op: "create", index: index, docID: docID, doc: doc})
	return q
}

// Update adds an update action (partial document update).
func (q *BulkQuery) Update(index, docID string, doc M) *BulkQuery {
	q.actions = append(q.actions, BulkAction{op: "update", index: index, docID: docID, doc: M{"doc": doc}})
	return q
}

// Delete adds a delete action.
func (q *BulkQuery) Delete(index, docID string) *BulkQuery {
	q.actions = append(q.actions, BulkAction{op: "delete", index: index, docID: docID})
	return q
}

// Refresh overrides the default refresh policy for this bulk operation.
func (q *BulkQuery) Refresh(r string) *BulkQuery {
	q.refresh = r
	return q
}

// GetActions returns the current actions. Useful for testing.
func (q *BulkQuery) GetActions() []BulkAction {
	return q.actions
}

// Exec executes the bulk operation.
func (q *BulkQuery) Exec(ctx context.Context) (*BulkResult, error) {
	if q.err != nil {
		return nil, q.err
	}
	if len(q.actions) == 0 {
		return nil, fmt.Errorf("esdriver: no bulk actions specified")
	}

	var buf bytes.Buffer
	for _, a := range q.actions {
		// Action/metadata line.
		meta := M{}
		if a.index != "" {
			meta["_index"] = a.index
		}
		if a.docID != "" {
			meta["_id"] = a.docID
		}
		if a.routing != "" {
			meta["routing"] = a.routing
		}

		action := M{a.op: meta}
		actionLine, err := json.Marshal(action)
		if err != nil {
			return nil, fmt.Errorf("esdriver: marshal bulk action: %w", err)
		}
		buf.Write(actionLine)
		buf.WriteByte('\n')

		// Document body (not for delete).
		if a.op != "delete" && a.doc != nil {
			docLine, err := json.Marshal(a.doc)
			if err != nil {
				return nil, fmt.Errorf("esdriver: marshal bulk doc: %w", err)
			}
			buf.Write(docLine)
			buf.WriteByte('\n')
		}
	}

	opts := []func(*bulkRequestOptions){
		withBulkContext(ctx),
	}
	if r := q.db.refreshOption(q.refresh); r != "" {
		opts = append(opts, withBulkRefresh(r))
	}

	result, err := q.db.bulkRequest(&buf, opts...)
	if err != nil {
		return nil, err
	}

	if result.Errors {
		return result, q.collectErrors(result)
	}

	return result, nil
}

func (q *BulkQuery) collectErrors(result *BulkResult) error {
	var errs []string
	for i, item := range result.Items {
		var r *BulkItemResult
		switch {
		case item.Index != nil && item.Index.Error != nil:
			r = item.Index
		case item.Create != nil && item.Create.Error != nil:
			r = item.Create
		case item.Update != nil && item.Update.Error != nil:
			r = item.Update
		case item.Delete != nil && item.Delete.Error != nil:
			r = item.Delete
		}
		if r != nil {
			errs = append(errs, fmt.Sprintf("item %d: %v", i, *r.Error))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("esdriver: bulk errors: %s", strings.Join(errs, "; "))
	}
	return nil
}
