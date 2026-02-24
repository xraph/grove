package esdriver

import "errors"

// ErrLastInsertIDNotSupported is returned by EsResult.LastInsertId
// because Elasticsearch does not use auto-incrementing integer IDs.
// Use the DocumentID method to get the generated document ID instead.
var ErrLastInsertIDNotSupported = errors.New("esdriver: LastInsertId is not supported; use DocumentID() instead")

// ErrNotSupported is returned for operations that are not applicable to Elasticsearch.
var ErrNotSupported = errors.New("esdriver: operation not supported")

// EsResult wraps the outcome of an Elasticsearch write operation and
// implements the driver.Result interface (RowsAffected, LastInsertId).
type EsResult struct {
	documentID string
	version    int64
	action     string // "created", "updated", "deleted", "noop"
	affected   int64  // for by-query operations (delete_by_query, update_by_query)
}

// RowsAffected returns the number of documents affected by the operation.
func (r *EsResult) RowsAffected() (int64, error) {
	if r.affected > 0 {
		return r.affected, nil
	}
	switch r.action {
	case "created", "updated", "deleted":
		return 1, nil
	}
	return 0, nil
}

// LastInsertId always returns 0 and an error because Elasticsearch does not
// provide auto-incrementing integer IDs. Use DocumentID() instead.
func (r *EsResult) LastInsertId() (int64, error) {
	return 0, ErrLastInsertIDNotSupported
}

// DocumentID returns the _id of the affected document.
func (r *EsResult) DocumentID() string {
	return r.documentID
}

// Version returns the document version after the operation.
func (r *EsResult) Version() int64 {
	return r.version
}

// Action returns the result action: "created", "updated", "deleted", or "noop".
func (r *EsResult) Action() string {
	return r.action
}
