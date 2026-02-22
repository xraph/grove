package mongodriver

import "errors"

// ErrLastInsertIDNotSupported is returned by mongoResult.LastInsertId
// because MongoDB does not use auto-incrementing integer IDs by default.
// Use the InsertedID field to get the generated ObjectID instead.
var ErrLastInsertIDNotSupported = errors.New("mongodriver: LastInsertId is not supported; use InsertedID() instead")

// ErrNotSupported is returned for operations that are not applicable to MongoDB.
var ErrNotSupported = errors.New("mongodriver: operation not supported")

// mongoResult wraps the outcome of a MongoDB write operation and implements
// the driver.Result interface (RowsAffected, LastInsertId).
type mongoResult struct {
	insertedID    any
	matchedCount  int64
	modifiedCount int64
	deletedCount  int64
	upsertedCount int64
}

// RowsAffected returns the number of documents affected by the operation.
// For inserts this counts inserted documents; for updates it counts modified
// documents; for deletes it counts deleted documents.
func (r *mongoResult) RowsAffected() (int64, error) {
	if r.deletedCount > 0 {
		return r.deletedCount, nil
	}
	if r.modifiedCount > 0 {
		return r.modifiedCount, nil
	}
	if r.matchedCount > 0 {
		return r.matchedCount, nil
	}
	if r.upsertedCount > 0 {
		return r.upsertedCount, nil
	}
	// For inserts, if we got an insertedID, consider it 1 row affected.
	if r.insertedID != nil {
		return 1, nil
	}
	return 0, nil
}

// LastInsertId always returns 0 and an error because MongoDB does not
// provide auto-incrementing integer IDs. Use InsertedID() instead.
func (r *mongoResult) LastInsertId() (int64, error) {
	return 0, ErrLastInsertIDNotSupported
}

// InsertedID returns the _id of the inserted document (typically an ObjectID).
func (r *mongoResult) InsertedID() any {
	return r.insertedID
}

// MatchedCount returns the number of documents matched by the filter.
func (r *mongoResult) MatchedCount() int64 {
	return r.matchedCount
}

// ModifiedCount returns the number of documents actually modified.
func (r *mongoResult) ModifiedCount() int64 {
	return r.modifiedCount
}

// DeletedCount returns the number of documents deleted.
func (r *mongoResult) DeletedCount() int64 {
	return r.deletedCount
}

// UpsertedCount returns the number of documents upserted.
func (r *mongoResult) UpsertedCount() int64 {
	return r.upsertedCount
}
