package mongodriver

import (
	"encoding/hex"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// Re-export commonly used BSON types for convenience so callers do not
// need to import the bson package directly.
type (
	// M is an unordered map representation of a BSON document.
	M = bson.M

	// D is an ordered representation of a BSON document.
	D = bson.D

	// A is an ordered representation of a BSON array.
	A = bson.A

	// E is a single element inside a D.
	E = bson.E
)

// ObjectID is the MongoDB 12-byte unique identifier.
type ObjectID = bson.ObjectID

// NewObjectID generates a new ObjectID.
func NewObjectID() ObjectID {
	return bson.NewObjectID()
}

// ObjectIDFromHex creates an ObjectID from a hex string.
func ObjectIDFromHex(s string) (ObjectID, error) {
	if len(s) != 24 {
		return ObjectID{}, fmt.Errorf("mongodriver: invalid ObjectID hex string length: %d", len(s))
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return ObjectID{}, fmt.Errorf("mongodriver: invalid ObjectID hex string: %w", err)
	}
	var oid ObjectID
	copy(oid[:], b)
	return oid, nil
}

// NilObjectID is the zero value for ObjectID.
var NilObjectID ObjectID

// IsValidObjectID returns true if the hex string is a valid 24-character ObjectID.
func IsValidObjectID(s string) bool {
	if len(s) != 24 {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}

// Timestamp is a convenience wrapper around time.Time for BSON date fields.
type Timestamp = bson.DateTime

// DateTimeFromTime converts a Go time.Time to a bson.DateTime.
func DateTimeFromTime(t time.Time) bson.DateTime {
	return bson.NewDateTimeFromTime(t)
}

// NowDateTime returns the current time as a bson.DateTime.
func NowDateTime() bson.DateTime {
	return bson.NewDateTimeFromTime(time.Now())
}
