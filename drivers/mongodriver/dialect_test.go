package mongodriver

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =========================================================================
// TYPE HELPER TESTS
// =========================================================================

func TestNewObjectID(t *testing.T) {
	oid := NewObjectID()
	assert.NotEqual(t, NilObjectID, oid, "NewObjectID should not return nil ObjectID")
}

func TestObjectIDFromHex_Valid(t *testing.T) {
	// A valid 24-char hex string.
	hexStr := "507f1f77bcf86cd799439011"
	oid, err := ObjectIDFromHex(hexStr)
	require.NoError(t, err)
	assert.NotEqual(t, NilObjectID, oid)
}

func TestObjectIDFromHex_InvalidLength(t *testing.T) {
	_, err := ObjectIDFromHex("short")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid ObjectID hex string length")
}

func TestObjectIDFromHex_InvalidChars(t *testing.T) {
	_, err := ObjectIDFromHex("zzzzzzzzzzzzzzzzzzzzzzzz")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid ObjectID hex string")
}

func TestIsValidObjectID(t *testing.T) {
	assert.True(t, IsValidObjectID("507f1f77bcf86cd799439011"))
	assert.False(t, IsValidObjectID("short"))
	assert.False(t, IsValidObjectID("zzzzzzzzzzzzzzzzzzzzzzzz"))
	assert.False(t, IsValidObjectID(""))
}

// =========================================================================
// DRIVER IDENTITY TESTS
// =========================================================================

func TestMongoDB_Name(t *testing.T) {
	db := New()
	assert.Equal(t, "mongo", db.Name())
}

func TestMongoDB_DatabaseNilClient(t *testing.T) {
	db := New()
	assert.Nil(t, db.Database())
}

func TestMongoDB_DatabaseName(t *testing.T) {
	db := &MongoDB{dbName: "testdb"}
	assert.Equal(t, "testdb", db.DatabaseName())
}

// =========================================================================
// OPTIONS TESTS
// =========================================================================

func TestWithDatabase(t *testing.T) {
	opts := defaultMongoOptions()
	WithDatabase("mydb")(opts)
	assert.Equal(t, "mydb", opts.Database)
}

func TestDefaultMongoOptions(t *testing.T) {
	opts := defaultMongoOptions()
	assert.Equal(t, "", opts.Database)
}

// =========================================================================
// RESULT TESTS
// =========================================================================

func TestMongoResult_RowsAffected_Insert(t *testing.T) {
	r := &mongoResult{insertedID: "abc123"}
	n, err := r.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)
}

func TestMongoResult_RowsAffected_Update(t *testing.T) {
	r := &mongoResult{matchedCount: 3, modifiedCount: 2}
	n, err := r.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(2), n)
}

func TestMongoResult_RowsAffected_Delete(t *testing.T) {
	r := &mongoResult{deletedCount: 5}
	n, err := r.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(5), n)
}

func TestMongoResult_RowsAffected_Zero(t *testing.T) {
	r := &mongoResult{}
	n, err := r.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)
}

func TestMongoResult_LastInsertId(t *testing.T) {
	r := &mongoResult{insertedID: "abc123"}
	_, err := r.LastInsertId()
	assert.ErrorIs(t, err, ErrLastInsertIDNotSupported)
}

func TestMongoResult_InsertedID(t *testing.T) {
	r := &mongoResult{insertedID: "abc123"}
	assert.Equal(t, "abc123", r.InsertedID())
}

func TestMongoResult_MatchedCount(t *testing.T) {
	r := &mongoResult{matchedCount: 10}
	assert.Equal(t, int64(10), r.MatchedCount())
}

func TestMongoResult_ModifiedCount(t *testing.T) {
	r := &mongoResult{modifiedCount: 7}
	assert.Equal(t, int64(7), r.ModifiedCount())
}

func TestMongoResult_DeletedCount(t *testing.T) {
	r := &mongoResult{deletedCount: 3}
	assert.Equal(t, int64(3), r.DeletedCount())
}

func TestMongoResult_UpsertedCount(t *testing.T) {
	r := &mongoResult{upsertedCount: 1}
	assert.Equal(t, int64(1), r.UpsertedCount())
}

// =========================================================================
// URI PARSING TESTS
// =========================================================================

func TestExtractDBName(t *testing.T) {
	tests := []struct {
		name string
		uri  string
		want string
	}{
		{
			name: "standard",
			uri:  "mongodb://localhost:27017/mydb",
			want: "mydb",
		},
		{
			name: "with options",
			uri:  "mongodb://localhost:27017/mydb?retryWrites=true&w=majority",
			want: "mydb",
		},
		{
			name: "with auth",
			uri:  "mongodb://user:pass@localhost:27017/mydb",
			want: "mydb",
		},
		{
			name: "replica set",
			uri:  "mongodb://host1:27017,host2:27017/mydb?replicaSet=rs0",
			want: "mydb",
		},
		{
			name: "mongodb+srv",
			uri:  "mongodb+srv://cluster.example.com/mydb",
			want: "mydb",
		},
		{
			name: "no database",
			uri:  "mongodb://localhost:27017",
			want: "",
		},
		{
			name: "empty database",
			uri:  "mongodb://localhost:27017/",
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractDBName(tt.uri)
			assert.Equal(t, tt.want, got)
		})
	}
}
