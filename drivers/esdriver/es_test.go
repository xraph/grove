package esdriver

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =========================================================================
// DRIVER IDENTITY TESTS
// =========================================================================

func TestElasticDB_Name(t *testing.T) {
	db := New()
	assert.Equal(t, "elasticsearch", db.Name())
}

func TestElasticDB_DefaultIndex(t *testing.T) {
	db := New()
	assert.Equal(t, "", db.DefaultIndex())
}

func TestElasticDB_NilClient(t *testing.T) {
	db := New()
	assert.Nil(t, db.Client())
}

// =========================================================================
// OPTIONS TESTS
// =========================================================================

func TestDefaultEsOptions(t *testing.T) {
	opts := defaultEsOptions()
	assert.Empty(t, opts.Addresses)
	assert.Empty(t, opts.Username)
	assert.Empty(t, opts.Password)
	assert.Empty(t, opts.APIKey)
	assert.Empty(t, opts.CloudID)
	assert.Nil(t, opts.CACert)
	assert.Empty(t, opts.Refresh)
	assert.Equal(t, 0, opts.MaxRetries)
	assert.Nil(t, opts.Transport)
}

func TestWithBasicAuth(t *testing.T) {
	opts := defaultEsOptions()
	WithBasicAuth("user", "pass")(opts)
	assert.Equal(t, "user", opts.Username)
	assert.Equal(t, "pass", opts.Password)
}

func TestWithAPIKey(t *testing.T) {
	opts := defaultEsOptions()
	WithAPIKey("my-api-key")(opts)
	assert.Equal(t, "my-api-key", opts.APIKey)
}

func TestWithCloudID(t *testing.T) {
	opts := defaultEsOptions()
	WithCloudID("my-cloud-id")(opts)
	assert.Equal(t, "my-cloud-id", opts.CloudID)
}

func TestWithCACert(t *testing.T) {
	cert := []byte("fake-cert-data")
	opts := defaultEsOptions()
	WithCACert(cert)(opts)
	assert.Equal(t, cert, opts.CACert)
}

func TestWithRefresh(t *testing.T) {
	opts := defaultEsOptions()
	WithRefresh("wait_for")(opts)
	assert.Equal(t, "wait_for", opts.Refresh)
}

func TestWithMaxRetries(t *testing.T) {
	opts := defaultEsOptions()
	WithMaxRetries(5)(opts)
	assert.Equal(t, 5, opts.MaxRetries)
}

func TestWithAddresses(t *testing.T) {
	opts := defaultEsOptions()
	WithAddresses("http://node1:9200", "http://node2:9200")(opts)
	assert.Equal(t, []string{"http://node1:9200", "http://node2:9200"}, opts.Addresses)
}

func TestOptionsApply(t *testing.T) {
	opts := defaultEsOptions()
	opts.apply([]EsOption{
		WithBasicAuth("admin", "secret"),
		WithRefresh("true"),
		WithMaxRetries(3),
	})
	assert.Equal(t, "admin", opts.Username)
	assert.Equal(t, "secret", opts.Password)
	assert.Equal(t, "true", opts.Refresh)
	assert.Equal(t, 3, opts.MaxRetries)
}

// =========================================================================
// RESULT TESTS
// =========================================================================

func TestEsResult_RowsAffected_Created(t *testing.T) {
	r := &EsResult{documentID: "abc123", action: "created"}
	n, err := r.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)
}

func TestEsResult_RowsAffected_Updated(t *testing.T) {
	r := &EsResult{documentID: "abc123", action: "updated"}
	n, err := r.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)
}

func TestEsResult_RowsAffected_Deleted(t *testing.T) {
	r := &EsResult{documentID: "abc123", action: "deleted"}
	n, err := r.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)
}

func TestEsResult_RowsAffected_Noop(t *testing.T) {
	r := &EsResult{action: "noop"}
	n, err := r.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)
}

func TestEsResult_RowsAffected_ByQuery(t *testing.T) {
	r := &EsResult{action: "updated", affected: 42}
	n, err := r.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(42), n)
}

func TestEsResult_RowsAffected_Zero(t *testing.T) {
	r := &EsResult{}
	n, err := r.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)
}

func TestEsResult_LastInsertId(t *testing.T) {
	r := &EsResult{documentID: "abc123"}
	_, err := r.LastInsertId()
	assert.ErrorIs(t, err, ErrLastInsertIDNotSupported)
}

func TestEsResult_DocumentID(t *testing.T) {
	r := &EsResult{documentID: "abc123"}
	assert.Equal(t, "abc123", r.DocumentID())
}

func TestEsResult_Version(t *testing.T) {
	r := &EsResult{version: 3}
	assert.Equal(t, int64(3), r.Version())
}

func TestEsResult_Action(t *testing.T) {
	r := &EsResult{action: "created"}
	assert.Equal(t, "created", r.Action())
}

// =========================================================================
// ADDRESS PARSING TESTS
// =========================================================================

func TestParseAddresses(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []string
	}{
		{
			name:  "single address",
			input: "http://localhost:9200",
			want:  []string{"http://localhost:9200"},
		},
		{
			name:  "comma separated",
			input: "http://node1:9200,http://node2:9200,http://node3:9200",
			want:  []string{"http://node1:9200", "http://node2:9200", "http://node3:9200"},
		},
		{
			name:  "with spaces",
			input: "http://node1:9200 , http://node2:9200",
			want:  []string{"http://node1:9200", "http://node2:9200"},
		},
		{
			name:  "empty string",
			input: "",
			want:  []string{"http://localhost:9200"},
		},
		{
			name:  "only commas",
			input: ",,",
			want:  []string{"http://localhost:9200"},
		},
		{
			name:  "https address",
			input: "https://es.example.com:9243",
			want:  []string{"https://es.example.com:9243"},
		},
		{
			name:  "trailing comma",
			input: "http://localhost:9200,",
			want:  []string{"http://localhost:9200"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseAddresses(tt.input)
			assert.Equal(t, tt.want, got)
		})
	}
}

// =========================================================================
// REFRESH OPTION TESTS
// =========================================================================

func TestRefreshOption(t *testing.T) {
	db := &ElasticDB{refresh: "wait_for"}

	// Override returns the override value.
	assert.Equal(t, "true", db.refreshOption("true"))

	// Empty override returns the default.
	assert.Equal(t, "wait_for", db.refreshOption(""))
}

func TestRefreshOption_NoDefault(t *testing.T) {
	db := &ElasticDB{}

	// No default, no override.
	assert.Equal(t, "", db.refreshOption(""))

	// Override still works.
	assert.Equal(t, "true", db.refreshOption("true"))
}

// =========================================================================
// RESOLVE INDEX TESTS
// =========================================================================

func TestResolveIndex_Explicit(t *testing.T) {
	db := &ElasticDB{defaultIndex: "default-idx"}
	assert.Equal(t, "my-index", db.resolveIndex("my-index"))
}

func TestResolveIndex_FallbackToDefault(t *testing.T) {
	db := &ElasticDB{defaultIndex: "default-idx"}
	assert.Equal(t, "default-idx", db.resolveIndex(""))
}

func TestResolveIndex_Empty(t *testing.T) {
	db := &ElasticDB{}
	assert.Equal(t, "", db.resolveIndex(""))
}
