package mongodriver

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpenPreservesExistingClient(t *testing.T) {
	db := New()
	require.NoError(t, db.Open(t.Context(), "mongodb://127.0.0.1:1/test", WithSkipPing(true)))
	defer db.Close()
	existing := db.client
	require.ErrorContains(t, db.Open(t.Context(), "unused"), "already opened")
	require.Same(t, existing, db.client)
}
