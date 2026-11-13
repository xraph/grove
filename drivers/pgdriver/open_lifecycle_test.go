package pgdriver

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpenPreservesExistingPool(t *testing.T) {
	db := New()
	require.NoError(t, db.Open(t.Context(), "postgres://test@127.0.0.1:1/test?sslmode=disable"))
	defer db.Close()
	existing := db.pool
	require.ErrorContains(t, db.Open(t.Context(), "unused"), "already opened")
	require.Same(t, existing, db.pool)
}
