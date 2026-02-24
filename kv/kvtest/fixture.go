package kvtest

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/xraph/grove/kv"
)

// SetupStore creates a Store with the mock driver for testing.
func SetupStore(t *testing.T, opts ...kv.Option) *kv.Store {
	t.Helper()

	drv := NewMockDriver()
	err := drv.Open(context.Background(), "mock://")
	require.NoError(t, err)

	store, err := kv.Open(drv, opts...)
	require.NoError(t, err)

	t.Cleanup(func() {
		store.Close()
	})

	return store
}

// SetupTwoStores creates two independent stores backed by separate mock drivers.
// Useful for CRDT syncer tests that need primary/replica pairs.
func SetupTwoStores(t *testing.T) (primary *kv.Store, replica *kv.Store) {
	t.Helper()

	drv1 := NewMockDriver()
	require.NoError(t, drv1.Open(context.Background(), "mock://primary"))
	s1, err := kv.Open(drv1)
	require.NoError(t, err)

	drv2 := NewMockDriver()
	require.NoError(t, drv2.Open(context.Background(), "mock://replica"))
	s2, err := kv.Open(drv2)
	require.NoError(t, err)

	t.Cleanup(func() {
		s1.Close()
		s2.Close()
	})

	return s1, s2
}

// BenchStore creates a Store with the mock driver for benchmarks.
func BenchStore(b *testing.B, opts ...kv.Option) *kv.Store {
	b.Helper()

	drv := NewMockDriver()
	if err := drv.Open(context.Background(), "mock://"); err != nil {
		b.Fatal(err)
	}

	store, err := kv.Open(drv, opts...)
	if err != nil {
		b.Fatal(err)
	}

	b.Cleanup(func() {
		store.Close()
	})

	return store
}

// PopulateStore pre-populates n keys formatted as "key:0" → "value:0", etc.
func PopulateStore(b *testing.B, store *kv.Store, n int) {
	b.Helper()
	ctx := context.Background()
	for i := 0; i < n; i++ {
		if err := store.Set(ctx, fmt.Sprintf("key:%d", i), fmt.Sprintf("value:%d", i)); err != nil {
			b.Fatal(err)
		}
	}
}
