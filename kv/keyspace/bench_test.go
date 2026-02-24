package keyspace_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/xraph/grove/kv/keyspace"
	"github.com/xraph/grove/kv/kvtest"
)

func BenchmarkKeyspace_Get(b *testing.B) {
	store := kvtest.BenchStore(b)
	ks := keyspace.New[string](store, "bench")
	ctx := context.Background()
	_ = ks.Set(ctx, "item", "hello")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ks.Get(ctx, "item")
	}
}

func BenchmarkKeyspace_Set(b *testing.B) {
	store := kvtest.BenchStore(b)
	ks := keyspace.New[string](store, "bench")
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ks.Set(ctx, fmt.Sprintf("item:%d", i), "hello")
	}
}

func BenchmarkComposeKey(b *testing.B) {
	for _, n := range []int{2, 5, 10} {
		b.Run(fmt.Sprintf("%d_segments", n), func(b *testing.B) {
			segments := make([]string, n)
			for i := range segments {
				segments[i] = fmt.Sprintf("seg%d", i)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = keyspace.ComposeKey(":", segments...)
			}
		})
	}
}

func BenchmarkParseKey(b *testing.B) {
	for _, n := range []int{2, 5} {
		b.Run(fmt.Sprintf("%d_segments", n), func(b *testing.B) {
			segments := make([]string, n)
			for i := range segments {
				segments[i] = fmt.Sprintf("seg%d", i)
			}
			key := keyspace.ComposeKey(":", segments...)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = keyspace.ParseKey(key, ":")
			}
		})
	}
}
