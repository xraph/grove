package kv_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/xraph/grove/kv"
	"github.com/xraph/grove/kv/kvtest"
)

func BenchmarkStore_Get(b *testing.B) {
	store := kvtest.BenchStore(b)
	ctx := context.Background()
	_ = store.Set(ctx, "bench:key", "bench:value")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var v string
		_ = store.Get(ctx, "bench:key", &v)
	}
}

func BenchmarkStore_Set(b *testing.B) {
	store := kvtest.BenchStore(b)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = store.Set(ctx, fmt.Sprintf("bench:%d", i), "value")
	}
}

func BenchmarkStore_GetSet_Roundtrip(b *testing.B) {
	store := kvtest.BenchStore(b)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench:%d", i)
		_ = store.Set(ctx, key, "value")
		var v string
		_ = store.Get(ctx, key, &v)
	}
}

func BenchmarkStore_MGet(b *testing.B) {
	for _, n := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			store := kvtest.BenchStore(b)
			kvtest.PopulateStore(b, store, n)
			keys := make([]string, n)
			for i := 0; i < n; i++ {
				keys[i] = fmt.Sprintf("key:%d", i)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				dest := make(map[string]any)
				_ = store.MGet(context.Background(), keys, dest)
			}
		})
	}
}

func BenchmarkStore_MSet(b *testing.B) {
	for _, n := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			store := kvtest.BenchStore(b)
			pairs := make(map[string]any, n)
			for i := 0; i < n; i++ {
				pairs[fmt.Sprintf("key:%d", i)] = fmt.Sprintf("value:%d", i)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = store.MSet(context.Background(), pairs)
			}
		})
	}
}

func BenchmarkStore_Delete(b *testing.B) {
	store := kvtest.BenchStore(b)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench:%d", i)
		_ = store.Set(ctx, key, "value")
		_ = store.Delete(ctx, key)
	}
}

func BenchmarkStore_Exists(b *testing.B) {
	store := kvtest.BenchStore(b)
	ctx := context.Background()
	kvtest.PopulateStore(b, store, 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = store.Exists(ctx, "key:0", "key:50", "key:99")
	}
}

func BenchmarkStore_Scan(b *testing.B) {
	for _, n := range []int{100, 1000} {
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			store := kvtest.BenchStore(b)
			kvtest.PopulateStore(b, store, n)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = store.Scan(context.Background(), "key:*", func(key string) error {
					return nil
				})
			}
		})
	}
}

func BenchmarkBatch_Exec(b *testing.B) {
	store := kvtest.BenchStore(b)
	ctx := context.Background()
	// Pre-populate some data for gets
	kvtest.PopulateStore(b, store, 50)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := kv.NewBatch(store)
		for j := 0; j < 50; j++ {
			batch.Set(fmt.Sprintf("batch:%d", j), "value")
		}
		batch.Get("key:0", "key:10", "key:20", "key:30", "key:40")
		batch.Delete("batch:0", "batch:1")
		_, _ = batch.Exec(ctx)
	}
}

func BenchmarkStore_SetWithHook(b *testing.B) {
	// This benchmark just measures Set with no hooks vs with the store overhead.
	// We can't easily add hooks in bench since hookEntries are internal,
	// but we can measure baseline.
	store := kvtest.BenchStore(b)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = store.Set(ctx, "bench:hook:key", "value")
	}
}
