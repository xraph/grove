package kvcrdt_test

import (
	"context"
	"fmt"
	"testing"

	kvcrdt "github.com/xraph/grove/kv/crdt"
	"github.com/xraph/grove/kv/kvtest"
)

func BenchmarkCRDTCounter_Increment(b *testing.B) {
	store := kvtest.BenchStore(b)
	ctr := kvcrdt.NewCounter(store, "crdt:bench:counter", kvcrdt.WithNodeID("node1"))
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ctr.Increment(ctx, 1)
	}
}

func BenchmarkCRDTCounter_Value(b *testing.B) {
	store := kvtest.BenchStore(b)
	ctr := kvcrdt.NewCounter(store, "crdt:bench:counter:val", kvcrdt.WithNodeID("node1"))
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		_ = ctr.Increment(ctx, 1)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ctr.Value(ctx)
	}
}

func BenchmarkCRDTRegister_Set(b *testing.B) {
	store := kvtest.BenchStore(b)
	reg := kvcrdt.NewRegister[string](store, "crdt:bench:reg:set", kvcrdt.WithNodeID("node1"))
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = reg.Set(ctx, "value")
	}
}

func BenchmarkCRDTRegister_Get(b *testing.B) {
	store := kvtest.BenchStore(b)
	reg := kvcrdt.NewRegister[string](store, "crdt:bench:reg:get", kvcrdt.WithNodeID("node1"))
	ctx := context.Background()
	_ = reg.Set(ctx, "value")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = reg.Get(ctx)
	}
}

func BenchmarkCRDTSet_Add(b *testing.B) {
	for _, n := range []int{10, 100} {
		b.Run(fmt.Sprintf("%d_elements", n), func(b *testing.B) {
			store := kvtest.BenchStore(b)
			set := kvcrdt.NewSet[string](store, "crdt:bench:set", kvcrdt.WithNodeID("node1"))
			ctx := context.Background()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = set.Add(ctx, fmt.Sprintf("elem:%d", i%n))
			}
		})
	}
}

func BenchmarkCRDTMap_Set(b *testing.B) {
	for _, n := range []int{10, 50} {
		b.Run(fmt.Sprintf("%d_fields", n), func(b *testing.B) {
			store := kvtest.BenchStore(b)
			m := kvcrdt.NewMap(store, "crdt:bench:map", kvcrdt.WithNodeID("node1"))
			ctx := context.Background()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = m.Set(ctx, fmt.Sprintf("field:%d", i%n), "value")
			}
		})
	}
}

func BenchmarkSyncer_Sync(b *testing.B) {
	for _, n := range []int{10, 100} {
		b.Run(fmt.Sprintf("%d_keys", n), func(b *testing.B) {
			store1 := kvtest.BenchStore(b)
			store2 := kvtest.BenchStore(b)
			ctx := context.Background()
			// Pre-populate primary with CRDT counter keys.
			for i := 0; i < n; i++ {
				ctr := kvcrdt.NewCounter(store1, fmt.Sprintf("crdt:bench:%d", i), kvcrdt.WithNodeID("node1"))
				_ = ctr.Increment(ctx, 1)
			}
			syncer := kvcrdt.NewSyncer(store1, store2, kvcrdt.WithKeyPattern("crdt:bench:*"))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = syncer.Sync(ctx)
			}
		})
	}
}
