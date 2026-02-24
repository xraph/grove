package extension_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/xraph/grove/kv/extension"
	"github.com/xraph/grove/kv/kvtest"
)

func BenchmarkAtomicCounter_Increment(b *testing.B) {
	store := kvtest.BenchStore(b)
	ctr := extension.NewAtomicCounter(store, "bench")
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ctr.Increment(ctx, 1)
	}
}

func BenchmarkLeaderboard_Add(b *testing.B) {
	for _, n := range []int{10, 100} {
		b.Run(fmt.Sprintf("%d_members", n), func(b *testing.B) {
			store := kvtest.BenchStore(b)
			lb := extension.NewLeaderboard(store, "bench")
			ctx := context.Background()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = lb.Add(ctx, fmt.Sprintf("player:%d", i%n), float64(i))
			}
		})
	}
}

func BenchmarkLeaderboard_TopN(b *testing.B) {
	for _, total := range []int{100, 1000} {
		b.Run(fmt.Sprintf("Top10_from_%d", total), func(b *testing.B) {
			store := kvtest.BenchStore(b)
			lb := extension.NewLeaderboard(store, "bench")
			ctx := context.Background()
			for i := 0; i < total; i++ {
				_ = lb.Add(ctx, fmt.Sprintf("player:%d", i), float64(i))
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = lb.TopN(ctx, 10)
			}
		})
	}
}

func BenchmarkQueue_EnqueueDequeue(b *testing.B) {
	store := kvtest.BenchStore(b)
	q := extension.NewQueue(store, "bench")
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id, _ := q.Enqueue(ctx, "payload")
		_, _ = q.Dequeue(ctx)
		_ = q.Ack(ctx, id)
	}
}

func BenchmarkSessionStore_CreateGet(b *testing.B) {
	store := kvtest.BenchStore(b)
	ss := extension.NewSessionStore(store)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id, _ := ss.Create(ctx, map[string]string{"user": "alice"})
		var data map[string]string
		_ = ss.Get(ctx, id, &data)
	}
}

func BenchmarkRateLimiter_Allow(b *testing.B) {
	store := kvtest.BenchStore(b)
	rl := extension.NewRateLimiter(store, "bench", 1000000, time.Minute)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = rl.Allow(ctx, "user1")
	}
}
