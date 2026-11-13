package redisdriver

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/xraph/grove/kv/driver"
)

func TestPoolLimitsForEveryMode(t *testing.T) {
	for _, size := range []int{0, -1, 2, 10} {
		common := driver.DefaultDriverOptions()
		common.PoolSize = size
		want := size
		if want <= 0 {
			want = 10
		}
		standalone, err := clientOptions("redis://localhost:6379", common)
		if err != nil {
			t.Fatal(err)
		}
		cluster := clusterOptions([]string{"localhost:6379"}, common)
		sentinel := sentinelOptions("master", []string{"localhost:26379"}, common)
		for mode, limits := range map[string][2]int{
			"standalone": {standalone.PoolSize, standalone.MaxActiveConns},
			"cluster":    {cluster.PoolSize, cluster.MaxActiveConns},
			"sentinel":   {sentinel.PoolSize, sentinel.MaxActiveConns},
		} {
			if limits != [2]int{want, want} {
				t.Errorf("%s pool size %d: got %v, want base and cap %d", mode, size, limits, want)
			}
		}
	}
	opts, err := clientOptions("redis://localhost:6379?max_active_conns=3", driver.DefaultDriverOptions())
	if err != nil {
		t.Fatal(err)
	}
	if opts.MaxActiveConns != 3 {
		t.Fatal("explicit DSN cap was overwritten")
	}
}

func TestDedicatedConnectionsCannotExceedPoolLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-backed integration test in -short mode")
	}
	ctr, err := tcredis.Run(t.Context(), "redis:7-alpine")
	if err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}
	testcontainers.CleanupContainer(t, ctr)
	uri, err := ctr.ConnectionString(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	db := New()
	if err := db.Open(t.Context(), uri, driver.WithPoolSize(2)); err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	client, ok := db.client.(*redis.Client)
	if !ok {
		t.Fatal("expected standalone client")
	}
	first, second := client.Conn(), client.Conn()
	defer first.Close()
	defer second.Close()
	if err := first.Ping(t.Context()).Err(); err != nil {
		t.Fatal(err)
	}
	if err := second.Ping(t.Context()).Err(); err != nil {
		t.Fatal(err)
	}
	third := client.Conn()
	defer third.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if err := third.Ping(ctx).Err(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("third connection exceeded the configured budget: %v", err)
	}
	if got := client.PoolStats().TotalConns; got > 2 {
		t.Fatalf("pool has %d connections, want at most 2", got)
	}
}
