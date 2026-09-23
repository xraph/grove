package redisdriver_test

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/xraph/grove/kv"
	"github.com/xraph/grove/kv/driver"
	"github.com/xraph/grove/kv/drivers/redisdriver"
	"github.com/xraph/grove/kv/middleware"
)

// ns is the namespace every store in this file writes under.
const ns = "app1"

// TestNamespace checks that a key-rewriting hook reaches the driver on
// every Store method, not only Get and Set.
//
// Each case asserts against the physical keyspace through a raw client,
// because a namespaced store that reads and writes the same wrong key
// looks correct from the store's side. Where it matters, a decoy sits at
// the un-namespaced key so that touching it shows up.
func TestNamespace(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-backed test in -short mode")
	}

	ctx := context.Background()

	ctr, err := tcredis.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}

	t.Cleanup(func() {
		if terr := testcontainers.TerminateContainer(ctr); terr != nil {
			t.Errorf("terminate redis: %v", terr)
		}
	})

	uri, err := ctr.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}

	drv := redisdriver.New()
	if oerr := drv.Open(ctx, uri); oerr != nil {
		t.Fatalf("open redisdriver: %v", oerr)
	}

	store, err := kv.Open(drv, kv.WithHook(middleware.NewNamespace(ns)))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	t.Cleanup(func() { _ = store.Close() })

	rdb := drv.Client()

	t.Run("Delete", func(t *testing.T) {
		mustSet(t, rdb, ns+":del:x", "ns")
		mustSet(t, rdb, "del:x", "decoy")

		if err := store.Delete(ctx, "del:x"); err != nil {
			t.Fatalf("Delete: %v", err)
		}

		assertExists(t, rdb, ns+":del:x", false)
		assertExists(t, rdb, "del:x", true)
	})

	t.Run("Exists", func(t *testing.T) {
		mustSet(t, rdb, "exists:x", "decoy")

		assertCount(t, "Exists before namespaced write", mustInt(store.Exists(ctx, "exists:x")), 0)

		mustSet(t, rdb, ns+":exists:x", "ns")

		assertCount(t, "Exists after namespaced write", mustInt(store.Exists(ctx, "exists:x")), 1)
	})

	t.Run("MGet", func(t *testing.T) {
		mustSet(t, rdb, ns+":mget:a", `"ns"`)
		mustSet(t, rdb, "mget:a", `"decoy"`)

		dest := map[string]any{}
		if err := store.MGet(ctx, []string{"mget:a"}, dest); err != nil {
			t.Fatalf("MGet: %v", err)
		}

		if got := dest["mget:a"]; got != "ns" {
			t.Fatalf("MGet[mget:a] = %v, want ns", got)
		}
	})

	t.Run("MGetRaw", func(t *testing.T) {
		mustSet(t, rdb, ns+":mgetraw:a", "ns")
		mustSet(t, rdb, "mgetraw:a", "decoy")

		got, err := store.MGetRaw(ctx, []string{"mgetraw:a", "mgetraw:missing"})
		if err != nil {
			t.Fatalf("MGetRaw: %v", err)
		}

		if len(got) != 2 || string(got[0]) != "ns" || got[1] != nil {
			t.Fatalf("MGetRaw = %q, want [ns <nil>]", got)
		}
	})

	t.Run("MSet", func(t *testing.T) {
		if err := store.MSet(ctx, map[string]any{"mset:a": "v", "mset:b": "w"}); err != nil {
			t.Fatalf("MSet: %v", err)
		}

		assertExists(t, rdb, ns+":mset:a", true)
		assertExists(t, rdb, ns+":mset:b", true)
		assertExists(t, rdb, "mset:a", false)
		assertExists(t, rdb, "mset:b", false)

		var got string
		if err := store.Get(ctx, "mset:b", &got); err != nil || got != "w" {
			t.Fatalf("Get(mset:b) = %q, %v; want w", got, err)
		}
	})

	t.Run("Expire", func(t *testing.T) {
		mustSet(t, rdb, ns+":expire:x", "ns")
		mustSet(t, rdb, "expire:x", "decoy")

		if err := store.Expire(ctx, "expire:x", time.Hour); err != nil {
			t.Fatalf("Expire: %v", err)
		}

		if ttl := rdb.TTL(ctx, ns+":expire:x").Val(); ttl <= 0 {
			t.Fatalf("namespaced key TTL = %v, want positive", ttl)
		}

		if ttl := rdb.TTL(ctx, "expire:x").Val(); ttl != -1 {
			t.Fatalf("decoy TTL = %v, want -1 (no expiry)", ttl)
		}
	})

	t.Run("TTL", func(t *testing.T) {
		if err := rdb.Set(ctx, ns+":ttl:x", "ns", time.Hour).Err(); err != nil {
			t.Fatalf("seed: %v", err)
		}

		ttl, err := store.TTL(ctx, "ttl:x")
		if err != nil {
			t.Fatalf("TTL: %v", err)
		}

		if ttl <= 0 {
			t.Fatalf("TTL = %v, want positive", ttl)
		}
	})

	t.Run("Scan", func(t *testing.T) {
		mustSet(t, rdb, ns+":scan:a", "ns")
		mustSet(t, rdb, ns+":scan:b", "ns")
		mustSet(t, rdb, "scan:decoy", "decoy")

		var got []string
		if err := store.Scan(ctx, "scan:*", func(key string) error {
			got = append(got, key)

			return nil
		}); err != nil {
			t.Fatalf("Scan: %v", err)
		}

		sort.Strings(got)

		// Scan yields the keys the caller used, so each one round-trips
		// through Get instead of being namespaced a second time.
		if want := []string{"scan:a", "scan:b"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("Scan = %q, want %q", got, want)
		}
	})

	t.Run("SortedSet", func(t *testing.T) {
		if err := rdb.ZAdd(ctx, "zset:q", redis.Z{Member: "decoy", Score: 1}).Err(); err != nil {
			t.Fatalf("seed decoy: %v", err)
		}

		if _, err := store.ZAdd(ctx, "zset:q", driver.ScoredMember{Member: "m", Score: 1}); err != nil {
			t.Fatalf("ZAdd: %v", err)
		}

		if got := rdb.ZRange(ctx, ns+":zset:q", 0, -1).Val(); !reflect.DeepEqual(got, []string{"m"}) {
			t.Fatalf("physical %s:zset:q = %q, want [m]", ns, got)
		}

		got, err := store.ZRange(ctx, "zset:q", driver.RangeSpec{})
		if err != nil {
			t.Fatalf("ZRange: %v", err)
		}

		if !reflect.DeepEqual(got, []string{"m"}) {
			t.Fatalf("ZRange = %q, want [m]", got)
		}
	})

	t.Run("Set", func(t *testing.T) {
		if err := rdb.SAdd(ctx, "set:s", "decoy").Err(); err != nil {
			t.Fatalf("seed decoy: %v", err)
		}

		if _, err := store.SAdd(ctx, "set:s", "m"); err != nil {
			t.Fatalf("SAdd: %v", err)
		}

		if got := rdb.SMembers(ctx, ns+":set:s").Val(); !reflect.DeepEqual(got, []string{"m"}) {
			t.Fatalf("physical %s:set:s = %q, want [m]", ns, got)
		}

		got, err := store.SMembers(ctx, "set:s")
		if err != nil {
			t.Fatalf("SMembers: %v", err)
		}

		if !reflect.DeepEqual(got, []string{"m"}) {
			t.Fatalf("SMembers = %q, want [m]", got)
		}
	})

	t.Run("Hash", func(t *testing.T) {
		if err := rdb.HSet(ctx, "hash:h", "f", "decoy").Err(); err != nil {
			t.Fatalf("seed decoy: %v", err)
		}

		if _, err := store.HSet(ctx, "hash:h", map[string][]byte{"f": []byte("ns")}); err != nil {
			t.Fatalf("HSet: %v", err)
		}

		if got := rdb.HGet(ctx, ns+":hash:h", "f").Val(); got != "ns" {
			t.Fatalf("physical %s:hash:h[f] = %q, want ns", ns, got)
		}

		got, err := store.HGet(ctx, "hash:h", "f")
		if err != nil {
			t.Fatalf("HGet: %v", err)
		}

		if string(got) != "ns" {
			t.Fatalf("HGet = %q, want ns", got)
		}
	})

	t.Run("Stream", func(t *testing.T) {
		if err := rdb.XAdd(ctx, &redis.XAddArgs{Stream: "stream:s", Values: map[string]any{"f": "decoy"}}).Err(); err != nil {
			t.Fatalf("seed decoy: %v", err)
		}

		if _, err := store.XAdd(ctx, "stream:s", map[string][]byte{"f": []byte("ns")}); err != nil {
			t.Fatalf("XAdd: %v", err)
		}

		if n := rdb.XLen(ctx, ns+":stream:s").Val(); n != 1 {
			t.Fatalf("physical %s:stream:s length = %d, want 1", ns, n)
		}

		assertCount(t, "XLen", mustInt(store.XLen(ctx, "stream:s")), 1)
	})

	t.Run("Eval", func(t *testing.T) {
		got, err := store.Eval(ctx, "return KEYS[1]", []string{"eval:k"})
		if err != nil {
			t.Fatalf("Eval: %v", err)
		}

		if got != ns+":eval:k" {
			t.Fatalf("script saw KEYS[1] = %v, want %s:eval:k", got, ns)
		}
	})

	t.Run("Publish", func(t *testing.T) {
		sub := rdb.Subscribe(ctx, ns+":chan")
		t.Cleanup(func() { _ = sub.Close() })

		if _, err := sub.Receive(ctx); err != nil {
			t.Fatalf("subscribe: %v", err)
		}

		if err := store.Publish(ctx, "chan", []byte("hi")); err != nil {
			t.Fatalf("Publish: %v", err)
		}

		select {
		case msg := <-sub.Channel():
			if msg.Payload != "hi" {
				t.Fatalf("payload = %q, want hi", msg.Payload)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("no message on %s:chan", ns)
		}
	})

	t.Run("Batch", func(t *testing.T) {
		mustSet(t, rdb, ns+":batch:get", `"ns"`)
		mustSet(t, rdb, "batch:get", `"decoy"`)
		mustSet(t, rdb, ns+":batch:del", "ns")
		mustSet(t, rdb, "batch:del", "decoy")

		res, err := kv.NewBatch(store).
			Set("batch:set", "v").
			Get("batch:get").
			Delete("batch:del").
			Exec(ctx)
		if err != nil {
			t.Fatalf("Exec: %v", err)
		}

		assertExists(t, rdb, ns+":batch:set", true)
		assertExists(t, rdb, "batch:set", false)
		assertExists(t, rdb, ns+":batch:del", false)
		assertExists(t, rdb, "batch:del", true)

		if got := string(res.Values["batch:get"]); got != `"ns"` {
			t.Fatalf("batch get = %q, want \"ns\"", got)
		}
	})
}

func mustSet(t *testing.T, rdb redis.UniversalClient, key, value string) {
	t.Helper()

	if err := rdb.Set(context.Background(), key, value, 0).Err(); err != nil {
		t.Fatalf("seed %s: %v", key, err)
	}
}

func assertExists(t *testing.T, rdb redis.UniversalClient, key string, want bool) {
	t.Helper()

	n, err := rdb.Exists(context.Background(), key).Result()
	if err != nil {
		t.Fatalf("exists %s: %v", key, err)
	}

	if got := n == 1; got != want {
		t.Fatalf("physical key %s exists = %v, want %v", key, got, want)
	}
}

type intResult struct {
	n   int64
	err error
}

func mustInt(n int64, err error) intResult { return intResult{n, err} }

func assertCount(t *testing.T, what string, got intResult, want int64) {
	t.Helper()

	if got.err != nil {
		t.Fatalf("%s: %v", what, got.err)
	}

	if got.n != want {
		t.Fatalf("%s = %d, want %d", what, got.n, want)
	}
}
