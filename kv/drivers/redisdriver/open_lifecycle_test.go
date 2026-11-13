package redisdriver

import (
	"context"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestFailedOpenReleasesClient(t *testing.T) {
	for _, mode := range []string{"standalone", "cluster", "sentinel"} {
		t.Run(mode, func(t *testing.T) {
			db := New()
			defer db.Close()
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			var err error
			switch mode {
			case "standalone":
				err = db.Open(ctx, "redis://127.0.0.1:1")
			case "cluster":
				err = db.OpenCluster(ctx, []string{"127.0.0.1:1"})
			case "sentinel":
				err = db.OpenSentinel(ctx, "test", []string{"127.0.0.1:1"})
			}
			if err == nil {
				t.Fatal("expected startup failure")
			}
			if db.client != nil {
				t.Fatal("failed Open retained an unclosed Redis client")
			}
		})
	}
}

func TestOpenPreservesExistingClient(t *testing.T) {
	db := New()
	db.client = redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	defer db.Close()
	existing := db.client
	for _, open := range []func() error{
		func() error { return db.Open(t.Context(), "unused") },
		func() error { return db.OpenCluster(t.Context(), nil) },
		func() error { return db.OpenSentinel(t.Context(), "unused", nil) },
	} {
		err := open()
		if err == nil || !strings.Contains(err.Error(), "already opened") {
			t.Fatalf("unexpected second Open result: %v", err)
		}
		if db.client != existing {
			t.Fatal("Open replaced the existing client")
		}
	}
}
