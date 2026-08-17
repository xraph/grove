package redisdriver_test

import (
	"context"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/xraph/grove/kv/drivers/redisdriver"
	"github.com/xraph/grove/kv/kvtest"
)

// TestCollections runs the shared collection suite against a real Redis.
func TestCollections(t *testing.T) {
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

	kvtest.RunCollectionSuite(t, drv)
}
