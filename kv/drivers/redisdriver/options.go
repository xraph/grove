package redisdriver

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/xraph/grove/kv/driver"
)

// RedisOption configures Redis-specific settings beyond the common driver options.
type RedisOption func(*redisOptions)

type redisOptions struct {
	clusterAddrs []string
	sentinelOpts *redis.FailoverOptions
}

// NewCluster creates a Redis Cluster driver.
func NewCluster(addrs []string, opts ...driver.Option) *RedisDB {
	db := &RedisDB{
		opts: driver.ApplyOptions(opts),
	}
	return db
}

// OpenCluster connects to a Redis Cluster.
func (db *RedisDB) OpenCluster(ctx context.Context, addrs []string, opts ...driver.Option) error {
	db.opts = driver.ApplyOptions(opts)

	clusterOpts := &redis.ClusterOptions{
		Addrs: addrs,
	}
	if db.opts.PoolSize > 0 {
		clusterOpts.PoolSize = db.opts.PoolSize
	}
	if db.opts.DialTimeout > 0 {
		clusterOpts.DialTimeout = db.opts.DialTimeout
	}
	if db.opts.ReadTimeout > 0 {
		clusterOpts.ReadTimeout = db.opts.ReadTimeout
	}
	if db.opts.WriteTimeout > 0 {
		clusterOpts.WriteTimeout = db.opts.WriteTimeout
	}
	if db.opts.TLSConfig != nil {
		clusterOpts.TLSConfig = db.opts.TLSConfig
	}

	db.client = redis.NewClusterClient(clusterOpts)
	return db.client.Ping(ctx).Err()
}

// OpenSentinel connects to Redis via Sentinel for high availability.
func (db *RedisDB) OpenSentinel(ctx context.Context, masterName string, sentinelAddrs []string, opts ...driver.Option) error {
	db.opts = driver.ApplyOptions(opts)

	failoverOpts := &redis.FailoverOptions{
		MasterName:    masterName,
		SentinelAddrs: sentinelAddrs,
	}
	if db.opts.PoolSize > 0 {
		failoverOpts.PoolSize = db.opts.PoolSize
	}
	if db.opts.DialTimeout > 0 {
		failoverOpts.DialTimeout = db.opts.DialTimeout
	}
	if db.opts.TLSConfig != nil {
		failoverOpts.TLSConfig = db.opts.TLSConfig
	}

	db.client = redis.NewFailoverClient(failoverOpts)
	if err := db.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redisdriver: sentinel ping: %w", err)
	}
	return nil
}
