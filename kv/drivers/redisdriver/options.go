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
	if db.client != nil {
		return fmt.Errorf("redisdriver: already opened; use New to create another client")
	}
	db.opts = driver.ApplyOptions(opts)

	return db.openClient(ctx, redis.NewClusterClient(clusterOptions(addrs, db.opts)))
}

// OpenSentinel connects to Redis via Sentinel for high availability.
func (db *RedisDB) OpenSentinel(ctx context.Context, masterName string, sentinelAddrs []string, opts ...driver.Option) error {
	if db.client != nil {
		return fmt.Errorf("redisdriver: already opened; use New to create another client")
	}
	db.opts = driver.ApplyOptions(opts)

	return db.openClient(ctx, redis.NewFailoverClient(sentinelOptions(masterName, sentinelAddrs, db.opts)))
}

func clusterOptions(addrs []string, common *driver.DriverOptions) *redis.ClusterOptions {
	clusterOpts := &redis.ClusterOptions{
		Addrs: addrs,
	}
	clusterOpts.PoolSize = poolLimit(common)
	clusterOpts.MaxActiveConns = clusterOpts.PoolSize
	if common.DialTimeout > 0 {
		clusterOpts.DialTimeout = common.DialTimeout
	}
	if common.ReadTimeout > 0 {
		clusterOpts.ReadTimeout = common.ReadTimeout
	}
	if common.WriteTimeout > 0 {
		clusterOpts.WriteTimeout = common.WriteTimeout
	}
	if common.TLSConfig != nil {
		clusterOpts.TLSConfig = common.TLSConfig
	}

	return clusterOpts
}

func sentinelOptions(masterName string, sentinelAddrs []string, common *driver.DriverOptions) *redis.FailoverOptions {
	failoverOpts := &redis.FailoverOptions{
		MasterName:    masterName,
		SentinelAddrs: sentinelAddrs,
	}
	failoverOpts.PoolSize = poolLimit(common)
	failoverOpts.MaxActiveConns = failoverOpts.PoolSize
	if common.DialTimeout > 0 {
		failoverOpts.DialTimeout = common.DialTimeout
	}
	if common.TLSConfig != nil {
		failoverOpts.TLSConfig = common.TLSConfig
	}

	return failoverOpts
}

// poolLimit is a hard per-node budget, including connections allocated above
// the SDK's base pool size. Invalid custom options cannot uncap it.
func poolLimit(common *driver.DriverOptions) int {
	if common.PoolSize > 0 {
		return common.PoolSize
	}
	return driver.DefaultDriverOptions().PoolSize
}

func clientOptions(dsn string, common *driver.DriverOptions) (*redis.Options, error) {
	redisOpts, err := redis.ParseURL(dsn)
	if err != nil {
		return nil, fmt.Errorf("redisdriver: parse dsn: %w", err)
	}
	redisOpts.PoolSize = poolLimit(common)
	// An explicit DSN cap is respected; zero must not inherit the SDK's
	// unlimited default. PoolSize alone is only a baseline in go-redis.
	if redisOpts.MaxActiveConns <= 0 {
		redisOpts.MaxActiveConns = redisOpts.PoolSize
	}
	if common.DialTimeout > 0 {
		redisOpts.DialTimeout = common.DialTimeout
	}
	if common.ReadTimeout > 0 {
		redisOpts.ReadTimeout = common.ReadTimeout
	}
	if common.WriteTimeout > 0 {
		redisOpts.WriteTimeout = common.WriteTimeout
	}
	if common.TLSConfig != nil {
		redisOpts.TLSConfig = common.TLSConfig
	}

	return redisOpts, nil
}
