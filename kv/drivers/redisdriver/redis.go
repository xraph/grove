// Package redisdriver provides a Redis driver for Grove KV.
//
// It wraps github.com/redis/go-redis/v9 and supports Redis, Valkey,
// and DragonflyDB. The driver implements all optional KV interfaces
// including batch, TTL, scan, and CAS operations.
package redisdriver

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	kv "github.com/xraph/grove/kv"
	"github.com/xraph/grove/kv/driver"
)

// RedisDB implements driver.Driver for Redis.
type RedisDB struct {
	client redis.UniversalClient
	opts   *driver.DriverOptions
}

var (
	_ driver.Driver      = (*RedisDB)(nil)
	_ driver.BatchDriver = (*RedisDB)(nil)
	_ driver.TTLDriver   = (*RedisDB)(nil)
	_ driver.ScanDriver  = (*RedisDB)(nil)
	_ driver.CASDriver   = (*RedisDB)(nil)
)

// New creates a new unconnected Redis driver.
func New() *RedisDB {
	return &RedisDB{}
}

func (db *RedisDB) Name() string { return "redis" }

// Open connects to Redis using the given DSN (e.g., "redis://localhost:6379/0").
func (db *RedisDB) Open(ctx context.Context, dsn string, opts ...driver.Option) error {
	db.opts = driver.ApplyOptions(opts)

	redisOpts, err := redis.ParseURL(dsn)
	if err != nil {
		return fmt.Errorf("redisdriver: parse dsn: %w", err)
	}
	if db.opts.PoolSize > 0 {
		redisOpts.PoolSize = db.opts.PoolSize
	}
	if db.opts.DialTimeout > 0 {
		redisOpts.DialTimeout = db.opts.DialTimeout
	}
	if db.opts.ReadTimeout > 0 {
		redisOpts.ReadTimeout = db.opts.ReadTimeout
	}
	if db.opts.WriteTimeout > 0 {
		redisOpts.WriteTimeout = db.opts.WriteTimeout
	}
	if db.opts.TLSConfig != nil {
		redisOpts.TLSConfig = db.opts.TLSConfig
	}

	db.client = redis.NewClient(redisOpts)
	return db.client.Ping(ctx).Err()
}

func (db *RedisDB) Close() error {
	if db.client == nil {
		return nil
	}
	return db.client.Close()
}

func (db *RedisDB) Ping(ctx context.Context) error {
	return db.client.Ping(ctx).Err()
}

func (db *RedisDB) Info() driver.DriverInfo {
	return driver.DriverInfo{
		Name:    "redis",
		Version: "7",
		Capabilities: driver.CapTTL | driver.CapCAS | driver.CapScan |
			driver.CapBatch | driver.CapPubSub | driver.CapStreams |
			driver.CapTransaction | driver.CapSortedSet,
	}
}

// --- Core Commands ---

func (db *RedisDB) Get(ctx context.Context, key string) ([]byte, error) {
	val, err := db.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, kv.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("redisdriver: get: %w", err)
	}
	return val, nil
}

func (db *RedisDB) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if err := db.client.Set(ctx, key, value, ttl).Err(); err != nil {
		return fmt.Errorf("redisdriver: set: %w", err)
	}
	return nil
}

func (db *RedisDB) Delete(ctx context.Context, keys ...string) (int64, error) {
	n, err := db.client.Del(ctx, keys...).Result()
	if err != nil {
		return 0, fmt.Errorf("redisdriver: del: %w", err)
	}
	return n, nil
}

func (db *RedisDB) Exists(ctx context.Context, keys ...string) (int64, error) {
	n, err := db.client.Exists(ctx, keys...).Result()
	if err != nil {
		return 0, fmt.Errorf("redisdriver: exists: %w", err)
	}
	return n, nil
}

// --- BatchDriver ---

func (db *RedisDB) MGet(ctx context.Context, keys []string) ([][]byte, error) {
	vals, err := db.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, fmt.Errorf("redisdriver: mget: %w", err)
	}

	result := make([][]byte, len(vals))
	for i, v := range vals {
		if v == nil {
			continue
		}
		switch val := v.(type) {
		case string:
			result[i] = []byte(val)
		case []byte:
			result[i] = val
		}
	}
	return result, nil
}

func (db *RedisDB) MSet(ctx context.Context, pairs map[string][]byte, ttl time.Duration) error {
	pipe := db.client.Pipeline()

	for k, v := range pairs {
		pipe.Set(ctx, k, v, ttl)
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("redisdriver: mset: %w", err)
	}
	return nil
}

// --- TTLDriver ---

func (db *RedisDB) TTL(ctx context.Context, key string) (time.Duration, error) {
	ttl, err := db.client.TTL(ctx, key).Result()
	if err != nil {
		return 0, fmt.Errorf("redisdriver: ttl: %w", err)
	}
	// Redis returns -2 for non-existent keys, -1 for keys with no expiry.
	if ttl == -2*time.Second {
		return 0, kv.ErrNotFound
	}
	return ttl, nil
}

func (db *RedisDB) Expire(ctx context.Context, key string, ttl time.Duration) error {
	ok, err := db.client.Expire(ctx, key, ttl).Result()
	if err != nil {
		return fmt.Errorf("redisdriver: expire: %w", err)
	}
	if !ok {
		return kv.ErrNotFound
	}
	return nil
}

// --- CASDriver ---

func (db *RedisDB) SetNX(ctx context.Context, key string, value []byte, ttl time.Duration) (bool, error) {
	ok, err := db.client.SetNX(ctx, key, value, ttl).Result()
	if err != nil {
		return false, fmt.Errorf("redisdriver: setnx: %w", err)
	}
	return ok, nil
}

func (db *RedisDB) SetXX(ctx context.Context, key string, value []byte, ttl time.Duration) (bool, error) {
	ok, err := db.client.SetXX(ctx, key, value, ttl).Result()
	if err != nil {
		return false, fmt.Errorf("redisdriver: setxx: %w", err)
	}
	return ok, nil
}

// Client returns the underlying go-redis UniversalClient.
func (db *RedisDB) Client() redis.UniversalClient {
	return db.client
}
