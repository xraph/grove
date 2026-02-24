package redisdriver

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// Pipeline returns a new Redis pipeline for batching commands.
// The pipeline accumulates commands and executes them in a single round-trip.
func (db *RedisDB) Pipeline() redis.Pipeliner {
	return db.client.Pipeline()
}

// TxPipeline returns a new transactional pipeline (MULTI/EXEC).
func (db *RedisDB) TxPipeline() redis.Pipeliner {
	return db.client.TxPipeline()
}

// Watch executes a transactional WATCH/MULTI/EXEC block on the given keys.
func (db *RedisDB) Watch(ctx context.Context, fn func(tx *redis.Tx) error, keys ...string) error {
	return db.client.Watch(ctx, fn, keys...)
}
