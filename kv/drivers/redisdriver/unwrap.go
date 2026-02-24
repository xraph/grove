package redisdriver

import (
	"github.com/redis/go-redis/v9"

	"github.com/xraph/grove/kv"
)

// Unwrap extracts the underlying RedisDB from a Store.
// Panics if the store's driver is not a *RedisDB.
func Unwrap(store *kv.Store) *RedisDB {
	rdb, ok := store.Driver().(*RedisDB)
	if !ok {
		panic("redisdriver: driver is not a *RedisDB")
	}
	return rdb
}

// UnwrapClient extracts the go-redis UniversalClient from a Store.
// Panics if the store's driver is not a *RedisDB.
func UnwrapClient(store *kv.Store) redis.UniversalClient {
	return Unwrap(store).Client()
}
