package redisdriver

import (
	"context"
	"fmt"
)

// Scan iterates over all keys matching pattern using Redis SCAN command.
// This is cursor-based and safe for production use (does not block the server).
func (db *RedisDB) Scan(ctx context.Context, pattern string, fn func(key string) error) error {
	var cursor uint64
	for {
		keys, nextCursor, err := db.client.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return fmt.Errorf("redisdriver: scan: %w", err)
		}

		for _, key := range keys {
			if err := fn(key); err != nil {
				return err
			}
		}

		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}
	return nil
}
