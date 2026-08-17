package redisdriver

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// Script provides Lua script execution on the Redis server.
type Script struct {
	script *redis.Script
}

// NewScript creates a new Redis Lua script.
func NewScript(src string) *Script {
	return &Script{
		script: redis.NewScript(src),
	}
}

// Run executes the script on the given store's Redis client.
func (s *Script) Run(ctx context.Context, db *RedisDB, keys []string, args ...any) *redis.Cmd {
	return s.script.Run(ctx, db.client, keys, args...)
}

// EvalCmd evaluates a Lua script and returns the raw go-redis command.
//
// Eval, which satisfies driver.ScriptDriver, is the portable form and
// should be preferred: it returns a plain value, so callers do not take
// on a go-redis dependency to read their own script's result. This
// remains for callers that need the command's typed accessors.
func (db *RedisDB) EvalCmd(ctx context.Context, script string, keys []string, args ...any) *redis.Cmd {
	return db.client.Eval(ctx, script, keys, args...)
}
