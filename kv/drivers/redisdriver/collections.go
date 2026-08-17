package redisdriver

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"

	"github.com/xraph/grove/kv"
	"github.com/xraph/grove/kv/driver"
)

// Compile-time checks that Redis provides the collection capabilities it
// advertises in Info().
var (
	_ driver.SortedSetDriver = (*RedisDB)(nil)
	_ driver.SetDriver       = (*RedisDB)(nil)
	_ driver.HashDriver      = (*RedisDB)(nil)
)

// ── Sorted sets ───────────────────────────────────────────────────

// ZAdd inserts or updates members of a sorted set.
func (db *RedisDB) ZAdd(ctx context.Context, key string, members ...driver.ScoredMember) (int64, error) {
	if len(members) == 0 {
		return 0, nil
	}

	zs := make([]redis.Z, 0, len(members))
	for _, m := range members {
		zs = append(zs, redis.Z{Score: m.Score, Member: m.Member})
	}

	n, err := db.client.ZAdd(ctx, key, zs...).Result()
	if err != nil {
		return 0, fmt.Errorf("redisdriver: zadd %q: %w", key, err)
	}

	return n, nil
}

// rangeArgs renders a RangeSpec as the string bounds Redis expects.
//
// An unset bound becomes an infinity rather than a sentinel number, which
// is why RangeSpec carries HasMin/HasMax: any float a caller might use as
// a score is also a legitimate bound.
func rangeArgs(spec driver.RangeSpec) (minArg, maxArg string) {
	minArg, maxArg = "-inf", "+inf"

	if spec.HasMin {
		minArg = strconv.FormatFloat(spec.Min, 'f', -1, 64)
	}

	if spec.HasMax {
		maxArg = strconv.FormatFloat(spec.Max, 'f', -1, 64)
	}

	return minArg, maxArg
}

func (db *RedisDB) zRangeBy(spec driver.RangeSpec) *redis.ZRangeBy {
	minArg, maxArg := rangeArgs(spec)

	return &redis.ZRangeBy{
		Min:    minArg,
		Max:    maxArg,
		Offset: spec.Offset,
		Count:  spec.Count,
	}
}

// ZRange returns members within the spec, ordered by score.
func (db *RedisDB) ZRange(ctx context.Context, key string, spec driver.RangeSpec) ([]string, error) {
	by := db.zRangeBy(spec)

	var (
		out []string
		err error
	)

	if spec.Reverse {
		out, err = db.client.ZRevRangeByScore(ctx, key, by).Result()
	} else {
		out, err = db.client.ZRangeByScore(ctx, key, by).Result()
	}

	if err != nil {
		return nil, fmt.Errorf("redisdriver: zrange %q: %w", key, err)
	}

	return out, nil
}

// ZRangeWithScores is ZRange carrying each member's score.
func (db *RedisDB) ZRangeWithScores(
	ctx context.Context, key string, spec driver.RangeSpec,
) ([]driver.ScoredMember, error) {
	by := db.zRangeBy(spec)

	var (
		zs  []redis.Z
		err error
	)

	if spec.Reverse {
		zs, err = db.client.ZRevRangeByScoreWithScores(ctx, key, by).Result()
	} else {
		zs, err = db.client.ZRangeByScoreWithScores(ctx, key, by).Result()
	}

	if err != nil {
		return nil, fmt.Errorf("redisdriver: zrange with scores %q: %w", key, err)
	}

	out := make([]driver.ScoredMember, 0, len(zs))

	for _, z := range zs {
		member, ok := z.Member.(string)
		if !ok {
			member = fmt.Sprint(z.Member)
		}

		out = append(out, driver.ScoredMember{Member: member, Score: z.Score})
	}

	return out, nil
}

// ZRem removes members from a sorted set.
func (db *RedisDB) ZRem(ctx context.Context, key string, members ...string) (int64, error) {
	if len(members) == 0 {
		return 0, nil
	}

	args := make([]any, 0, len(members))
	for _, m := range members {
		args = append(args, m)
	}

	n, err := db.client.ZRem(ctx, key, args...).Result()
	if err != nil {
		return 0, fmt.Errorf("redisdriver: zrem %q: %w", key, err)
	}

	return n, nil
}

// ZCard returns the number of members in a sorted set.
func (db *RedisDB) ZCard(ctx context.Context, key string) (int64, error) {
	n, err := db.client.ZCard(ctx, key).Result()
	if err != nil {
		return 0, fmt.Errorf("redisdriver: zcard %q: %w", key, err)
	}

	return n, nil
}

// ZScore returns a member's score, and false when it is absent.
func (db *RedisDB) ZScore(ctx context.Context, key, member string) (float64, bool, error) {
	score, err := db.client.ZScore(ctx, key, member).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return 0, false, nil
		}

		return 0, false, fmt.Errorf("redisdriver: zscore %q: %w", key, err)
	}

	return score, true, nil
}

// ── Sets ──────────────────────────────────────────────────────────

// SAdd adds members to a set.
func (db *RedisDB) SAdd(ctx context.Context, key string, members ...string) (int64, error) {
	if len(members) == 0 {
		return 0, nil
	}

	args := make([]any, 0, len(members))
	for _, m := range members {
		args = append(args, m)
	}

	n, err := db.client.SAdd(ctx, key, args...).Result()
	if err != nil {
		return 0, fmt.Errorf("redisdriver: sadd %q: %w", key, err)
	}

	return n, nil
}

// SRem removes members from a set.
func (db *RedisDB) SRem(ctx context.Context, key string, members ...string) (int64, error) {
	if len(members) == 0 {
		return 0, nil
	}

	args := make([]any, 0, len(members))
	for _, m := range members {
		args = append(args, m)
	}

	n, err := db.client.SRem(ctx, key, args...).Result()
	if err != nil {
		return 0, fmt.Errorf("redisdriver: srem %q: %w", key, err)
	}

	return n, nil
}

// SMembers returns every member of a set.
func (db *RedisDB) SMembers(ctx context.Context, key string) ([]string, error) {
	out, err := db.client.SMembers(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("redisdriver: smembers %q: %w", key, err)
	}

	return out, nil
}

// SCard returns the number of members in a set.
func (db *RedisDB) SCard(ctx context.Context, key string) (int64, error) {
	n, err := db.client.SCard(ctx, key).Result()
	if err != nil {
		return 0, fmt.Errorf("redisdriver: scard %q: %w", key, err)
	}

	return n, nil
}

// SIsMember reports whether a member is present in a set.
func (db *RedisDB) SIsMember(ctx context.Context, key, member string) (bool, error) {
	ok, err := db.client.SIsMember(ctx, key, member).Result()
	if err != nil {
		return false, fmt.Errorf("redisdriver: sismember %q: %w", key, err)
	}

	return ok, nil
}

// ── Hashes ────────────────────────────────────────────────────────

// HSet sets fields on a hash.
func (db *RedisDB) HSet(ctx context.Context, key string, fields map[string][]byte) (int64, error) {
	if len(fields) == 0 {
		return 0, nil
	}

	args := make([]any, 0, len(fields)*2)
	for f, v := range fields {
		args = append(args, f, v)
	}

	n, err := db.client.HSet(ctx, key, args...).Result()
	if err != nil {
		return 0, fmt.Errorf("redisdriver: hset %q: %w", key, err)
	}

	return n, nil
}

// HGet returns one field's value, or kv.ErrNotFound when it is absent.
func (db *RedisDB) HGet(ctx context.Context, key, field string) ([]byte, error) {
	out, err := db.client.HGet(ctx, key, field).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, kv.ErrNotFound
		}

		return nil, fmt.Errorf("redisdriver: hget %q: %w", key, err)
	}

	return out, nil
}

// HGetAll returns every field of a hash.
func (db *RedisDB) HGetAll(ctx context.Context, key string) (map[string][]byte, error) {
	raw, err := db.client.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("redisdriver: hgetall %q: %w", key, err)
	}

	out := make(map[string][]byte, len(raw))
	for f, v := range raw {
		out[f] = []byte(v)
	}

	return out, nil
}

// HDel removes fields from a hash.
func (db *RedisDB) HDel(ctx context.Context, key string, fields ...string) (int64, error) {
	if len(fields) == 0 {
		return 0, nil
	}

	n, err := db.client.HDel(ctx, key, fields...).Result()
	if err != nil {
		return 0, fmt.Errorf("redisdriver: hdel %q: %w", key, err)
	}

	return n, nil
}

// HLen returns the number of fields in a hash.
func (db *RedisDB) HLen(ctx context.Context, key string) (int64, error) {
	n, err := db.client.HLen(ctx, key).Result()
	if err != nil {
		return 0, fmt.Errorf("redisdriver: hlen %q: %w", key, err)
	}

	return n, nil
}

// ── Pub/Sub ───────────────────────────────────────────────────────

var _ driver.PubSubDriver = (*RedisDB)(nil)

// Publish sends a message to a channel.
func (db *RedisDB) Publish(ctx context.Context, channel string, message []byte) error {
	if err := db.client.Publish(ctx, channel, message).Err(); err != nil {
		return fmt.Errorf("redisdriver: publish %q: %w", channel, err)
	}

	return nil
}

// Subscribe calls handler for every message on a channel until the
// context ends.
//
// The subscription is torn down when ctx is cancelled, which is also the
// only way this returns: a subscriber that stops listening without
// cancelling would leak the connection.
func (db *RedisDB) Subscribe(ctx context.Context, channel string, handler func(msg []byte)) error {
	sub := db.client.Subscribe(ctx, channel)

	go func() {
		defer func() { _ = sub.Close() }()

		ch := sub.Channel()

		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}

				handler([]byte(msg.Payload))
			}
		}
	}()

	return nil
}

// ── Scripts ───────────────────────────────────────────────────────

var _ driver.ScriptDriver = (*RedisDB)(nil)

// Eval runs a script against the given keys and arguments.
func (db *RedisDB) Eval(ctx context.Context, script string, keys []string, args ...any) (any, error) {
	out, err := db.client.Eval(ctx, script, keys, args...).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			// A script returning nothing is a result, not a failure.
			return nil, nil
		}

		return nil, fmt.Errorf("redisdriver: eval: %w", err)
	}

	return out, nil
}

// EvalSHA runs a loaded script by digest.
func (db *RedisDB) EvalSHA(ctx context.Context, sha string, keys []string, args ...any) (any, error) {
	out, err := db.client.EvalSha(ctx, sha, keys, args...).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}

		// Redis reports a forgotten script as a NOSCRIPT error string.
		// Translating it lets callers reload and retry instead of parsing
		// server text themselves.
		if strings.Contains(strings.ToUpper(err.Error()), "NOSCRIPT") {
			return nil, kv.ErrScriptNotLoaded
		}

		return nil, fmt.Errorf("redisdriver: evalsha: %w", err)
	}

	return out, nil
}

// ScriptLoad caches a script and returns its digest.
func (db *RedisDB) ScriptLoad(ctx context.Context, script string) (string, error) {
	sha, err := db.client.ScriptLoad(ctx, script).Result()
	if err != nil {
		return "", fmt.Errorf("redisdriver: script load: %w", err)
	}

	return sha, nil
}

// ── Streams ───────────────────────────────────────────────────────

var _ driver.StreamDriver = (*RedisDB)(nil)

// XAdd appends an entry to a stream and returns its assigned ID.
func (db *RedisDB) XAdd(ctx context.Context, stream string, values map[string][]byte) (string, error) {
	fields := make(map[string]any, len(values))
	for f, v := range values {
		fields[f] = v
	}

	id, err := db.client.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: fields,
	}).Result()
	if err != nil {
		return "", fmt.Errorf("redisdriver: xadd %q: %w", stream, err)
	}

	return id, nil
}

// XRange returns entries between start and stop inclusive, oldest first.
//
// Empty bounds become the stream's own minimum and maximum sentinels, so
// a caller wanting everything passes two empty strings rather than having
// to know Redis spells them "-" and "+".
func (db *RedisDB) XRange(
	ctx context.Context, stream, start, stop string, count int64,
) ([]driver.StreamMessage, error) {
	if start == "" {
		start = "-"
	}

	if stop == "" {
		stop = "+"
	}

	var (
		msgs []redis.XMessage
		err  error
	)

	if count > 0 {
		msgs, err = db.client.XRangeN(ctx, stream, start, stop, count).Result()
	} else {
		msgs, err = db.client.XRange(ctx, stream, start, stop).Result()
	}

	if err != nil {
		return nil, fmt.Errorf("redisdriver: xrange %q: %w", stream, err)
	}

	out := make([]driver.StreamMessage, 0, len(msgs))

	for _, m := range msgs {
		values := make(map[string][]byte, len(m.Values))

		for f, v := range m.Values {
			switch typed := v.(type) {
			case string:
				values[f] = []byte(typed)
			case []byte:
				values[f] = typed
			default:
				values[f] = []byte(fmt.Sprint(typed))
			}
		}

		out = append(out, driver.StreamMessage{ID: m.ID, Values: values})
	}

	return out, nil
}

// XDel removes entries from a stream by ID.
func (db *RedisDB) XDel(ctx context.Context, stream string, ids ...string) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}

	n, err := db.client.XDel(ctx, stream, ids...).Result()
	if err != nil {
		return 0, fmt.Errorf("redisdriver: xdel %q: %w", stream, err)
	}

	return n, nil
}

// XLen returns the number of entries in a stream.
func (db *RedisDB) XLen(ctx context.Context, stream string) (int64, error) {
	n, err := db.client.XLen(ctx, stream).Result()
	if err != nil {
		return 0, fmt.Errorf("redisdriver: xlen %q: %w", stream, err)
	}

	return n, nil
}
