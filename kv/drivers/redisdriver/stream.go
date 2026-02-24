package redisdriver

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// Stream provides access to Redis Streams (XADD, XREAD, consumer groups).
type Stream struct {
	client redis.UniversalClient
}

// NewStream returns a Stream handle from the underlying Redis client.
func (db *RedisDB) NewStream() *Stream {
	return &Stream{client: db.client}
}

// XAdd adds an entry to a stream.
func (s *Stream) XAdd(ctx context.Context, stream string, values map[string]any) (string, error) {
	return s.client.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: values,
	}).Result()
}

// XRead reads entries from one or more streams.
func (s *Stream) XRead(ctx context.Context, streams []string, count int64, block ...int64) ([]redis.XStream, error) {
	args := &redis.XReadArgs{
		Streams: streams,
		Count:   count,
	}
	if len(block) > 0 {
		args.Block = -1 // block indefinitely unless specified
	}
	return s.client.XRead(ctx, args).Result()
}

// XGroupCreate creates a consumer group on a stream.
func (s *Stream) XGroupCreate(ctx context.Context, stream, group, start string) error {
	return s.client.XGroupCreateMkStream(ctx, stream, group, start).Err()
}

// XReadGroup reads entries from a stream using a consumer group.
func (s *Stream) XReadGroup(ctx context.Context, group, consumer string, streams []string, count int64) ([]redis.XStream, error) {
	return s.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  streams,
		Count:    count,
	}).Result()
}

// XAck acknowledges messages in a consumer group.
func (s *Stream) XAck(ctx context.Context, stream, group string, ids ...string) (int64, error) {
	return s.client.XAck(ctx, stream, group, ids...).Result()
}

// XLen returns the number of entries in a stream.
func (s *Stream) XLen(ctx context.Context, stream string) (int64, error) {
	return s.client.XLen(ctx, stream).Result()
}
