package redisdriver

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// PubSub provides access to Redis Pub/Sub functionality.
type PubSub struct {
	client redis.UniversalClient
}

// NewPubSub returns a PubSub handle from the underlying Redis client.
func (db *RedisDB) NewPubSub() *PubSub {
	return &PubSub{client: db.client}
}

// Publish sends a message to a channel.
func (ps *PubSub) Publish(ctx context.Context, channel string, message any) error {
	return ps.client.Publish(ctx, channel, message).Err()
}

// Subscribe subscribes to one or more channels and calls the handler for each message.
func (ps *PubSub) Subscribe(ctx context.Context, handler func(channel string, payload string), channels ...string) error {
	sub := ps.client.Subscribe(ctx, channels...)
	defer sub.Close()

	ch := sub.Channel()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-ch:
			if !ok {
				return nil
			}
			handler(msg.Channel, msg.Payload)
		}
	}
}

// SubscribeRaw returns the underlying go-redis PubSub for advanced use.
func (ps *PubSub) SubscribeRaw(ctx context.Context, channels ...string) *redis.PubSub {
	return ps.client.Subscribe(ctx, channels...)
}
