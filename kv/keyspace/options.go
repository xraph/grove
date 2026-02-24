package keyspace

import (
	"time"

	"github.com/xraph/grove/kv/codec"
)

// Option configures a Keyspace.
type Option func(*config)

type config struct {
	ttl       time.Duration
	codec     codec.Codec
	separator string
}

// WithTTL sets the default TTL for all Set operations in the keyspace.
func WithTTL(d time.Duration) Option {
	return func(c *config) { c.ttl = d }
}

// WithCodec sets the codec used for serialization within the keyspace.
// This overrides the store-level codec.
func WithCodec(cc codec.Codec) Option {
	return func(c *config) { c.codec = cc }
}

// WithSeparator sets the key segment separator (default ":").
func WithSeparator(sep string) Option {
	return func(c *config) { c.separator = sep }
}
