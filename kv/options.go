package kv

import (
	"time"

	"github.com/xraph/grove/hook"
	"github.com/xraph/grove/kv/codec"
)

// Option configures a Store during Open.
type Option func(*options)

type hookEntry struct {
	hook  any
	scope hook.Scope
}

type options struct {
	codec codec.Codec
	hooks []hookEntry
}

func defaultOptions() *options {
	return &options{
		codec: codec.JSON(),
	}
}

func (o *options) apply(opts []Option) {
	for _, opt := range opts {
		opt(o)
	}
}

// WithCodec sets the default codec for the store.
func WithCodec(c codec.Codec) Option {
	return func(o *options) { o.codec = c }
}

// WithHook registers a middleware hook with the store.
// The hook must implement hook.PreQueryHook and/or hook.PostQueryHook.
func WithHook(h any, scope ...hook.Scope) Option {
	return func(o *options) {
		entry := hookEntry{hook: h}
		if len(scope) > 0 {
			entry.scope = scope[0]
		}
		o.hooks = append(o.hooks, entry)
	}
}

// SetOption configures a single Set operation.
type SetOption func(*setOptions)

type setOptions struct {
	ttl time.Duration
	nx  bool   // SET if Not eXists
	xx  bool   // SET if eXists
	cas uint64 // Compare-And-Swap version
}

func defaultSetOptions() *setOptions {
	return &setOptions{}
}

func applySetOptions(opts []SetOption) *setOptions {
	o := defaultSetOptions()
	for _, opt := range opts {
		opt(o)
	}
	return o
}

// WithTTL sets the TTL for the Set operation.
func WithTTL(d time.Duration) SetOption {
	return func(o *setOptions) { o.ttl = d }
}

// WithNX sets the key only if it does not already exist.
func WithNX() SetOption {
	return func(o *setOptions) { o.nx = true }
}

// WithXX sets the key only if it already exists.
func WithXX() SetOption {
	return func(o *setOptions) { o.xx = true }
}

// WithCAS sets the expected version for a Compare-And-Swap operation.
func WithCAS(version uint64) SetOption {
	return func(o *setOptions) { o.cas = version }
}
