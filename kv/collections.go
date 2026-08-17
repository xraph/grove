package kv

import (
	"context"

	"github.com/xraph/grove/kv/driver"
)

// Collection accessors.
//
// Sorted sets, sets, and hashes are optional driver capabilities: a
// key-value store like Bolt or Memcached has no equivalent, so these
// return ErrNotSupported there rather than being emulated. Emulating an
// ordered structure over plain keys would turn an ordered read into a
// keyspace scan, which is the operation callers reach for a sorted set
// to avoid.
//
// The Supports* helpers let a caller pick a data model at construction
// instead of discovering the gap on the first write.

// SupportsSortedSets reports whether the driver has sorted sets.
func (s *Store) SupportsSortedSets() bool {
	_, ok := s.drv.(driver.SortedSetDriver)

	return ok
}

// SupportsSets reports whether the driver has unordered sets.
func (s *Store) SupportsSets() bool {
	_, ok := s.drv.(driver.SetDriver)

	return ok
}

// SupportsHashes reports whether the driver has hash maps.
func (s *Store) SupportsHashes() bool {
	_, ok := s.drv.(driver.HashDriver)

	return ok
}

// ── Sorted sets ───────────────────────────────────────────────────

// ZAdd inserts or updates members of a sorted set.
func (s *Store) ZAdd(ctx context.Context, key string, members ...driver.ScoredMember) (int64, error) {
	if err := s.checkClosed(); err != nil {
		return 0, err
	}

	drv, ok := s.drv.(driver.SortedSetDriver)
	if !ok {
		return 0, ErrNotSupported
	}

	return drv.ZAdd(ctx, key, members...)
}

// ZRange returns members within the spec, ordered by score.
func (s *Store) ZRange(ctx context.Context, key string, spec driver.RangeSpec) ([]string, error) {
	if err := s.checkClosed(); err != nil {
		return nil, err
	}

	drv, ok := s.drv.(driver.SortedSetDriver)
	if !ok {
		return nil, ErrNotSupported
	}

	return drv.ZRange(ctx, key, spec)
}

// ZRangeWithScores is ZRange carrying each member's score.
func (s *Store) ZRangeWithScores(
	ctx context.Context, key string, spec driver.RangeSpec,
) ([]driver.ScoredMember, error) {
	if err := s.checkClosed(); err != nil {
		return nil, err
	}

	drv, ok := s.drv.(driver.SortedSetDriver)
	if !ok {
		return nil, ErrNotSupported
	}

	return drv.ZRangeWithScores(ctx, key, spec)
}

// ZRem removes members from a sorted set.
func (s *Store) ZRem(ctx context.Context, key string, members ...string) (int64, error) {
	if err := s.checkClosed(); err != nil {
		return 0, err
	}

	drv, ok := s.drv.(driver.SortedSetDriver)
	if !ok {
		return 0, ErrNotSupported
	}

	return drv.ZRem(ctx, key, members...)
}

// ZCard returns the number of members in a sorted set.
func (s *Store) ZCard(ctx context.Context, key string) (int64, error) {
	if err := s.checkClosed(); err != nil {
		return 0, err
	}

	drv, ok := s.drv.(driver.SortedSetDriver)
	if !ok {
		return 0, ErrNotSupported
	}

	return drv.ZCard(ctx, key)
}

// ZScore returns a member's score, and false when it is absent.
func (s *Store) ZScore(ctx context.Context, key, member string) (float64, bool, error) {
	if err := s.checkClosed(); err != nil {
		return 0, false, err
	}

	drv, ok := s.drv.(driver.SortedSetDriver)
	if !ok {
		return 0, false, ErrNotSupported
	}

	return drv.ZScore(ctx, key, member)
}

// ── Sets ──────────────────────────────────────────────────────────

// SAdd adds members to a set.
func (s *Store) SAdd(ctx context.Context, key string, members ...string) (int64, error) {
	if err := s.checkClosed(); err != nil {
		return 0, err
	}

	drv, ok := s.drv.(driver.SetDriver)
	if !ok {
		return 0, ErrNotSupported
	}

	return drv.SAdd(ctx, key, members...)
}

// SRem removes members from a set.
func (s *Store) SRem(ctx context.Context, key string, members ...string) (int64, error) {
	if err := s.checkClosed(); err != nil {
		return 0, err
	}

	drv, ok := s.drv.(driver.SetDriver)
	if !ok {
		return 0, ErrNotSupported
	}

	return drv.SRem(ctx, key, members...)
}

// SMembers returns every member of a set.
func (s *Store) SMembers(ctx context.Context, key string) ([]string, error) {
	if err := s.checkClosed(); err != nil {
		return nil, err
	}

	drv, ok := s.drv.(driver.SetDriver)
	if !ok {
		return nil, ErrNotSupported
	}

	return drv.SMembers(ctx, key)
}

// SCard returns the number of members in a set.
func (s *Store) SCard(ctx context.Context, key string) (int64, error) {
	if err := s.checkClosed(); err != nil {
		return 0, err
	}

	drv, ok := s.drv.(driver.SetDriver)
	if !ok {
		return 0, ErrNotSupported
	}

	return drv.SCard(ctx, key)
}

// SIsMember reports whether a member is present in a set.
func (s *Store) SIsMember(ctx context.Context, key, member string) (bool, error) {
	if err := s.checkClosed(); err != nil {
		return false, err
	}

	drv, ok := s.drv.(driver.SetDriver)
	if !ok {
		return false, ErrNotSupported
	}

	return drv.SIsMember(ctx, key, member)
}

// ── Hashes ────────────────────────────────────────────────────────

// HSet sets fields on a hash.
func (s *Store) HSet(ctx context.Context, key string, fields map[string][]byte) (int64, error) {
	if err := s.checkClosed(); err != nil {
		return 0, err
	}

	drv, ok := s.drv.(driver.HashDriver)
	if !ok {
		return 0, ErrNotSupported
	}

	return drv.HSet(ctx, key, fields)
}

// HGet returns one field's value, or ErrNotFound when it is absent.
func (s *Store) HGet(ctx context.Context, key, field string) ([]byte, error) {
	if err := s.checkClosed(); err != nil {
		return nil, err
	}

	drv, ok := s.drv.(driver.HashDriver)
	if !ok {
		return nil, ErrNotSupported
	}

	return drv.HGet(ctx, key, field)
}

// HGetAll returns every field of a hash.
func (s *Store) HGetAll(ctx context.Context, key string) (map[string][]byte, error) {
	if err := s.checkClosed(); err != nil {
		return nil, err
	}

	drv, ok := s.drv.(driver.HashDriver)
	if !ok {
		return nil, ErrNotSupported
	}

	return drv.HGetAll(ctx, key)
}

// HDel removes fields from a hash.
func (s *Store) HDel(ctx context.Context, key string, fields ...string) (int64, error) {
	if err := s.checkClosed(); err != nil {
		return 0, err
	}

	drv, ok := s.drv.(driver.HashDriver)
	if !ok {
		return 0, ErrNotSupported
	}

	return drv.HDel(ctx, key, fields...)
}

// HLen returns the number of fields in a hash.
func (s *Store) HLen(ctx context.Context, key string) (int64, error) {
	if err := s.checkClosed(); err != nil {
		return 0, err
	}

	drv, ok := s.drv.(driver.HashDriver)
	if !ok {
		return 0, ErrNotSupported
	}

	return drv.HLen(ctx, key)
}

// ── Pub/Sub ───────────────────────────────────────────────────────

// SupportsPubSub reports whether the driver has publish/subscribe.
func (s *Store) SupportsPubSub() bool {
	_, ok := s.drv.(driver.PubSubDriver)

	return ok
}

// Publish sends a message to a channel.
//
// Delivery is at-most-once and only to subscribers connected at the
// moment of publication: this is a notification primitive, not a queue.
// Callers that must not miss a message need a durable structure behind
// it, with the message serving only to shorten the latency.
func (s *Store) Publish(ctx context.Context, channel string, message []byte) error {
	if err := s.checkClosed(); err != nil {
		return err
	}

	drv, ok := s.drv.(driver.PubSubDriver)
	if !ok {
		return ErrNotSupported
	}

	return drv.Publish(ctx, channel, message)
}

// Subscribe registers a handler for a channel, called for every message
// until the context ends.
func (s *Store) Subscribe(ctx context.Context, channel string, handler func(msg []byte)) error {
	if err := s.checkClosed(); err != nil {
		return err
	}

	drv, ok := s.drv.(driver.PubSubDriver)
	if !ok {
		return ErrNotSupported
	}

	return drv.Subscribe(ctx, channel, handler)
}

// ── Scripts ───────────────────────────────────────────────────────

// SupportsScripts reports whether the driver runs server-side scripts.
func (s *Store) SupportsScripts() bool {
	_, ok := s.drv.(driver.ScriptDriver)

	return ok
}

// Eval runs a script against the given keys and arguments.
func (s *Store) Eval(ctx context.Context, script string, keys []string, args ...any) (any, error) {
	if err := s.checkClosed(); err != nil {
		return nil, err
	}

	drv, ok := s.drv.(driver.ScriptDriver)
	if !ok {
		return nil, ErrNotSupported
	}

	return drv.Eval(ctx, script, keys, args...)
}

// EvalSHA runs a loaded script by digest.
//
// It returns ErrScriptNotLoaded when the server has dropped the script,
// which happens across restarts: callers reload and retry rather than
// treating it as a failure.
func (s *Store) EvalSHA(ctx context.Context, sha string, keys []string, args ...any) (any, error) {
	if err := s.checkClosed(); err != nil {
		return nil, err
	}

	drv, ok := s.drv.(driver.ScriptDriver)
	if !ok {
		return nil, ErrNotSupported
	}

	return drv.EvalSHA(ctx, sha, keys, args...)
}

// ScriptLoad caches a script and returns its digest.
func (s *Store) ScriptLoad(ctx context.Context, script string) (string, error) {
	if err := s.checkClosed(); err != nil {
		return "", err
	}

	drv, ok := s.drv.(driver.ScriptDriver)
	if !ok {
		return "", ErrNotSupported
	}

	return drv.ScriptLoad(ctx, script)
}

// ── Streams ───────────────────────────────────────────────────────

// SupportsStreams reports whether the driver has append-only streams.
func (s *Store) SupportsStreams() bool {
	_, ok := s.drv.(driver.StreamDriver)

	return ok
}

// XAdd appends an entry to a stream and returns its assigned ID.
func (s *Store) XAdd(ctx context.Context, stream string, values map[string][]byte) (string, error) {
	if err := s.checkClosed(); err != nil {
		return "", err
	}

	drv, ok := s.drv.(driver.StreamDriver)
	if !ok {
		return "", ErrNotSupported
	}

	return drv.XAdd(ctx, stream, values)
}

// XRange returns entries between start and stop inclusive, oldest first.
func (s *Store) XRange(
	ctx context.Context, stream, start, stop string, count int64,
) ([]driver.StreamMessage, error) {
	if err := s.checkClosed(); err != nil {
		return nil, err
	}

	drv, ok := s.drv.(driver.StreamDriver)
	if !ok {
		return nil, ErrNotSupported
	}

	return drv.XRange(ctx, stream, start, stop, count)
}

// XDel removes entries from a stream by ID.
func (s *Store) XDel(ctx context.Context, stream string, ids ...string) (int64, error) {
	if err := s.checkClosed(); err != nil {
		return 0, err
	}

	drv, ok := s.drv.(driver.StreamDriver)
	if !ok {
		return 0, ErrNotSupported
	}

	return drv.XDel(ctx, stream, ids...)
}

// XLen returns the number of entries in a stream.
func (s *Store) XLen(ctx context.Context, stream string) (int64, error) {
	if err := s.checkClosed(); err != nil {
		return 0, err
	}

	drv, ok := s.drv.(driver.StreamDriver)
	if !ok {
		return 0, ErrNotSupported
	}

	return drv.XLen(ctx, stream)
}
