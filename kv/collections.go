package kv

import (
	"context"

	"github.com/xraph/grove/hook"
	"github.com/xraph/grove/kv/driver"
)

// observe runs the hook engine around one collection command.
//
// The scalar commands in store.go have always done this; collections did
// not, which meant metrics, tracing, and policy middleware saw the plain
// key reads around a queue but never the queue itself. keys is what the
// command touches, so a hook can account work by key rather than only by
// call.
func (s *Store) observe(ctx context.Context, op hook.Operation, keys []string, fn func() error) error {
	qc := newCommandContext(op, keys, nil)

	result, err := s.hooks.RunPreQuery(ctx, qc)
	if err != nil {
		return err
	}

	if result != nil && result.Decision == hook.Deny {
		return ErrHookDenied
	}

	if err := fn(); err != nil {
		return err
	}

	return s.hooks.RunPostQuery(ctx, qc, nil)
}

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

	var out int64

	err := s.observe(ctx, OpZAdd, []string{key}, func() error {
		var cerr error
		out, cerr = drv.ZAdd(ctx, key, members...)

		return cerr
	})

	return out, err
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

	var out []string

	err := s.observe(ctx, OpZRange, []string{key}, func() error {
		var cerr error
		out, cerr = drv.ZRange(ctx, key, spec)

		return cerr
	})

	return out, err
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

	var out []driver.ScoredMember

	err := s.observe(ctx, OpZRange, []string{key}, func() error {
		var cerr error
		out, cerr = drv.ZRangeWithScores(ctx, key, spec)

		return cerr
	})

	return out, err
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

	var out int64

	err := s.observe(ctx, OpZRem, []string{key}, func() error {
		var cerr error
		out, cerr = drv.ZRem(ctx, key, members...)

		return cerr
	})

	return out, err
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

	var out int64

	err := s.observe(ctx, OpZCard, []string{key}, func() error {
		var cerr error
		out, cerr = drv.ZCard(ctx, key)

		return cerr
	})

	return out, err
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

	var (
		score float64
		found bool
	)

	err := s.observe(ctx, OpZScore, []string{key}, func() error {
		var cerr error
		score, found, cerr = drv.ZScore(ctx, key, member)

		return cerr
	})

	return score, found, err
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

	var out int64

	err := s.observe(ctx, OpSAdd, []string{key}, func() error {
		var cerr error
		out, cerr = drv.SAdd(ctx, key, members...)

		return cerr
	})

	return out, err
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

	var out int64

	err := s.observe(ctx, OpSRem, []string{key}, func() error {
		var cerr error
		out, cerr = drv.SRem(ctx, key, members...)

		return cerr
	})

	return out, err
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

	var out []string

	err := s.observe(ctx, OpSMbrs, []string{key}, func() error {
		var cerr error
		out, cerr = drv.SMembers(ctx, key)

		return cerr
	})

	return out, err
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

	var out int64

	err := s.observe(ctx, OpSCard, []string{key}, func() error {
		var cerr error
		out, cerr = drv.SCard(ctx, key)

		return cerr
	})

	return out, err
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

	var out bool

	err := s.observe(ctx, OpSIsMbr, []string{key}, func() error {
		var cerr error
		out, cerr = drv.SIsMember(ctx, key, member)

		return cerr
	})

	return out, err
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

	var out int64

	err := s.observe(ctx, OpHSet, []string{key}, func() error {
		var cerr error
		out, cerr = drv.HSet(ctx, key, fields)

		return cerr
	})

	return out, err
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

	var out []byte

	err := s.observe(ctx, OpHGet, []string{key}, func() error {
		var cerr error
		out, cerr = drv.HGet(ctx, key, field)

		return cerr
	})

	return out, err
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

	var out map[string][]byte

	err := s.observe(ctx, OpHGetAll, []string{key}, func() error {
		var cerr error
		out, cerr = drv.HGetAll(ctx, key)

		return cerr
	})

	return out, err
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

	var out int64

	err := s.observe(ctx, OpHDel, []string{key}, func() error {
		var cerr error
		out, cerr = drv.HDel(ctx, key, fields...)

		return cerr
	})

	return out, err
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

	var out int64

	err := s.observe(ctx, OpHLen, []string{key}, func() error {
		var cerr error
		out, cerr = drv.HLen(ctx, key)

		return cerr
	})

	return out, err
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

	return s.observe(ctx, OpPublish, []string{channel}, func() error {
		return drv.Publish(ctx, channel, message)
	})
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

	// Only the subscription is observed, not each delivered message: this
	// returns once and the handler then runs for the life of the context,
	// so a per-message hook here would be a hook on a callback the store
	// no longer controls.
	return s.observe(ctx, OpSubscr, []string{channel}, func() error {
		return drv.Subscribe(ctx, channel, handler)
	})
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

	var out any

	err := s.observe(ctx, OpEval, keys, func() error {
		var cerr error
		out, cerr = drv.Eval(ctx, script, keys, args...)

		return cerr
	})

	return out, err
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

	var out any

	err := s.observe(ctx, OpEval, keys, func() error {
		var cerr error
		out, cerr = drv.EvalSHA(ctx, sha, keys, args...)

		return cerr
	})

	return out, err
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

	var sha string

	err := s.observe(ctx, OpEval, nil, func() error {
		var cerr error
		sha, cerr = drv.ScriptLoad(ctx, script)

		return cerr
	})

	return sha, err
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

	var out string

	err := s.observe(ctx, OpXAdd, []string{stream}, func() error {
		var cerr error
		out, cerr = drv.XAdd(ctx, stream, values)

		return cerr
	})

	return out, err
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

	var out []driver.StreamMessage

	err := s.observe(ctx, OpXRange, []string{stream}, func() error {
		var cerr error
		out, cerr = drv.XRange(ctx, stream, start, stop, count)

		return cerr
	})

	return out, err
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

	var out int64

	err := s.observe(ctx, OpXDel, []string{stream}, func() error {
		var cerr error
		out, cerr = drv.XDel(ctx, stream, ids...)

		return cerr
	})

	return out, err
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

	var out int64

	err := s.observe(ctx, OpXLen, []string{stream}, func() error {
		var cerr error
		out, cerr = drv.XLen(ctx, stream)

		return cerr
	})

	return out, err
}

// MGetRaw reads many keys, returning a slice positionally aligned with
// keys and holding nil where a key was absent.
//
// It is the raw counterpart to MGet, which decodes into a map and so
// cannot say which key produced which value when some are missing.
// Positional results are what a caller filtering a batch needs.
func (s *Store) MGetRaw(ctx context.Context, keys []string) ([][]byte, error) {
	if err := s.checkClosed(); err != nil {
		return nil, err
	}

	if len(keys) == 0 {
		return nil, nil
	}

	drv, ok := s.drv.(driver.BatchDriver)
	if !ok {
		return nil, ErrNotSupported
	}

	var out [][]byte

	// Every key is reported, not just the first: a caller accounting work
	// by key is exactly who needs to see that one call read a thousand of
	// them.
	err := s.observe(ctx, OpMGet, keys, func() error {
		var cerr error
		out, cerr = drv.MGet(ctx, keys)

		return cerr
	})

	return out, err
}
