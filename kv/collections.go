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
