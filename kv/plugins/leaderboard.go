package plugins

import (
	"context"
	"fmt"
	"sort"

	"github.com/xraph/grove/kv"
)

// Leaderboard provides a sorted leaderboard backed by a KV store.
// For Redis, this could be backed by ZSET natively via Unwrap.
// This generic implementation stores scores in a map.
type Leaderboard struct {
	store *kv.Store
	key   string
}

// LeaderboardEntry represents a single entry in the leaderboard.
type LeaderboardEntry struct {
	Member string  `json:"member"`
	Score  float64 `json:"score"`
}

// NewLeaderboard creates a new leaderboard at the given key.
func NewLeaderboard(store *kv.Store, key string) *Leaderboard {
	return &Leaderboard{
		store: store,
		key:   "lb:" + key,
	}
}

// Add adds or updates a member's score.
func (lb *Leaderboard) Add(ctx context.Context, member string, score float64) error {
	scores, err := lb.loadScores(ctx)
	if err != nil {
		return err
	}
	scores[member] = score
	return lb.store.Set(ctx, lb.key, scores)
}

// Score returns the score for a member.
func (lb *Leaderboard) Score(ctx context.Context, member string) (float64, error) {
	scores, err := lb.loadScores(ctx)
	if err != nil {
		return 0, err
	}
	score, ok := scores[member]
	if !ok {
		return 0, kv.ErrNotFound
	}
	return score, nil
}

// TopN returns the top N entries in descending order.
func (lb *Leaderboard) TopN(ctx context.Context, n int) ([]LeaderboardEntry, error) {
	scores, err := lb.loadScores(ctx)
	if err != nil {
		return nil, err
	}

	entries := make([]LeaderboardEntry, 0, len(scores))
	for member, score := range scores {
		entries = append(entries, LeaderboardEntry{Member: member, Score: score})
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Score > entries[j].Score
	})
	if n > len(entries) {
		n = len(entries)
	}
	return entries[:n], nil
}

// Rank returns the 1-based rank of a member (1 = highest score).
func (lb *Leaderboard) Rank(ctx context.Context, member string) (int, error) {
	scores, err := lb.loadScores(ctx)
	if err != nil {
		return 0, err
	}

	targetScore, ok := scores[member]
	if !ok {
		return 0, kv.ErrNotFound
	}

	rank := 1
	for _, score := range scores {
		if score > targetScore {
			rank++
		}
	}
	return rank, nil
}

// Remove removes a member from the leaderboard.
func (lb *Leaderboard) Remove(ctx context.Context, member string) error {
	scores, err := lb.loadScores(ctx)
	if err != nil {
		return err
	}
	delete(scores, member)
	return lb.store.Set(ctx, lb.key, scores)
}

// Size returns the number of members in the leaderboard.
func (lb *Leaderboard) Size(ctx context.Context) (int, error) {
	scores, err := lb.loadScores(ctx)
	if err != nil {
		return 0, err
	}
	return len(scores), nil
}

func (lb *Leaderboard) loadScores(ctx context.Context) (map[string]float64, error) {
	var scores map[string]float64
	err := lb.store.Get(ctx, lb.key, &scores)
	if err != nil {
		if err == kv.ErrNotFound {
			return make(map[string]float64), nil
		}
		return nil, fmt.Errorf("leaderboard: load: %w", err)
	}
	if scores == nil {
		scores = make(map[string]float64)
	}
	return scores, nil
}
