package extension

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/xraph/grove/kv"
)

// SessionStore provides HTTP session storage backed by a KV store.
type SessionStore struct {
	store  *kv.Store
	prefix string
	ttl    time.Duration
}

// NewSessionStore creates a new session store.
func NewSessionStore(store *kv.Store, opts ...SessionOption) *SessionStore {
	ss := &SessionStore{
		store:  store,
		prefix: "sess",
		ttl:    30 * time.Minute,
	}
	for _, opt := range opts {
		opt(ss)
	}
	return ss
}

// SessionOption configures a SessionStore.
type SessionOption func(*SessionStore)

// WithSessionPrefix sets the key prefix for sessions.
func WithSessionPrefix(prefix string) SessionOption {
	return func(ss *SessionStore) { ss.prefix = prefix }
}

// WithSessionTTL sets the default session TTL.
func WithSessionTTL(ttl time.Duration) SessionOption {
	return func(ss *SessionStore) { ss.ttl = ttl }
}

// Create creates a new session with the given data and returns the session ID.
func (ss *SessionStore) Create(ctx context.Context, data any) (string, error) {
	id := generateSessionID()
	key := ss.key(id)
	if err := ss.store.Set(ctx, key, data, kv.WithTTL(ss.ttl)); err != nil {
		return "", fmt.Errorf("session: create: %w", err)
	}
	return id, nil
}

// Get retrieves session data by ID.
func (ss *SessionStore) Get(ctx context.Context, id string, dest any) error {
	return ss.store.Get(ctx, ss.key(id), dest)
}

// Update replaces session data and resets the TTL.
func (ss *SessionStore) Update(ctx context.Context, id string, data any) error {
	return ss.store.Set(ctx, ss.key(id), data, kv.WithTTL(ss.ttl))
}

// Delete removes a session.
func (ss *SessionStore) Delete(ctx context.Context, id string) error {
	return ss.store.Delete(ctx, ss.key(id))
}

// Touch refreshes the session TTL without changing data.
func (ss *SessionStore) Touch(ctx context.Context, id string) error {
	return ss.store.Expire(ctx, ss.key(id), ss.ttl)
}

// Exists checks if a session exists.
func (ss *SessionStore) Exists(ctx context.Context, id string) (bool, error) {
	count, err := ss.store.Exists(ctx, ss.key(id))
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (ss *SessionStore) key(id string) string {
	return ss.prefix + ":" + id
}

func generateSessionID() string {
	b := make([]byte, 32)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
