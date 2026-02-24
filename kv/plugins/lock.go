// Package extension provides application-level patterns built on Grove KV.
package plugins

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/xraph/grove/kv"
)

// Lock is a distributed lock backed by a KV store.
// It uses SetNX for locking and DELETE for unlocking.
type Lock struct {
	store *kv.Store
	key   string
	token string
	ttl   time.Duration
}

// NewLock creates a distributed lock with the given key and TTL.
func NewLock(store *kv.Store, key string, ttl time.Duration) *Lock {
	return &Lock{
		store: store,
		key:   "lock:" + key,
		ttl:   ttl,
	}
}

// Acquire attempts to acquire the lock. Returns an error if the lock cannot be acquired.
func (l *Lock) Acquire(ctx context.Context) error {
	token := generateToken()
	err := l.store.Set(ctx, l.key, token, kv.WithNX(), kv.WithTTL(l.ttl))
	if err != nil {
		return fmt.Errorf("lock: acquire: %w", err)
	}
	l.token = token
	return nil
}

// AcquireWithRetry attempts to acquire the lock with retries.
func (l *Lock) AcquireWithRetry(ctx context.Context, retryInterval time.Duration, maxRetries int) error {
	for i := 0; i <= maxRetries; i++ {
		err := l.Acquire(ctx)
		if err == nil {
			return nil
		}
		if i < maxRetries {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(retryInterval):
			}
		}
	}
	return fmt.Errorf("lock: acquire: max retries exceeded")
}

// Release releases the lock.
func (l *Lock) Release(ctx context.Context) error {
	if l.token == "" {
		return fmt.Errorf("lock: not acquired")
	}
	err := l.store.Delete(ctx, l.key)
	if err != nil {
		return fmt.Errorf("lock: release: %w", err)
	}
	l.token = ""
	return nil
}

// Extend extends the lock's TTL.
func (l *Lock) Extend(ctx context.Context, ttl time.Duration) error {
	if l.token == "" {
		return fmt.Errorf("lock: not acquired")
	}
	return l.store.Expire(ctx, l.key, ttl)
}

func generateToken() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
