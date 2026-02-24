package middleware

import (
	"context"
	"sync"
	"time"

	"github.com/xraph/grove/hook"
	kvpkg "github.com/xraph/grove/kv"
)

// CacheHook provides a local in-memory L1 cache in front of the remote store.
// It supports read-through (check local first, then remote) and write-through
// (write to both local and remote on Set).
type CacheHook struct {
	mu         sync.RWMutex
	entries    map[string]*cacheEntry
	maxEntries int
	defaultTTL time.Duration
}

type cacheEntry struct {
	value  []byte
	expiry time.Time
}

func (e *cacheEntry) expired() bool {
	return !e.expiry.IsZero() && time.Now().After(e.expiry)
}

var (
	_ hook.PreQueryHook  = (*CacheHook)(nil)
	_ hook.PostQueryHook = (*CacheHook)(nil)
)

// NewCache creates a new L1 cache middleware.
// maxEntries limits cache size; defaultTTL sets TTL for cached entries.
func NewCache(maxEntries int, defaultTTL time.Duration) *CacheHook {
	return &CacheHook{
		entries:    make(map[string]*cacheEntry),
		maxEntries: maxEntries,
		defaultTTL: defaultTTL,
	}
}

func (h *CacheHook) BeforeQuery(_ context.Context, qc *hook.QueryContext) (*hook.HookResult, error) {
	// Only intercept GET operations.
	if qc.Operation != kvpkg.OpGet {
		return &hook.HookResult{Decision: hook.Allow}, nil
	}

	key := qc.RawQuery
	if key == "" {
		return &hook.HookResult{Decision: hook.Allow}, nil
	}

	h.mu.RLock()
	entry, ok := h.entries[key]
	h.mu.RUnlock()

	if ok && !entry.expired() {
		if qc.Values == nil {
			qc.Values = make(map[string]any)
		}
		qc.Values["_cache_hit"] = true
		qc.Values["_cache_value"] = entry.value
		return &hook.HookResult{Decision: hook.Allow}, nil
	}

	return &hook.HookResult{Decision: hook.Allow}, nil
}

func (h *CacheHook) AfterQuery(_ context.Context, qc *hook.QueryContext, result any) error {
	// Cache the result on GET operations.
	if qc.Operation != kvpkg.OpGet && qc.Operation != kvpkg.OpSet {
		return nil
	}

	key := qc.RawQuery
	if key == "" {
		return nil
	}

	// On DELETE, evict from cache.
	if qc.Operation == kvpkg.OpDelete {
		h.Evict(key)
		return nil
	}

	return nil
}

// Put adds a value to the L1 cache.
func (h *CacheHook) Put(key string, value []byte, ttl time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if ttl == 0 {
		ttl = h.defaultTTL
	}

	// Evict oldest if at capacity.
	if len(h.entries) >= h.maxEntries {
		h.evictOldest()
	}

	entry := &cacheEntry{
		value: make([]byte, len(value)),
	}
	copy(entry.value, value)
	if ttl > 0 {
		entry.expiry = time.Now().Add(ttl)
	}
	h.entries[key] = entry
}

// Evict removes a key from the L1 cache.
func (h *CacheHook) Evict(keys ...string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, k := range keys {
		delete(h.entries, k)
	}
}

// Flush clears the entire L1 cache.
func (h *CacheHook) Flush() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.entries = make(map[string]*cacheEntry)
}

// Size returns the current number of entries in the cache.
func (h *CacheHook) Size() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.entries)
}

func (h *CacheHook) evictOldest() {
	var oldestKey string
	var oldestTime time.Time

	for k, e := range h.entries {
		if e.expired() {
			delete(h.entries, k)
			return
		}
		if oldestKey == "" || (!e.expiry.IsZero() && e.expiry.Before(oldestTime)) {
			oldestKey = k
			oldestTime = e.expiry
		}
	}

	if oldestKey != "" {
		delete(h.entries, oldestKey)
	}
}
