package middleware

import (
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/xraph/grove/hook"
	kvpkg "github.com/xraph/grove/kv"
	"github.com/xraph/grove/kv/internal/readhook"
)

// CacheHook caches raw GET results locally. Writes through the store invalidate
// affected keys. Changes made outside this store become visible after defaultTTL.
type CacheHook struct {
	mu         sync.RWMutex
	entries    map[readhook.Key]*cacheEntry
	maxEntries int
	defaultTTL time.Duration
	generation uint64
}

type cacheEntry struct {
	value  []byte
	expiry time.Time
}

func (e *cacheEntry) expired() bool {
	return !e.expiry.IsZero() && !time.Now().Before(e.expiry)
}

var (
	_ hook.PreQueryHook  = (*CacheHook)(nil)
	_ hook.PostQueryHook = (*CacheHook)(nil)
)

// NewCache creates a local read cache. A non-positive maxEntries disables it.
func NewCache(maxEntries int, defaultTTL time.Duration) *CacheHook {
	return &CacheHook{entries: make(map[readhook.Key]*cacheEntry), maxEntries: maxEntries, defaultTTL: defaultTTL}
}

func (h *CacheHook) BeforeQuery(_ context.Context, qc *hook.QueryContext) (*hook.HookResult, error) {
	if qc.Operation == kvpkg.OpGet {
		readhook.Add(qc, h.wrapRead)
	} else if cacheMutation(qc.Operation) {
		readhook.OnFinish(qc, func() {
			keys, ok := qc.Values["_kv_keys"].([]string)
			if !ok {
				keys = []string{qc.RawQuery}
			}
			h.invalidate(qc.Values["_kv_owner"], keys)
		})
	}
	return &hook.HookResult{Decision: hook.Allow}, nil
}

func cacheMutation(op hook.Operation) bool {
	switch op {
	case kvpkg.OpSet, kvpkg.OpMSet, kvpkg.OpDelete, kvpkg.OpExpire,
		kvpkg.OpZAdd, kvpkg.OpZRem, kvpkg.OpSAdd, kvpkg.OpSRem,
		kvpkg.OpHSet, kvpkg.OpHDel, kvpkg.OpEval, kvpkg.OpXAdd, kvpkg.OpXDel:
		return true
	default:
		return false
	}
}

func (h *CacheHook) wrapRead(next readhook.Read) readhook.Read {
	return func(ctx context.Context, key readhook.Key) ([]byte, error) {
		h.mu.RLock()
		entry := h.entries[key]
		if entry == nil {
			entry = h.entries[readhook.Key{Name: key.Name}]
		}
		generation := h.generation
		if entry != nil && !entry.expired() {
			raw := bytes.Clone(entry.value)
			h.mu.RUnlock()
			return raw, nil
		}
		h.mu.RUnlock()
		raw, err := next(ctx, key)
		if err != nil {
			return nil, err
		}
		h.mu.Lock()
		if h.generation == generation {
			h.put(key, raw, h.defaultTTL)
		}
		h.mu.Unlock()
		return raw, nil
	}
}

// AfterQuery retains the post-query hook interface. Read completion and write
// invalidation run inside the store, even if another hook returns an error.
func (h *CacheHook) AfterQuery(_ context.Context, _ *hook.QueryContext, _ any) error { return nil }

// Put inserts raw bytes for a physical key. Explicit entries apply to every
// store using this hook; entries fetched by GET remain scoped to their store.
func (h *CacheHook) Put(key string, value []byte, ttl time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.generation++
	for k := range h.entries {
		if k.Name == key {
			delete(h.entries, k)
		}
	}
	if ttl == 0 {
		ttl = h.defaultTTL
	}
	h.put(readhook.Key{Name: key}, value, ttl)
}

func (h *CacheHook) put(key readhook.Key, value []byte, ttl time.Duration) {
	if h.maxEntries <= 0 {
		return
	}
	if _, exists := h.entries[key]; !exists && len(h.entries) >= h.maxEntries {
		h.evictOldest()
	}
	entry := &cacheEntry{value: bytes.Clone(value)}
	if ttl > 0 {
		entry.expiry = time.Now().Add(ttl)
	}
	h.entries[key] = entry
}

func (h *CacheHook) invalidate(owner any, keys []string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.generation++
	for _, key := range keys {
		delete(h.entries, readhook.Key{Owner: owner, Name: key})
		delete(h.entries, readhook.Key{Name: key})
	}
}

// Evict removes keys from every store sharing this cache.
func (h *CacheHook) Evict(keys ...string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.generation++
	for key := range h.entries {
		for _, name := range keys {
			if key.Name == name {
				delete(h.entries, key)
				break
			}
		}
	}
}

// Flush clears all cached entries.
func (h *CacheHook) Flush() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.generation++
	h.entries = make(map[readhook.Key]*cacheEntry)
}

// Size returns the number of cached entries, including expired ones.
func (h *CacheHook) Size() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.entries)
}

func (h *CacheHook) evictOldest() {
	var oldestKey readhook.Key
	var oldestTime time.Time
	found := false
	for key, entry := range h.entries {
		if entry.expired() {
			delete(h.entries, key)
			return
		}
		if !found || (!entry.expiry.IsZero() && (oldestTime.IsZero() || entry.expiry.Before(oldestTime))) {
			oldestKey, oldestTime, found = key, entry.expiry, true
		}
	}
	if found {
		delete(h.entries, oldestKey)
	}
}
