// Package memcacheddriver provides a Memcached driver for Grove KV.
//
// It wraps github.com/bradfitz/gomemcache/memcache and supports
// basic Get/Set/Delete operations plus CAS (Check-And-Set).
package memcacheddriver

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/bradfitz/gomemcache/memcache"

	kv "github.com/xraph/grove/kv"
	"github.com/xraph/grove/kv/driver"
)

// MemcachedDB implements driver.Driver for Memcached.
type MemcachedDB struct {
	client    *memcache.Client
	opts      *driver.DriverOptions
	mu        sync.Mutex
	changed   chan struct{}
	idle      []*requestClient
	managed   int
	idleLimit int
	selector  *memcache.ServerList
	clients   map[*requestClient]struct{}
	shutdown  context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once
	closeErr  error
}

var (
	_ driver.Driver    = (*MemcachedDB)(nil)
	_ driver.CASDriver = (*MemcachedDB)(nil)
)

// New creates a new unconnected Memcached driver.
func New() *MemcachedDB {
	return &MemcachedDB{}
}

func (db *MemcachedDB) Name() string { return "memcached" }

// Open connects to Memcached using the given DSN.
// The DSN should be a comma-separated list of server addresses (e.g., "localhost:11211").
func (db *MemcachedDB) Open(ctx context.Context, dsn string, opts ...driver.Option) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.client != nil {
		return fmt.Errorf("memcacheddriver: already opened; use New to create another client")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	selector := new(memcache.ServerList)
	servers := strings.Split(dsn, ",")
	for i := range servers {
		servers[i] = strings.TrimSpace(servers[i])
	}
	if err := selector.SetServers(servers...); err != nil {
		return fmt.Errorf("memcacheddriver: servers: %w", err)
	}
	db.opts = driver.ApplyOptions(opts)
	if db.opts.PoolSize <= 0 {
		db.opts.PoolSize = driver.DefaultDriverOptions().PoolSize
	}
	db.shutdown, db.cancel = context.WithCancel(context.Background())
	db.selector = selector
	db.changed = make(chan struct{})
	db.clients = make(map[*requestClient]struct{})
	db.idleLimit = memcache.DefaultMaxIdleConns
	if n, ok := db.opts.Extra["max_idle_conns"].(int); ok && n > 0 {
		db.idleLimit = n
	}
	db.idleLimit = min(db.idleLimit, db.opts.PoolSize)
	native := db.newRequestClient(selector)
	if n, ok := db.opts.Extra["max_idle_conns"].(int); ok && n > 0 {
		native.client.MaxIdleConns = n
	}
	db.clients[native] = struct{}{}
	db.client = native.client
	return nil
}

func (db *MemcachedDB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.client == nil {
		return nil
	}
	db.closeOnce.Do(func() {
		db.cancel()
		for c := range db.clients {
			db.closeErr = errors.Join(db.closeErr, c.closeConnections(true))
		}
	})
	return db.closeErr
}

func (db *MemcachedDB) Ping(ctx context.Context) error {
	_, err := withClient(ctx, db, func(c *memcache.Client) (*memcache.Item, error) { return c.Get("__ping__") })
	if errors.Is(err, memcache.ErrCacheMiss) {
		return nil
	}
	return err
}

func (db *MemcachedDB) Info() driver.DriverInfo {
	return driver.DriverInfo{
		Name:         "memcached",
		Version:      "1",
		Capabilities: driver.CapTTL | driver.CapCAS | driver.CapBatch,
	}
}

func (db *MemcachedDB) Get(ctx context.Context, key string) ([]byte, error) {
	item, err := withClient(ctx, db, func(c *memcache.Client) (*memcache.Item, error) { return c.Get(key) })
	if errors.Is(err, memcache.ErrCacheMiss) {
		return nil, kv.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("memcacheddriver: get: %w", err)
	}
	return item.Value, nil
}

func (db *MemcachedDB) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	item := &memcache.Item{
		Key:   key,
		Value: value,
	}
	if ttl > 0 {
		item.Expiration = int32(ttl.Seconds())
	}
	if _, err := withClient(ctx, db, func(c *memcache.Client) (struct{}, error) { return struct{}{}, c.Set(item) }); err != nil {
		return fmt.Errorf("memcacheddriver: set: %w", err)
	}
	return nil
}

func (db *MemcachedDB) Delete(ctx context.Context, keys ...string) (int64, error) {
	var count int64
	for _, k := range keys {
		if _, err := withClient(ctx, db, func(c *memcache.Client) (struct{}, error) { return struct{}{}, c.Delete(k) }); err != nil {
			if errors.Is(err, memcache.ErrCacheMiss) {
				continue
			}
			return count, fmt.Errorf("memcacheddriver: delete: %w", err)
		}
		count++
	}
	return count, nil
}

func (db *MemcachedDB) Exists(ctx context.Context, keys ...string) (int64, error) {
	if len(keys) == 0 {
		return 0, ctx.Err()
	}
	unique := make([]string, 0, len(keys))
	seen := make(map[string]bool, len(keys))
	for _, key := range keys {
		if !seen[key] {
			seen[key] = true
			unique = append(unique, key)
		}
	}
	items, err := withClient(ctx, db, func(c *memcache.Client) (map[string]*memcache.Item, error) { return c.GetMulti(unique) })
	if err != nil {
		return 0, fmt.Errorf("memcacheddriver: exists: %w", err)
	}
	var count int64
	for _, key := range keys {
		if items[key] != nil {
			count++
		}
	}
	return count, nil
}

// CASDriver

func (db *MemcachedDB) SetNX(ctx context.Context, key string, value []byte, ttl time.Duration) (bool, error) {
	item := &memcache.Item{
		Key:   key,
		Value: value,
	}
	if ttl > 0 {
		item.Expiration = int32(ttl.Seconds())
	}
	_, err := withClient(ctx, db, func(c *memcache.Client) (struct{}, error) { return struct{}{}, c.Add(item) })
	if errors.Is(err, memcache.ErrNotStored) {
		return false, nil // key already exists
	}
	if err != nil {
		return false, fmt.Errorf("memcacheddriver: setnx: %w", err)
	}
	return true, nil
}

func (db *MemcachedDB) SetXX(ctx context.Context, key string, value []byte, ttl time.Duration) (bool, error) {
	item := &memcache.Item{
		Key:   key,
		Value: value,
	}
	if ttl > 0 {
		item.Expiration = int32(ttl.Seconds())
	}
	_, err := withClient(ctx, db, func(c *memcache.Client) (struct{}, error) { return struct{}{}, c.Replace(item) })
	if errors.Is(err, memcache.ErrNotStored) {
		return false, nil // key doesn't exist
	}
	if err != nil {
		return false, fmt.Errorf("memcacheddriver: setxx: %w", err)
	}
	return true, nil
}

// Client returns a low-level gomemcache client owned by this driver.
// Direct calls bypass Grove request cancellation and its concurrency limit.
// Close releases this client's connections along with the managed pool.
func (db *MemcachedDB) Client() *memcache.Client {
	return db.client
}
