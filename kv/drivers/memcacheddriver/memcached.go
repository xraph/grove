// Package memcacheddriver provides a Memcached driver for Grove KV.
//
// It wraps github.com/bradfitz/gomemcache/memcache and supports
// basic Get/Set/Delete operations plus CAS (Check-And-Set).
package memcacheddriver

import (
	"context"
	"fmt"
	"time"

	"github.com/bradfitz/gomemcache/memcache"

	kv "github.com/xraph/grove/kv"
	"github.com/xraph/grove/kv/driver"
)

// MemcachedDB implements driver.Driver for Memcached.
type MemcachedDB struct {
	client *memcache.Client
	opts   *driver.DriverOptions
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
func (db *MemcachedDB) Open(_ context.Context, dsn string, opts ...driver.Option) error {
	db.opts = driver.ApplyOptions(opts)
	db.client = memcache.New(dsn)
	if db.opts.DialTimeout > 0 {
		db.client.Timeout = db.opts.DialTimeout
	}
	return nil
}

func (db *MemcachedDB) Close() error {
	// gomemcache doesn't have a Close method; connections are pooled and reused.
	return nil
}

func (db *MemcachedDB) Ping(ctx context.Context) error {
	_, err := db.client.Get("__ping__")
	if err == memcache.ErrCacheMiss {
		return nil // server is reachable
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

func (db *MemcachedDB) Get(_ context.Context, key string) ([]byte, error) {
	item, err := db.client.Get(key)
	if err == memcache.ErrCacheMiss {
		return nil, kv.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("memcacheddriver: get: %w", err)
	}
	return item.Value, nil
}

func (db *MemcachedDB) Set(_ context.Context, key string, value []byte, ttl time.Duration) error {
	item := &memcache.Item{
		Key:   key,
		Value: value,
	}
	if ttl > 0 {
		item.Expiration = int32(ttl.Seconds())
	}
	if err := db.client.Set(item); err != nil {
		return fmt.Errorf("memcacheddriver: set: %w", err)
	}
	return nil
}

func (db *MemcachedDB) Delete(_ context.Context, keys ...string) (int64, error) {
	var count int64
	for _, k := range keys {
		if err := db.client.Delete(k); err != nil {
			if err == memcache.ErrCacheMiss {
				continue
			}
			return count, fmt.Errorf("memcacheddriver: delete: %w", err)
		}
		count++
	}
	return count, nil
}

func (db *MemcachedDB) Exists(_ context.Context, keys ...string) (int64, error) {
	var count int64
	for _, k := range keys {
		_, err := db.client.Get(k)
		if err == nil {
			count++
		} else if err != memcache.ErrCacheMiss {
			return count, fmt.Errorf("memcacheddriver: exists: %w", err)
		}
	}
	return count, nil
}

// CASDriver

func (db *MemcachedDB) SetNX(_ context.Context, key string, value []byte, ttl time.Duration) (bool, error) {
	item := &memcache.Item{
		Key:   key,
		Value: value,
	}
	if ttl > 0 {
		item.Expiration = int32(ttl.Seconds())
	}
	err := db.client.Add(item)
	if err == memcache.ErrNotStored {
		return false, nil // key already exists
	}
	if err != nil {
		return false, fmt.Errorf("memcacheddriver: setnx: %w", err)
	}
	return true, nil
}

func (db *MemcachedDB) SetXX(_ context.Context, key string, value []byte, ttl time.Duration) (bool, error) {
	item := &memcache.Item{
		Key:   key,
		Value: value,
	}
	if ttl > 0 {
		item.Expiration = int32(ttl.Seconds())
	}
	err := db.client.Replace(item)
	if err == memcache.ErrNotStored {
		return false, nil // key doesn't exist
	}
	if err != nil {
		return false, fmt.Errorf("memcacheddriver: setxx: %w", err)
	}
	return true, nil
}

// Client returns the underlying gomemcache client.
func (db *MemcachedDB) Client() *memcache.Client {
	return db.client
}
