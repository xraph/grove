// Package boltdriver provides a bbolt (BoltDB) embedded KV driver for Grove KV.
//
// BoltDB is a fast, pure-Go B+ tree key-value store. It provides ACID
// transactions, bucket-based namespacing, and crash safety via memory-mapped I/O.
package boltdriver

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"time"

	"go.etcd.io/bbolt"

	"github.com/xraph/grove/kv"
	"github.com/xraph/grove/kv/driver"
)

var (
	dataBucket = []byte("kv_data")
	ttlBucket  = []byte("kv_ttl")
)

// BoltDB implements driver.Driver backed by bbolt (BoltDB).
type BoltDB struct {
	db     *bbolt.DB
	bucket []byte
	opts   *driver.DriverOptions
}

var (
	_ driver.Driver      = (*BoltDB)(nil)
	_ driver.TTLDriver   = (*BoltDB)(nil)
	_ driver.ScanDriver  = (*BoltDB)(nil)
	_ driver.BatchDriver = (*BoltDB)(nil)
	_ driver.CASDriver   = (*BoltDB)(nil)
)

// New creates a new unconnected BoltDB driver.
func New() *BoltDB {
	return &BoltDB{}
}

// Name returns the driver name.
func (b *BoltDB) Name() string { return "bbolt" }

// Open opens a bbolt database at the path specified by dsn.
func (b *BoltDB) Open(_ context.Context, dsn string, opts ...driver.Option) error {
	b.opts = driver.ApplyOptions(opts)

	cfg := defaultConfig()
	boltOpts := &bbolt.Options{
		Timeout:      cfg.timeout,
		NoGrowSync:   cfg.noGrowSync,
		ReadOnly:     cfg.readOnly,
		FreelistType: bbolt.FreelistArrayType,
	}

	db, err := bbolt.Open(dsn, cfg.fileMode, boltOpts)
	if err != nil {
		return fmt.Errorf("boltdriver: open: %w", err)
	}

	bucket := dataBucket
	if cfg.bucket != "" {
		bucket = []byte(cfg.bucket)
	}

	// Create default buckets.
	if !cfg.readOnly {
		err = db.Update(func(tx *bbolt.Tx) error {
			if _, e := tx.CreateBucketIfNotExists(bucket); e != nil {
				return e
			}
			_, e := tx.CreateBucketIfNotExists(ttlBucket)
			return e
		})
		if err != nil {
			db.Close()
			return fmt.Errorf("boltdriver: create buckets: %w", err)
		}
	}

	b.db = db
	b.bucket = bucket
	return nil
}

// Close closes the underlying bbolt database.
func (b *BoltDB) Close() error {
	if b.db == nil {
		return nil
	}
	return b.db.Close()
}

// Ping verifies the database is accessible.
func (b *BoltDB) Ping(_ context.Context) error {
	if b.db == nil {
		return fmt.Errorf("boltdriver: not connected")
	}
	return b.db.View(func(tx *bbolt.Tx) error {
		_ = tx.Bucket(b.bucket)
		return nil
	})
}

// Info returns driver capabilities.
func (b *BoltDB) Info() driver.DriverInfo {
	return driver.DriverInfo{
		Name:    "bbolt",
		Version: "1",
		Capabilities: driver.CapTTL | driver.CapScan | driver.CapBatch |
			driver.CapCAS | driver.CapTransaction,
	}
}

// Get retrieves a value by key.
func (b *BoltDB) Get(_ context.Context, key string) ([]byte, error) {
	var result []byte
	err := b.db.View(func(tx *bbolt.Tx) error {
		if b.isExpired(tx, key) {
			return kv.ErrNotFound
		}
		bkt := tx.Bucket(b.bucket)
		if bkt == nil {
			return kv.ErrNotFound
		}
		v := bkt.Get([]byte(key))
		if v == nil {
			return kv.ErrNotFound
		}
		result = make([]byte, len(v))
		copy(result, v)
		return nil
	})
	return result, err
}

// Set stores a key-value pair.
func (b *BoltDB) Set(_ context.Context, key string, value []byte, ttl time.Duration) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		bkt := tx.Bucket(b.bucket)
		if bkt == nil {
			return kv.ErrStoreClosed
		}
		if err := bkt.Put([]byte(key), value); err != nil {
			return err
		}
		return b.setTTL(tx, key, ttl)
	})
}

// Delete removes one or more keys and returns the count deleted.
func (b *BoltDB) Delete(_ context.Context, keys ...string) (int64, error) {
	var count int64
	err := b.db.Update(func(tx *bbolt.Tx) error {
		bkt := tx.Bucket(b.bucket)
		if bkt == nil {
			return nil
		}
		tBkt := tx.Bucket(ttlBucket)
		for _, key := range keys {
			if bkt.Get([]byte(key)) == nil {
				continue
			}
			if err := bkt.Delete([]byte(key)); err != nil {
				return err
			}
			count++
			if tBkt != nil {
				_ = tBkt.Delete([]byte(key))
			}
		}
		return nil
	})
	return count, err
}

// Exists checks if keys exist.
func (b *BoltDB) Exists(_ context.Context, keys ...string) (int64, error) {
	var count int64
	err := b.db.View(func(tx *bbolt.Tx) error {
		bkt := tx.Bucket(b.bucket)
		if bkt == nil {
			return nil
		}
		for _, key := range keys {
			if b.isExpired(tx, key) {
				continue
			}
			if bkt.Get([]byte(key)) != nil {
				count++
			}
		}
		return nil
	})
	return count, err
}

// --- TTLDriver ---

// TTL returns the remaining time-to-live for a key.
func (b *BoltDB) TTL(_ context.Context, key string) (time.Duration, error) {
	var ttl time.Duration
	err := b.db.View(func(tx *bbolt.Tx) error {
		tBkt := tx.Bucket(ttlBucket)
		if tBkt == nil {
			return kv.ErrNotFound
		}
		v := tBkt.Get([]byte(key))
		if v == nil {
			ttl = -1 // no expiry
			return nil
		}
		expireAt := int64(binary.BigEndian.Uint64(v))
		remaining := time.Until(time.Unix(0, expireAt))
		if remaining <= 0 {
			return kv.ErrNotFound
		}
		ttl = remaining
		return nil
	})
	return ttl, err
}

// Expire sets a TTL on an existing key.
func (b *BoltDB) Expire(_ context.Context, key string, ttl time.Duration) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		bkt := tx.Bucket(b.bucket)
		if bkt == nil || bkt.Get([]byte(key)) == nil {
			return kv.ErrNotFound
		}
		return b.setTTL(tx, key, ttl)
	})
}

// --- ScanDriver ---

// Scan iterates over keys matching the given pattern (prefix-based with glob).
func (b *BoltDB) Scan(_ context.Context, pattern string, fn func(key string) error) error {
	prefix := extractPrefix(pattern)
	return b.db.View(func(tx *bbolt.Tx) error {
		bkt := tx.Bucket(b.bucket)
		if bkt == nil {
			return nil
		}
		c := bkt.Cursor()
		startKey := []byte(prefix)

		for k, _ := c.Seek(startKey); k != nil; k, _ = c.Next() {
			key := string(k)
			if prefix != "" && !strings.HasPrefix(key, prefix) {
				break
			}
			if b.isExpired(tx, key) {
				continue
			}
			if matchGlob(pattern, key) {
				if err := fn(key); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// --- BatchDriver ---

// MGet retrieves multiple keys.
func (b *BoltDB) MGet(_ context.Context, keys []string) ([][]byte, error) {
	results := make([][]byte, len(keys))
	err := b.db.View(func(tx *bbolt.Tx) error {
		bkt := tx.Bucket(b.bucket)
		if bkt == nil {
			return nil
		}
		for i, key := range keys {
			if b.isExpired(tx, key) {
				continue
			}
			v := bkt.Get([]byte(key))
			if v != nil {
				results[i] = make([]byte, len(v))
				copy(results[i], v)
			}
		}
		return nil
	})
	return results, err
}

// MSet stores multiple key-value pairs.
func (b *BoltDB) MSet(_ context.Context, pairs map[string][]byte, ttl time.Duration) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		bkt := tx.Bucket(b.bucket)
		if bkt == nil {
			return kv.ErrStoreClosed
		}
		for key, val := range pairs {
			if err := bkt.Put([]byte(key), val); err != nil {
				return err
			}
			if err := b.setTTL(tx, key, ttl); err != nil {
				return err
			}
		}
		return nil
	})
}

// --- CASDriver ---

// SetNX sets a key only if it does not exist.
func (b *BoltDB) SetNX(_ context.Context, key string, value []byte, ttl time.Duration) (bool, error) {
	var set bool
	err := b.db.Update(func(tx *bbolt.Tx) error {
		bkt := tx.Bucket(b.bucket)
		if bkt == nil {
			return kv.ErrStoreClosed
		}
		existing := bkt.Get([]byte(key))
		if existing != nil && !b.isExpired(tx, key) {
			set = false
			return nil
		}
		if err := bkt.Put([]byte(key), value); err != nil {
			return err
		}
		set = true
		return b.setTTL(tx, key, ttl)
	})
	return set, err
}

// SetXX sets a key only if it already exists.
func (b *BoltDB) SetXX(_ context.Context, key string, value []byte, ttl time.Duration) (bool, error) {
	var set bool
	err := b.db.Update(func(tx *bbolt.Tx) error {
		bkt := tx.Bucket(b.bucket)
		if bkt == nil {
			return kv.ErrStoreClosed
		}
		existing := bkt.Get([]byte(key))
		if existing == nil || b.isExpired(tx, key) {
			set = false
			return nil
		}
		if err := bkt.Put([]byte(key), value); err != nil {
			return err
		}
		set = true
		return b.setTTL(tx, key, ttl)
	})
	return set, err
}

// DB returns the underlying bbolt.DB.
func (b *BoltDB) DB() *bbolt.DB {
	return b.db
}

// --- Helpers ---

func (b *BoltDB) setTTL(tx *bbolt.Tx, key string, ttl time.Duration) error {
	tBkt := tx.Bucket(ttlBucket)
	if tBkt == nil {
		return nil
	}
	if ttl <= 0 {
		return tBkt.Delete([]byte(key))
	}
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(time.Now().Add(ttl).UnixNano()))
	return tBkt.Put([]byte(key), buf)
}

func (b *BoltDB) isExpired(tx *bbolt.Tx, key string) bool {
	tBkt := tx.Bucket(ttlBucket)
	if tBkt == nil {
		return false
	}
	v := tBkt.Get([]byte(key))
	if v == nil {
		return false
	}
	expireAt := int64(binary.BigEndian.Uint64(v))
	return time.Now().UnixNano() > expireAt
}

func extractPrefix(pattern string) string {
	idx := strings.IndexAny(pattern, "*?[")
	if idx < 0 {
		return pattern
	}
	return pattern[:idx]
}

func matchGlob(pattern, str string) bool {
	if pattern == "*" {
		return true
	}
	for i := 0; i < len(pattern); i++ {
		if pattern[i] == '*' {
			return len(str) >= i && str[:i] == pattern[:i]
		}
		if i >= len(str) || (pattern[i] != '?' && pattern[i] != str[i]) {
			return false
		}
	}
	return len(str) == len(pattern)
}
