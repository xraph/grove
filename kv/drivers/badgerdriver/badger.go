// Package badgerdriver provides a Badger embedded KV driver for Grove KV.
//
// Badger is a fast, embeddable LSM-tree key-value store written in Go.
// It supports TTL natively, prefix iteration, and transactions.
package badgerdriver

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"

	kv "github.com/xraph/grove/kv"
	"github.com/xraph/grove/kv/driver"
)

// BadgerDB implements driver.Driver for Badger.
type BadgerDB struct {
	db   *badger.DB
	opts *driver.DriverOptions
}

var (
	_ driver.Driver      = (*BadgerDB)(nil)
	_ driver.TTLDriver   = (*BadgerDB)(nil)
	_ driver.ScanDriver  = (*BadgerDB)(nil)
	_ driver.BatchDriver = (*BadgerDB)(nil)
	_ driver.CASDriver   = (*BadgerDB)(nil)
)

// New creates a new unconnected Badger driver.
func New() *BadgerDB {
	return &BadgerDB{}
}

func (db *BadgerDB) Name() string { return "badger" }

// Open opens a Badger database at the given path.
// Use "memory://" for an in-memory database.
func (db *BadgerDB) Open(_ context.Context, dsn string, opts ...driver.Option) error {
	db.opts = driver.ApplyOptions(opts)

	var badgerOpts badger.Options
	if dsn == "memory://" || dsn == ":memory:" {
		badgerOpts = badger.DefaultOptions("").WithInMemory(true)
	} else {
		badgerOpts = badger.DefaultOptions(dsn)
	}
	badgerOpts = badgerOpts.WithLoggingLevel(badger.WARNING)

	var err error
	db.db, err = badger.Open(badgerOpts)
	if err != nil {
		return fmt.Errorf("badgerdriver: open: %w", err)
	}
	return nil
}

func (db *BadgerDB) Close() error {
	if db.db == nil {
		return nil
	}
	return db.db.Close()
}

func (db *BadgerDB) Ping(_ context.Context) error {
	if db.db == nil {
		return fmt.Errorf("badgerdriver: not connected")
	}
	if db.db.IsClosed() {
		return fmt.Errorf("badgerdriver: database is closed")
	}
	return nil
}

func (db *BadgerDB) Info() driver.DriverInfo {
	return driver.DriverInfo{
		Name:    "badger",
		Version: "4",
		Capabilities: driver.CapTTL | driver.CapCAS | driver.CapScan |
			driver.CapBatch | driver.CapTransaction,
	}
}

func (db *BadgerDB) Get(_ context.Context, key string) ([]byte, error) {
	var val []byte
	err := db.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err == badger.ErrKeyNotFound {
			return kv.ErrNotFound
		}
		if err != nil {
			return err
		}
		val, err = item.ValueCopy(nil)
		return err
	})
	if err != nil {
		return nil, err
	}
	return val, nil
}

func (db *BadgerDB) Set(_ context.Context, key string, value []byte, ttl time.Duration) error {
	return db.db.Update(func(txn *badger.Txn) error {
		entry := badger.NewEntry([]byte(key), value)
		if ttl > 0 {
			entry = entry.WithTTL(ttl)
		}
		return txn.SetEntry(entry)
	})
}

func (db *BadgerDB) Delete(_ context.Context, keys ...string) (int64, error) {
	var count int64
	err := db.db.Update(func(txn *badger.Txn) error {
		for _, k := range keys {
			// Check if key exists before deleting.
			_, err := txn.Get([]byte(k))
			if err == badger.ErrKeyNotFound {
				continue
			}
			if err != nil {
				return err
			}
			if err := txn.Delete([]byte(k)); err != nil {
				return err
			}
			count++
		}
		return nil
	})
	return count, err
}

func (db *BadgerDB) Exists(_ context.Context, keys ...string) (int64, error) {
	var count int64
	err := db.db.View(func(txn *badger.Txn) error {
		for _, k := range keys {
			_, err := txn.Get([]byte(k))
			if err == nil {
				count++
			} else if err != badger.ErrKeyNotFound {
				return err
			}
		}
		return nil
	})
	return count, err
}

// TTLDriver — Badger supports TTL natively but doesn't expose remaining TTL.

func (db *BadgerDB) TTL(_ context.Context, key string) (time.Duration, error) {
	var ttl time.Duration
	err := db.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err == badger.ErrKeyNotFound {
			return kv.ErrNotFound
		}
		if err != nil {
			return err
		}
		expiresAt := item.ExpiresAt()
		if expiresAt == 0 {
			ttl = -1 // no expiry
			return nil
		}
		remaining := time.Until(time.Unix(int64(expiresAt), 0))
		if remaining <= 0 {
			return kv.ErrNotFound
		}
		ttl = remaining
		return nil
	})
	return ttl, err
}

func (db *BadgerDB) Expire(_ context.Context, key string, ttl time.Duration) error {
	return db.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err == badger.ErrKeyNotFound {
			return kv.ErrNotFound
		}
		if err != nil {
			return err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		entry := badger.NewEntry([]byte(key), val)
		if ttl > 0 {
			entry = entry.WithTTL(ttl)
		}
		return txn.SetEntry(entry)
	})
}

// ScanDriver

func (db *BadgerDB) Scan(_ context.Context, pattern string, fn func(key string) error) error {
	prefix := extractPrefix(pattern)
	return db.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		if prefix != "" {
			opts.Prefix = []byte(prefix)
		}
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := string(item.Key())
			if matchGlob(pattern, key) {
				if err := fn(key); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// BatchDriver

func (db *BadgerDB) MGet(_ context.Context, keys []string) ([][]byte, error) {
	results := make([][]byte, len(keys))
	err := db.db.View(func(txn *badger.Txn) error {
		for i, k := range keys {
			item, err := txn.Get([]byte(k))
			if err == badger.ErrKeyNotFound {
				continue
			}
			if err != nil {
				return err
			}
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			results[i] = val
		}
		return nil
	})
	return results, err
}

func (db *BadgerDB) MSet(_ context.Context, pairs map[string][]byte, ttl time.Duration) error {
	wb := db.db.NewWriteBatch()
	defer wb.Cancel()

	for k, v := range pairs {
		entry := badger.NewEntry([]byte(k), v)
		if ttl > 0 {
			entry = entry.WithTTL(ttl)
		}
		if err := wb.SetEntry(entry); err != nil {
			return err
		}
	}
	return wb.Flush()
}

// CASDriver

func (db *BadgerDB) SetNX(_ context.Context, key string, value []byte, ttl time.Duration) (bool, error) {
	set := false
	err := db.db.Update(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte(key))
		if err == nil {
			return nil // key exists, don't set
		}
		if err != badger.ErrKeyNotFound {
			return err
		}
		entry := badger.NewEntry([]byte(key), value)
		if ttl > 0 {
			entry = entry.WithTTL(ttl)
		}
		set = true
		return txn.SetEntry(entry)
	})
	return set, err
}

func (db *BadgerDB) SetXX(_ context.Context, key string, value []byte, ttl time.Duration) (bool, error) {
	set := false
	err := db.db.Update(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte(key))
		if err == badger.ErrKeyNotFound {
			return nil // key doesn't exist, don't set
		}
		if err != nil {
			return err
		}
		entry := badger.NewEntry([]byte(key), value)
		if ttl > 0 {
			entry = entry.WithTTL(ttl)
		}
		set = true
		return txn.SetEntry(entry)
	})
	return set, err
}

// DB returns the underlying badger.DB.
func (db *BadgerDB) DB() *badger.DB {
	return db.db
}

// extractPrefix extracts the literal prefix before any glob wildcard.
func extractPrefix(pattern string) string {
	idx := strings.IndexAny(pattern, "*?[")
	if idx < 0 {
		return pattern
	}
	return pattern[:idx]
}

// matchGlob performs simple glob matching (* matches any sequence).
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
