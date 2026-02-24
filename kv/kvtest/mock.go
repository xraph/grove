// Package kvtest provides a mock KV driver and conformance test suite.
package kvtest

import (
	"context"
	"sync"
	"time"

	"github.com/xraph/grove/kv"
	"github.com/xraph/grove/kv/driver"
)

// entry represents a stored value with optional expiry.
type entry struct {
	value  []byte
	expiry time.Time // zero value means no expiry
}

func (e *entry) expired() bool {
	return !e.expiry.IsZero() && time.Now().After(e.expiry)
}

// MockDriver is an in-memory KV driver for unit testing.
// It implements all optional driver interfaces.
type MockDriver struct {
	mu   sync.RWMutex
	data map[string]*entry
	info driver.DriverInfo
}

var (
	_ driver.Driver      = (*MockDriver)(nil)
	_ driver.BatchDriver = (*MockDriver)(nil)
	_ driver.TTLDriver   = (*MockDriver)(nil)
	_ driver.ScanDriver  = (*MockDriver)(nil)
	_ driver.CASDriver   = (*MockDriver)(nil)
)

// NewMockDriver creates a new in-memory mock driver.
func NewMockDriver() *MockDriver {
	return &MockDriver{
		data: make(map[string]*entry),
		info: driver.DriverInfo{
			Name:    "mock",
			Version: "1.0",
			Capabilities: driver.CapTTL | driver.CapCAS | driver.CapScan |
				driver.CapBatch | driver.CapTransaction,
		},
	}
}

func (m *MockDriver) Name() string { return "mock" }

func (m *MockDriver) Open(_ context.Context, _ string, _ ...driver.Option) error {
	return nil
}

func (m *MockDriver) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = make(map[string]*entry)
	return nil
}

func (m *MockDriver) Ping(_ context.Context) error { return nil }

func (m *MockDriver) Info() driver.DriverInfo { return m.info }

func (m *MockDriver) Get(_ context.Context, key string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	e, ok := m.data[key]
	if !ok || e.expired() {
		return nil, kv.ErrNotFound
	}
	cp := make([]byte, len(e.value))
	copy(cp, e.value)
	return cp, nil
}

func (m *MockDriver) Set(_ context.Context, key string, value []byte, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	e := &entry{value: make([]byte, len(value))}
	copy(e.value, value)
	if ttl > 0 {
		e.expiry = time.Now().Add(ttl)
	}
	m.data[key] = e
	return nil
}

func (m *MockDriver) Delete(_ context.Context, keys ...string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var count int64
	for _, k := range keys {
		if _, ok := m.data[k]; ok {
			delete(m.data, k)
			count++
		}
	}
	return count, nil
}

func (m *MockDriver) Exists(_ context.Context, keys ...string) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var count int64
	for _, k := range keys {
		if e, ok := m.data[k]; ok && !e.expired() {
			count++
		}
	}
	return count, nil
}

// BatchDriver

func (m *MockDriver) MGet(_ context.Context, keys []string) ([][]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([][]byte, len(keys))
	for i, k := range keys {
		if e, ok := m.data[k]; ok && !e.expired() {
			cp := make([]byte, len(e.value))
			copy(cp, e.value)
			result[i] = cp
		}
	}
	return result, nil
}

func (m *MockDriver) MSet(_ context.Context, pairs map[string][]byte, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for k, v := range pairs {
		e := &entry{value: make([]byte, len(v))}
		copy(e.value, v)
		if ttl > 0 {
			e.expiry = time.Now().Add(ttl)
		}
		m.data[k] = e
	}
	return nil
}

// TTLDriver

func (m *MockDriver) TTL(_ context.Context, key string) (time.Duration, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	e, ok := m.data[key]
	if !ok || e.expired() {
		return 0, kv.ErrNotFound
	}
	if e.expiry.IsZero() {
		return -1, nil // no expiry
	}
	remaining := time.Until(e.expiry)
	if remaining <= 0 {
		return 0, kv.ErrNotFound
	}
	return remaining, nil
}

func (m *MockDriver) Expire(_ context.Context, key string, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	e, ok := m.data[key]
	if !ok || e.expired() {
		return kv.ErrNotFound
	}
	if ttl > 0 {
		e.expiry = time.Now().Add(ttl)
	} else {
		e.expiry = time.Time{} // remove expiry
	}
	return nil
}

// ScanDriver

func (m *MockDriver) Scan(_ context.Context, pattern string, fn func(key string) error) error {
	m.mu.RLock()
	keys := make([]string, 0)
	for k, e := range m.data {
		if !e.expired() && matchPattern(pattern, k) {
			keys = append(keys, k)
		}
	}
	m.mu.RUnlock()

	for _, k := range keys {
		if err := fn(k); err != nil {
			return err
		}
	}
	return nil
}

// CASDriver

func (m *MockDriver) SetNX(_ context.Context, key string, value []byte, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if e, ok := m.data[key]; ok && !e.expired() {
		return false, nil
	}

	e := &entry{value: make([]byte, len(value))}
	copy(e.value, value)
	if ttl > 0 {
		e.expiry = time.Now().Add(ttl)
	}
	m.data[key] = e
	return true, nil
}

func (m *MockDriver) SetXX(_ context.Context, key string, value []byte, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if e, ok := m.data[key]; !ok || e.expired() {
		return false, nil
	}

	e := &entry{value: make([]byte, len(value))}
	copy(e.value, value)
	if ttl > 0 {
		e.expiry = time.Now().Add(ttl)
	}
	m.data[key] = e
	return true, nil
}

// matchPattern performs simple glob pattern matching (* matches any).
func matchPattern(pattern, key string) bool {
	if pattern == "*" {
		return true
	}
	// Simple prefix match for patterns like "prefix:*"
	for i := 0; i < len(pattern); i++ {
		if pattern[i] == '*' {
			return len(key) >= i && key[:i] == pattern[:i]
		}
		if i >= len(key) || pattern[i] != key[i] {
			return false
		}
	}
	return len(key) == len(pattern)
}
