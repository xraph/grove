package extension

import (
	"fmt"
	"sync"

	"github.com/xraph/grove/kv"
)

// StoreManager manages multiple named kv.Store instances.
// It provides named access, a default store, and bulk close.
type StoreManager struct {
	mu         sync.RWMutex
	stores     map[string]*kv.Store
	defaultKey string
}

// NewStoreManager creates an empty StoreManager.
func NewStoreManager() *StoreManager {
	return &StoreManager{
		stores: make(map[string]*kv.Store),
	}
}

// Add registers a named store. The first store added becomes the
// default unless SetDefault is called explicitly.
func (m *StoreManager) Add(name string, s *kv.Store) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.stores[name] = s
	if m.defaultKey == "" {
		m.defaultKey = name
	}
}

// Get returns the store registered under name.
func (m *StoreManager) Get(name string) (*kv.Store, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	s, ok := m.stores[name]
	if !ok {
		return nil, fmt.Errorf("kv: store %q not found", name)
	}
	return s, nil
}

// Default returns the default store.
func (m *StoreManager) Default() (*kv.Store, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.defaultKey == "" {
		return nil, fmt.Errorf("kv: no default store configured")
	}
	s, ok := m.stores[m.defaultKey]
	if !ok {
		return nil, fmt.Errorf("kv: default store %q not found", m.defaultKey)
	}
	return s, nil
}

// DefaultName returns the name of the default store.
func (m *StoreManager) DefaultName() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.defaultKey
}

// SetDefault sets the default store by name.
// Returns an error if the name is not registered.
func (m *StoreManager) SetDefault(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.stores[name]; !ok {
		return fmt.Errorf("kv: cannot set default: store %q not found", name)
	}
	m.defaultKey = name
	return nil
}

// All returns a shallow copy of the name-to-Store map.
func (m *StoreManager) All() map[string]*kv.Store {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make(map[string]*kv.Store, len(m.stores))
	for k, v := range m.stores {
		out[k] = v
	}
	return out
}

// Len returns the number of registered stores.
func (m *StoreManager) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.stores)
}

// Close closes all registered stores and returns the first error encountered.
func (m *StoreManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var firstErr error
	for name, s := range m.stores {
		if err := s.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("kv: close store %q: %w", name, err)
		}
	}
	return firstErr
}
