package extension

import (
	"fmt"
	"sync"

	"github.com/xraph/grove"
)

// DBManager manages multiple named grove.DB instances.
// It provides named access, a default database, and bulk close.
type DBManager struct {
	mu         sync.RWMutex
	dbs        map[string]*grove.DB
	defaultKey string
}

// NewDBManager creates an empty DBManager.
func NewDBManager() *DBManager {
	return &DBManager{
		dbs: make(map[string]*grove.DB),
	}
}

// Add registers a named database. The first database added becomes the
// default unless SetDefault is called explicitly.
func (m *DBManager) Add(name string, db *grove.DB) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.dbs[name] = db
	if m.defaultKey == "" {
		m.defaultKey = name
	}
}

// Get returns the database registered under name.
func (m *DBManager) Get(name string) (*grove.DB, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	db, ok := m.dbs[name]
	if !ok {
		return nil, fmt.Errorf("grove: database %q not found", name)
	}
	return db, nil
}

// Default returns the default database.
func (m *DBManager) Default() (*grove.DB, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.defaultKey == "" {
		return nil, fmt.Errorf("grove: no default database configured")
	}
	db, ok := m.dbs[m.defaultKey]
	if !ok {
		return nil, fmt.Errorf("grove: default database %q not found", m.defaultKey)
	}
	return db, nil
}

// DefaultName returns the name of the default database.
func (m *DBManager) DefaultName() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.defaultKey
}

// SetDefault sets the default database by name.
// Returns an error if the name is not registered.
func (m *DBManager) SetDefault(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.dbs[name]; !ok {
		return fmt.Errorf("grove: cannot set default: database %q not found", name)
	}
	m.defaultKey = name
	return nil
}

// All returns a shallow copy of the name-to-DB map.
func (m *DBManager) All() map[string]*grove.DB {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make(map[string]*grove.DB, len(m.dbs))
	for k, v := range m.dbs {
		out[k] = v
	}
	return out
}

// Len returns the number of registered databases.
func (m *DBManager) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.dbs)
}

// Close closes all registered databases and returns the first error encountered.
func (m *DBManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var firstErr error
	for name, db := range m.dbs {
		if err := db.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("grove: close database %q: %w", name, err)
		}
	}
	return firstErr
}
