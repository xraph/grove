package extension

import (
	"errors"
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
	closed     bool
	closeErr   error
}

// NewDBManager creates an empty DBManager.
func NewDBManager() *DBManager {
	return &DBManager{
		dbs: make(map[string]*grove.DB),
	}
}

// Add registers a named database. The first database added becomes the
// default unless SetDefault is called explicitly. Ownership transfers to the
// manager. Replacing a name closes the previous handle once no other name
// refers to it. Close reports replacement errors. After Close, Add immediately
// closes any incoming handle.
func (m *DBManager) Add(name string, db *grove.DB) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		m.closeHandle(name, db)
		return
	}
	previous := m.dbs[name]
	m.dbs[name] = db
	if m.defaultKey == "" {
		m.defaultKey = name
	}
	if previous == nil || previous == db {
		return
	}
	for _, registered := range m.dbs {
		if registered == previous {
			return
		}
	}
	m.closeHandle(name, previous)
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

// Close closes each owned database once and reports shutdown or replacement
// errors. Repeated calls do not close handles again.
func (m *DBManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return m.closeErr
	}
	m.closed = true
	seen := make(map[*grove.DB]bool)
	for name, handle := range m.dbs {
		if !seen[handle] {
			seen[handle] = true
			m.closeHandle(name, handle)
		}
	}
	clear(m.dbs)
	m.defaultKey = ""
	return m.closeErr
}

func (m *DBManager) closeHandle(name string, handle *grove.DB) {
	if handle == nil {
		return
	}
	if err := handle.Close(); err != nil && !errors.Is(err, grove.ErrDriverClosed) {
		m.closeErr = errors.Join(m.closeErr, fmt.Errorf("grove: close database %q: %w", name, err))
	}
}
