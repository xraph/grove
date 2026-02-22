package migrate

import "sync"

// DefaultRegistry is the global migration registry.
// Modules register their groups here via init() functions.
var DefaultRegistry = NewMigrationRegistry()

// MigrationRegistry holds registered migration groups.
type MigrationRegistry struct {
	mu     sync.RWMutex
	groups []*Group
}

// NewMigrationRegistry creates an empty MigrationRegistry.
func NewMigrationRegistry() *MigrationRegistry {
	return &MigrationRegistry{}
}

// Register adds one or more migration groups to the registry.
func (r *MigrationRegistry) Register(groups ...*Group) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.groups = append(r.groups, groups...)
}

// Groups returns a copy of all registered migration groups.
func (r *MigrationRegistry) Groups() []*Group {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]*Group, len(r.groups))
	copy(out, r.groups)
	return out
}
