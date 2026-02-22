package migrate

import (
	"fmt"
	"sort"
	"sync"
)

// GroupOption configures a migration group.
type GroupOption func(*Group)

// DependsOn declares that this group's migrations must run after the
// specified groups have completed.
func DependsOn(groups ...string) GroupOption {
	return func(g *Group) {
		g.dependsOn = append(g.dependsOn, groups...)
	}
}

// Group represents a collection of migrations owned by a module or extension.
// Each group has a unique name (e.g., "core", "forge.billing") and can declare
// dependencies on other groups.
type Group struct {
	name       string
	dependsOn  []string
	migrations []*Migration

	mu sync.Mutex
}

// NewGroup creates a new migration group with the given name and options.
//
//	var Migrations = migrate.NewGroup("core")
//	var Migrations = migrate.NewGroup("forge.billing", migrate.DependsOn("core"))
func NewGroup(name string, opts ...GroupOption) *Group {
	g := &Group{name: name}
	for _, opt := range opts {
		opt(g)
	}
	return g
}

// Name returns the group name.
func (g *Group) Name() string {
	return g.name
}

// DependsOnGroups returns the list of group names this group depends on.
func (g *Group) DependsOnGroups() []string {
	return g.dependsOn
}

// Migrations returns a copy of the group's migrations sorted by version.
func (g *Group) Migrations() []*Migration {
	g.mu.Lock()
	defer g.mu.Unlock()

	ms := make([]*Migration, len(g.migrations))
	copy(ms, g.migrations)
	sort.Slice(ms, func(i, j int) bool {
		return ms[i].Version < ms[j].Version
	})
	return ms
}

// Register adds migrations to the group. The group name is automatically set
// on each migration. Returns an error if any migration has a duplicate version
// within this group.
func (g *Group) Register(migrations ...*Migration) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	versions := make(map[string]bool, len(g.migrations))
	for _, m := range g.migrations {
		versions[m.Version] = true
	}

	for _, m := range migrations {
		if versions[m.Version] {
			return fmt.Errorf("migrate: duplicate version %q in group %q", m.Version, g.name)
		}
		m.Group = g.name
		g.migrations = append(g.migrations, m)
		versions[m.Version] = true
	}
	return nil
}

// MustRegister is like Register but panics on error.
func (g *Group) MustRegister(migrations ...*Migration) {
	if err := g.Register(migrations...); err != nil {
		panic(err)
	}
}
