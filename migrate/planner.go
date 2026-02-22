package migrate

import "fmt"

// planMigrations resolves group dependencies via topological sort and returns
// all migrations in the correct execution order.
//
// Within each group, migrations are ordered by Version (ascending).
// Between groups, order follows the dependency graph: if group B depends on
// group A, all of A's migrations run before any of B's.
func planMigrations(groups []*Group) ([]*Migration, error) {
	sorted, err := topoSort(groups)
	if err != nil {
		return nil, err
	}

	var plan []*Migration
	for _, g := range sorted {
		plan = append(plan, g.Migrations()...)
	}
	return plan, nil
}

// topoSort performs a topological sort of groups based on their DependsOn
// declarations. Returns an error if a cycle is detected.
func topoSort(groups []*Group) ([]*Group, error) {
	byName := make(map[string]*Group, len(groups))
	for _, g := range groups {
		byName[g.Name()] = g
	}

	const (
		unvisited = 0
		visiting  = 1
		visited   = 2
	)

	state := make(map[string]int, len(groups))
	var sorted []*Group

	var visit func(g *Group) error
	visit = func(g *Group) error {
		switch state[g.Name()] {
		case visited:
			return nil
		case visiting:
			return fmt.Errorf("migrate: cyclic dependency detected involving group %q", g.Name())
		}

		state[g.Name()] = visiting

		for _, depName := range g.DependsOnGroups() {
			dep, ok := byName[depName]
			if !ok {
				return fmt.Errorf("migrate: group %q depends on unknown group %q", g.Name(), depName)
			}
			if err := visit(dep); err != nil {
				return err
			}
		}

		state[g.Name()] = visited
		sorted = append(sorted, g)
		return nil
	}

	for _, g := range groups {
		if err := visit(g); err != nil {
			return nil, err
		}
	}

	return sorted, nil
}
