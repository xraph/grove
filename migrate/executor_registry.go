package migrate

import (
	"fmt"
	"sync"
)

// ExecutorFactory is a function that creates a migration Executor from a driver.
// The driver parameter is typed as any to support both SQL-based drivers
// (driver.Driver) and non-SQL drivers (e.g., *mongodriver.MongoDB).
// Each factory performs its own type assertion on the driver.
type ExecutorFactory func(drv any) Executor

var (
	executorsMu       sync.RWMutex
	executorFactories = make(map[string]ExecutorFactory)
)

// RegisterExecutor registers a migration executor factory for a given driver name.
// It is typically called from a driver's migrate package init() function for
// auto-registration, or explicitly by user code during setup.
//
// Subsequent calls with the same name overwrite the previous registration.
//
// Auto-registration example (in pgmigrate package):
//
//	func init() {
//	    migrate.RegisterExecutor("pg", func(drv any) migrate.Executor {
//	        return New(drv.(driver.Driver))
//	    })
//	}
//
// Explicit registration example:
//
//	migrate.RegisterExecutor("pg", func(drv any) migrate.Executor {
//	    return pgmigrate.New(drv.(driver.Driver))
//	})
func RegisterExecutor(driverName string, factory ExecutorFactory) {
	executorsMu.Lock()
	defer executorsMu.Unlock()
	executorFactories[driverName] = factory
}

// driverNamer is the minimal interface needed to look up the executor factory.
type driverNamer interface {
	Name() string
}

// NewExecutorFor creates a migration Executor for the given driver using the
// registered factory. The driver must implement a Name() string method
// (satisfied by both grove.GroveDriver and driver.Driver).
// Returns an error if no factory is registered for the driver's name.
func NewExecutorFor(drv any) (Executor, error) {
	namer, ok := drv.(driverNamer)
	if !ok {
		return nil, fmt.Errorf("migrate: driver does not implement Name() string")
	}

	name := namer.Name()

	executorsMu.RLock()
	factory, ok := executorFactories[name]
	executorsMu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("migrate: no executor registered for driver %q; "+
			"import the driver's migrate package or call migrate.RegisterExecutor", name)
	}

	return factory(drv), nil
}

// Executors returns the names of all registered executor factories.
func Executors() []string {
	executorsMu.RLock()
	defer executorsMu.RUnlock()

	names := make([]string, 0, len(executorFactories))
	for name := range executorFactories {
		names = append(names, name)
	}
	return names
}
