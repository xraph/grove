package schema

import (
	"fmt"
	"reflect"
	"sync"
)

// Registry is a thread-safe cache of Table metadata.
// Tables are computed once per model type via sync.Map.
type Registry struct {
	tables sync.Map // map[reflect.Type]*Table
}

// NewRegistry creates a new empty Registry.
func NewRegistry() *Registry {
	return &Registry{}
}

// Register registers a model and returns its Table metadata.
// If already registered, returns the cached Table.
// The model can be a pointer or value (e.g., (*User)(nil) or User{}).
func (r *Registry) Register(model any) (*Table, error) {
	typ := resolveModelType(model)
	if typ == nil {
		return nil, fmt.Errorf("%w: nil model", ErrInvalidModel)
	}

	// Fast path: return cached table.
	if v, ok := r.tables.Load(typ); ok {
		return v.(*Table), nil //nolint:errcheck // sync.Map always stores *Table
	}

	// Slow path: build the table metadata.
	table, err := NewTable(model)
	if err != nil {
		return nil, err
	}

	// Store or load the existing value in case of a concurrent registration.
	actual, _ := r.tables.LoadOrStore(typ, table)
	return actual.(*Table), nil //nolint:errcheck // sync.Map always stores *Table
}

// Get returns the Table for a registered model type.
// Returns nil if the model hasn't been registered.
func (r *Registry) Get(model any) *Table {
	typ := resolveModelType(model)
	if typ == nil {
		return nil
	}

	v, ok := r.tables.Load(typ)
	if !ok {
		return nil
	}
	return v.(*Table) //nolint:errcheck // sync.Map always stores *Table
}

// MustGet is like Get but panics if the model is not registered.
func (r *Registry) MustGet(model any) *Table {
	t := r.Get(model)
	if t == nil {
		typ := reflect.TypeOf(model)
		panic(fmt.Sprintf("grove: model %v is not registered", typ))
	}
	return t
}
