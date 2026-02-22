// Package scan maps database result sets to Go structs using cached field
// metadata from the schema package. It is driver-agnostic: callers provide
// implementations of the Row and Rows interfaces (compatible with the driver
// package), and the scanner handles column-to-field resolution, pointer
// construction, and slice population.
package scan

import "github.com/xraph/grove/schema"

// ColumnMap maps database column names to schema fields for efficient scanning.
// It is built once from a Table's field list and reused across queries.
type ColumnMap struct {
	fields   []*schema.Field
	fieldMap map[string]*schema.Field // column name -> field
}

// NewColumnMap creates a ColumnMap from a Table's fields.
// The map is keyed by the database column name (Field.Options.Column).
func NewColumnMap(table *schema.Table) *ColumnMap {
	cm := &ColumnMap{
		fields:   table.Fields,
		fieldMap: make(map[string]*schema.Field, len(table.Fields)),
	}
	for _, f := range table.Fields {
		cm.fieldMap[f.Options.Column] = f
	}
	return cm
}

// Resolve returns the fields that correspond to the given column names,
// in the order the columns appear. If a column has no matching field in
// the map, the corresponding entry in the returned slice is nil.
func (cm *ColumnMap) Resolve(columns []string) []*schema.Field {
	resolved := make([]*schema.Field, len(columns))
	for i, col := range columns {
		resolved[i] = cm.fieldMap[col] // nil if not found
	}
	return resolved
}
