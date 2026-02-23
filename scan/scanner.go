package scan

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/xraph/grove/schema"
)

var scanPtrPool = sync.Pool{
	New: func() any {
		s := make([]any, 0, 16)
		return &s
	},
}

func getScanPtrs(n int) *[]any {
	sp := scanPtrPool.Get().(*[]any)
	s := *sp
	if cap(s) < n {
		*sp = make([]any, n)
	} else {
		*sp = s[:n]
	}
	return sp
}

func putScanPtrs(sp *[]any) {
	s := *sp
	for i := range s {
		s[i] = nil
	}
	scanPtrPool.Put(sp)
}

// Row interface compatible with driver.Row.
// It represents a single result row from a query.
type Row interface {
	Scan(dest ...any) error
}

// Rows interface compatible with driver.Rows.
// It represents a multi-row result set from a query.
type Rows interface {
	Next() bool
	Scan(dest ...any) error
	Columns() ([]string, error)
	Close() error
	Err() error
}

// ScanRow scans a single row into a struct using the provided table metadata.
// dest must be a pointer to a struct. The function builds a slice of field
// pointers from the struct based on the Table's Fields ordering and calls
// row.Scan with those pointers.
func ScanRow(row Row, dest any, table *schema.Table) error {
	v := reflect.ValueOf(dest)
	if v.Kind() != reflect.Ptr || v.IsNil() {
		return fmt.Errorf("scan: dest must be a non-nil pointer to a struct, got %T", dest)
	}
	v = v.Elem()
	if v.Kind() != reflect.Struct {
		return fmt.Errorf("scan: dest must be a pointer to a struct, got pointer to %s", v.Kind())
	}

	// Build scan targets from the table's field list.
	sp := getScanPtrs(len(table.Fields))
	ptrs := *sp
	for i, field := range table.Fields {
		ptrs[i] = FieldPtr(v, field)
	}
	err := row.Scan(ptrs...)
	putScanPtrs(sp)
	return err
}

// ScanRows scans all rows from a result set into a slice of structs using the
// provided table metadata. dest must be a pointer to a slice of structs
// (e.g., *[]User). The function uses column names from the result set to
// resolve which table fields correspond to which scan positions, allowing
// queries that return a subset of columns to work correctly.
//
// After initial column resolution the hot loop performs zero allocations
// beyond the new struct values appended to the slice.
func ScanRows(rows Rows, dest any, table *schema.Table) error {
	// Validate dest type.
	destVal := reflect.ValueOf(dest)
	if destVal.Kind() != reflect.Ptr || destVal.IsNil() {
		return fmt.Errorf("scan: dest must be a non-nil pointer to a slice, got %T", dest)
	}
	sliceVal := destVal.Elem()
	if sliceVal.Kind() != reflect.Slice {
		return fmt.Errorf("scan: dest must be a pointer to a slice, got pointer to %s", sliceVal.Kind())
	}

	elemType := sliceVal.Type().Elem()
	if elemType.Kind() == reflect.Ptr {
		elemType = elemType.Elem()
	}
	if elemType.Kind() != reflect.Struct {
		return fmt.Errorf("scan: slice element must be a struct or pointer to struct, got %s", elemType.Kind())
	}

	isPtr := sliceVal.Type().Elem().Kind() == reflect.Ptr

	// Get column names from the result set.
	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("scan: failed to get columns: %w", err)
	}

	// Build column map and resolve fields for the returned columns.
	cm := NewColumnMap(table)
	fields := cm.Resolve(columns)

	// Pre-allocate the scan target slice (reused each iteration).
	ptrs := make([]any, len(columns))

	// discard is a reusable target for columns that have no matching field.
	var discard any

	for rows.Next() {
		// Create a new struct instance.
		elemPtr := reflect.New(elemType)
		elem := elemPtr.Elem()

		// Build scan targets for this row.
		for i, field := range fields {
			if field != nil {
				ptrs[i] = FieldPtr(elem, field)
			} else {
				// Unknown column: scan into discard.
				ptrs[i] = &discard
			}
		}

		if err := rows.Scan(ptrs...); err != nil {
			return fmt.Errorf("scan: row scan failed: %w", err)
		}

		// Append to the slice.
		if isPtr {
			sliceVal.Set(reflect.Append(sliceVal, elemPtr))
		} else {
			sliceVal.Set(reflect.Append(sliceVal, elem))
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("scan: rows iteration error: %w", err)
	}

	return nil
}
