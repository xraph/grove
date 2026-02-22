package sqlitedriver

import (
	"context"
	"fmt"
	"reflect"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/scan"
)

// RawQuery executes arbitrary SQL with optional model scanning.
type RawQuery struct {
	db    *SqliteDB
	query string
	args  []any
	err   error
}

// NewRaw creates a raw SQL query.
func (db *SqliteDB) NewRaw(query string, args ...any) *RawQuery {
	return &RawQuery{
		db:    db,
		query: query,
		args:  args,
	}
}

// Exec executes the raw query without returning rows.
func (q *RawQuery) Exec(ctx context.Context) (driver.Result, error) {
	if q.err != nil {
		return nil, q.err
	}
	return q.db.Exec(ctx, q.query, q.args...)
}

// Scan executes the raw query and scans results into dest.
// dest can be:
//   - *[]Model (slice pointer for multi-row)
//   - *Model (struct pointer for single row)
//   - scalar pointers (passed directly to row.Scan)
func (q *RawQuery) Scan(ctx context.Context, dest ...any) error {
	if q.err != nil {
		return q.err
	}

	if len(dest) == 0 {
		return fmt.Errorf("sqlitedriver: Scan requires at least one destination")
	}

	// If we have a single dest that is a struct or slice of structs,
	// use the schema-aware scanner.
	if len(dest) == 1 {
		target := dest[0]
		targetType := reflect.TypeOf(target)

		if targetType.Kind() == reflect.Ptr {
			innerType := targetType.Elem()

			// Slice pointer: multi-row scan.
			if innerType.Kind() == reflect.Slice {
				elemType := innerType.Elem()
				for elemType.Kind() == reflect.Ptr {
					elemType = elemType.Elem()
				}
				if elemType.Kind() == reflect.Struct {
					table, err := resolveTable(target)
					if err == nil {
						rows, qerr := q.db.Query(ctx, q.query, q.args...)
						if qerr != nil {
							return qerr
						}
						defer func() { _ = rows.Close() }()
						return scan.ScanRows(rows, target, table)
					}
				}
			}

			// Struct pointer: single-row scan.
			if innerType.Kind() == reflect.Struct {
				table, err := resolveTable(target)
				if err == nil {
					row := q.db.QueryRow(ctx, q.query, q.args...)
					return scan.ScanRow(row, target, table)
				}
			}
		}
	}

	// Fallback: pass scalars directly to row.Scan.
	row := q.db.QueryRow(ctx, q.query, q.args...)
	return row.Scan(dest...)
}
