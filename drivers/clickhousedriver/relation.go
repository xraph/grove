package clickhousedriver

import (
	"context"
	"fmt"
	"reflect"

	"github.com/xraph/grove/internal/pool"
	"github.com/xraph/grove/scan"
	"github.com/xraph/grove/schema"
)

// loadRelations executes separate queries for each requested relation and
// assigns the results to the appropriate struct fields.
//
// target must be a pointer to a struct or a pointer to a slice of structs.
func (q *SelectQuery) loadRelations(ctx context.Context, target any) error {
	if len(q.relations) == 0 || q.table == nil {
		return nil
	}

	// Build a map of relation name -> *Relation for quick lookup.
	relMap := make(map[string]*schema.Relation, len(q.table.Relations))
	for _, rel := range q.table.Relations {
		relMap[rel.Field.GoName] = rel
	}

	for _, relName := range q.relations {
		rel, ok := relMap[relName]
		if !ok {
			return fmt.Errorf("clickhousedriver: unknown relation %q on table %q", relName, q.table.Name)
		}

		switch rel.Type {
		case schema.HasOne:
			if err := q.loadHasOne(ctx, target, rel); err != nil {
				return fmt.Errorf("clickhousedriver: load has-one %q: %w", relName, err)
			}
		case schema.HasMany:
			if err := q.loadHasMany(ctx, target, rel); err != nil {
				return fmt.Errorf("clickhousedriver: load has-many %q: %w", relName, err)
			}
		case schema.BelongsTo:
			if err := q.loadBelongsTo(ctx, target, rel); err != nil {
				return fmt.Errorf("clickhousedriver: load belongs-to %q: %w", relName, err)
			}
		default:
			return fmt.Errorf("clickhousedriver: relation type %s not yet supported for eager loading", rel.Type)
		}
	}

	return nil
}

// loadHasOne loads a has-one relation.
func (q *SelectQuery) loadHasOne(ctx context.Context, target any, rel *schema.Relation) error {
	targetVal := reflect.ValueOf(target)
	for targetVal.Kind() == reflect.Ptr {
		targetVal = targetVal.Elem()
	}

	if targetVal.Kind() == reflect.Struct {
		baseVal := getFieldByColumn(targetVal, q.table, rel.BaseColumn)
		if !baseVal.IsValid() {
			return nil
		}

		relFieldVal := targetVal.FieldByIndex(rel.Field.GoIndex)
		relType := rel.Field.GoType
		for relType.Kind() == reflect.Ptr {
			relType = relType.Elem()
		}

		relModel := reflect.New(relType).Interface()
		relTable, err := resolveTable(q.db.registry, relModel)
		if err != nil {
			return err
		}

		sqlStr, args := buildRelationQuery(q.db.dialect, relTable, rel.JoinColumn, baseVal.Interface())

		row := q.db.QueryRow(ctx, sqlStr, args...)
		if err := scan.ScanRow(row, relModel, relTable); err != nil {
			return nil // No related row found is not an error for HasOne.
		}

		if relFieldVal.Kind() == reflect.Ptr {
			relFieldVal.Set(reflect.ValueOf(relModel))
		} else {
			relFieldVal.Set(reflect.ValueOf(relModel).Elem())
		}
	}

	return nil
}

// loadHasMany loads a has-many relation.
func (q *SelectQuery) loadHasMany(ctx context.Context, target any, rel *schema.Relation) error {
	targetVal := reflect.ValueOf(target)
	for targetVal.Kind() == reflect.Ptr {
		targetVal = targetVal.Elem()
	}

	if targetVal.Kind() == reflect.Struct {
		baseVal := getFieldByColumn(targetVal, q.table, rel.BaseColumn)
		if !baseVal.IsValid() {
			return nil
		}

		relFieldVal := targetVal.FieldByIndex(rel.Field.GoIndex)

		sliceType := rel.Field.GoType
		if sliceType.Kind() == reflect.Ptr {
			sliceType = sliceType.Elem()
		}
		elemType := sliceType.Elem()
		for elemType.Kind() == reflect.Ptr {
			elemType = elemType.Elem()
		}

		relModel := reflect.New(elemType).Interface()
		relTable, err := resolveTable(q.db.registry, relModel)
		if err != nil {
			return err
		}

		sqlStr, args := buildRelationQuery(q.db.dialect, relTable, rel.JoinColumn, baseVal.Interface())

		rows, err := q.db.Query(ctx, sqlStr, args...)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()

		resultSlice := reflect.New(sliceType).Interface()
		if err := scan.ScanRows(rows, resultSlice, relTable); err != nil {
			return err
		}

		relFieldVal.Set(reflect.ValueOf(resultSlice).Elem())
	}

	if targetVal.Kind() == reflect.Slice {
		return q.loadHasManyForSlice(ctx, targetVal, rel)
	}

	return nil
}

// loadHasManyForSlice loads has-many relations for each element in a slice result.
func (q *SelectQuery) loadHasManyForSlice(ctx context.Context, sliceVal reflect.Value, rel *schema.Relation) error {
	if sliceVal.Len() == 0 {
		return nil
	}

	var baseVals []any
	for i := 0; i < sliceVal.Len(); i++ {
		elem := sliceVal.Index(i)
		for elem.Kind() == reflect.Ptr {
			elem = elem.Elem()
		}
		bv := getFieldByColumn(elem, q.table, rel.BaseColumn)
		if bv.IsValid() {
			baseVals = append(baseVals, bv.Interface())
		}
	}

	if len(baseVals) == 0 {
		return nil
	}

	elemType := rel.Field.GoType
	if elemType.Kind() == reflect.Ptr {
		elemType = elemType.Elem()
	}
	innerType := elemType.Elem()
	for innerType.Kind() == reflect.Ptr {
		innerType = innerType.Elem()
	}

	relModel := reflect.New(innerType).Interface()
	relTable, err := resolveTable(q.db.registry, relModel)
	if err != nil {
		return err
	}

	sqlStr, args := buildRelationInQuery(q.db.dialect, relTable, rel.JoinColumn, baseVals)

	rows, err := q.db.Query(ctx, sqlStr, args...)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	allRelated := reflect.New(elemType).Interface()
	if err := scan.ScanRows(rows, allRelated, relTable); err != nil {
		return err
	}

	allSlice := reflect.ValueOf(allRelated).Elem()
	grouped := groupByColumn(allSlice, relTable, rel.JoinColumn)

	for i := 0; i < sliceVal.Len(); i++ {
		elem := sliceVal.Index(i)
		for elem.Kind() == reflect.Ptr {
			elem = elem.Elem()
		}
		bv := getFieldByColumn(elem, q.table, rel.BaseColumn)
		if !bv.IsValid() {
			continue
		}

		key := fmt.Sprintf("%v", bv.Interface())
		if related, ok := grouped[key]; ok {
			relField := elem.FieldByIndex(rel.Field.GoIndex)
			relField.Set(related)
		}
	}

	return nil
}

// loadBelongsTo loads a belongs-to relation.
func (q *SelectQuery) loadBelongsTo(ctx context.Context, target any, rel *schema.Relation) error {
	targetVal := reflect.ValueOf(target)
	for targetVal.Kind() == reflect.Ptr {
		targetVal = targetVal.Elem()
	}

	if targetVal.Kind() != reflect.Struct {
		return nil
	}

	fkVal := getFieldByColumn(targetVal, q.table, rel.BaseColumn)
	if !fkVal.IsValid() {
		return nil
	}

	relFieldVal := targetVal.FieldByIndex(rel.Field.GoIndex)
	relType := rel.Field.GoType
	for relType.Kind() == reflect.Ptr {
		relType = relType.Elem()
	}

	relModel := reflect.New(relType).Interface()
	relTable, err := resolveTable(q.db.registry, relModel)
	if err != nil {
		return err
	}

	sqlStr, args := buildRelationQuery(q.db.dialect, relTable, rel.JoinColumn, fkVal.Interface())

	row := q.db.QueryRow(ctx, sqlStr, args...)
	if err := scan.ScanRow(row, relModel, relTable); err != nil {
		return nil // Not found is not an error for BelongsTo.
	}

	if relFieldVal.Kind() == reflect.Ptr {
		relFieldVal.Set(reflect.ValueOf(relModel))
	} else {
		relFieldVal.Set(reflect.ValueOf(relModel).Elem())
	}

	return nil
}

// buildRelationQuery builds a SELECT * FROM table WHERE col = ? query.
func buildRelationQuery(dialect *ClickHouseDialect, table *schema.Table, col string, val any) (string, []any) {
	buf := pool.GetBuffer()
	defer pool.PutBuffer(buf)

	buf.WriteString("SELECT ")
	for i, f := range table.Fields {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(dialect.Quote(f.Options.Column))
	}
	buf.WriteString(" FROM ")
	buf.WriteString(dialect.Quote(table.Name))
	buf.WriteString(" WHERE ")
	buf.WriteString(dialect.Quote(col))
	buf.WriteString(" = ?")

	return buf.String(), []any{val}
}

// buildRelationInQuery builds a SELECT * FROM table WHERE col IN (?, ?, ...) query.
func buildRelationInQuery(dialect *ClickHouseDialect, table *schema.Table, col string, vals []any) (string, []any) {
	buf := pool.GetBuffer()
	defer pool.PutBuffer(buf)

	buf.WriteString("SELECT ")
	for i, f := range table.Fields {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(dialect.Quote(f.Options.Column))
	}
	buf.WriteString(" FROM ")
	buf.WriteString(dialect.Quote(table.Name))
	buf.WriteString(" WHERE ")
	buf.WriteString(dialect.Quote(col))
	buf.WriteString(" IN (")
	for i := range vals {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString("?")
	}
	_ = buf.WriteByte(')')

	return buf.String(), vals
}

// getFieldByColumn returns the reflect.Value of the struct field matching the
// given column name.
func getFieldByColumn(structVal reflect.Value, table *schema.Table, column string) reflect.Value {
	for _, f := range table.Fields {
		if f.Options.Column == column {
			return structVal.FieldByIndex(f.GoIndex)
		}
	}
	return reflect.Value{}
}

// groupByColumn groups slice elements by the value of the specified column.
func groupByColumn(sliceVal reflect.Value, table *schema.Table, column string) map[string]reflect.Value {
	result := make(map[string]reflect.Value)

	for i := 0; i < sliceVal.Len(); i++ {
		elem := sliceVal.Index(i)
		for elem.Kind() == reflect.Ptr {
			elem = elem.Elem()
		}
		fv := getFieldByColumn(elem, table, column)
		if !fv.IsValid() {
			continue
		}
		key := fmt.Sprintf("%v", fv.Interface())
		if existing, ok := result[key]; ok {
			result[key] = reflect.Append(existing, sliceVal.Index(i))
		} else {
			newSlice := reflect.MakeSlice(sliceVal.Type(), 0, 1)
			result[key] = reflect.Append(newSlice, sliceVal.Index(i))
		}
	}

	return result
}
