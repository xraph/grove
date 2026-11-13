package crdt

import (
	"encoding/json"
	"fmt"
	"reflect"
	"slices"

	"github.com/xraph/grove/hook"
	"github.com/xraph/grove/schema"
)

var mutationSchemas = schema.NewRegistry()

func mutationSchema(qc *hook.QueryContext, data any) *schema.Table {
	typ := qc.ModelType
	if typ == nil {
		typ = reflect.TypeOf(data)
	}
	for typ != nil && (typ.Kind() == reflect.Pointer || typ.Kind() == reflect.Slice || typ.Kind() == reflect.Array) {
		typ = typ.Elem()
	}
	if typ == nil || typ.Kind() != reflect.Struct {
		return nil
	}
	table, err := mutationSchemas.Register(reflect.New(typ).Interface())
	if err != nil {
		return nil
	}
	return table
}

func extractCRDTFields(qc *hook.QueryContext) map[string]CRDTType {
	fields := make(map[string]CRDTType)
	if table := mutationSchema(qc, nil); table != nil {
		for _, f := range table.Fields {
			if !f.Options.ScanOnly && ValidCRDTType(f.Options.CRDTType) {
				fields[f.Options.Column] = CRDTType(f.Options.CRDTType)
			}
		}
	}
	if explicit, ok := qc.Values["_crdt_fields"].(map[string]string); ok {
		fields = make(map[string]CRDTType, len(explicit))
		for name, typ := range explicit {
			if ValidCRDTType(typ) {
				fields[name] = CRDTType(typ)
			}
		}
	}
	if qc.Columns != nil && qc.Operation != hook.OpDelete {
		for field := range fields {
			if !slices.Contains(qc.Columns, field) {
				delete(fields, field)
			}
		}
	}
	return fields
}

func modelValue(value reflect.Value) reflect.Value {
	for value.IsValid() && (value.Kind() == reflect.Pointer || value.Kind() == reflect.Interface) {
		if value.IsNil() {
			return reflect.Value{}
		}
		value = value.Elem()
	}
	return value
}

func fieldValue(value reflect.Value, index []int) reflect.Value {
	for _, i := range index {
		value = modelValue(value)
		if !value.IsValid() || value.Kind() != reflect.Struct {
			return reflect.Value{}
		}
		value = value.Field(i)
	}
	return value
}

func extractFieldValues(data any) map[string]any {
	if values, ok := data.(map[string]any); ok {
		return values
	}
	values := make(map[string]any)
	table := mutationSchema(&hook.QueryContext{}, data)
	if table == nil {
		return values
	}
	value := modelValue(reflect.ValueOf(data))
	for _, field := range table.Fields {
		v := fieldValue(value, field.GoIndex)
		if v.IsValid() && v.CanInterface() {
			values[field.Options.Column] = v.Interface()
		}
	}
	return values
}

func extractMutationPK(qc *hook.QueryContext, data, result any) (string, error) {
	if pk, ok := qc.Values["_crdt_pk"]; ok {
		return fmt.Sprint(pk), nil
	}
	table := mutationSchema(qc, data)
	if table == nil || len(table.PKFields) == 0 {
		return "", fmt.Errorf("crdt: mutation requires a primary key")
	}
	values := extractFieldValues(data)
	keys := make([]any, 0, len(table.PKFields))
	for _, f := range table.PKFields {
		value, ok := values[f.Options.Column]
		if !ok {
			return "", fmt.Errorf("crdt: mutation missing primary key %s", f.Options.Column)
		}
		v := modelValue(reflect.ValueOf(value))
		if f.Options.AutoIncrement && (!v.IsValid() || v.IsZero()) {
			generated, supportsID := result.(interface{ LastInsertId() (int64, error) })
			if qc.Operation != hook.OpInsert || !supportsID || len(table.PKFields) != 1 {
				return "", fmt.Errorf("crdt: mutation requires the generated primary key %s", f.Options.Column)
			}
			id, err := generated.LastInsertId()
			if err != nil {
				return "", fmt.Errorf("crdt: read generated primary key: %w", err)
			}
			v = reflect.ValueOf(id)
		}
		if !v.IsValid() {
			return "", fmt.Errorf("crdt: mutation missing primary key %s", f.Options.Column)
		}
		keys = append(keys, v.Interface())
	}
	if len(keys) == 1 {
		return fmt.Sprint(keys[0]), nil
	}
	raw, err := json.Marshal(keys)
	if err != nil {
		return "", fmt.Errorf("crdt: encode composite primary key: %w", err)
	}
	return string(raw), nil
}
