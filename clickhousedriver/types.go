package clickhousedriver

// LowCardinality wraps a type name with LowCardinality().
// Use this for columns with low cardinality (few distinct values), which
// ClickHouse can optimize with dictionary encoding.
//
// Example: LowCardinality("String") returns "LowCardinality(String)"
func LowCardinality(typeName string) string {
	return "LowCardinality(" + typeName + ")"
}

// ArrayType wraps a type name with Array().
// ClickHouse supports array columns natively.
//
// Example: ArrayType("String") returns "Array(String)"
func ArrayType(typeName string) string {
	return "Array(" + typeName + ")"
}

// NullableType wraps a type name with Nullable().
// By default, ClickHouse columns are NOT nullable. Use this to explicitly
// allow NULL values.
//
// Example: NullableType("String") returns "Nullable(String)"
func NullableType(typeName string) string {
	return "Nullable(" + typeName + ")"
}
