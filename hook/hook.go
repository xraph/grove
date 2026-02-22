// Package hook provides the privacy and lifecycle hook system for Grove.
//
// Hooks run before and after database operations, enabling:
//   - Multi-tenant isolation (inject WHERE tenant_id = ?)
//   - PII redaction (redact fields tagged with grove:",privacy:pii")
//   - Audit logging (log mutations to Chronicle)
//   - Access control (deny unauthorized operations)
//
// The hook system does not implement authorization logic — it provides
// the integration point for any permissions library.
package hook

import (
	"context"
	"reflect"
)

// Operation represents the type of database operation.
type Operation int

const (
	OpSelect Operation = iota
	OpInsert
	OpUpdate
	OpDelete
	OpBulkInsert
	OpBulkUpdate
	OpBulkDelete
	OpAggregate // For NoSQL aggregation pipelines
)

// String returns a human-readable name for the operation.
func (op Operation) String() string {
	switch op {
	case OpSelect:
		return "SELECT"
	case OpInsert:
		return "INSERT"
	case OpUpdate:
		return "UPDATE"
	case OpDelete:
		return "DELETE"
	case OpBulkInsert:
		return "BULK_INSERT"
	case OpBulkUpdate:
		return "BULK_UPDATE"
	case OpBulkDelete:
		return "BULK_DELETE"
	case OpAggregate:
		return "AGGREGATE"
	default:
		return "UNKNOWN"
	}
}

// Decision represents what a hook wants to do.
type Decision int

const (
	// Allow proceeds with the query.
	Allow Decision = iota
	// Deny blocks the query and returns an error.
	Deny
	// Modify indicates the hook modified the query context; re-evaluate.
	Modify
	// Skip excludes this row/document in results (post-query).
	Skip
)

// TagSource indicates which tag system was used for a model field.
// This mirrors schema.TagSource to avoid an import cycle.
type TagSource int

const (
	// TagSourceGrove means grove:"..." tag was present and used.
	TagSourceGrove TagSource = iota
	// TagSourceBun means bun:"..." fallback was used.
	TagSourceBun
	// TagSourceNone means no tag — field name used as column (snake_case).
	TagSourceNone
)

// QueryContext carries metadata about the pending operation.
type QueryContext struct {
	// Operation type.
	Operation Operation

	// Table or collection name.
	Table string

	// Model type (reflect.Type of the struct).
	ModelType reflect.Type

	// Columns being accessed (SELECT) or mutated (INSERT/UPDATE).
	Columns []string

	// PrivacyColumns maps columns to their grove:",privacy:X" classification.
	// Only populated for columns that have a privacy tag.
	PrivacyColumns map[string]string

	// Conditions extracted from WHERE/filter (informational).
	Conditions []Condition

	// RawQuery is the built query string (available after building, before execution).
	RawQuery string

	// RawArgs are the query arguments.
	RawArgs []any

	// TenantID from context, if set.
	TenantID string

	// TagSource indicates whether the model uses grove:"..." or bun:"..." tags.
	TagSource TagSource

	// Values holds user-supplied context values.
	Values map[string]any
}

// Condition represents a parsed condition from a query (informational).
type Condition struct {
	Column   string
	Operator string
	Value    any
}

// HookResult is returned by pre-query/pre-mutation hooks.
type HookResult struct {
	Decision Decision
	Error    error         // Set when Decision == Deny.
	Filters  []ExtraFilter // Additional conditions to inject (pre-query).
}

// ExtraFilter is a condition that a hook wants to inject into the query.
type ExtraFilter struct {
	// Clause is a raw WHERE fragment with placeholders.
	// e.g., "tenant_id = $1"
	Clause string
	Args   []any

	// NativeFilter is a driver-specific filter document (e.g., bson.M for MongoDB).
	NativeFilter any
}

// PreQueryHook runs before the query is executed.
type PreQueryHook interface {
	BeforeQuery(ctx context.Context, qc *QueryContext) (*HookResult, error)
}

// PostQueryHook runs after query execution with the results.
type PostQueryHook interface {
	AfterQuery(ctx context.Context, qc *QueryContext, result any) error
}

// PreMutationHook runs before mutations (INSERT/UPDATE/DELETE).
type PreMutationHook interface {
	BeforeMutation(ctx context.Context, qc *QueryContext, data any) (*HookResult, error)
}

// PostMutationHook runs after mutations.
type PostMutationHook interface {
	AfterMutation(ctx context.Context, qc *QueryContext, data any, result any) error
}

// StreamRowHook runs on every row yielded by a stream.
// This is critical for long-lived streams where permissions can change.
// Pre-query hooks run once when the stream is opened (filter injection, deny).
// StreamRowHook runs per-row as each row is decoded from the cursor.
type StreamRowHook interface {
	OnStreamRow(ctx context.Context, qc *QueryContext, row any) (Decision, error)
}
