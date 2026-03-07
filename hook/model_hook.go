package hook

import (
	"context"
	"fmt"
	"reflect"
)

// ---------------------------------------------------------------------------
// Model-level hook interfaces
//
// Models implement these to receive lifecycle callbacks. Unlike operation-level
// hooks (PreQueryHook, PostQueryHook, etc.) which are registered globally on
// the Engine, model hooks are declared by the model struct itself.
//
// Model hooks run BEFORE operation-level hooks (pre-hooks) and AFTER
// operation-level hooks (post-hooks).
// ---------------------------------------------------------------------------

// BeforeInsertHook is called before an INSERT operation.
// The model can modify itself (e.g., set CreatedAt) or return an error to abort.
type BeforeInsertHook interface {
	BeforeInsert(ctx context.Context, qc *QueryContext) error
}

// AfterInsertHook is called after a successful INSERT operation.
type AfterInsertHook interface {
	AfterInsert(ctx context.Context, qc *QueryContext) error
}

// BeforeUpdateHook is called before an UPDATE operation.
type BeforeUpdateHook interface {
	BeforeUpdate(ctx context.Context, qc *QueryContext) error
}

// AfterUpdateHook is called after a successful UPDATE operation.
type AfterUpdateHook interface {
	AfterUpdate(ctx context.Context, qc *QueryContext) error
}

// BeforeDeleteHook is called before a DELETE operation.
type BeforeDeleteHook interface {
	BeforeDelete(ctx context.Context, qc *QueryContext) error
}

// AfterDeleteHook is called after a successful DELETE operation.
type AfterDeleteHook interface {
	AfterDelete(ctx context.Context, qc *QueryContext) error
}

// BeforeScanHook is called before scanning query results into the model.
type BeforeScanHook interface {
	BeforeScan(ctx context.Context, qc *QueryContext) error
}

// AfterScanHook is called after scanning query results into the model.
// Useful for computed fields, decryption, or post-load transformations.
type AfterScanHook interface {
	AfterScan(ctx context.Context, qc *QueryContext) error
}

// ---------------------------------------------------------------------------
// Model hook runners
//
// These functions check if the model implements a hook interface and call it.
// They handle slice models by iterating each element. Drivers call these
// helper functions instead of doing the type-assertions themselves.
// ---------------------------------------------------------------------------

// RunModelBeforeInsert calls BeforeInsertHook on the model if implemented.
func RunModelBeforeInsert(ctx context.Context, qc *QueryContext, model any) error {
	return runModelHook(ctx, qc, model, func(ctx context.Context, qc *QueryContext, m any) error {
		if h, ok := m.(BeforeInsertHook); ok {
			return h.BeforeInsert(ctx, qc)
		}
		return nil
	})
}

// RunModelAfterInsert calls AfterInsertHook on the model if implemented.
func RunModelAfterInsert(ctx context.Context, qc *QueryContext, model any) error {
	return runModelHook(ctx, qc, model, func(ctx context.Context, qc *QueryContext, m any) error {
		if h, ok := m.(AfterInsertHook); ok {
			return h.AfterInsert(ctx, qc)
		}
		return nil
	})
}

// RunModelBeforeUpdate calls BeforeUpdateHook on the model if implemented.
func RunModelBeforeUpdate(ctx context.Context, qc *QueryContext, model any) error {
	return runModelHook(ctx, qc, model, func(ctx context.Context, qc *QueryContext, m any) error {
		if h, ok := m.(BeforeUpdateHook); ok {
			return h.BeforeUpdate(ctx, qc)
		}
		return nil
	})
}

// RunModelAfterUpdate calls AfterUpdateHook on the model if implemented.
func RunModelAfterUpdate(ctx context.Context, qc *QueryContext, model any) error {
	return runModelHook(ctx, qc, model, func(ctx context.Context, qc *QueryContext, m any) error {
		if h, ok := m.(AfterUpdateHook); ok {
			return h.AfterUpdate(ctx, qc)
		}
		return nil
	})
}

// RunModelBeforeDelete calls BeforeDeleteHook on the model if implemented.
func RunModelBeforeDelete(ctx context.Context, qc *QueryContext, model any) error {
	return runModelHook(ctx, qc, model, func(ctx context.Context, qc *QueryContext, m any) error {
		if h, ok := m.(BeforeDeleteHook); ok {
			return h.BeforeDelete(ctx, qc)
		}
		return nil
	})
}

// RunModelAfterDelete calls AfterDeleteHook on the model if implemented.
func RunModelAfterDelete(ctx context.Context, qc *QueryContext, model any) error {
	return runModelHook(ctx, qc, model, func(ctx context.Context, qc *QueryContext, m any) error {
		if h, ok := m.(AfterDeleteHook); ok {
			return h.AfterDelete(ctx, qc)
		}
		return nil
	})
}

// RunModelBeforeScan calls BeforeScanHook on the model if implemented.
func RunModelBeforeScan(ctx context.Context, qc *QueryContext, model any) error {
	return runModelHook(ctx, qc, model, func(ctx context.Context, qc *QueryContext, m any) error {
		if h, ok := m.(BeforeScanHook); ok {
			return h.BeforeScan(ctx, qc)
		}
		return nil
	})
}

// RunModelAfterScan calls AfterScanHook on the model if implemented.
func RunModelAfterScan(ctx context.Context, qc *QueryContext, model any) error {
	return runModelHook(ctx, qc, model, func(ctx context.Context, qc *QueryContext, m any) error {
		if h, ok := m.(AfterScanHook); ok {
			return h.AfterScan(ctx, qc)
		}
		return nil
	})
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

// hookFn is the function signature for a single model hook call.
type hookFn func(ctx context.Context, qc *QueryContext, model any) error

// runModelHook handles both single structs and slices of structs.
// If the model is a slice, it iterates and calls fn on each addressable element.
// If the model is a pointer to a struct, it calls fn once.
func runModelHook(ctx context.Context, qc *QueryContext, model any, fn hookFn) error {
	if model == nil {
		return nil
	}

	val := reflect.ValueOf(model)

	// Dereference pointer(s).
	for val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return nil
		}
		val = val.Elem()
	}

	// If it's a slice, iterate each element.
	if val.Kind() == reflect.Slice {
		for i := 0; i < val.Len(); i++ {
			elem := val.Index(i)
			var iface any
			switch {
			case elem.Kind() == reflect.Ptr:
				if elem.IsNil() {
					continue
				}
				iface = elem.Interface()
			case elem.CanAddr():
				iface = elem.Addr().Interface()
			default:
				continue
			}
			if err := fn(ctx, qc, iface); err != nil {
				return fmt.Errorf("model hook (element %d): %w", i, err)
			}
		}
		return nil
	}

	// Single struct: use the original model (which should be a pointer).
	return fn(ctx, qc, model)
}
