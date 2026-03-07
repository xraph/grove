package hook

import (
	"context"
	"errors"
	"testing"
)

// testModel implements BeforeInsertHook and AfterInsertHook for testing.
type testModel struct {
	Name          string
	insertCalled  bool
	afterCalled   bool
	updateCalled  bool
	deleteCalled  bool
	scanCalled    bool
	afterScanCall bool
}

func (m *testModel) BeforeInsert(_ context.Context, _ *QueryContext) error {
	m.insertCalled = true
	return nil
}

func (m *testModel) AfterInsert(_ context.Context, _ *QueryContext) error {
	m.afterCalled = true
	return nil
}

func (m *testModel) BeforeUpdate(_ context.Context, _ *QueryContext) error {
	m.updateCalled = true
	return nil
}

func (m *testModel) BeforeDelete(_ context.Context, _ *QueryContext) error {
	m.deleteCalled = true
	return nil
}

func (m *testModel) BeforeScan(_ context.Context, _ *QueryContext) error {
	m.scanCalled = true
	return nil
}

func (m *testModel) AfterScan(_ context.Context, _ *QueryContext) error {
	m.afterScanCall = true
	return nil
}

// errorModel returns an error from BeforeInsert.
type errorModel struct{}

func (m *errorModel) BeforeInsert(_ context.Context, _ *QueryContext) error {
	return errors.New("validation failed")
}

// plainModel does not implement any hook interfaces.
type plainModel struct {
	Name string
}

func TestRunModelBeforeInsert_SingleStruct(t *testing.T) {
	m := &testModel{Name: "alice"}
	qc := &QueryContext{Operation: OpInsert, Table: "users"}

	if err := RunModelBeforeInsert(context.Background(), qc, m); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !m.insertCalled {
		t.Fatal("BeforeInsert was not called")
	}
}

func TestRunModelAfterInsert_SingleStruct(t *testing.T) {
	m := &testModel{Name: "bob"}
	qc := &QueryContext{Operation: OpInsert, Table: "users"}

	if err := RunModelAfterInsert(context.Background(), qc, m); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !m.afterCalled {
		t.Fatal("AfterInsert was not called")
	}
}

func TestRunModelBeforeUpdate_SingleStruct(t *testing.T) {
	m := &testModel{Name: "carol"}
	qc := &QueryContext{Operation: OpUpdate, Table: "users"}

	if err := RunModelBeforeUpdate(context.Background(), qc, m); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !m.updateCalled {
		t.Fatal("BeforeUpdate was not called")
	}
}

func TestRunModelBeforeDelete_SingleStruct(t *testing.T) {
	m := &testModel{Name: "dave"}
	qc := &QueryContext{Operation: OpDelete, Table: "users"}

	if err := RunModelBeforeDelete(context.Background(), qc, m); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !m.deleteCalled {
		t.Fatal("BeforeDelete was not called")
	}
}

func TestRunModelBeforeScan_SingleStruct(t *testing.T) {
	m := &testModel{Name: "eve"}
	qc := &QueryContext{Operation: OpSelect, Table: "users"}

	if err := RunModelBeforeScan(context.Background(), qc, m); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !m.scanCalled {
		t.Fatal("BeforeScan was not called")
	}
}

func TestRunModelAfterScan_SingleStruct(t *testing.T) {
	m := &testModel{Name: "frank"}
	qc := &QueryContext{Operation: OpSelect, Table: "users"}

	if err := RunModelAfterScan(context.Background(), qc, m); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !m.afterScanCall {
		t.Fatal("AfterScan was not called")
	}
}

func TestRunModelBeforeInsert_Slice(t *testing.T) {
	models := []testModel{
		{Name: "alice"},
		{Name: "bob"},
		{Name: "carol"},
	}
	qc := &QueryContext{Operation: OpBulkInsert, Table: "users"}

	if err := RunModelBeforeInsert(context.Background(), qc, &models); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for i, m := range models {
		if !m.insertCalled {
			t.Fatalf("element %d: BeforeInsert was not called", i)
		}
	}
}

func TestRunModelBeforeInsert_PointerSlice(t *testing.T) {
	models := []*testModel{
		{Name: "alice"},
		{Name: "bob"},
		nil, // nil pointers should be skipped
	}
	qc := &QueryContext{Operation: OpBulkInsert, Table: "users"}

	if err := RunModelBeforeInsert(context.Background(), qc, &models); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !models[0].insertCalled {
		t.Fatal("element 0: BeforeInsert was not called")
	}
	if !models[1].insertCalled {
		t.Fatal("element 1: BeforeInsert was not called")
	}
}

func TestRunModelBeforeInsert_ErrorPropagation(t *testing.T) {
	m := &errorModel{}
	qc := &QueryContext{Operation: OpInsert, Table: "users"}

	err := RunModelBeforeInsert(context.Background(), qc, m)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, errors.Unwrap(err)) && err.Error() != "validation failed" {
		// The error may be wrapped
		t.Logf("got error: %v", err)
	}
}

func TestRunModelBeforeInsert_SliceErrorPropagation(t *testing.T) {
	models := []errorModel{{}, {}}
	qc := &QueryContext{Operation: OpBulkInsert, Table: "users"}

	err := RunModelBeforeInsert(context.Background(), qc, &models)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestRunModelBeforeInsert_NonImplementing(t *testing.T) {
	m := &plainModel{Name: "test"}
	qc := &QueryContext{Operation: OpInsert, Table: "users"}

	if err := RunModelBeforeInsert(context.Background(), qc, m); err != nil {
		t.Fatalf("unexpected error for non-implementing model: %v", err)
	}
}

func TestRunModelBeforeInsert_Nil(t *testing.T) {
	qc := &QueryContext{Operation: OpInsert, Table: "users"}

	if err := RunModelBeforeInsert(context.Background(), qc, nil); err != nil {
		t.Fatalf("unexpected error for nil model: %v", err)
	}
}

func TestRunModelBeforeInsert_NilPointer(t *testing.T) {
	var m *testModel
	qc := &QueryContext{Operation: OpInsert, Table: "users"}

	if err := RunModelBeforeInsert(context.Background(), qc, m); err != nil {
		t.Fatalf("unexpected error for nil pointer model: %v", err)
	}
}
