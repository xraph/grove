package middleware_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/grove/hook"
	kv "github.com/xraph/grove/kv"
	"github.com/xraph/grove/kv/kvtest"
	"github.com/xraph/grove/kv/middleware"
)

func TestCircuitBreaker_InitialState(t *testing.T) {
	cb := middleware.NewCircuitBreaker(3, 100*time.Millisecond)

	assert.Equal(t, middleware.StateClosed, cb.State())
}

func TestCircuitBreaker_OpenAfterThreshold(t *testing.T) {
	cb := middleware.NewCircuitBreaker(3, 100*time.Millisecond)

	cb.RecordFailure()
	cb.RecordFailure()
	assert.Equal(t, middleware.StateClosed, cb.State(), "should still be closed below threshold")

	cb.RecordFailure()
	assert.Equal(t, middleware.StateOpen, cb.State(), "should be open after reaching threshold")
}

func TestCircuitBreaker_OpenDenies(t *testing.T) {
	cb := middleware.NewCircuitBreaker(1, time.Minute)

	cb.RecordFailure() // opens the circuit

	ctx := context.Background()
	qc := &hook.QueryContext{
		Operation: kv.OpGet,
		RawQuery:  "key",
	}

	result, err := cb.BeforeQuery(ctx, qc)
	assert.Error(t, err)
	assert.ErrorIs(t, err, middleware.ErrCircuitOpen)
	assert.Equal(t, hook.Deny, result.Decision)
}

func TestCircuitBreaker_HalfOpenAfterTimeout(t *testing.T) {
	cb := middleware.NewCircuitBreaker(1, 50*time.Millisecond)

	cb.RecordFailure() // opens the circuit
	assert.Equal(t, middleware.StateOpen, cb.State())

	time.Sleep(100 * time.Millisecond)

	ctx := context.Background()
	qc := &hook.QueryContext{
		Operation: kv.OpGet,
		RawQuery:  "key",
	}

	result, err := cb.BeforeQuery(ctx, qc)
	require.NoError(t, err)
	assert.Equal(t, hook.Allow, result.Decision)
	assert.Equal(t, middleware.StateHalfOpen, cb.State())
}

func TestCircuitBreaker_HalfOpenToClosedOnSuccess(t *testing.T) {
	cb := middleware.NewCircuitBreaker(1, 50*time.Millisecond)

	// Open the circuit.
	cb.RecordFailure()
	assert.Equal(t, middleware.StateOpen, cb.State())

	// Wait for timeout to transition to half-open.
	time.Sleep(100 * time.Millisecond)

	ctx := context.Background()
	qc := &hook.QueryContext{
		Operation: kv.OpGet,
		RawQuery:  "key",
	}
	_, err := cb.BeforeQuery(ctx, qc)
	require.NoError(t, err)
	assert.Equal(t, middleware.StateHalfOpen, cb.State())

	// Record success while half-open to close the circuit.
	cb.RecordSuccess()
	assert.Equal(t, middleware.StateClosed, cb.State())
}

func TestCircuitBreaker_HalfOpenToOpenOnFailure(t *testing.T) {
	cb := middleware.NewCircuitBreaker(1, 50*time.Millisecond)

	// Open the circuit.
	cb.RecordFailure()
	assert.Equal(t, middleware.StateOpen, cb.State())

	// Wait for timeout to transition to half-open.
	time.Sleep(100 * time.Millisecond)

	ctx := context.Background()
	qc := &hook.QueryContext{
		Operation: kv.OpGet,
		RawQuery:  "key",
	}
	_, err := cb.BeforeQuery(ctx, qc)
	require.NoError(t, err)
	assert.Equal(t, middleware.StateHalfOpen, cb.State())

	// Record failure while half-open to re-open the circuit.
	cb.RecordFailure()
	assert.Equal(t, middleware.StateOpen, cb.State())
}

func TestCircuitBreaker_RecordSuccessResetsFailures(t *testing.T) {
	cb := middleware.NewCircuitBreaker(3, time.Minute)

	cb.RecordFailure()
	cb.RecordFailure()
	assert.Equal(t, middleware.StateClosed, cb.State(), "below threshold, still closed")

	cb.RecordSuccess()
	assert.Equal(t, middleware.StateClosed, cb.State(), "success resets failures")

	// After reset, we need full threshold to open again.
	cb.RecordFailure()
	cb.RecordFailure()
	assert.Equal(t, middleware.StateClosed, cb.State(), "2 failures after reset is still below threshold")

	cb.RecordFailure()
	assert.Equal(t, middleware.StateOpen, cb.State(), "3rd failure after reset opens the circuit")
}

func TestCircuitBreaker_IntegrationWithStore(t *testing.T) {
	cb := middleware.NewCircuitBreaker(1, time.Minute)

	store := kvtest.SetupStore(t, kv.WithHook(cb))

	// Open the circuit breaker.
	cb.RecordFailure()
	assert.Equal(t, middleware.StateOpen, cb.State())

	// Store operations should be denied.
	var val string
	err := store.Get(context.Background(), "somekey", &val)
	assert.Error(t, err, "store.Get should fail when circuit is open")
}
