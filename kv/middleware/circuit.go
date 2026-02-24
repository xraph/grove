package middleware

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/xraph/grove/hook"
)

// CircuitState represents the state of the circuit breaker.
type CircuitState int

const (
	// StateClosed means the circuit is healthy — requests flow normally.
	StateClosed CircuitState = iota
	// StateOpen means the circuit has tripped — requests are rejected immediately.
	StateOpen
	// StateHalfOpen means the circuit is testing — a limited number of requests are allowed.
	StateHalfOpen
)

// ErrCircuitOpen is returned when the circuit breaker is open.
var ErrCircuitOpen = errors.New("kv: circuit breaker is open")

// CircuitBreakerHook protects against cascading failures when the backing store is unavailable.
type CircuitBreakerHook struct {
	mu              sync.RWMutex
	state           CircuitState
	failures        int
	threshold       int
	timeout         time.Duration
	halfOpenMax     int
	halfOpenCount   int
	lastStateChange time.Time
}

var _ hook.PreQueryHook = (*CircuitBreakerHook)(nil)

// NewCircuitBreaker creates a new circuit breaker middleware.
//
// threshold is the number of consecutive failures before opening the circuit.
// timeout is the duration the circuit stays open before transitioning to half-open.
func NewCircuitBreaker(threshold int, timeout time.Duration) *CircuitBreakerHook {
	return &CircuitBreakerHook{
		state:       StateClosed,
		threshold:   threshold,
		timeout:     timeout,
		halfOpenMax: 1,
	}
}

func (h *CircuitBreakerHook) BeforeQuery(_ context.Context, _ *hook.QueryContext) (*hook.HookResult, error) {
	h.mu.RLock()
	state := h.state
	lastChange := h.lastStateChange
	h.mu.RUnlock()

	switch state {
	case StateClosed:
		return &hook.HookResult{Decision: hook.Allow}, nil
	case StateOpen:
		// Check if timeout has elapsed.
		if time.Since(lastChange) >= h.timeout {
			h.mu.Lock()
			if h.state == StateOpen {
				h.state = StateHalfOpen
				h.halfOpenCount = 0
				h.lastStateChange = time.Now()
			}
			h.mu.Unlock()
			return &hook.HookResult{Decision: hook.Allow}, nil
		}
		return &hook.HookResult{Decision: hook.Deny, Error: ErrCircuitOpen}, ErrCircuitOpen
	case StateHalfOpen:
		h.mu.Lock()
		if h.halfOpenCount >= h.halfOpenMax {
			h.mu.Unlock()
			return &hook.HookResult{Decision: hook.Deny, Error: ErrCircuitOpen}, ErrCircuitOpen
		}
		h.halfOpenCount++
		h.mu.Unlock()
		return &hook.HookResult{Decision: hook.Allow}, nil
	}

	return &hook.HookResult{Decision: hook.Allow}, nil
}

// RecordSuccess records a successful operation, potentially closing the circuit.
func (h *CircuitBreakerHook) RecordSuccess() {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.failures = 0
	if h.state == StateHalfOpen {
		h.state = StateClosed
		h.lastStateChange = time.Now()
	}
}

// RecordFailure records a failed operation, potentially opening the circuit.
func (h *CircuitBreakerHook) RecordFailure() {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.failures++
	if h.failures >= h.threshold {
		h.state = StateOpen
		h.lastStateChange = time.Now()
	}
}

// State returns the current circuit breaker state.
func (h *CircuitBreakerHook) State() CircuitState {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.state
}
