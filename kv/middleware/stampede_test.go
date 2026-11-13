package middleware_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/xraph/grove/kv"
	"github.com/xraph/grove/kv/kvtest"
	"github.com/xraph/grove/kv/middleware"
)

type waitingContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (c *waitingContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

type readResult struct {
	raw []byte
	err error
}

func TestStampedeSharesReadsAndErrors(t *testing.T) {
	for _, readErr := range []error{nil, kv.ErrNotFound, errors.New("backend failed")} {
		t.Run(fmtErrorName(readErr), func(t *testing.T) {
			started, release := make(chan struct{}), make(chan struct{})
			d := &countedReads{MockDriver: kvtest.NewMockDriver()}
			d.get = func(context.Context, string) ([]byte, error) {
				if d.reads.Load() == 1 {
					close(started)
					<-release
				}
				return []byte(`"value"`), readErr
			}
			s, err := kv.Open(d, kv.WithHook(middleware.NewStampede()))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, s.Close()) })
			results := make(chan readResult, 9)
			go func() { raw, resultErr := s.GetRaw(t.Context(), "key"); results <- readResult{raw, resultErr} }()
			<-started
			for range 8 {
				ctx := &waitingContext{Context: t.Context(), waiting: make(chan struct{})}
				go func() { raw, resultErr := s.GetRaw(ctx, "key"); results <- readResult{raw, resultErr} }()
				<-ctx.waiting
			}
			close(release)
			for range 9 {
				result := <-results
				require.ErrorIs(t, result.err, readErr)
				if readErr == nil {
					require.Equal(t, []byte(`"value"`), result.raw)
					result.raw[0] = 'x'
				}
			}
			require.EqualValues(t, 1, d.reads.Load())
			_, err = s.GetRaw(t.Context(), "key")
			require.ErrorIs(t, err, readErr)
			require.EqualValues(t, 2, d.reads.Load())
		})
	}
}

func fmtErrorName(err error) string {
	if err == nil {
		return "success"
	}
	return err.Error()
}

func TestStampedeCanceledWaiterLeavesLeaderRunning(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	d := &countedReads{MockDriver: kvtest.NewMockDriver()}
	d.get = func(context.Context, string) ([]byte, error) { close(started); <-release; return []byte("value"), nil }
	s, err := kv.Open(d, kv.WithHook(middleware.NewStampede()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	done := make(chan error, 1)
	go func() { _, readErr := s.GetRaw(t.Context(), "key"); done <- readErr }()
	<-started
	base, cancel := context.WithCancel(t.Context())
	ctx := &waitingContext{Context: base, waiting: make(chan struct{})}
	waiter := make(chan error, 1)
	go func() { _, readErr := s.GetRaw(ctx, "key"); waiter <- readErr }()
	<-ctx.waiting
	cancel()
	require.ErrorIs(t, <-waiter, context.Canceled)
	require.EqualValues(t, 1, d.reads.Load())
	close(release)
	require.NoError(t, <-done)
}

func TestStampedePanicReleasesWaiters(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	d := &countedReads{MockDriver: kvtest.NewMockDriver()}
	d.get = func(context.Context, string) ([]byte, error) {
		if d.reads.Load() == 1 {
			close(started)
			<-release
			panic("backend panic")
		}
		return []byte("value"), nil
	}
	s, err := kv.Open(d, kv.WithHook(middleware.NewStampede()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	recovered := make(chan any, 1)
	go func() { defer func() { recovered <- recover() }(); _, _ = s.GetRaw(t.Context(), "key") }()
	<-started
	ctx := &waitingContext{Context: t.Context(), waiting: make(chan struct{})}
	done := make(chan error, 1)
	go func() { _, readErr := s.GetRaw(ctx, "key"); done <- readErr }()
	<-ctx.waiting
	close(release)
	require.Equal(t, "backend panic", <-recovered)
	require.ErrorContains(t, <-done, "shared read did not complete")
	raw, err := s.GetRaw(t.Context(), "key")
	require.NoError(t, err)
	require.Equal(t, []byte("value"), raw)
}
