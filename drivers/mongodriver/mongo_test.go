package mongodriver

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type fakePinger struct {
	calls       atomic.Int32
	failN       int // first N calls fail; subsequent calls succeed
	failErr     error
	delay       time.Duration // sleep on each call (simulate latency)
	respectsCtx bool          // honor ctx cancellation while sleeping
}

func (f *fakePinger) Ping(ctx context.Context, _ any) error {
	f.calls.Add(1)
	if f.delay > 0 {
		if f.respectsCtx {
			t := time.NewTimer(f.delay)
			defer t.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-t.C:
			}
		} else {
			time.Sleep(f.delay)
		}
	}
	if int(f.calls.Load()) <= f.failN {
		return f.failErr
	}
	return nil
}

func TestPingWithRetry_succeedsAfterTransientFailure(t *testing.T) {
	f := &fakePinger{failN: 2, failErr: errors.New("boom")}
	mopts := &mongoOptions{
		PingTimeout:      50 * time.Millisecond,
		PingRetries:      3,
		PingRetryBackoff: 1 * time.Millisecond,
	}

	if err := pingWithRetry(context.Background(), f, mopts); err != nil {
		t.Fatalf("expected success after retries, got %v", err)
	}
	if got := f.calls.Load(); got != 3 {
		t.Fatalf("expected 3 attempts, got %d", got)
	}
}

func TestPingWithRetry_returnsLastErrorAfterExhausted(t *testing.T) {
	want := errors.New("permanently down")
	f := &fakePinger{failN: 100, failErr: want}
	mopts := &mongoOptions{
		PingTimeout:      50 * time.Millisecond,
		PingRetries:      2,
		PingRetryBackoff: 1 * time.Millisecond,
	}

	err := pingWithRetry(context.Background(), f, mopts)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, want) {
		t.Fatalf("expected wrapped %v, got %v", want, err)
	}
	if got := f.calls.Load(); got != 3 { // 1 + 2 retries
		t.Fatalf("expected 3 attempts, got %d", got)
	}
}

func TestPingWithRetry_respectsContextCancel(t *testing.T) {
	f := &fakePinger{failN: 100, failErr: errors.New("boom")}
	mopts := &mongoOptions{
		PingTimeout:      10 * time.Millisecond,
		PingRetries:      10,
		PingRetryBackoff: 50 * time.Millisecond,
	}

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- pingWithRetry(ctx, f, mopts)
	}()

	// Let the first attempt fail, then cancel during the backoff.
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected error after cancel, got nil")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("pingWithRetry did not return after context cancel")
	}
}

func TestBackoffFor(t *testing.T) {
	base := 100 * time.Millisecond
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{0, 100 * time.Millisecond},
		{1, 200 * time.Millisecond},
		{2, 400 * time.Millisecond},
		{6, pingRetryMaxBackoff},  // 100ms << 6 = 6.4s, capped
		{60, pingRetryMaxBackoff}, // overflow safety
	}
	for _, tc := range cases {
		got := backoffFor(tc.attempt, base)
		if got != tc.want {
			t.Errorf("backoffFor(%d, %v) = %v, want %v", tc.attempt, base, got, tc.want)
		}
	}

	if got := backoffFor(3, 0); got != 0 {
		t.Errorf("backoffFor(_, 0) = %v, want 0", got)
	}
}

func TestPingWithRetry_singleAttemptWhenRetriesZero(t *testing.T) {
	f := &fakePinger{failN: 1, failErr: errors.New("boom")}
	mopts := &mongoOptions{
		PingTimeout: 50 * time.Millisecond,
		PingRetries: 0,
	}

	if err := pingWithRetry(context.Background(), f, mopts); err == nil {
		t.Fatal("expected error from single attempt, got nil")
	}
	if got := f.calls.Load(); got != 1 {
		t.Fatalf("expected 1 attempt, got %d", got)
	}
}
