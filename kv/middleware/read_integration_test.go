package middleware_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/xraph/grove/hook"
	"github.com/xraph/grove/kv"
	"github.com/xraph/grove/kv/kvtest"
	"github.com/xraph/grove/kv/middleware"
	"github.com/xraph/grove/observability/telemetrytest"
)

type countedReads struct {
	*kvtest.MockDriver
	reads atomic.Int64
	get   func(context.Context, string) ([]byte, error)
}

func (d *countedReads) Get(ctx context.Context, key string) ([]byte, error) {
	d.reads.Add(1)
	if d.get != nil {
		return d.get(ctx, key)
	}
	return d.MockDriver.Get(ctx, key)
}

func TestCacheStoreReads(t *testing.T) {
	d := &countedReads{MockDriver: kvtest.NewMockDriver()}
	require.NoError(t, d.Set(t.Context(), "key", []byte(`"value"`), 0))
	s, err := kv.Open(d, kv.WithHook(middleware.NewCache(10, time.Minute)))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	var value string
	require.NoError(t, s.Get(t.Context(), "key", &value))
	require.Equal(t, "value", value)
	raw, err := s.GetRaw(t.Context(), "key")
	require.NoError(t, err)
	require.Equal(t, []byte(`"value"`), raw)
	require.EqualValues(t, 1, d.reads.Load())
}

func TestStampedeStoreCompletesReads(t *testing.T) {
	d := &countedReads{MockDriver: kvtest.NewMockDriver()}
	require.NoError(t, d.Set(t.Context(), "key", []byte(`"value"`), 0))
	h := middleware.NewStampede()
	s, err := kv.Open(d, kv.WithHook(h))
	require.NoError(t, err)
	t.Cleanup(func() { h.Complete("key"); require.NoError(t, s.Close()) })
	_, err = s.GetRaw(t.Context(), "key")
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { _, readErr := s.GetRaw(t.Context(), "key"); done <- readErr }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("the completed read left the next request blocked")
	}
}

func TestCacheInvalidatesWrites(t *testing.T) {
	tests := []struct {
		name    string
		write   func(*kv.Store) error
		want    string
		missing bool
	}{
		{name: "set", write: func(s *kv.Store) error { return s.Set(t.Context(), "key", "new") }, want: `"new"`},
		{name: "set raw", write: func(s *kv.Store) error { return s.SetRaw(t.Context(), "key", []byte(`"raw"`)) }, want: `"raw"`},
		{name: "multi set", write: func(s *kv.Store) error { return s.MSet(t.Context(), map[string]any{"key": "multi"}) }, want: `"multi"`},
		{name: "multi delete", write: func(s *kv.Store) error { return s.Delete(t.Context(), "other", "key") }, missing: true},
		{name: "expire", write: func(s *kv.Store) error { return s.Expire(t.Context(), "key", time.Nanosecond) }, missing: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &countedReads{MockDriver: kvtest.NewMockDriver()}
			require.NoError(t, d.Set(t.Context(), "tenant:key", []byte(`"old"`), 0))
			s, err := kv.Open(d, kv.WithHook(middleware.NewCache(10, time.Minute)), kv.WithHook(middleware.NewNamespace("tenant")))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, s.Close()) })
			_, err = s.GetRaw(t.Context(), "key")
			require.NoError(t, err)
			require.NoError(t, tt.write(s))
			raw, err := s.GetRaw(t.Context(), "key")
			if tt.missing {
				require.ErrorIs(t, err, kv.ErrNotFound)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want, string(raw))
			}
			require.EqualValues(t, 2, d.reads.Load())
		})
	}
}

type denyRead struct{ deny bool }

func (h *denyRead) BeforeQuery(context.Context, *hook.QueryContext) (*hook.HookResult, error) {
	if h.deny {
		return &hook.HookResult{Decision: hook.Deny}, nil
	}
	return nil, nil
}

func TestReadMiddlewareCannotBypassPolicy(t *testing.T) {
	d := &countedReads{MockDriver: kvtest.NewMockDriver()}
	require.NoError(t, d.Set(t.Context(), "key", []byte(`"value"`), 0))
	policy := &denyRead{}
	s, err := kv.Open(d, kv.WithHook(middleware.NewCache(10, time.Minute)), kv.WithHook(middleware.NewStampede()), kv.WithHook(policy))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	_, err = s.GetRaw(t.Context(), "key")
	require.NoError(t, err)
	policy.deny = true
	_, err = s.GetRaw(t.Context(), "key")
	require.ErrorIs(t, err, kv.ErrHookDenied)
	policy.deny = false
	_, err = s.GetRaw(t.Context(), "key")
	require.NoError(t, err)
	require.EqualValues(t, 1, d.reads.Load())
}

func TestCacheCopiesBytesAndScopesStores(t *testing.T) {
	cache := middleware.NewCache(10, time.Minute)
	stampede := middleware.NewStampede()
	for _, value := range []string{"one", "two"} {
		d := &countedReads{MockDriver: kvtest.NewMockDriver()}
		require.NoError(t, d.Set(t.Context(), "key", []byte(value), 0))
		s, err := kv.Open(d, kv.WithHook(cache), kv.WithHook(stampede))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, s.Close()) })
		raw, err := s.GetRaw(t.Context(), "key")
		require.NoError(t, err)
		require.Equal(t, value, string(raw))
		raw[0] = 'x'
		raw, err = s.GetRaw(t.Context(), "key")
		require.NoError(t, err)
		require.Equal(t, value, string(raw))
		require.EqualValues(t, 1, d.reads.Load())
	}
}

func TestCacheDoesNotPublishReadOverlappingWrite(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	d := &countedReads{MockDriver: kvtest.NewMockDriver()}
	require.NoError(t, d.Set(t.Context(), "key", []byte("old"), 0))
	d.get = func(ctx context.Context, key string) ([]byte, error) {
		raw, err := d.MockDriver.Get(ctx, key)
		if d.reads.Load() == 1 {
			close(started)
			<-release
		}
		return raw, err
	}
	s, err := kv.Open(d, kv.WithHook(middleware.NewCache(10, time.Minute)))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	done := make(chan error, 1)
	go func() { _, readErr := s.GetRaw(t.Context(), "key"); done <- readErr }()
	<-started
	require.NoError(t, s.SetRaw(t.Context(), "key", []byte("new")))
	close(release)
	require.NoError(t, <-done)
	raw, err := s.GetRaw(t.Context(), "key")
	require.NoError(t, err)
	require.Equal(t, "new", string(raw))
	require.EqualValues(t, 2, d.reads.Load())
}

func TestCacheExpiryAndDisabledCapacity(t *testing.T) {
	for _, capacity := range []int{0, -1, 2} {
		t.Run(fmt.Sprint(capacity), func(t *testing.T) {
			d := &countedReads{MockDriver: kvtest.NewMockDriver()}
			require.NoError(t, d.Set(t.Context(), "key", []byte("old"), 0))
			s, err := kv.Open(d, kv.WithHook(middleware.NewCache(capacity, time.Nanosecond)))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, s.Close()) })
			_, err = s.GetRaw(t.Context(), "key")
			require.NoError(t, err)
			require.NoError(t, d.Set(t.Context(), "key", []byte("new"), 0))
			raw, err := s.GetRaw(t.Context(), "key")
			require.NoError(t, err)
			require.Equal(t, "new", string(raw))
			require.EqualValues(t, 2, d.reads.Load())
		})
	}
}

type failingPostHook struct{ fail bool }

func (h *failingPostHook) AfterQuery(context.Context, *hook.QueryContext, any) error {
	if h.fail {
		return errors.New("post hook failed")
	}
	return nil
}

func TestCacheInvalidatesWhenPostHookFails(t *testing.T) {
	d := &countedReads{MockDriver: kvtest.NewMockDriver()}
	require.NoError(t, d.Set(t.Context(), "key", []byte("old"), 0))
	post := &failingPostHook{}
	s, err := kv.Open(d, kv.WithHook(post), kv.WithHook(middleware.NewCache(10, time.Minute)))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	_, err = s.GetRaw(t.Context(), "key")
	require.NoError(t, err)
	post.fail = true
	require.ErrorContains(t, s.SetRaw(t.Context(), "key", []byte("new")), "post hook failed")
	post.fail = false
	raw, err := s.GetRaw(t.Context(), "key")
	require.NoError(t, err)
	require.Equal(t, "new", string(raw))
	post.fail = true
	_, err = s.GetRaw(t.Context(), "key")
	require.ErrorContains(t, err, "post hook failed")
}

func TestCacheAndStampedeDoNotReuseReadAfterWrite(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	d := &countedReads{MockDriver: kvtest.NewMockDriver()}
	require.NoError(t, d.Set(t.Context(), "key", []byte("old"), 0))
	d.get = func(ctx context.Context, key string) ([]byte, error) {
		raw, err := d.MockDriver.Get(ctx, key)
		if d.reads.Load() == 1 {
			close(started)
			<-release
		}
		return raw, err
	}
	s, err := kv.Open(d, kv.WithHook(middleware.NewCache(10, time.Minute)), kv.WithHook(middleware.NewStampede()))
	require.NoError(t, err)
	done := make(chan error, 1)
	t.Cleanup(func() { require.NoError(t, <-done); require.NoError(t, s.Close()) })
	go func() { _, readErr := s.GetRaw(t.Context(), "key"); done <- readErr }()
	<-started
	require.NoError(t, s.SetRaw(t.Context(), "key", []byte("new")))
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	raw, err := s.GetRaw(ctx, "key")
	require.NoError(t, err)
	require.Equal(t, "new", string(raw))
	require.EqualValues(t, 2, d.reads.Load())
}

func TestReadMiddlewareHonorsCanceledContextAndDecodeErrors(t *testing.T) {
	d := &countedReads{MockDriver: kvtest.NewMockDriver()}
	require.NoError(t, d.Set(t.Context(), "key", []byte(`"value"`), 0))
	s, err := kv.Open(d, kv.WithHook(middleware.NewCache(10, time.Minute)), kv.WithHook(middleware.NewStampede()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	var invalid int
	require.ErrorIs(t, s.Get(t.Context(), "key", &invalid), kv.ErrCodecDecode)
	var value string
	require.NoError(t, s.Get(t.Context(), "key", &value))
	require.Equal(t, "value", value)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = s.GetRaw(ctx, "key")
	require.ErrorIs(t, err, context.Canceled)
	require.EqualValues(t, 1, d.reads.Load())
}

func TestCacheRecordsOnlyBackendReads(t *testing.T) {
	recorder := telemetrytest.Spans(t)
	d := &countedReads{MockDriver: kvtest.NewMockDriver()}
	require.NoError(t, d.Set(t.Context(), "key", []byte("value"), 0))
	s, err := kv.Open(d, kv.WithHook(middleware.NewCache(10, time.Minute)))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	ctx, parent := telemetrytest.Parent(t.Context())
	for range 3 {
		_, err := s.GetRaw(ctx, "key")
		require.NoError(t, err)
	}
	parent.End()
	spans := telemetrytest.Ended(recorder)
	require.Len(t, spans, 1)
	require.Equal(t, "GET", spans[0].Name())
	require.Equal(t, parent.SpanContext().SpanID(), spans[0].Parent().SpanID())
}
