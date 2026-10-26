package crdt

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordingTransport records pushes and fails pulls or pushes on request.
type recordingTransport struct {
	mu      sync.Mutex
	pushes  []*PushRequest
	pulls   int
	pullErr error
	pushErr error
}

func (r *recordingTransport) Pull(context.Context, *PullRequest) (*PullResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pulls++
	if r.pullErr != nil {
		return nil, r.pullErr
	}

	return &PullResponse{}, nil
}

func (r *recordingTransport) Push(_ context.Context, req *PushRequest) (*PushResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pushes = append(r.pushes, req)
	if r.pushErr != nil {
		return nil, r.pushErr
	}

	return &PushResponse{}, nil
}

func (r *recordingTransport) pullCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.pulls
}

// outboundHook skips or rejects outbound changes.
type outboundHook struct {
	BaseSyncHook
	skip bool
	err  error
}

func (h *outboundHook) BeforeOutboundChange(_ context.Context, c *ChangeRecord) (*ChangeRecord, error) {
	if h.err != nil {
		return nil, h.err
	}
	if h.skip {
		return nil, nil
	}

	return c, nil
}

func TestSyncer_PushChangeReachesEveryPeer(t *testing.T) {
	primary, peerA, peerB := &recordingTransport{}, &recordingTransport{}, &recordingTransport{}
	s := NewSyncer(newTestPlugin(), WithTransport(primary), WithPeers(peerA, peerB))
	clock := HLC{Timestamp: 77, Counter: 1, NodeID: "test-node"}

	err := s.PushChange(t.Context(), "notes", "1", "title", TypeLWW, json.RawMessage(`"hi"`), clock)
	require.NoError(t, err)

	for _, tr := range []*recordingTransport{primary, peerA, peerB} {
		require.Len(t, tr.pushes, 1)
		req := tr.pushes[0]
		assert.Equal(t, "test-node", req.NodeID)
		require.Len(t, req.Changes, 1)
		c := req.Changes[0]
		assert.Equal(t, "notes", c.Table)
		assert.Equal(t, "1", c.PK)
		assert.Equal(t, "title", c.Field)
		assert.Equal(t, TypeLWW, c.CRDTType)
		assert.Equal(t, clock, c.HLC)
		assert.Equal(t, "test-node", c.NodeID)
		assert.JSONEq(t, `"hi"`, string(c.Value))
	}
}

func TestSyncer_PushChangeHooksAndErrors(t *testing.T) {
	ctx := t.Context()

	// A hook that skips the change means nothing is pushed.
	tr := &recordingTransport{}
	p := New(WithNodeID("n"), WithSyncHook(&outboundHook{skip: true}))
	require.NoError(t, NewSyncer(p, WithTransport(tr)).PushChange(ctx, "t", "1", "f", TypeLWW, nil, HLC{}))
	assert.Empty(t, tr.pushes)

	// A hook error stops the push.
	hookErr := errors.New("not allowed")
	p = New(WithNodeID("n"), WithSyncHook(&outboundHook{err: hookErr}))
	err := NewSyncer(p, WithTransport(tr)).PushChange(ctx, "t", "1", "f", TypeLWW, nil, HLC{})
	require.ErrorIs(t, err, hookErr)
	assert.Contains(t, err.Error(), "outbound change hook")

	// A transport error is returned and later peers are not tried.
	pushErr := errors.New("503")
	failing, after := &recordingTransport{pushErr: pushErr}, &recordingTransport{}
	err = NewSyncer(New(WithNodeID("n")), WithPeers(failing, after)).PushChange(ctx, "t", "1", "f", TypeLWW, nil, HLC{})
	require.ErrorIs(t, err, pushErr)
	assert.Empty(t, after.pushes)
}

func TestSyncer_SyncRetriesThenGivesUp(t *testing.T) {
	tr := &recordingTransport{pullErr: errors.New("connection refused")}
	s := NewSyncer(newTestPlugin(), WithTransport(tr), WithRetry(2, time.Millisecond, 2*time.Millisecond))

	report, err := s.Sync(t.Context())

	// A peer that keeps failing is logged and skipped, not returned.
	require.NoError(t, err)
	assert.Equal(t, &SyncReport{}, report)
	assert.Equal(t, 3, tr.pullCount(), "one attempt plus two retries")
}

func TestSyncer_SyncRetryStopsOnCancel(t *testing.T) {
	tr := &recordingTransport{pullErr: errors.New("down")}
	s := NewSyncer(newTestPlugin(), WithTransport(tr), WithRetry(5, time.Hour, time.Hour))
	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = s.Sync(ctx)
	}()
	require.Eventually(t, func() bool { return tr.pullCount() == 1 }, 5*time.Second, time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Sync did not return after cancel during backoff")
	}
	assert.Equal(t, 1, tr.pullCount())
}

func TestSyncer_RunSyncsUntilCancelled(t *testing.T) {
	tr := &recordingTransport{}
	s := NewSyncer(newTestPlugin(), WithTransport(tr), WithSyncInterval(time.Millisecond))
	ctx, cancel := context.WithCancel(t.Context())

	errc := make(chan error, 1)
	go func() { errc <- s.Run(ctx) }()

	require.Eventually(t, func() bool { return tr.pullCount() >= 2 }, 5*time.Second, time.Millisecond)
	cancel()

	select {
	case err := <-errc:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop after cancel")
	}
}

func TestSyncer_RunLogsSyncErrors(t *testing.T) {
	// No transports: every tick's Sync fails, and Run keeps going.
	s := NewSyncer(newTestPlugin(), WithSyncInterval(time.Millisecond))
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()

	require.ErrorIs(t, s.Run(ctx), context.DeadlineExceeded)
}

func TestSyncer_StreamSyncNeedsStreamingTransport(t *testing.T) {
	err := NewSyncer(newTestPlugin()).StreamSync(t.Context())
	require.ErrorContains(t, err, "no transports configured")

	err = NewSyncer(newTestPlugin(), WithTransport(&recordingTransport{})).StreamSync(t.Context())
	require.ErrorContains(t, err, "no streaming transports configured")
}
