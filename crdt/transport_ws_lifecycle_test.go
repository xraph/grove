package crdt

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type lifecycleWSConn struct {
	*mockWSConn
	reading  chan struct{}
	readOnce sync.Once
	writing  atomic.Int32
	overlap  atomic.Bool
	writes   atomic.Int32
	closes   atomic.Int32
}

func newLifecycleWSConn() *lifecycleWSConn {
	return &lifecycleWSConn{mockWSConn: newMockWSConn(), reading: make(chan struct{})}
}
func (c *lifecycleWSConn) ReadMessage() ([]byte, error) {
	c.readOnce.Do(func() { close(c.reading) })
	return c.mockWSConn.ReadMessage()
}
func (c *lifecycleWSConn) WriteMessage(data []byte) error {
	if c.writing.Add(1) > 1 {
		c.overlap.Store(true)
	}
	defer c.writing.Add(-1)
	c.writes.Add(1)
	time.Sleep(time.Millisecond) // Make competing writes overlap on an unsynchronized transport.
	return c.mockWSConn.WriteMessage(data)
}
func (c *lifecycleWSConn) Close() error {
	c.closes.Add(1)
	return c.mockWSConn.Close()
}

func awaitWSResult(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("websocket operation did not finish")
		return nil
	}
}

func TestWSClientSerializesRequestsAndHeartbeat(t *testing.T) {
	conn := newLifecycleWSConn()
	transport := NewWebSocketTransport(conn, WithWSPingInterval(time.Millisecond))
	t.Cleanup(func() { require.NoError(t, transport.Close()) })
	streamDone := make(chan error, 1)
	go func() { streamDone <- transport.Start(context.Background()) }()
	serverDone := make(chan struct{})
	stopServer := make(chan struct{})
	go func() {
		defer close(serverDone)
		for {
			select {
			case <-stopServer:
				return
			case raw := <-conn.writeCh:
				var request WebSocketMessage
				if json.Unmarshal(raw, &request) != nil || request.RequestID == "" {
					continue
				}
				kind := WSPullResponse
				if request.Type == WSPushRequest {
					kind = WSPushResponse
				}
				reply, _ := json.Marshal(WebSocketMessage{Type: kind, RequestID: request.RequestID, Payload: json.RawMessage(`{}`)})
				conn.readCh <- reply
			}
		}
	}()
	done := make(chan error, 24)
	for i := range 24 {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if i%2 == 0 {
				_, err := transport.Pull(ctx, &PullRequest{})
				done <- err
			} else {
				_, err := transport.Push(ctx, &PushRequest{})
				done <- err
			}
		}()
	}
	for range 24 {
		require.NoError(t, awaitWSResult(t, done))
	}
	close(stopServer)
	<-serverDone
	require.NoError(t, transport.Close())
	require.NoError(t, awaitWSResult(t, streamDone))
	require.False(t, conn.overlap.Load(), "requests and heartbeat must share one writer")
	require.EqualValues(t, 1, conn.closes.Load())
}

func TestWSClientCancellationUnblocksRead(t *testing.T) {
	conn := newLifecycleWSConn()
	transport := NewWebSocketTransport(conn)
	t.Cleanup(func() { require.NoError(t, transport.Close()) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- transport.Start(ctx) }()
	select {
	case <-conn.reading:
	case <-time.After(time.Second):
		t.Fatal("read never started")
	}
	cancel()
	require.ErrorIs(t, awaitWSResult(t, done), context.Canceled)
	require.EqualValues(t, 1, conn.closes.Load())
}

func TestWSClientReturnsCorrelatedErrorsWithoutDeadline(t *testing.T) {
	for _, test := range []struct {
		name    string
		payload string
		want    string
	}{
		{"rejection", `{"error":"permission denied"}`, "permission denied"},
		{"malformed", `[]`, "invalid remote error response"},
	} {
		t.Run(test.name, func(t *testing.T) {
			conn := newMockWSConn()
			transport := NewWebSocketTransport(conn)
			t.Cleanup(func() { require.NoError(t, transport.Close()) })
			streamDone := make(chan error, 1)
			go func() { streamDone <- transport.Start(context.Background()) }()
			for _, push := range []bool{false, true} {
				done := make(chan error, 1)
				go func() {
					if push {
						_, err := transport.Push(context.Background(), &PushRequest{})
						done <- err
					} else {
						_, err := transport.Pull(context.Background(), &PullRequest{})
						done <- err
					}
				}()
				request := readWS(t, conn)
				reply, _ := json.Marshal(WebSocketMessage{Type: WSError, RequestID: request.RequestID, Payload: json.RawMessage(test.payload)})
				conn.readCh <- reply
				require.ErrorContains(t, awaitWSResult(t, done), test.want)
				// Duplicate replies must not block the reader or affect the next request.
				conn.readCh <- reply
			}
			require.NoError(t, transport.Close())
			require.NoError(t, awaitWSResult(t, streamDone))
		})
	}
}

func TestWSClientCloseWakesPendingRequests(t *testing.T) {
	conn := newMockWSConn()
	transport := NewWebSocketTransport(conn)
	done := make(chan error, 1)
	go func() { _, err := transport.Pull(context.Background(), &PullRequest{}); done <- err }()
	readWS(t, conn)
	require.NoError(t, transport.Close())
	require.ErrorIs(t, awaitWSResult(t, done), ErrWebSocketClosed)
	_, err := transport.Pull(context.Background(), &PullRequest{})
	require.ErrorIs(t, err, ErrWebSocketClosed)
}

type writeHookWSConn struct {
	*mockWSConn
	write func([]byte) error
}

func (c *writeHookWSConn) WriteMessage(data []byte) error { return c.write(data) }

func TestWSClientCancellationUnblocksWriterAndWaiters(t *testing.T) {
	conn := &writeHookWSConn{mockWSConn: newMockWSConn()}
	started := make(chan struct{})
	conn.write = func([]byte) error {
		close(started)
		<-conn.readCh // Close unblocks the active write.
		return ErrWebSocketClosed
	}
	transport := NewWebSocketTransport(conn)
	t.Cleanup(func() { require.NoError(t, transport.Close()) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := transport.Pull(ctx, &PullRequest{}); done <- err }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("write never started")
	}
	// Timing out while waiting for the writer must not close another request's socket.
	waitCtx, stopWait := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer stopWait()
	_, err := transport.Push(waitCtx, &PushRequest{})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	select {
	case <-transport.connection().done:
		t.Fatal("waiting request closed the connection")
	default:
	}
	cancel()
	require.ErrorIs(t, awaitWSResult(t, done), context.Canceled)
}

func TestWSClientPreservesResponseBeforeDisconnect(t *testing.T) {
	for range 20 {
		conn := &writeHookWSConn{mockWSConn: newMockWSConn()}
		transport := NewWebSocketTransport(conn)
		conn.write = func(data []byte) error {
			var request WebSocketMessage
			require.NoError(t, json.Unmarshal(data, &request))
			transport.handleMessage(WebSocketMessage{Type: WSError, RequestID: request.RequestID,
				Payload: json.RawMessage(`{"error":"permission denied"}`)})
			require.NoError(t, transport.Close())
			return nil
		}
		_, err := transport.Pull(context.Background(), &PullRequest{})
		require.ErrorContains(t, err, "permission denied")
	}
}

func TestWSClientReconnectStopsOldHeartbeatAndRequests(t *testing.T) {
	first, second := newLifecycleWSConn(), newLifecycleWSConn()
	transport := NewWebSocketTransport(first, WithWSPingInterval(5*time.Millisecond), WithWSReconnectDelay(time.Millisecond))
	t.Cleanup(func() { require.NoError(t, transport.Close()) })
	done := make(chan error, 1)
	go func() {
		done <- transport.StartWithReconnect(context.Background(), func(context.Context) (WebSocketConn, error) { return second, nil })
	}()
	<-first.reading
	pending := make(chan error, 1)
	go func() { _, err := transport.Pull(context.Background(), &PullRequest{}); pending <- err }()
	for msg := readWS(t, first.mockWSConn); msg.Type != WSPullRequest; msg = readWS(t, first.mockWSConn) {
		require.Equal(t, WSPing, msg.Type)
	}
	require.NoError(t, first.mockWSConn.Close()) // Remote disconnect.
	require.ErrorIs(t, awaitWSResult(t, pending), ErrWebSocketClosed)
	select {
	case <-second.reading:
	case <-time.After(time.Second):
		t.Fatal("did not reconnect")
	}
	oldWrites := first.writes.Load()
	time.Sleep(30 * time.Millisecond)
	require.Equal(t, oldWrites, first.writes.Load(), "old heartbeat survived reconnect")
	require.Positive(t, second.writes.Load())
	require.NoError(t, transport.Close())
	require.NoError(t, awaitWSResult(t, done))
	require.False(t, second.overlap.Load())
}

func TestWSClientCloseDuringDialCannotResurrectConnection(t *testing.T) {
	first := newMockWSConn()
	transport := NewWebSocketTransport(first, WithWSReconnectDelay(time.Millisecond))
	t.Cleanup(func() { require.NoError(t, transport.Close()) })
	fresh := newLifecycleWSConn()
	dialStarted := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- transport.StartWithReconnect(context.Background(), func(ctx context.Context) (WebSocketConn, error) {
			close(dialStarted)
			<-ctx.Done()
			return fresh, nil // A dialer that completed just as cancellation arrived.
		})
	}()
	require.NoError(t, first.Close())
	select {
	case <-dialStarted:
	case <-time.After(time.Second):
		t.Fatal("dial did not start")
	}
	require.NoError(t, transport.Close())
	require.NoError(t, awaitWSResult(t, done))
	require.EqualValues(t, 1, fresh.closes.Load())
	_, err := transport.Pull(context.Background(), &PullRequest{})
	require.True(t, errors.Is(err, ErrWebSocketClosed))
}
