package memcacheddriver

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xraph/grove/kv"
	"github.com/xraph/grove/kv/driver"
)

type poolServer struct {
	listener net.Listener
	requests chan string
	active   atomic.Int32
	accepted atomic.Int32
	mu       sync.Mutex
	conns    map[net.Conn]bool
	wg       sync.WaitGroup
	release  chan struct{}
}

func startPoolServer(t *testing.T) *poolServer {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &poolServer{listener: ln, requests: make(chan string, 100), conns: make(map[net.Conn]bool), release: make(chan struct{})}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			conn, acceptErr := ln.Accept()
			if acceptErr != nil {
				return
			}
			s.mu.Lock()
			s.conns[conn] = true
			s.mu.Unlock()
			s.active.Add(1)
			s.accepted.Add(1)
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				defer conn.Close()
				defer s.active.Add(-1)
				r := bufio.NewReader(conn)
				for {
					line, readErr := r.ReadString('\n')
					if readErr != nil {
						return
					}
					s.requests <- strings.TrimSpace(line)
					if strings.Contains(line, "hold") {
						select {
						case <-s.release:
						case <-t.Context().Done():
							return
						}
					}
					if strings.Contains(line, "block") {
						_, _ = r.ReadByte()
						return
					}
					for _, key := range strings.Fields(line)[1:] {
						if strings.HasPrefix(key, "present") {
							_, _ = fmt.Fprintf(conn, "VALUE %s 0 1 1\r\nx\r\n", key)
						}
					}
					if _, writeErr := fmt.Fprint(conn, "END\r\n"); writeErr != nil {
						return
					}
				}
			}()
		}
	}()
	t.Cleanup(func() {
		ln.Close()
		s.mu.Lock()
		for conn := range s.conns {
			conn.Close()
		}
		s.mu.Unlock()
		s.wg.Wait()
	})
	return s
}

func openPoolDB(t *testing.T, s *poolServer, opts ...driver.Option) *MemcachedDB {
	t.Helper()
	db := New()
	if err := db.Open(t.Context(), s.listener.Addr().String(), opts...); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	return db
}

func receiveRequest(t *testing.T, s *poolServer) string {
	t.Helper()
	select {
	case request := <-s.requests:
		return request
	case <-time.After(time.Second):
		t.Fatal("request did not reach server")
		return ""
	}
}

func waitForNoConnections(t *testing.T, s *poolServer) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for s.active.Load() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if n := s.active.Load(); n != 0 {
		t.Fatalf("%d connections remain after cleanup", n)
	}
}

func TestPoolLimitsAndCancellation(t *testing.T) {
	s := startPoolServer(t)
	db := openPoolDB(t, s, driver.WithPoolSize(1))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	first := make(chan error, 1)
	go func() { _, err := db.Get(ctx, "block"); first <- err }()
	receiveRequest(t, s)
	waitCtx, stop := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer stop()
	if _, err := db.Get(waitCtx, "queued"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("queued request: %v", err)
	}
	if n := s.accepted.Load(); n != 1 {
		t.Fatalf("pool size 1 opened %d connections", n)
	}
	select {
	case request := <-s.requests:
		t.Fatalf("queued request reached server: %s", request)
	default:
	}
	cancel()
	select {
	case err := <-first:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("active cancellation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation did not interrupt active socket")
	}
	waitForNoConnections(t, s)
	for range 3 {
		if _, err := db.Get(t.Context(), "present"); err != nil {
			t.Fatal(err)
		}
		receiveRequest(t, s)
	}
	if n := s.accepted.Load(); n != 2 {
		t.Fatalf("successful requests did not reuse the connection: %d", n)
	}
}

func TestCloseInterruptsActiveRequestsAndWaiters(t *testing.T) {
	s := startPoolServer(t)
	db := openPoolDB(t, s, driver.WithPoolSize(1))
	results := make(chan error, 2)
	go func() { _, err := db.Get(t.Context(), "block"); results <- err }()
	receiveRequest(t, s)
	go func() { _, err := db.Get(t.Context(), "queued"); results <- err }()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		select {
		case err := <-results:
			if !errors.Is(err, kv.ErrStoreClosed) {
				t.Fatalf("shutdown: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("shutdown left request waiting")
		}
	}
	waitForNoConnections(t, s)
	if _, err := db.Get(t.Context(), "after-close"); !errors.Is(err, kv.ErrStoreClosed) {
		t.Fatalf("read after close: %v", err)
	}
	if s.accepted.Load() != 1 {
		t.Fatal("shutdown opened another connection")
	}
}

func TestCanceledOperationsDoNotSendCommands(t *testing.T) {
	s := startPoolServer(t)
	db := openPoolDB(t, s)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	ops := []func() error{
		func() error { return db.Ping(ctx) },
		func() error { _, err := db.Get(ctx, "present"); return err },
		func() error { return db.Set(ctx, "key", []byte("x"), 0) },
		func() error { _, err := db.Delete(ctx, "key"); return err },
		func() error { _, err := db.Exists(ctx, "key"); return err },
		func() error { _, err := db.SetNX(ctx, "key", nil, 0); return err },
		func() error { _, err := db.SetXX(ctx, "key", nil, 0); return err },
		func() error {
			return db.CompareAndSwap(ctx, "present", func([]byte) ([]byte, error) { t.Fatal("callback ran after cancellation"); return nil, nil })
		},
	}
	for i, op := range ops {
		if err := op(); !errors.Is(err, context.Canceled) {
			t.Fatalf("operation %d: %v", i, err)
		}
	}
	if s.accepted.Load() != 0 {
		t.Fatal("canceled operation opened a connection")
	}
}

func TestExistsUsesOneRequestPerServer(t *testing.T) {
	s := startPoolServer(t)
	db := openPoolDB(t, s)
	keys := []string{"present1", "missing", "present1", "present2"}
	count, err := db.Exists(t.Context(), keys...)
	if err != nil || count != 3 {
		t.Fatalf("Exists = %d, %v", count, err)
	}
	request := receiveRequest(t, s)
	if strings.Count(request, "present1") != 1 {
		t.Fatalf("duplicate key sent: %s", request)
	}
	select {
	case extra := <-s.requests:
		t.Fatalf("extra exists query: %s", extra)
	default:
	}
	if _, err := db.Exists(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case extra := <-s.requests:
		t.Fatalf("empty exists query: %s", extra)
	default:
	}
}

func TestCanceledCASAndPanicReleasePoolSlot(t *testing.T) {
	s := startPoolServer(t)
	db := openPoolDB(t, s, driver.WithPoolSize(1))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	err := db.CompareAndSwap(ctx, "present", func([]byte) ([]byte, error) { cancel(); return []byte("y"), nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("CAS cancellation: %v", err)
	}
	receiveRequest(t, s)
	select {
	case extra := <-s.requests:
		t.Fatalf("CAS wrote after cancellation: %s", extra)
	default:
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Error("missing callback panic")
			}
		}()
		_ = db.CompareAndSwap(t.Context(), "present", func([]byte) ([]byte, error) { panic("callback failed") })
	}()
	receiveRequest(t, s)
	ctx, cancel = context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if _, err := db.Get(ctx, "present"); err != nil {
		t.Fatal(err)
	}
}

func TestDefaultPoolBoundsActiveAndIdleConnections(t *testing.T) {
	s := startPoolServer(t)
	db := openPoolDB(t, s, driver.WithPoolSize(0))
	results := make(chan error, 10)
	for range 10 {
		go func() { _, err := db.Get(t.Context(), "hold"); results <- err }()
	}
	for range 10 {
		receiveRequest(t, s)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	if _, err := db.Get(ctx, "queued"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("eleventh request: %v", err)
	}
	if s.accepted.Load() != 10 {
		t.Fatalf("default pool opened %d sockets", s.accepted.Load())
	}
	close(s.release)
	for range 10 {
		if err := <-results; !errors.Is(err, kv.ErrNotFound) {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(time.Second)
	for s.active.Load() > 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if s.active.Load() > 2 {
		t.Fatalf("retained %d idle sockets", s.active.Load())
	}
	for range 15 {
		if _, err := db.Get(t.Context(), "present"); err != nil {
			t.Fatal(err)
		}
		receiveRequest(t, s)
	}
	if s.accepted.Load() != 10 {
		t.Fatal("sequential calls did not reuse idle connections")
	}
}

func TestReadTimeoutAndCancellationAreIsolated(t *testing.T) {
	s := startPoolServer(t)
	db := openPoolDB(t, s, WithTimeout(30*time.Millisecond), driver.WithDialTimeout(time.Second))
	started := time.Now()
	_, err := db.Get(t.Context(), "block")
	var networkErr net.Error
	if !errors.As(err, &networkErr) || !networkErr.Timeout() {
		t.Fatalf("read timeout: %v", err)
	}
	if time.Since(started) > 500*time.Millisecond {
		t.Fatal("read used dial timeout")
	}
	receiveRequest(t, s)
	waitForNoConnections(t, s)
}

func TestCancellationDoesNotInterruptOtherRequests(t *testing.T) {
	s := startPoolServer(t)
	db := openPoolDB(t, s, driver.WithPoolSize(2))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	first := make(chan error, 1)
	second := make(chan error, 1)
	go func() { _, err := db.Get(ctx, "block-first"); first <- err }()
	receiveRequest(t, s)
	go func() { _, err := db.Get(t.Context(), "block-second"); second <- err }()
	receiveRequest(t, s)
	cancel()
	select {
	case err := <-first:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("first request did not cancel")
	}
	select {
	case err := <-second:
		t.Fatalf("other request interrupted: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-second:
		if !errors.Is(err, kv.ErrStoreClosed) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("second request did not stop")
	}
}

func TestCloseTracksNativeClientSockets(t *testing.T) {
	s := startPoolServer(t)
	db := openPoolDB(t, s)
	result := make(chan error, 1)
	go func() { _, err := db.Client().Get("block"); result <- err }()
	receiveRequest(t, s)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("native command succeeded after close")
		}
	case <-time.After(time.Second):
		t.Fatal("native socket stayed open")
	}
	waitForNoConnections(t, s)
}
