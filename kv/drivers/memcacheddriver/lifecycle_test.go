package memcacheddriver

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"
)

func TestCloseReleasesIdleConnection(t *testing.T) {
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	finished := make(chan error, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			finished <- acceptErr
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
		reader := bufio.NewReader(conn)
		if _, readErr := reader.ReadString('\n'); readErr != nil {
			finished <- readErr
			return
		}
		if _, writeErr := fmt.Fprint(conn, "END\r\n"); writeErr != nil {
			finished <- writeErr
			return
		}
		_, readErr := reader.ReadByte()
		finished <- readErr
	}()
	db := New()
	if err := db.Open(t.Context(), listener.Addr().String()); err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Ping(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case closeErr := <-finished:
		if !errors.Is(closeErr, io.EOF) {
			t.Fatalf("want closed socket, got %v", closeErr)
		}
	case <-time.After(time.Second):
		t.Fatal("Close left the idle socket open")
	}
}

func TestOpenPreservesExistingClient(t *testing.T) {
	db := New()
	if err := db.Open(t.Context(), "127.0.0.1:1"); err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	existing := db.client
	if err := db.Open(t.Context(), "127.0.0.1:2"); err == nil {
		t.Fatal("second Open silently replaced the client")
	}
	if db.client != existing {
		t.Fatal("existing client lost")
	}
}
