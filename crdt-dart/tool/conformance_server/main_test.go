package main

import (
	"bufio"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestExitOnEOFWaitsForTheEndOfStdin(t *testing.T) {
	r, w := io.Pipe()
	exited := make(chan struct{})
	go exitOnEOF(r, func() { close(exited) })

	if _, err := w.Write([]byte("still here\n")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-exited:
		t.Fatal("exited while stdin was still open")
	case <-time.After(50 * time.Millisecond):
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-exited:
	case <-time.After(5 * time.Second):
		t.Fatal("did not exit after stdin closed")
	}
}

// helperEnv makes TestHelperServer run main instead of testing anything.
const helperEnv = "CONFORMANCE_SERVER_HELPER"

// TestHelperServer is the server process for
// TestServerExitsWhenStdinCloses: the test binary re-executed with helperEnv
// set runs main with the arguments after "--".
func TestHelperServer(t *testing.T) {
	if os.Getenv(helperEnv) != "1" {
		t.Skip("helper process for TestServerExitsWhenStdinCloses")
	}
	args := os.Args
	for i, a := range args {
		if a == "--" {
			args = args[i+1:]
			break
		}
	}
	os.Args = append([]string{"conformance_server"}, args...)
	main()
}

func TestServerExitsWhenStdinCloses(t *testing.T) {
	cmd := exec.Command(os.Args[0], "-test.run=^TestHelperServer$", "--",
		"-addr", "127.0.0.1:0", "-tables", "notes")
	cmd.Env = append(os.Environ(), helperEnv+"=1")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waited := make(chan error, 1)
	go func() { waited <- cmd.Wait() }()
	// Never leave the child behind, whatever happens below. It is killed
	// through its own handle, never by name.
	defer func() {
		select {
		case <-waited:
		default:
			_ = cmd.Process.Kill()
			<-waited
		}
	}()

	lines := bufio.NewScanner(stdout)
	var base string
	for lines.Scan() {
		if line := lines.Text(); strings.HasPrefix(line, "LISTENING ") {
			base = strings.TrimPrefix(line, "LISTENING ")
			break
		}
	}
	if base == "" {
		t.Fatal("the server never printed LISTENING")
	}
	go func() { _, _ = io.Copy(io.Discard, stdout) }()

	resp, err := http.Get(base + "/admin/state?table=notes&pk=nope")
	if err != nil {
		t.Fatalf("server not serving: %v", err)
	}
	_ = resp.Body.Close()

	select {
	case err := <-waited:
		t.Fatalf("exited with stdin still open: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	if err := stdin.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-waited:
		if err != nil {
			t.Fatalf("exit after stdin closed: %v", err)
		}
		waited <- nil
	case <-time.After(10 * time.Second):
		t.Fatal("still running 10s after stdin closed")
	}
}
