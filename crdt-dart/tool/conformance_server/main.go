// Command conformance_server is a grove crdt sync server for grove_crdt's
// Dart conformance tests. It is not a production server.
package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:0", "listen address")
	tables := flag.String("tables", "notes,tasks", "comma-separated tables")
	validate := flag.Bool("validate", false, "enable crdt.DefaultValidationConfig")
	watchStdin := flag.Bool("watch-stdin", true,
		"exit when stdin reaches end of file (the test harness holds it open)")
	flag.Parse()

	if *watchStdin {
		go exitOnEOF(os.Stdin, func() {
			log.Print("stdin closed; exiting")
			os.Exit(0)
		})
	}

	srv, err := newServer(strings.Split(*tables, ","), *validate)
	if err != nil {
		log.Fatal(err)
	}
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Fprintf(os.Stdout, "LISTENING http://%s\n", ln.Addr())
	log.Fatal(http.Serve(ln, srv.routes()))
}

// exitOnEOF reads r until it ends, then calls exit. The Dart harness keeps
// the child's stdin open for the server's whole life, so stdin ends only when
// the harness closes it or the test runner dies. A runner killed outside its
// tearDown then cannot leave the server running. A read error counts as the
// end too: either way nobody is left to stop the server.
func exitOnEOF(r io.Reader, exit func()) {
	_, _ = io.Copy(io.Discard, r)
	exit()
}
