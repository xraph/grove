// Command conformance_server is a grove crdt sync server for grove_crdt's
// Dart conformance tests. It is not a production server.
package main

import (
	"flag"
	"fmt"
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
	flag.Parse()

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
