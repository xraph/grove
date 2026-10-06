package extension

import (
	"context"
	"encoding/json"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/xraph/forge"

	"github.com/xraph/grove/crdt"
)

func syncDoc(t *testing.T, entities []crdtEntity) map[string]map[string]map[string]any {
	t.Helper()

	r := forge.NewRouter(forge.WithOpenAPI(forge.OpenAPIConfig{Title: "grove", Version: "1.0.0"}))
	ctrl := &crdtForgeController{ctrl: crdt.NewSyncController(crdt.New(crdt.WithNodeID("test"))), entities: entities}
	if err := ctrl.Routes(r); err != nil {
		t.Fatal(err)
	}

	raw, err := json.Marshal(r.OpenAPISpec())
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Paths map[string]map[string]map[string]any `json:"paths"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}

	return doc.Paths
}

func TestCRDTRoutesDeclareXForgeSyncPerEntity(t *testing.T) {
	paths := syncDoc(t, []crdtEntity{{table: "documents", entity: "Document"}})

	for _, tc := range []struct{ path, method, role string }{
		{"/sync/pull", "post", "pull"},
		{"/sync/push", "post", "push"},
		{"/sync/stream", "get", "stream"},
		{"/sync/ws", "get", "socket"},
	} {
		got := paths[tc.path][tc.method]["x-forge-sync"]
		want := map[string]any{"protocol": "grove-crdt", "entity": "Document", "table": "documents", "role": tc.role}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s %s x-forge-sync = %#v, want %#v", tc.method, tc.path, got, want)
		}
	}
}

func TestCRDTRoutesDeclareEveryMappedTable(t *testing.T) {
	paths := syncDoc(t, []crdtEntity{{table: "documents", entity: "Document"}, {table: "comments", entity: "Comment"}})

	list, ok := paths["/sync/pull"]["post"]["x-forge-sync"].([]any)
	if !ok || len(list) != 2 {
		t.Fatalf("x-forge-sync = %#v, want two declarations", paths["/sync/pull"]["post"]["x-forge-sync"])
	}
}

func TestCRDTRoutesWithoutEntitiesDeclareNothing(t *testing.T) {
	paths := syncDoc(t, nil)

	if _, ok := paths["/sync/pull"]["post"]["x-forge-sync"]; ok {
		t.Fatalf("x-forge-sync emitted without an entity mapping")
	}
}

func TestWithCRDTEntityAccumulates(t *testing.T) {
	e := &Extension{}
	WithCRDTEntity("documents", "Document")(e)
	WithCRDTEntity("comments", "Comment")(e)

	if len(e.crdtEntities) != 2 || e.crdtEntities[1].entity != "Comment" {
		t.Fatalf("crdtEntities = %#v", e.crdtEntities)
	}
}

// recordingStream is a forge.Stream that records the frames the handler writes.
// Only the methods handleStream uses are implemented.
type recordingStream struct {
	forge.Stream
	ctx context.Context

	mu       sync.Mutex
	comments []string
	frames   chan string
}

func (s *recordingStream) Context() context.Context { return s.ctx }

func (s *recordingStream) Send(event string, _ []byte) error {
	s.frames <- "event:" + event
	return nil
}

func (s *recordingStream) SendComment(comment string) error {
	s.mu.Lock()
	s.comments = append(s.comments, comment)
	s.mu.Unlock()
	s.frames <- "comment:" + comment

	return nil
}

// queryContext is the slice of forge.Context that handleStream reads.
type queryContext struct {
	forgeContext
}

// forgeContext names the embedded interface something other than Context,
// which would collide with its Context method.
type forgeContext = forge.Context

func (queryContext) Query(string) string { return "" }

// emptyExecutor is a crdt.Executor over an empty metadata store.
type emptyExecutor struct{}

func (emptyExecutor) ExecContext(context.Context, string, ...any) (crdt.ExecResult, error) {
	return emptyResult{}, nil
}

func (emptyExecutor) QueryContext(context.Context, string, ...any) (crdt.Rows, error) {
	return emptyRows{}, nil
}

type emptyResult struct{}

func (emptyResult) RowsAffected() (int64, error) { return 0, nil }

type emptyRows struct{}

func (emptyRows) Next() bool        { return false }
func (emptyRows) Scan(...any) error { return nil }
func (emptyRows) Close() error      { return nil }
func (emptyRows) Err() error        { return nil }

func TestCRDTStreamSendsKeepAliveWhenIdle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	plugin := crdt.New(crdt.WithNodeID("test"))
	plugin.SetExecutor(emptyExecutor{})

	ctrl := &crdtForgeController{
		ctrl:      crdt.NewSyncController(plugin),
		keepAlive: 20 * time.Millisecond,
	}
	stream := &recordingStream{ctx: ctx, frames: make(chan string, 16)}

	done := make(chan error, 1)
	go func() { done <- ctrl.handleStream(queryContext{}, stream) }()

	for range 2 {
		select {
		case frame := <-stream.frames:
			if frame != "comment:keep-alive" {
				t.Fatalf("frame = %q, want comment:keep-alive", frame)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("idle stream sent no keep-alive within the interval")
		}
	}

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("handleStream returned %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handleStream did not return after the stream closed")
	}
}

func TestCRDTStreamDefaultKeepAliveIsFifteenSeconds(t *testing.T) {
	if defaultStreamKeepAlive != 15*time.Second {
		t.Fatalf("defaultStreamKeepAlive = %v, want 15s", defaultStreamKeepAlive)
	}
}
