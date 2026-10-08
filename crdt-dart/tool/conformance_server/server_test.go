package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/xraph/grove/crdt"
)

func TestPushThenPullRoundTrips(t *testing.T) {
	s, err := newServer([]string{"notes"}, false)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(s.routes())
	defer ts.Close()

	push := crdt.PushRequest{NodeID: "a", Changes: []crdt.ChangeRecord{{
		Table: "notes", PK: "1", Field: "title", CRDTType: crdt.TypeLWW,
		HLC: crdt.HLC{Timestamp: 10, NodeID: "a"}, NodeID: "a", Value: json.RawMessage(`"hi"`),
	}}}
	body, _ := json.Marshal(push)
	resp, err := http.Post(ts.URL+"/sync/push", "application/json", bytes.NewReader(body))
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("push: %v %v", err, resp.StatusCode)
	}

	body, _ = json.Marshal(crdt.PullRequest{Tables: []string{"notes"}, NodeID: "b"})
	resp, err = http.Post(ts.URL+"/sync/pull", "application/json", bytes.NewReader(body))
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("pull: %v %v", err, resp.StatusCode)
	}
	var pulled crdt.PullResponse
	if err := json.NewDecoder(resp.Body).Decode(&pulled); err != nil {
		t.Fatal(err)
	}
	if len(pulled.Changes) != 1 || string(pulled.Changes[0].Value) != `"hi"` {
		t.Fatalf("pulled %+v", pulled.Changes)
	}
}

func TestUnknownDatasetIs404(t *testing.T) {
	s, _ := newServer([]string{"notes"}, false)
	ts := httptest.NewServer(s.routes())
	defer ts.Close()
	resp, _ := http.Post(ts.URL+"/api/v1/datasets/nope/sync/pull", "application/json", bytes.NewReader([]byte(`{}`)))
	if resp.StatusCode != 404 {
		t.Fatalf("status %d", resp.StatusCode)
	}
}

func presenceNodes(t *testing.T, base, topic string) []string {
	t.Helper()
	resp, err := http.Get(base + "/sync/presence?topic=" + topic)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var snap crdt.PresenceSnapshot
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		t.Fatal(err)
	}
	var nodes []string
	for _, st := range snap.States {
		nodes = append(nodes, st.NodeID)
	}
	return nodes
}

func TestStreamDisconnectRemovesThatNodesPresence(t *testing.T) {
	s, err := newServer([]string{"notes"}, false)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(s.routes())
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/sync/stream?tables=notes&node_id=a", nil)
	stream, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Body.Close()

	for _, node := range []string{"a", "b"} {
		body, _ := json.Marshal(crdt.PresenceUpdate{NodeID: node, Topic: "notes:n1", Data: json.RawMessage(`{"cursor":1}`)})
		resp, err := http.Post(ts.URL+"/sync/presence", "application/json", bytes.NewReader(body))
		if err != nil || resp.StatusCode != 200 {
			t.Fatalf("presence %s: %v", node, err)
		}
		resp.Body.Close()
	}
	if got := presenceNodes(t, ts.URL, "notes:n1"); len(got) != 2 {
		t.Fatalf("before disconnect: %v", got)
	}

	cancel()
	deadline := time.Now().Add(3 * time.Second)
	for {
		got := presenceNodes(t, ts.URL, "notes:n1")
		if len(got) == 1 && got[0] == "b" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("after disconnect want only b, got %v", got)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
