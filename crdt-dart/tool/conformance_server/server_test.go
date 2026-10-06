package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

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
