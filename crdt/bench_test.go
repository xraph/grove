package crdt

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

// --- Merge Benchmarks ---

func BenchmarkMergeLWW(b *testing.B) {
	local := &LWWRegister{
		Value:  json.RawMessage(`"local value"`),
		Clock:  HLC{Timestamp: 100, Counter: 1, NodeID: "node-a"},
		NodeID: "node-a",
	}
	remote := &LWWRegister{
		Value:  json.RawMessage(`"remote value"`),
		Clock:  HLC{Timestamp: 200, Counter: 1, NodeID: "node-b"},
		NodeID: "node-b",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MergeLWW(local, remote)
	}
}

func BenchmarkMergeCounter(b *testing.B) {
	for _, nodes := range []int{2, 10, 100} {
		b.Run(fmt.Sprintf("%d-nodes", nodes), func(b *testing.B) {
			local := NewPNCounterState()
			remote := NewPNCounterState()
			for i := 0; i < nodes; i++ {
				nodeID := fmt.Sprintf("node-%d", i)
				local.Increments[nodeID] = int64(i * 10)
				local.Decrements[nodeID] = int64(i)
				remote.Increments[nodeID] = int64(i * 10)
				remote.Decrements[nodeID] = int64(i + 1)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				MergeCounter(local, remote)
			}
		})
	}
}

func BenchmarkMergeSet(b *testing.B) {
	for _, size := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("%d-elements", size), func(b *testing.B) {
			local := NewORSetState()
			remote := NewORSetState()
			for i := 0; i < size; i++ {
				elem := fmt.Sprintf("elem-%d", i)
				local.Entries[elem] = &ORSetEntry{
					Value: json.RawMessage(fmt.Sprintf(`"%s"`, elem)),
					Clock: HLC{Timestamp: int64(i), NodeID: "node-a"},
					Alive: true,
				}
				remote.Entries[elem] = &ORSetEntry{
					Value: json.RawMessage(fmt.Sprintf(`"%s"`, elem)),
					Clock: HLC{Timestamp: int64(i + 1), NodeID: "node-b"},
					Alive: true,
				}
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				MergeSet(local, remote)
			}
		})
	}
}

func BenchmarkMergeEngine_MergeField(b *testing.B) {
	engine := NewMergeEngine()

	b.Run("LWW", func(b *testing.B) {
		local := &FieldState{
			Type:   TypeLWW,
			HLC:    HLC{Timestamp: 100, NodeID: "node-a"},
			NodeID: "node-a",
			Value:  json.RawMessage(`"local"`),
		}
		remote := &FieldState{
			Type:   TypeLWW,
			HLC:    HLC{Timestamp: 200, NodeID: "node-b"},
			NodeID: "node-b",
			Value:  json.RawMessage(`"remote"`),
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			engine.MergeField(local, remote)
		}
	})

	b.Run("Counter", func(b *testing.B) {
		cs1 := NewPNCounterState()
		cs1.Increments["a"] = 10
		cs2 := NewPNCounterState()
		cs2.Increments["b"] = 20
		local := &FieldState{Type: TypeCounter, CounterState: cs1}
		remote := &FieldState{Type: TypeCounter, CounterState: cs2}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			engine.MergeField(local, remote)
		}
	})

	b.Run("Set", func(b *testing.B) {
		s1 := NewORSetState()
		s1.Entries["x"] = &ORSetEntry{
			Value: json.RawMessage(`"x"`),
			Clock: HLC{Timestamp: 100, NodeID: "a"},
			Alive: true,
		}
		s2 := NewORSetState()
		s2.Entries["y"] = &ORSetEntry{
			Value: json.RawMessage(`"y"`),
			Clock: HLC{Timestamp: 200, NodeID: "b"},
			Alive: true,
		}
		local := &FieldState{Type: TypeSet, SetState: s1}
		remote := &FieldState{Type: TypeSet, SetState: s2}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			engine.MergeField(local, remote)
		}
	})
}

func BenchmarkMergeEngine_MergeState(b *testing.B) {
	engine := NewMergeEngine()

	for _, fieldCount := range []int{5, 20} {
		b.Run(fmt.Sprintf("%d-fields", fieldCount), func(b *testing.B) {
			local := NewState("docs", "1")
			remote := NewState("docs", "1")
			for i := 0; i < fieldCount; i++ {
				f := fmt.Sprintf("field_%d", i)
				local.Fields[f] = &FieldState{
					Type:   TypeLWW,
					HLC:    HLC{Timestamp: int64(i * 10), NodeID: "a"},
					NodeID: "a",
					Value:  json.RawMessage(fmt.Sprintf(`"val_a_%d"`, i)),
				}
				remote.Fields[f] = &FieldState{
					Type:   TypeLWW,
					HLC:    HLC{Timestamp: int64(i*10 + 5), NodeID: "b"},
					NodeID: "b",
					Value:  json.RawMessage(fmt.Sprintf(`"val_b_%d"`, i)),
				}
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				engine.MergeState(local, remote)
			}
		})
	}
}

// --- HLC Benchmarks ---

func BenchmarkHLC_Now(b *testing.B) {
	clock := NewHybridClock("bench-node")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		clock.Now()
	}
}

func BenchmarkHLC_Update(b *testing.B) {
	clock := NewHybridClock("bench-node")
	remote := HLC{Timestamp: 100, Counter: 5, NodeID: "remote"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		clock.Update(remote)
	}
}

func BenchmarkHLC_Compare(b *testing.B) {
	h1 := HLC{Timestamp: 100, Counter: 5, NodeID: "a"}
	h2 := HLC{Timestamp: 100, Counter: 5, NodeID: "b"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h1.After(h2)
	}
}

// --- Sync Hook Benchmarks ---

func BenchmarkSyncHookChain(b *testing.B) {
	change := &ChangeRecord{
		Table:    "docs",
		PK:       "1",
		Field:    "title",
		CRDTType: TypeLWW,
		HLC:      HLC{Timestamp: 100, NodeID: "node-a"},
		NodeID:   "node-a",
		Value:    json.RawMessage(`"hello"`),
	}
	ctx := context.Background()

	b.Run("0-hooks", func(b *testing.B) {
		chain := NewSyncHookChain()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			chain.BeforeInboundChange(ctx, change)
		}
	})

	b.Run("1-hook", func(b *testing.B) {
		chain := NewSyncHookChain(&BaseSyncHook{})
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			chain.BeforeInboundChange(ctx, change)
		}
	})

	b.Run("5-hooks", func(b *testing.B) {
		chain := NewSyncHookChain(
			&BaseSyncHook{}, &BaseSyncHook{}, &BaseSyncHook{},
			&BaseSyncHook{}, &BaseSyncHook{},
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			chain.BeforeInboundChange(ctx, change)
		}
	})
}

// --- Server Handler Benchmarks ---

func BenchmarkHandlePull(b *testing.B) {
	lwwState, _ := json.Marshal(&FieldState{
		Type:   TypeLWW,
		Value:  json.RawMessage(`"hello"`),
		NodeID: "node-a",
	})

	for _, count := range []int{0, 10, 100} {
		b.Run(fmt.Sprintf("%d-changes", count), func(b *testing.B) {
			var rows []mockRow
			for i := 0; i < count; i++ {
				rows = append(rows, mockRow{
					pkHash:    fmt.Sprintf("pk-%d", i),
					fieldName: "title",
					hlcTS:     int64(i * 100),
					hlcCount:  1,
					nodeID:    "node-a",
					crdtState: lwwState,
				})
			}
			plugin := newTestPluginWithChanges(rows)
			handler := NewHTTPHandler(plugin)
			server := httptest.NewServer(handler)
			defer server.Close()

			reqBody, _ := json.Marshal(PullRequest{
				Tables: []string{"docs"},
				NodeID: "bench-client",
			})

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				resp, err := http.Post(server.URL+"/pull", "application/json", bytes.NewReader(reqBody))
				if err != nil {
					b.Fatal(err)
				}
				resp.Body.Close()
			}
		})
	}
}

func BenchmarkHandlePush(b *testing.B) {
	for _, count := range []int{1, 10, 100} {
		b.Run(fmt.Sprintf("%d-changes", count), func(b *testing.B) {
			plugin := newTestPluginWithChanges(nil)
			handler := NewHTTPHandler(plugin)
			server := httptest.NewServer(handler)
			defer server.Close()

			var changes []ChangeRecord
			for i := 0; i < count; i++ {
				changes = append(changes, ChangeRecord{
					Table:    "docs",
					PK:       fmt.Sprintf("pk-%d", i),
					Field:    "title",
					CRDTType: TypeLWW,
					HLC:      HLC{Timestamp: int64(i * 100), Counter: 1, NodeID: "bench-remote"},
					NodeID:   "bench-remote",
					Value:    json.RawMessage(`"value"`),
				})
			}
			reqBody, _ := json.Marshal(PushRequest{
				Changes: changes,
				NodeID:  "bench-remote",
			})

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				resp, err := http.Post(server.URL+"/push", "application/json", bytes.NewReader(reqBody))
				if err != nil {
					b.Fatal(err)
				}
				resp.Body.Close()
			}
		})
	}
}

// --- JSON Serialization Benchmarks ---

func BenchmarkChangeRecord_Marshal(b *testing.B) {
	record := ChangeRecord{
		Table:    "documents",
		PK:       "doc-123",
		Field:    "title",
		CRDTType: TypeLWW,
		HLC:      HLC{Timestamp: 1000000, Counter: 5, NodeID: "node-us-east-1"},
		NodeID:   "node-us-east-1",
		Value:    json.RawMessage(`"The quick brown fox jumps over the lazy dog"`),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		json.Marshal(record)
	}
}

func BenchmarkChangeRecord_Unmarshal(b *testing.B) {
	data := []byte(`{"table":"documents","pk":"doc-123","field":"title","crdt_type":"lww","hlc":{"ts":1000000,"counter":5,"node_id":"node-us-east-1"},"node_id":"node-us-east-1","value":"The quick brown fox jumps over the lazy dog"}`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var record ChangeRecord
		json.Unmarshal(data, &record)
	}
}
