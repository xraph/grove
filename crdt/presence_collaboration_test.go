package crdt

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPresencePatchConcurrentFields(t *testing.T) {
	pm := NewPresenceManager(time.Minute, nil, nil)
	defer pm.Close()
	pm.Update(PresenceUpdate{NodeID: "a", Topic: "room", Data: json.RawMessage(`{"custom":{"role":"editor"}}`)})
	var wg sync.WaitGroup
	for i := range 32 {
		wg.Go(func() {
			_, err := pm.Patch(PresenceUpdate{NodeID: "a", Topic: "room", Data: json.RawMessage(fmt.Sprintf(`{"field%d":%d}`, i, i))})
			require.NoError(t, err)
		})
	}
	wg.Wait()
	var data map[string]any
	require.NoError(t, json.Unmarshal(pm.Get("room")[0].Data, &data))
	require.Len(t, data, 33)
	require.Equal(t, map[string]any{"role": "editor"}, data["custom"])
	for _, invalid := range []string{`null`, `[]`, `true`, `"bad"`} {
		_, err := pm.Patch(PresenceUpdate{NodeID: "a", Topic: "room", Data: json.RawMessage(invalid)})
		require.Error(t, err)
	}
	require.Len(t, pm.Get("room"), 1)
}

func TestPresenceCopiesPayloadAndAllowsReentrantCallbacks(t *testing.T) {
	var events []string
	var pm *PresenceManager
	pm = NewPresenceManager(time.Minute, func(event PresenceEvent) {
		require.Len(t, pm.Get("room"), 1)
		events = append(events, event.Type)
		if event.Type == PresenceJoin {
			event.Data[0] = '!'
			_, err := pm.Patch(PresenceUpdate{NodeID: "a", Topic: "room", Data: json.RawMessage(`{"typing":true}`)})
			require.NoError(t, err)
		}
	}, nil)
	defer pm.Close()
	data := json.RawMessage(`{"name":"alice"}`)
	result := pm.Update(PresenceUpdate{NodeID: "a", Topic: "room", Data: data})
	data[0] = '!'
	result.Data[0] = '!'
	states := pm.Get("room")
	states[0].Data[0] = '!'
	require.JSONEq(t, `{"name":"alice","typing":true}`, string(pm.Get("room")[0].Data))
	require.Equal(t, []string{PresenceJoin, PresenceUpdateEvt}, events)
}

func TestPresenceReadHidesExpiredEntries(t *testing.T) {
	pm := NewPresenceManager(time.Minute, nil, nil)
	pm.Update(PresenceUpdate{NodeID: "a", Topic: "room", Data: json.RawMessage(`{}`)})
	pm.mu.Lock()
	pm.states["room"]["a"].ExpiresAt = time.Now().Add(-time.Second)
	pm.mu.Unlock()
	require.Empty(t, pm.Get("room"))
	require.Empty(t, pm.GetTopicsForNode("a"))
	event := pm.Update(PresenceUpdate{NodeID: "a", Topic: "room", Data: json.RawMessage(`{}`)})
	require.Equal(t, PresenceJoin, event.Type)
	pm.Close()
	pm.Close()
}

func TestRoomCursorAndTypingPreserveCustomData(t *testing.T) {
	pm := NewPresenceManager(time.Minute, nil, nil)
	defer pm.Close()
	rooms := NewRoomManager(pm, nil)
	pm.Update(PresenceUpdate{NodeID: "a", Topic: "room", Data: json.RawMessage(`{"custom":"keep","name":"alice"}`)})
	var wg sync.WaitGroup
	wg.Go(func() {
		for range 100 {
			rooms.UpdateCursor("room", "a", CursorPosition{X: 10, Y: 20})
		}
	})
	wg.Go(func() {
		for range 100 {
			rooms.UpdateTypingStatus("room", "a", true)
		}
	})
	wg.Wait()
	require.JSONEq(t, `{"custom":"keep","name":"alice","cursor":{"x":10,"y":20},"is_typing":true}`, string(pm.Get("room")[0].Data))
}

func TestPresenceCallbackPanicDoesNotDisableLaterEvents(t *testing.T) {
	calls := 0
	pm := NewPresenceManager(time.Minute, func(PresenceEvent) {
		calls++
		if calls == 1 {
			panic("callback failed")
		}
	}, nil)
	defer pm.Close()
	update := PresenceUpdate{NodeID: "a", Topic: "room", Data: json.RawMessage(`{}`)}
	require.Panics(t, func() { pm.Update(update) })
	pm.Update(update)
	require.Equal(t, 2, calls)
}
