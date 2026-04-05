package crdt

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMetrics_Snapshot(t *testing.T) {
	m := &Metrics{}
	m.PullCount.Store(5)
	m.PushCount.Store(3)
	m.PullErrors.Store(1)
	m.PushErrors.Store(2)
	m.ChangesPulled.Store(50)
	m.ChangesPushed.Store(30)
	m.ChangesMerged.Store(20)
	m.ConflictsTotal.Store(10)
	m.ConflictsLWW.Store(4)
	m.ConflictsSet.Store(3)
	m.ConflictsList.Store(3)
	m.ActiveRooms.Store(2)
	m.ActiveParticipants.Store(7)
	m.PresenceUpdates.Store(100)
	m.SSEConnections.Store(1)
	m.WSConnections.Store(2)
	m.ValidationErrors.Store(5)

	snap := m.Snapshot()
	assert.Equal(t, int64(5), snap.PullCount)
	assert.Equal(t, int64(3), snap.PushCount)
	assert.Equal(t, int64(1), snap.PullErrors)
	assert.Equal(t, int64(2), snap.PushErrors)
	assert.Equal(t, int64(50), snap.ChangesPulled)
	assert.Equal(t, int64(30), snap.ChangesPushed)
	assert.Equal(t, int64(20), snap.ChangesMerged)
	assert.Equal(t, int64(10), snap.ConflictsTotal)
	assert.Equal(t, int64(4), snap.ConflictsLWW)
	assert.Equal(t, int64(3), snap.ConflictsSet)
	assert.Equal(t, int64(3), snap.ConflictsList)
	assert.Equal(t, int64(2), snap.ActiveRooms)
	assert.Equal(t, int64(7), snap.ActiveParticipants)
	assert.Equal(t, int64(100), snap.PresenceUpdates)
	assert.Equal(t, int64(1), snap.SSEConnections)
	assert.Equal(t, int64(2), snap.WSConnections)
	assert.Equal(t, int64(5), snap.ValidationErrors)
	assert.False(t, snap.Timestamp.IsZero())
}

func TestMetrics_RecordConflict_LWW(t *testing.T) {
	m := &Metrics{}
	m.RecordConflict(TypeLWW)
	assert.Equal(t, int64(1), m.ConflictsTotal.Load())
	assert.Equal(t, int64(1), m.ConflictsLWW.Load())
	assert.Equal(t, int64(0), m.ConflictsSet.Load())
	assert.Equal(t, int64(0), m.ConflictsList.Load())
}

func TestMetrics_RecordConflict_Set(t *testing.T) {
	m := &Metrics{}
	m.RecordConflict(TypeSet)
	assert.Equal(t, int64(1), m.ConflictsTotal.Load())
	assert.Equal(t, int64(0), m.ConflictsLWW.Load())
	assert.Equal(t, int64(1), m.ConflictsSet.Load())
}

func TestMetrics_RecordConflict_List(t *testing.T) {
	m := &Metrics{}
	m.RecordConflict(TypeList)
	assert.Equal(t, int64(1), m.ConflictsTotal.Load())
	assert.Equal(t, int64(1), m.ConflictsList.Load())
}

func TestMetrics_RecordConflict_UnknownType(t *testing.T) {
	m := &Metrics{}
	m.RecordConflict(CRDTType("unknown"))
	// Only total should be incremented, no specific counter.
	assert.Equal(t, int64(1), m.ConflictsTotal.Load())
	assert.Equal(t, int64(0), m.ConflictsLWW.Load())
	assert.Equal(t, int64(0), m.ConflictsSet.Load())
	assert.Equal(t, int64(0), m.ConflictsList.Load())
}

func TestMetrics_Reset(t *testing.T) {
	m := &Metrics{}
	m.PullCount.Store(10)
	m.PushCount.Store(5)
	m.ConflictsTotal.Store(3)
	m.ActiveRooms.Store(2)
	m.SSEConnections.Store(1)
	m.WSConnections.Store(4)
	m.ValidationErrors.Store(7)

	m.Reset()

	snap := m.Snapshot()
	assert.Equal(t, int64(0), snap.PullCount)
	assert.Equal(t, int64(0), snap.PushCount)
	assert.Equal(t, int64(0), snap.ConflictsTotal)
	assert.Equal(t, int64(0), snap.ActiveRooms)
	assert.Equal(t, int64(0), snap.SSEConnections)
	assert.Equal(t, int64(0), snap.WSConnections)
	assert.Equal(t, int64(0), snap.ValidationErrors)
}

func TestMetrics_ConcurrentAccess(t *testing.T) {
	m := &Metrics{}
	const goroutines = 100
	const iterations = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				m.PullCount.Add(1)
				m.PushCount.Add(1)
				m.RecordConflict(TypeLWW)
				m.PresenceUpdates.Add(1)
				_ = m.Snapshot()
			}
		}()
	}

	wg.Wait()

	expected := int64(goroutines * iterations)
	assert.Equal(t, expected, m.PullCount.Load())
	assert.Equal(t, expected, m.PushCount.Load())
	assert.Equal(t, expected, m.ConflictsTotal.Load())
	assert.Equal(t, expected, m.ConflictsLWW.Load())
	assert.Equal(t, expected, m.PresenceUpdates.Load())
}
