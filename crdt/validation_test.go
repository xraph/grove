package crdt

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultValidationConfig(t *testing.T) {
	cfg := DefaultValidationConfig()
	require.NotNil(t, cfg)
	assert.Equal(t, 1*1024*1024, cfg.MaxChangeValueSize)
	assert.Equal(t, 10000, cfg.MaxChangesPerPush)
	assert.Equal(t, 1*time.Hour, cfg.MaxHLCDrift)
	assert.Equal(t, 100*1024, cfg.MaxRoomMetadataSize)
	assert.Equal(t, 10*1024, cfg.MaxParticipantDataSize)
}

func TestValidateChangeRecord_Valid(t *testing.T) {
	cfg := DefaultValidationConfig()
	change := &ChangeRecord{
		Table:    "documents",
		PK:       "doc-1",
		Field:    "title",
		NodeID:   "node-a",
		CRDTType: TypeLWW,
		HLC:      HLC{Timestamp: time.Now().UnixNano(), Counter: 0, NodeID: "node-a"},
		Value:    json.RawMessage(`"hello"`),
	}
	assert.NoError(t, cfg.ValidateChangeRecord(change))
}

func TestValidateChangeRecord_EmptyTable(t *testing.T) {
	cfg := DefaultValidationConfig()
	change := &ChangeRecord{
		Table:    "",
		PK:       "doc-1",
		NodeID:   "node-a",
		CRDTType: TypeLWW,
		HLC:      HLC{Timestamp: time.Now().UnixNano(), NodeID: "node-a"},
	}
	err := cfg.ValidateChangeRecord(change)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "table is required")
}

func TestValidateChangeRecord_EmptyPK(t *testing.T) {
	cfg := DefaultValidationConfig()
	change := &ChangeRecord{
		Table:    "documents",
		PK:       "",
		NodeID:   "node-a",
		CRDTType: TypeLWW,
		HLC:      HLC{Timestamp: time.Now().UnixNano(), NodeID: "node-a"},
	}
	err := cfg.ValidateChangeRecord(change)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pk is required")
}

func TestValidateChangeRecord_EmptyNodeID(t *testing.T) {
	cfg := DefaultValidationConfig()
	change := &ChangeRecord{
		Table:    "documents",
		PK:       "doc-1",
		NodeID:   "",
		CRDTType: TypeLWW,
		HLC:      HLC{Timestamp: time.Now().UnixNano()},
	}
	err := cfg.ValidateChangeRecord(change)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "node_id is required")
}

func TestValidateChangeRecord_UnknownCRDTType(t *testing.T) {
	cfg := DefaultValidationConfig()
	change := &ChangeRecord{
		Table:    "documents",
		PK:       "doc-1",
		NodeID:   "node-a",
		CRDTType: CRDTType("banana"),
		HLC:      HLC{Timestamp: time.Now().UnixNano(), NodeID: "node-a"},
	}
	err := cfg.ValidateChangeRecord(change)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown crdt type")
}

func TestValidateChangeRecord_TombstoneSkipsCRDTTypeCheck(t *testing.T) {
	cfg := DefaultValidationConfig()
	change := &ChangeRecord{
		Table:     "documents",
		PK:        "doc-1",
		NodeID:    "node-a",
		CRDTType:  CRDTType(""),
		HLC:       HLC{Timestamp: time.Now().UnixNano(), NodeID: "node-a"},
		Tombstone: true,
	}
	assert.NoError(t, cfg.ValidateChangeRecord(change))
}

func TestValidateChangeRecord_ValueTooLarge(t *testing.T) {
	cfg := &ValidationConfig{MaxChangeValueSize: 10}
	change := &ChangeRecord{
		Table:    "documents",
		PK:       "doc-1",
		NodeID:   "node-a",
		CRDTType: TypeLWW,
		HLC:      HLC{Timestamp: time.Now().UnixNano(), NodeID: "node-a"},
		Value:    json.RawMessage(strings.Repeat("x", 20)),
	}
	err := cfg.ValidateChangeRecord(change)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds max size")
}

func TestValidateChangeRecord_HLCDriftTooLarge(t *testing.T) {
	cfg := DefaultValidationConfig()
	change := &ChangeRecord{
		Table:    "documents",
		PK:       "doc-1",
		NodeID:   "node-a",
		CRDTType: TypeLWW,
		HLC:      HLC{Timestamp: time.Now().Add(2 * time.Hour).UnixNano(), NodeID: "node-a"},
		Value:    json.RawMessage(`"v"`),
	}
	err := cfg.ValidateChangeRecord(change)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "drift too large")
}

func TestValidatePushRequest_Valid(t *testing.T) {
	cfg := DefaultValidationConfig()
	req := &PushRequest{
		NodeID: "node-a",
		Changes: []ChangeRecord{
			{
				Table:    "docs",
				PK:       "1",
				Field:    "title",
				NodeID:   "node-a",
				CRDTType: TypeLWW,
				HLC:      HLC{Timestamp: time.Now().UnixNano(), NodeID: "node-a"},
				Value:    json.RawMessage(`"v"`),
			},
		},
	}
	assert.NoError(t, cfg.ValidatePushRequest(req))
}

func TestValidatePushRequest_EmptyNodeID(t *testing.T) {
	cfg := DefaultValidationConfig()
	req := &PushRequest{NodeID: "", Changes: nil}
	err := cfg.ValidatePushRequest(req)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "push node_id is required")
}

func TestValidatePushRequest_TooManyChanges(t *testing.T) {
	cfg := &ValidationConfig{MaxChangesPerPush: 2}
	req := &PushRequest{
		NodeID: "node-a",
		Changes: []ChangeRecord{
			{Table: "a", PK: "1", NodeID: "n", CRDTType: TypeLWW},
			{Table: "b", PK: "2", NodeID: "n", CRDTType: TypeLWW},
			{Table: "c", PK: "3", NodeID: "n", CRDTType: TypeLWW},
		},
	}
	err := cfg.ValidatePushRequest(req)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds max changes")
}

func TestValidatePushRequest_IndividualChangeValidationCascades(t *testing.T) {
	cfg := DefaultValidationConfig()
	req := &PushRequest{
		NodeID: "node-a",
		Changes: []ChangeRecord{
			{Table: "", PK: "1", NodeID: "node-a", CRDTType: TypeLWW}, // empty table
		},
	}
	err := cfg.ValidatePushRequest(req)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "change[0]")
	assert.Contains(t, err.Error(), "table is required")
}

func TestValidatePresenceData_Valid(t *testing.T) {
	cfg := DefaultValidationConfig()
	data := []byte(`{"name":"Alice"}`)
	assert.NoError(t, cfg.ValidatePresenceData(data))
}

func TestValidatePresenceData_TooLarge(t *testing.T) {
	cfg := &ValidationConfig{MaxParticipantDataSize: 5}
	data := []byte(strings.Repeat("x", 10))
	err := cfg.ValidatePresenceData(data)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "presence data exceeds max size")
}

func TestValidateRoomMetadata_Valid(t *testing.T) {
	cfg := DefaultValidationConfig()
	data := []byte(`{"title":"My Room"}`)
	assert.NoError(t, cfg.ValidateRoomMetadata(data))
}

func TestValidateRoomMetadata_TooLarge(t *testing.T) {
	cfg := &ValidationConfig{MaxRoomMetadataSize: 5}
	data := []byte(strings.Repeat("x", 10))
	err := cfg.ValidateRoomMetadata(data)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "room metadata exceeds max size")
}
