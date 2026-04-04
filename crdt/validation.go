package crdt

import (
	"fmt"
	"time"
)

// ValidationConfig controls input validation for sync operations.
type ValidationConfig struct {
	// MaxChangeValueSize is the maximum byte size for a single field value (default: 1MB).
	MaxChangeValueSize int
	// MaxChangesPerPush is the maximum number of changes in a single push (default: 10000).
	MaxChangesPerPush int
	// MaxHLCDrift is the maximum allowed HLC timestamp drift from server time (default: 1 hour).
	MaxHLCDrift time.Duration
	// MaxRoomMetadataSize is the maximum byte size for room metadata (default: 100KB).
	MaxRoomMetadataSize int
	// MaxParticipantDataSize is the maximum byte size for participant presence data (default: 10KB).
	MaxParticipantDataSize int
}

// DefaultValidationConfig returns sensible defaults.
func DefaultValidationConfig() *ValidationConfig {
	return &ValidationConfig{
		MaxChangeValueSize:     1 * 1024 * 1024, // 1MB
		MaxChangesPerPush:      10000,
		MaxHLCDrift:            1 * time.Hour,
		MaxRoomMetadataSize:    100 * 1024, // 100KB
		MaxParticipantDataSize: 10 * 1024,  // 10KB
	}
}

// ValidateChangeRecord validates a single change record.
func (vc *ValidationConfig) ValidateChangeRecord(change *ChangeRecord) error {
	if change.Table == "" {
		return fmt.Errorf("crdt: change table is required")
	}
	if change.PK == "" {
		return fmt.Errorf("crdt: change pk is required")
	}
	if change.NodeID == "" {
		return fmt.Errorf("crdt: change node_id is required")
	}
	if !ValidCRDTType(string(change.CRDTType)) && !change.Tombstone {
		return fmt.Errorf("crdt: unknown crdt type: %s", change.CRDTType)
	}
	if vc.MaxChangeValueSize > 0 && len(change.Value) > vc.MaxChangeValueSize {
		return fmt.Errorf("crdt: change value exceeds max size (%d > %d bytes)", len(change.Value), vc.MaxChangeValueSize)
	}
	if vc.MaxHLCDrift > 0 {
		now := time.Now().UnixNano()
		drift := change.HLC.Timestamp - now
		if drift < 0 {
			drift = -drift
		}
		if drift > vc.MaxHLCDrift.Nanoseconds() {
			return fmt.Errorf("crdt: change HLC timestamp drift too large (%v)", time.Duration(drift))
		}
	}
	return nil
}

// ValidatePushRequest validates a push request.
func (vc *ValidationConfig) ValidatePushRequest(req *PushRequest) error {
	if req.NodeID == "" {
		return fmt.Errorf("crdt: push node_id is required")
	}
	if vc.MaxChangesPerPush > 0 && len(req.Changes) > vc.MaxChangesPerPush {
		return fmt.Errorf("crdt: push exceeds max changes (%d > %d)", len(req.Changes), vc.MaxChangesPerPush)
	}
	for i := range req.Changes {
		if err := vc.ValidateChangeRecord(&req.Changes[i]); err != nil {
			return fmt.Errorf("crdt: change[%d]: %w", i, err)
		}
	}
	return nil
}

// ValidatePresenceData validates presence payload size.
func (vc *ValidationConfig) ValidatePresenceData(data []byte) error {
	if vc.MaxParticipantDataSize > 0 && len(data) > vc.MaxParticipantDataSize {
		return fmt.Errorf("crdt: presence data exceeds max size (%d > %d bytes)", len(data), vc.MaxParticipantDataSize)
	}
	return nil
}

// ValidateRoomMetadata validates room metadata payload size.
func (vc *ValidationConfig) ValidateRoomMetadata(data []byte) error {
	if vc.MaxRoomMetadataSize > 0 && len(data) > vc.MaxRoomMetadataSize {
		return fmt.Errorf("crdt: room metadata exceeds max size (%d > %d bytes)", len(data), vc.MaxRoomMetadataSize)
	}
	return nil
}

// --- Options ---

// WithValidation enables input validation with the given config.
// Use DefaultValidationConfig() for sensible defaults.
func WithValidation(cfg *ValidationConfig) SyncControllerOption {
	return func(c *SyncController) {
		c.validation = cfg
	}
}
