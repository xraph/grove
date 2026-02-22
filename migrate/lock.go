package migrate

import "errors"

// ErrLockHeld is returned when a migration lock is already held.
var ErrLockHeld = errors.New("migrate: lock is held by another process")

// LockInfo describes the current migration lock state.
type LockInfo struct {
	Held     bool
	LockedBy string
	LockedAt string
}

// IsLockError returns true if the error indicates a lock conflict.
func IsLockError(err error) bool {
	return errors.Is(err, ErrLockHeld)
}
