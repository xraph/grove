package kv

import "github.com/xraph/grove/hook"

// KV-specific operations. These extend hook.Operation at an offset to avoid
// collision with ORM operations (OpSelect, OpInsert, etc.).
const (
	OpGet    hook.Operation = 100 + iota // GET key
	OpSet                                // SET key value
	OpDelete                             // DEL key [key ...]
	OpExists                             // EXISTS key [key ...]
	OpMGet                               // MGET key [key ...]
	OpMSet                               // MSET key value [key value ...]
	OpTTL                                // TTL key
	OpExpire                             // EXPIRE key seconds
	OpScan                               // SCAN cursor MATCH pattern
)

// CommandName returns a human-readable name for a KV operation.
func CommandName(op hook.Operation) string {
	switch op {
	case OpGet:
		return "GET"
	case OpSet:
		return "SET"
	case OpDelete:
		return "DEL"
	case OpExists:
		return "EXISTS"
	case OpMGet:
		return "MGET"
	case OpMSet:
		return "MSET"
	case OpTTL:
		return "TTL"
	case OpExpire:
		return "EXPIRE"
	case OpScan:
		return "SCAN"
	default:
		return "UNKNOWN"
	}
}
