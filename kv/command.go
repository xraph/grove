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

	// Collection operations. A hook sees these the same way it sees the
	// scalar commands above, which is what lets metrics, tracing, and
	// tests observe the work a queue or an index actually does rather
	// than only the plain key reads around it.
	OpZAdd    // ZADD key score member [score member ...]
	OpZRange  // ZRANGEBYSCORE key min max
	OpZRem    // ZREM key member [member ...]
	OpZCard   // ZCARD key
	OpZScore  // ZSCORE key member
	OpSAdd    // SADD key member [member ...]
	OpSRem    // SREM key member [member ...]
	OpSMbrs   // SMEMBERS key
	OpSCard   // SCARD key
	OpSIsMbr  // SISMEMBER key member
	OpHSet    // HSET key field value [field value ...]
	OpHGet    // HGET key field
	OpHGetAll // HGETALL key
	OpHDel    // HDEL key field [field ...]
	OpHLen    // HLEN key
	OpPublish // PUBLISH channel message
	OpSubscr  // SUBSCRIBE channel
	OpEval    // EVAL script numkeys key [key ...] arg [arg ...]
	OpXAdd    // XADD stream field value [field value ...]
	OpXRange  // XRANGE stream start end
	OpXDel    // XDEL stream id [id ...]
	OpXLen    // XLEN stream
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
	case OpZAdd:
		return "ZADD"
	case OpZRange:
		return "ZRANGE"
	case OpZRem:
		return "ZREM"
	case OpZCard:
		return "ZCARD"
	case OpZScore:
		return "ZSCORE"
	case OpSAdd:
		return "SADD"
	case OpSRem:
		return "SREM"
	case OpSMbrs:
		return "SMEMBERS"
	case OpSCard:
		return "SCARD"
	case OpSIsMbr:
		return "SISMEMBER"
	case OpHSet:
		return "HSET"
	case OpHGet:
		return "HGET"
	case OpHGetAll:
		return "HGETALL"
	case OpHDel:
		return "HDEL"
	case OpHLen:
		return "HLEN"
	case OpPublish:
		return "PUBLISH"
	case OpSubscr:
		return "SUBSCRIBE"
	case OpEval:
		return "EVAL"
	case OpXAdd:
		return "XADD"
	case OpXRange:
		return "XRANGE"
	case OpXDel:
		return "XDEL"
	case OpXLen:
		return "XLEN"
	default:
		return "UNKNOWN"
	}
}
