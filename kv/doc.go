// Package kv provides a command-oriented data access layer for key-value,
// cache, and document stores. It is part of the Grove ecosystem and shares
// Grove's hook, plugin, CRDT, and internal infrastructure.
//
// Grove KV is not a fake SQL abstraction over SET/GET — it exposes native
// idioms per store (Redis pipelines, DynamoDB expressions, Memcached CAS)
// while providing a unified Store interface for common operations.
//
// # Quick Start
//
//	rdb := redisdriver.New()  // import "github.com/xraph/grove/kv/drivers/redisdriver"
//	rdb.Open(ctx, "redis://localhost:6379/0")
//
//	store, _ := kv.Open(rdb,
//	    kv.WithCodec(codec.JSON()),
//	)
//	defer store.Close()
//
//	// Basic Get/Set
//	store.Set(ctx, "user:1", user, kv.WithTTL(time.Hour))
//	store.Get(ctx, "user:1", &user)
//
// # Design Principles
//
//   - Command-oriented, not query-oriented
//   - Native idioms per store via Unwrap()
//   - Shared DNA with Grove (hooks, plugins, CRDT, observability)
//   - CRDT-native distributed state
//   - Struct-aware serialization via codecs
//   - First-class TTL and eviction
package kv
