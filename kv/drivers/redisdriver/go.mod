module github.com/xraph/grove/kv/drivers/redisdriver

go 1.25.7

replace (
	github.com/xraph/grove => ../../../
	github.com/xraph/grove/kv => ../../
)

require (
	github.com/redis/go-redis/v9 v9.18.0
	github.com/xraph/grove/kv v0.0.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/vmihailenco/msgpack/v5 v5.4.1 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
	github.com/xraph/grove v0.0.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	google.golang.org/protobuf v1.36.6 // indirect
)
