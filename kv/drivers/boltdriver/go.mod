module github.com/xraph/grove/kv/drivers/boltdriver

go 1.25.7

replace (
	github.com/xraph/grove => ../../../
	github.com/xraph/grove/kv => ../../
)

require (
	github.com/xraph/grove/kv v0.0.0
	go.etcd.io/bbolt v1.4.0
)

require (
	github.com/vmihailenco/msgpack/v5 v5.4.1 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
	github.com/xraph/grove v0.0.0 // indirect
	golang.org/x/sys v0.31.0 // indirect
	google.golang.org/protobuf v1.36.6 // indirect
)
