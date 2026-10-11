module github.com/xraph/grove/kv/drivers/memcacheddriver

go 1.25.7

replace (
	github.com/xraph/grove => ../../../
	github.com/xraph/grove/kv => ../../
)

require (
	github.com/bradfitz/gomemcache v0.0.0-20230905024940-24af94b03874
	github.com/xraph/grove/kv v1.7.2
)

require (
	github.com/vmihailenco/msgpack/v5 v5.4.1 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
	github.com/xraph/go-utils v1.3.0 // indirect
	github.com/xraph/grove v1.6.2 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/term v0.45.0 // indirect
	google.golang.org/protobuf v1.36.6 // indirect
)
