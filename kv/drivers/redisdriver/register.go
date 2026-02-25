package redisdriver

import (
	kv "github.com/xraph/grove/kv"
	"github.com/xraph/grove/kv/driver"
)

func init() {
	kv.RegisterDriver("redis", func() driver.Driver { return New() })
}
