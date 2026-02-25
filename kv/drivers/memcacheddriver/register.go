package memcacheddriver

import (
	kv "github.com/xraph/grove/kv"
	"github.com/xraph/grove/kv/driver"
)

func init() {
	kv.RegisterDriver("memcached", func() driver.Driver { return New() })
}
