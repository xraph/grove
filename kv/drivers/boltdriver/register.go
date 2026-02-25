package boltdriver

import (
	kv "github.com/xraph/grove/kv"
	"github.com/xraph/grove/kv/driver"
)

func init() {
	factory := func() driver.Driver { return New() }
	kv.RegisterDriver("boltdb", factory)
	kv.RegisterDriver("bbolt", factory)
}
