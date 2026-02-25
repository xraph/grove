package badgerdriver

import (
	kv "github.com/xraph/grove/kv"
	"github.com/xraph/grove/kv/driver"
)

func init() {
	kv.RegisterDriver("badger", func() driver.Driver { return New() })
}
