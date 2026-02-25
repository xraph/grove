package esdriver

import (
	"context"

	"github.com/xraph/grove"
)

func init() {
	factory := func(ctx context.Context, dsn string) (grove.GroveDriver, error) {
		db := New()
		if err := db.Open(ctx, dsn); err != nil {
			return nil, err
		}
		return db, nil
	}
	grove.RegisterDriver("elasticsearch", factory)
}
