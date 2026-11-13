package mongodriver

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestClientOptions_PoolSettings(t *testing.T) {
	for _, tt := range []struct {
		name string
		uri  string
		opts []MongoOption
		max  uint64
		idle time.Duration
	}{
		{name: "defaults", uri: "mongodb://localhost/db", max: 10, idle: time.Minute},
		{name: "uri", uri: "mongodb://localhost/db?maxPoolSize=2&maxIdleTimeMS=15000", max: 2, idle: 15 * time.Second},
		{name: "uri disables limits", uri: "mongodb://localhost/db?maxPoolSize=0&maxIdleTimeMS=0", max: 0, idle: 0},
		{name: "option wins", uri: "mongodb://localhost/db?maxPoolSize=2&maxIdleTimeMS=15000", opts: []MongoOption{WithMaxPoolSize(10), WithMaxConnIdleTime(time.Minute)}, max: 10, idle: time.Minute},
		{name: "zero size keeps cap", uri: "mongodb://localhost/db", opts: []MongoOption{WithMaxPoolSize(0)}, max: 10, idle: time.Minute},
		{name: "explicit zero", uri: "mongodb://localhost/db?maxPoolSize=2&minPoolSize=1&maxIdleTimeMS=15000", opts: []MongoOption{WithMaxPoolSize(0), WithMinPoolSize(0), WithMaxConnIdleTime(0)}, max: 2, idle: 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			opts := defaultMongoOptions()
			opts.apply(tt.opts)
			cfg := opts.clientOptions(tt.uri)
			require.NoError(t, cfg.Validate())
			require.NotNil(t, cfg.MaxPoolSize)
			require.Equal(t, tt.max, *cfg.MaxPoolSize)
			require.NotNil(t, cfg.MaxConnIdleTime)
			require.Equal(t, tt.idle, *cfg.MaxConnIdleTime)
			if cfg.MinPoolSize != nil {
				require.Zero(t, *cfg.MinPoolSize)
			}
		})
	}
}

func TestOpen_MissingDatabaseDoesNotCreateClient(t *testing.T) {
	db := New()
	err := db.Open(t.Context(), "mongodb://localhost", WithSkipPing(true))
	require.ErrorContains(t, err, "no database name")
	require.Nil(t, db.client)
}
