package driver_test

import (
	"crypto/tls"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	log "github.com/xraph/go-utils/log"
	"github.com/xraph/grove/kv/driver"
)

func TestDefaultDriverOptions(t *testing.T) {
	opts := driver.DefaultDriverOptions()

	assert.Equal(t, 10, opts.PoolSize)
	assert.Equal(t, 5*time.Second, opts.DialTimeout)
	assert.Equal(t, 3*time.Second, opts.ReadTimeout)
	assert.Equal(t, 3*time.Second, opts.WriteTimeout)
	assert.Nil(t, opts.TLSConfig)
	assert.Nil(t, opts.Logger)
	assert.NotNil(t, opts.Extra)
	assert.Empty(t, opts.Extra)
}

func TestApplyOptions_PoolSize(t *testing.T) {
	opts := driver.ApplyOptions([]driver.Option{
		driver.WithPoolSize(25),
	})
	assert.Equal(t, 25, opts.PoolSize)
}

func TestApplyOptions_DialTimeout(t *testing.T) {
	opts := driver.ApplyOptions([]driver.Option{
		driver.WithDialTimeout(10 * time.Second),
	})
	assert.Equal(t, 10*time.Second, opts.DialTimeout)
}

func TestApplyOptions_ReadTimeout(t *testing.T) {
	opts := driver.ApplyOptions([]driver.Option{
		driver.WithReadTimeout(7 * time.Second),
	})
	assert.Equal(t, 7*time.Second, opts.ReadTimeout)
}

func TestApplyOptions_WriteTimeout(t *testing.T) {
	opts := driver.ApplyOptions([]driver.Option{
		driver.WithWriteTimeout(8 * time.Second),
	})
	assert.Equal(t, 8*time.Second, opts.WriteTimeout)
}

func TestApplyOptions_TLSConfig(t *testing.T) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS13}
	opts := driver.ApplyOptions([]driver.Option{
		driver.WithTLSConfig(cfg),
	})
	assert.Same(t, cfg, opts.TLSConfig)
}

func TestApplyOptions_Logger(t *testing.T) {
	logger := log.NewNoopLogger()
	opts := driver.ApplyOptions([]driver.Option{
		driver.WithLogger(logger),
	})
	assert.Same(t, logger, opts.Logger)
}

func TestApplyOptions_Combined(t *testing.T) {
	logger := log.NewNoopLogger()
	cfg := &tls.Config{MinVersion: tls.VersionTLS13}

	opts := driver.ApplyOptions([]driver.Option{
		driver.WithPoolSize(50),
		driver.WithDialTimeout(15 * time.Second),
		driver.WithReadTimeout(10 * time.Second),
		driver.WithWriteTimeout(12 * time.Second),
		driver.WithTLSConfig(cfg),
		driver.WithLogger(logger),
	})

	assert.Equal(t, 50, opts.PoolSize)
	assert.Equal(t, 15*time.Second, opts.DialTimeout)
	assert.Equal(t, 10*time.Second, opts.ReadTimeout)
	assert.Equal(t, 12*time.Second, opts.WriteTimeout)
	assert.Same(t, cfg, opts.TLSConfig)
	assert.Same(t, logger, opts.Logger)
}

func TestApplyOptions_NoOptions(t *testing.T) {
	opts := driver.ApplyOptions(nil)

	// Should match defaults when no options are applied.
	defaults := driver.DefaultDriverOptions()
	assert.Equal(t, defaults.PoolSize, opts.PoolSize)
	assert.Equal(t, defaults.DialTimeout, opts.DialTimeout)
	assert.Equal(t, defaults.ReadTimeout, opts.ReadTimeout)
	assert.Equal(t, defaults.WriteTimeout, opts.WriteTimeout)
}
