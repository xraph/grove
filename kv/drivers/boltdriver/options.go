package boltdriver

import (
	"os"
	"time"
)

type config struct {
	timeout    time.Duration
	fileMode   os.FileMode
	bucket     string
	noGrowSync bool
	readOnly   bool
}

func defaultConfig() config {
	return config{
		timeout:  1 * time.Second,
		fileMode: 0600,
	}
}

// Option configures a BoltDB driver.
type Option func(*config)

// WithTimeout sets the timeout for obtaining a file lock on the database.
func WithTimeout(d time.Duration) Option {
	return func(c *config) { c.timeout = d }
}

// WithFileMode sets the file mode for the database file.
func WithFileMode(mode os.FileMode) Option {
	return func(c *config) { c.fileMode = mode }
}

// WithBucket sets a custom bucket name (default: "kv_data").
func WithBucket(name string) Option {
	return func(c *config) { c.bucket = name }
}

// WithReadOnly opens the database in read-only mode.
func WithReadOnly(readOnly bool) Option {
	return func(c *config) { c.readOnly = readOnly }
}

// WithNoGrowSync disables grow-sync for faster writes at the cost of data safety.
func WithNoGrowSync(noGrowSync bool) Option {
	return func(c *config) { c.noGrowSync = noGrowSync }
}
