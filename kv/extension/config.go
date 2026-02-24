package extension

import (
	"errors"
	"fmt"
)

// Config holds the KV extension configuration.
// Fields can be set programmatically via ExtOption functions or loaded from
// YAML configuration files (under "extensions.grove_kv" or "grove_kv" keys).
type Config struct {
	// Driver name: "redis", "memcached", "badger", "boltdb", "dynamodb".
	Driver string `json:"driver" mapstructure:"driver" yaml:"driver"`

	// DSN is the data source name / connection string.
	DSN string `json:"dsn" mapstructure:"dsn" yaml:"dsn"`

	// Stores defines additional named store connections.
	// When set, each entry creates a separate kv.Store registered in DI.
	Stores []StoreConfig `json:"stores" mapstructure:"stores" yaml:"stores"`

	// Default is the name of the default store when using multi-store.
	// If empty, the first entry in Stores is the default.
	Default string `json:"default" mapstructure:"default" yaml:"default"`

	// RequireConfig requires config to be present in YAML files.
	// If true and no config is found, Register returns an error.
	RequireConfig bool `json:"-" yaml:"-"`
}

// StoreConfig defines a single named store connection.
type StoreConfig struct {
	// Name is the unique identifier for this store.
	Name string `json:"name" mapstructure:"name" yaml:"name"`

	// Driver name: "redis", "memcached", "badger", "boltdb", "dynamodb".
	Driver string `json:"driver" mapstructure:"driver" yaml:"driver"`

	// DSN is the data source name / connection string.
	DSN string `json:"dsn" mapstructure:"dsn" yaml:"dsn"`
}

// DefaultConfig returns the default configuration.
// Driver and DSN are empty — callers must provide them via
// WithDriver(), WithDSN(), or YAML configuration.
func DefaultConfig() Config {
	return Config{}
}

// Validate checks that the configuration is usable.
func (c *Config) Validate() error {
	if c.Driver != "" && c.DSN == "" {
		return errors.New("kv: driver specified without dsn")
	}
	if c.DSN != "" && c.Driver == "" {
		return errors.New("kv: dsn specified without driver name")
	}

	// Validate named stores.
	seen := make(map[string]bool, len(c.Stores))
	for i, s := range c.Stores {
		if s.Name == "" {
			return fmt.Errorf("kv: stores[%d]: name is required", i)
		}
		if seen[s.Name] {
			return fmt.Errorf("kv: stores[%d]: duplicate name %q", i, s.Name)
		}
		seen[s.Name] = true

		if s.Driver == "" {
			return fmt.Errorf("kv: stores[%d] %q: driver is required", i, s.Name)
		}
		if s.DSN == "" {
			return fmt.Errorf("kv: stores[%d] %q: dsn is required", i, s.Name)
		}
	}

	// Validate default references an existing store.
	if c.Default != "" && len(c.Stores) > 0 {
		if !seen[c.Default] {
			return fmt.Errorf("kv: default store %q not found in stores list", c.Default)
		}
	}

	return nil
}
