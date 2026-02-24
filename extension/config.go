package extension

import (
	"errors"
	"fmt"
)

// Config holds the Grove extension configuration.
// Fields can be set programmatically via ExtOption functions or loaded from
// YAML configuration files (under "extensions.grove" or "grove" keys).
type Config struct {
	// Driver name: "postgres", "sqlite", "mysql", "mongodb", "turso", "clickhouse".
	Driver string `json:"driver" mapstructure:"driver" yaml:"driver"`

	// DSN is the data source name / connection string.
	DSN string `json:"dsn" mapstructure:"dsn" yaml:"dsn"`

	// Databases defines additional named database connections.
	// When set, each entry creates a separate grove.DB registered in DI.
	Databases []DatabaseConfig `json:"databases" mapstructure:"databases" yaml:"databases"`

	// Default is the name of the default database when using multi-DB.
	// If empty, the first entry in Databases is the default.
	Default string `json:"default" mapstructure:"default" yaml:"default"`

	// DisableRoutes skips CRDT sync route registration.
	DisableRoutes bool `json:"disable_routes" mapstructure:"disable_routes" yaml:"disable_routes"`

	// DisableMigrate disables automatic migration execution.
	DisableMigrate bool `json:"disable_migrate" mapstructure:"disable_migrate" yaml:"disable_migrate"`

	// BasePath is the URL prefix for CRDT sync routes (default: "/sync").
	BasePath string `json:"base_path" mapstructure:"base_path" yaml:"base_path"`

	// RequireConfig requires config to be present in YAML files.
	// If true and no config is found, Register returns an error.
	RequireConfig bool `json:"-" yaml:"-"`
}

// DatabaseConfig defines a single named database connection.
type DatabaseConfig struct {
	// Name is the unique identifier for this database.
	Name string `json:"name" mapstructure:"name" yaml:"name"`

	// Driver name: "postgres", "sqlite", "mysql", "mongodb", "turso", "clickhouse".
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
	// Driver and DSN are validated in resolveDriver() —
	// they're only required if no programmatic driver is set.
	if c.Driver != "" && c.DSN == "" {
		return errors.New("grove: driver specified without dsn")
	}
	if c.DSN != "" && c.Driver == "" {
		return errors.New("grove: dsn specified without driver name")
	}

	// Validate named databases.
	seen := make(map[string]bool, len(c.Databases))
	for i, db := range c.Databases {
		if db.Name == "" {
			return fmt.Errorf("grove: databases[%d]: name is required", i)
		}
		if seen[db.Name] {
			return fmt.Errorf("grove: databases[%d]: duplicate name %q", i, db.Name)
		}
		seen[db.Name] = true

		if db.Driver == "" {
			return fmt.Errorf("grove: databases[%d] %q: driver is required", i, db.Name)
		}
		if db.DSN == "" {
			return fmt.Errorf("grove: databases[%d] %q: dsn is required", i, db.Name)
		}
	}

	// Validate default references an existing database.
	if c.Default != "" && len(c.Databases) > 0 {
		if !seen[c.Default] {
			return fmt.Errorf("grove: default database %q not found in databases list", c.Default)
		}
	}

	return nil
}
