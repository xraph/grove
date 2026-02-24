package extension

import "errors"

// Config holds the Grove extension configuration.
// Fields can be set programmatically via ExtOption functions or loaded from
// YAML configuration files (under "extensions.grove" or "grove" keys).
type Config struct {
	// Driver name: "postgres", "sqlite", "mysql", "mongodb", "turso", "clickhouse".
	Driver string `json:"driver" mapstructure:"driver" yaml:"driver"`

	// DSN is the data source name / connection string.
	DSN string `json:"dsn" mapstructure:"dsn" yaml:"dsn"`

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
	return nil
}
