package extension

import (
	"github.com/xraph/grove/hook"
	"github.com/xraph/grove/kv"
	"github.com/xraph/grove/kv/driver"
)

// ExtOption is a functional option for the KV Forge extension.
type ExtOption func(*Extension)

// storeEntry holds the configuration for a named store
// provided via WithStore or WithStoreDSN options.
type storeEntry struct {
	name       string
	driver     driver.Driver
	driverName string // for DSN-based resolution
	dsn        string
}

type hookEntry struct {
	hook  any
	scope hook.Scope
}

// --- Single-Store Options (backward compatible) ---

// WithDriver sets a pre-configured KV driver for the extension.
// When set, this takes precedence over YAML driver/dsn configuration.
func WithDriver(drv driver.Driver) ExtOption { return func(e *Extension) { e.driver = drv } }

// WithDSN sets the driver name and DSN for the extension.
// The driver will be created from the registry during Register().
// If WithDriver() is also set, it takes precedence.
func WithDSN(driverName, dsn string) ExtOption {
	return func(e *Extension) {
		e.config.Driver = driverName
		e.config.DSN = dsn
	}
}

// WithStoreOptions adds kv.Option values applied when opening the store.
func WithStoreOptions(opts ...kv.Option) ExtOption {
	return func(e *Extension) { e.storeOpts = append(e.storeOpts, opts...) }
}

// WithHook adds a lifecycle hook to the KV store.
func WithHook(h any, scope ...hook.Scope) ExtOption {
	return func(e *Extension) {
		s := hook.Scope{Priority: 100}
		if len(scope) > 0 {
			s = scope[0]
		}
		e.hooks = append(e.hooks, hookEntry{hook: h, scope: s})
	}
}

// --- Multi-Store Options ---

// WithStore adds a named store with a pre-configured driver.
// Multiple calls create multiple named stores.
//
// Example:
//
//	ext := forgeext.New(
//	    forgeext.WithStore("cache", redisDriver),
//	    forgeext.WithStore("sessions", badgerDriver),
//	    forgeext.WithDefaultStore("cache"),
//	)
func WithStore(name string, drv driver.Driver) ExtOption {
	return func(e *Extension) {
		e.stores = append(e.stores, storeEntry{
			name:   name,
			driver: drv,
		})
	}
}

// WithStoreDSN adds a named store using a driver name and DSN.
// The driver will be created from the registry during Register().
//
// Example:
//
//	ext := forgeext.New(
//	    forgeext.WithStoreDSN("cache", "redis", "redis://localhost:6379/0"),
//	    forgeext.WithStoreDSN("sessions", "badger", "/tmp/sessions.db"),
//	)
func WithStoreDSN(name, driverName, dsn string) ExtOption {
	return func(e *Extension) {
		e.stores = append(e.stores, storeEntry{
			name:       name,
			driverName: driverName,
			dsn:        dsn,
		})
	}
}

// WithDefaultStore sets which named store is the default.
// The default is used for backward-compatible Store() access and unnamed DI injection.
func WithDefaultStore(name string) ExtOption {
	return func(e *Extension) { e.defaultStore = name }
}

// WithHookFor adds a hook scoped to a specific named store.
func WithHookFor(storeName string, h any, scope ...hook.Scope) ExtOption {
	return func(e *Extension) {
		s := hook.Scope{Priority: 100}
		if len(scope) > 0 {
			s = scope[0]
		}
		if e.storeHooks == nil {
			e.storeHooks = make(map[string][]hookEntry)
		}
		e.storeHooks[storeName] = append(e.storeHooks[storeName], hookEntry{hook: h, scope: s})
	}
}

// WithStoreOptionsFor adds kv.Option values for a specific named store.
func WithStoreOptionsFor(storeName string, opts ...kv.Option) ExtOption {
	return func(e *Extension) {
		if e.storeOptions == nil {
			e.storeOptions = make(map[string][]kv.Option)
		}
		e.storeOptions[storeName] = append(e.storeOptions[storeName], opts...)
	}
}

// --- Configuration Options ---

// WithRequireConfig requires config to be present in YAML files.
// If true and no config is found, Register returns an error.
func WithRequireConfig(require bool) ExtOption {
	return func(e *Extension) { e.config.RequireConfig = require }
}
