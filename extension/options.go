package extension

import (
	"github.com/xraph/grove"
	"github.com/xraph/grove/crdt"
	"github.com/xraph/grove/hook"
	"github.com/xraph/grove/migrate"
)

// ExtOption is a functional option for the Forge extension.
type ExtOption func(*Extension)

// WithDriver sets a pre-configured database driver for the extension.
// When set, this takes precedence over YAML driver/dsn configuration.
func WithDriver(drv grove.GroveDriver) ExtOption { return func(e *Extension) { e.driver = drv } }

// WithDSN sets the driver name and DSN for the extension.
// The driver will be created from the registry during Register().
// If WithDriver() is also set, it takes precedence.
func WithDSN(driver, dsn string) ExtOption {
	return func(e *Extension) {
		e.config.Driver = driver
		e.config.DSN = dsn
	}
}

// WithMigrations adds migration groups to the extension.
func WithMigrations(groups ...*migrate.Group) ExtOption {
	return func(e *Extension) { e.groups = append(e.groups, groups...) }
}

// WithHook adds a lifecycle hook to the Grove DB.
func WithHook(h any, scope ...hook.Scope) ExtOption {
	return func(e *Extension) {
		s := hook.Scope{Priority: 100}
		if len(scope) > 0 {
			s = scope[0]
		}
		e.hooks = append(e.hooks, hookEntry{hook: h, scope: s})
	}
}

// WithRequireConfig requires config to be present in YAML files.
// If true and no config is found, Register returns an error.
func WithRequireConfig(require bool) ExtOption {
	return func(e *Extension) { e.config.RequireConfig = require }
}

// WithDisableRoutes disables CRDT sync route registration.
func WithDisableRoutes() ExtOption {
	return func(e *Extension) { e.config.DisableRoutes = true }
}

// WithDisableMigrate disables automatic migration execution.
func WithDisableMigrate() ExtOption {
	return func(e *Extension) { e.config.DisableMigrate = true }
}

// WithBasePath sets the URL prefix for CRDT sync routes.
func WithBasePath(path string) ExtOption {
	return func(e *Extension) { e.config.BasePath = path }
}

// --- CRDT Options ---

// WithCRDT enables the CRDT plugin and registers the sync controller.
// The plugin's hooks are automatically added to the Grove DB, and the
// SyncController is registered with the Forge router for sync endpoints.
//
// Example:
//
//	ext := extension.New(
//	    extension.WithDriver(pgdb),
//	    extension.WithCRDT(crdtPlugin, hook.Scope{Tables: []string{"documents"}}),
//	)
func WithCRDT(plugin *crdt.Plugin, scope ...hook.Scope) ExtOption {
	return func(e *Extension) {
		e.crdtPlugin = plugin
		s := hook.Scope{Priority: 50} // CRDT hooks run before user hooks.
		if len(scope) > 0 {
			s = scope[0]
		}
		e.hooks = append(e.hooks, hookEntry{hook: plugin, scope: s})
	}
}

// WithSyncer configures the CRDT background syncer. Requires WithCRDT.
// The syncer is started in the extension's Start method and runs until
// the context is cancelled.
//
// Example:
//
//	ext := extension.New(
//	    extension.WithDriver(pgdb),
//	    extension.WithCRDT(crdtPlugin),
//	    extension.WithSyncer(syncer),
//	)
func WithSyncer(syncer *crdt.Syncer) ExtOption {
	return func(e *Extension) { e.syncer = syncer }
}

// WithSyncController overrides the default sync controller options.
// These options configure the sync endpoints (poll interval, keep-alive, hooks).
//
// Example:
//
//	ext := extension.New(
//	    extension.WithDriver(pgdb),
//	    extension.WithCRDT(crdtPlugin),
//	    extension.WithSyncController(
//	        crdt.WithStreamPollInterval(2 * time.Second),
//	        crdt.WithStreamKeepAlive(30 * time.Second),
//	    ),
//	)
func WithSyncController(opts ...crdt.SyncControllerOption) ExtOption {
	return func(e *Extension) { e.syncControllerOpts = opts }
}
