package extension

import (
	"log/slog"

	"github.com/xraph/grove"
	"github.com/xraph/grove/crdt"
	"github.com/xraph/grove/hook"
	"github.com/xraph/grove/migrate"
)

// Config holds Forge extension configuration.
type Config struct {
	DisableMigrate bool
}

// ExtOption is a functional option for the Forge extension.
type ExtOption func(*Extension)

// WithConfig sets the extension config.
func WithConfig(c Config) ExtOption { return func(e *Extension) { e.config = c } }

// WithLogger sets the logger.
func WithLogger(l *slog.Logger) ExtOption { return func(e *Extension) { e.logger = l } }

// WithDriver sets the database driver for the extension.
func WithDriver(drv grove.GroveDriver) ExtOption { return func(e *Extension) { e.driver = drv } }

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
