package extension

import (
	"log/slog"

	"github.com/xraph/grove"
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
