// Package extension provides the Grove ORM Forge extension entry point.
// It registers Grove as a Forge-managed database layer, handling connection
// lifecycle, migration execution, and hook setup.
package extension

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/xraph/grove"
	"github.com/xraph/grove/hook"
	"github.com/xraph/grove/migrate"
)

// Extension implements the Forge extension pattern for Grove.
type Extension struct {
	db     *grove.DB
	opts   *extensionOptions
	groups []*migrate.Group
	hooks  []hookEntry
	logger *slog.Logger
}

type hookEntry struct {
	hook  any
	scope hook.Scope
}

type extensionOptions struct {
	driver     grove.GroveDriver
	migrations []*migrate.Group
	hooks      []hookEntry
	logger     *slog.Logger
}

// Option configures the extension.
type Option func(*extensionOptions)

// WithDriver sets the database driver for the extension.
func WithDriver(drv grove.GroveDriver) Option {
	return func(o *extensionOptions) {
		o.driver = drv
	}
}

// WithMigrations adds migration groups to the extension.
func WithMigrations(groups ...*migrate.Group) Option {
	return func(o *extensionOptions) {
		o.migrations = append(o.migrations, groups...)
	}
}

// WithHook adds a hook to the extension.
func WithHook(h any, scope ...hook.Scope) Option {
	return func(o *extensionOptions) {
		s := hook.Scope{Priority: 100}
		if len(scope) > 0 {
			s = scope[0]
		}
		o.hooks = append(o.hooks, hookEntry{hook: h, scope: s})
	}
}

// WithLogger sets the logger for the extension.
func WithLogger(l *slog.Logger) Option {
	return func(o *extensionOptions) {
		o.logger = l
	}
}

// New creates a new Grove extension with the given options.
func New(opts ...Option) *Extension {
	o := &extensionOptions{
		logger: slog.Default(),
	}
	for _, opt := range opts {
		opt(o)
	}
	return &Extension{
		opts:   o,
		groups: o.migrations,
		hooks:  o.hooks,
		logger: o.logger,
	}
}

// Name returns the extension name.
func (e *Extension) Name() string {
	return "grove"
}

// Init initializes the Grove extension — creates the DB, registers hooks,
// and prepares migrations.
func (e *Extension) Init(ctx context.Context) error {
	if e.opts.driver == nil {
		return fmt.Errorf("grove extension: no driver configured; use WithDriver()")
	}

	db, err := grove.Open(e.opts.driver)
	if err != nil {
		return fmt.Errorf("grove extension: open: %w", err)
	}
	e.db = db

	// Register hooks
	for _, h := range e.hooks {
		db.Hooks().AddHook(h.hook, h.scope)
	}

	e.logger.Info("grove extension initialized",
		slog.String("driver", e.opts.driver.Name()),
	)
	return nil
}

// DB returns the Grove DB instance. Returns nil if Init hasn't been called.
func (e *Extension) DB() *grove.DB {
	return e.db
}

// MigrationGroups returns all registered migration groups.
func (e *Extension) MigrationGroups() []*migrate.Group {
	return e.groups
}

// Close shuts down the Grove extension.
func (e *Extension) Close() error {
	if e.db != nil {
		return e.db.Close()
	}
	return nil
}
