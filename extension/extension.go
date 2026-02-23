// Package extension adapts Grove as a Forge extension.
package extension

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/xraph/forge"
	"github.com/xraph/vessel"

	"github.com/xraph/grove"
	"github.com/xraph/grove/hook"
	"github.com/xraph/grove/migrate"
)

// ExtensionName is the name registered with Forge.
const ExtensionName = "grove"

// ExtensionDescription is the human-readable description.
const ExtensionDescription = "Polyglot Go ORM with native query syntax per database"

// ExtensionVersion is the semantic version.
const ExtensionVersion = "0.1.0"

// Ensure Extension implements forge.Extension at compile time.
var _ forge.Extension = (*Extension)(nil)

// Extension adapts Grove as a Forge extension.
type Extension struct {
	config Config
	db     *grove.DB
	logger *slog.Logger
	driver grove.GroveDriver
	groups []*migrate.Group
	hooks  []hookEntry
}

type hookEntry struct {
	hook  any
	scope hook.Scope
}

// New creates a Grove Forge extension with the given options.
func New(opts ...ExtOption) *Extension {
	e := &Extension{}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// Name returns the extension name.
func (e *Extension) Name() string { return ExtensionName }

// Description returns the extension description.
func (e *Extension) Description() string { return ExtensionDescription }

// Version returns the extension version.
func (e *Extension) Version() string { return ExtensionVersion }

// Dependencies returns the list of extension names this extension depends on.
func (e *Extension) Dependencies() []string { return []string{} }

// DB returns the Grove DB instance (nil until Register is called).
func (e *Extension) DB() *grove.DB { return e.db }

// MigrationGroups returns all registered migration groups.
func (e *Extension) MigrationGroups() []*migrate.Group { return e.groups }

// Register implements [forge.Extension].
func (e *Extension) Register(fapp forge.App) error {
	if err := e.Init(fapp); err != nil {
		return err
	}

	if err := vessel.Provide(fapp.Container(), func() (*grove.DB, error) {
		return e.db, nil
	}); err != nil {
		return fmt.Errorf("grove: register db in container: %w", err)
	}

	return nil
}

// Init builds the DB and registers hooks.
func (e *Extension) Init(_ forge.App) error {
	if e.driver == nil {
		return errors.New("grove: no driver configured; use WithDriver()")
	}

	logger := e.logger
	if logger == nil {
		logger = slog.Default()
	}
	e.logger = logger

	db, err := grove.Open(e.driver)
	if err != nil {
		return fmt.Errorf("grove: open: %w", err)
	}
	e.db = db

	for _, h := range e.hooks {
		db.Hooks().AddHook(h.hook, h.scope)
	}

	e.logger.Info("grove extension initialized",
		slog.String("driver", e.driver.Name()),
	)
	return nil
}

// Start implements [forge.Extension].
func (e *Extension) Start(_ context.Context) error {
	if e.db == nil {
		return errors.New("grove: extension not initialized")
	}
	return nil
}

// Stop gracefully shuts down the Grove DB.
func (e *Extension) Stop(_ context.Context) error {
	if e.db == nil {
		return nil
	}
	return e.db.Close()
}

// Health implements [forge.Extension].
func (e *Extension) Health(_ context.Context) error {
	if e.db == nil {
		return errors.New("grove: extension not initialized")
	}
	return nil
}
