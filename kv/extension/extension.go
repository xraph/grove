// Package extension adapts the Grove KV store as a Forge extension.
package extension

import (
	"context"
	"errors"
	"fmt"

	"github.com/xraph/forge"
	"github.com/xraph/vessel"

	"github.com/xraph/grove/kv"
	"github.com/xraph/grove/kv/driver"
)

// ExtensionName is the name registered with Forge.
const ExtensionName = "grove.kv"

// ExtensionDescription is the human-readable description.
const ExtensionDescription = "Universal key-value store with multiple backend support"

// ExtensionVersion is the semantic version.
const ExtensionVersion = "0.1.0"

// Ensure Extension implements forge.Extension at compile time.
var _ forge.Extension = (*Extension)(nil)

// Extension adapts the Grove KV store as a Forge extension.
type Extension struct {
	*forge.BaseExtension

	config    Config
	store     *kv.Store     // single-store mode
	driver    driver.Driver // single-store mode
	hooks     []hookEntry
	storeOpts []kv.Option // global kv.Option (codec, hooks)

	// Multi-store support.
	stores       []storeEntry
	defaultStore string
	manager      *StoreManager
	storeHooks   map[string][]hookEntry
	storeOptions map[string][]kv.Option
}

// New creates a KV Forge extension with the given options.
func New(opts ...ExtOption) *Extension {
	e := &Extension{
		BaseExtension: forge.NewBaseExtension(ExtensionName, ExtensionVersion, ExtensionDescription),
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// Store returns the default KV store instance (nil until Register is called).
func (e *Extension) Store() *kv.Store { return e.store }

// Manager returns the StoreManager for multi-store mode.
// Returns nil in single-store mode.
func (e *Extension) Manager() *StoreManager { return e.manager }

// isMultiStore returns true if multiple named stores are configured.
func (e *Extension) isMultiStore() bool {
	return len(e.stores) > 0 || len(e.config.Stores) > 0
}

// Register implements [forge.Extension].
func (e *Extension) Register(fapp forge.App) error {
	// 1. BaseExtension.Register stores app, logger, metrics.
	if err := e.BaseExtension.Register(fapp); err != nil {
		return err
	}

	// 2. Load config: YAML -> merge with programmatic -> fallback.
	if err := e.loadConfiguration(); err != nil {
		return err
	}

	// 3. Route to single-store or multi-store initialization.
	if e.isMultiStore() {
		if err := e.registerMultiStore(fapp); err != nil {
			return err
		}
	} else {
		if err := e.registerSingleStore(fapp); err != nil {
			return err
		}
	}

	return nil
}

// registerSingleStore handles the original single-store path.
func (e *Extension) registerSingleStore(fapp forge.App) error {
	// Build driver from config if not set via WithDriver().
	if err := e.resolveDriver(); err != nil {
		return err
	}

	// Init store, register hooks.
	if err := e.initStore(); err != nil {
		return err
	}

	// Register *kv.Store in DI container.
	if err := vessel.Provide(fapp.Container(), func() (*kv.Store, error) {
		return e.store, nil
	}); err != nil {
		return fmt.Errorf("kv: register store in container: %w", err)
	}

	e.Logger().Info("kv extension registered",
		forge.F("driver", e.driver.Name()),
	)
	return nil
}

// registerMultiStore handles the multi-store path.
func (e *Extension) registerMultiStore(fapp forge.App) error {
	mgr := NewStoreManager()

	// Merge programmatic entries with config entries.
	entries := e.buildStoreEntries()

	// Open each store.
	for _, entry := range entries {
		drv, err := e.resolveStoreDriver(entry)
		if err != nil {
			return fmt.Errorf("kv: store %q: %w", entry.name, err)
		}

		// Collect options: global + per-store.
		opts := make([]kv.Option, len(e.storeOpts))
		copy(opts, e.storeOpts)
		if perStoreOpts, ok := e.storeOptions[entry.name]; ok {
			opts = append(opts, perStoreOpts...)
		}

		s, err := kv.Open(drv, opts...)
		if err != nil {
			return fmt.Errorf("kv: open store %q: %w", entry.name, err)
		}

		// Apply global hooks.
		for _, h := range e.hooks {
			s.Hooks().AddHook(h.hook, h.scope)
		}

		// Apply per-store hooks.
		if storeHooks, ok := e.storeHooks[entry.name]; ok {
			for _, h := range storeHooks {
				s.Hooks().AddHook(h.hook, h.scope)
			}
		}

		mgr.Add(entry.name, s)

		e.Logger().Info("kv: store opened",
			forge.F("name", entry.name),
			forge.F("driver", drv.Name()),
		)
	}

	// Set default store.
	defaultName := e.resolveDefaultName(entries)
	if defaultName != "" {
		if err := mgr.SetDefault(defaultName); err != nil {
			return fmt.Errorf("kv: set default store: %w", err)
		}
	}

	e.manager = mgr

	// Set e.store to the default for backward-compatible Store() accessor.
	defaultStore, err := mgr.Default()
	if err != nil {
		return fmt.Errorf("kv: get default store: %w", err)
	}
	e.store = defaultStore

	// Register in DI.
	if err := e.registerMultiStoreInDI(fapp); err != nil {
		return err
	}

	e.Logger().Info("kv extension registered",
		forge.F("mode", "multi-store"),
		forge.F("stores", mgr.Len()),
		forge.F("default", defaultName),
	)
	return nil
}

// buildStoreEntries merges programmatic and config-based store entries.
// Programmatic entries take precedence over config entries with the same name.
func (e *Extension) buildStoreEntries() []storeEntry {
	entries := make([]storeEntry, len(e.stores))
	copy(entries, e.stores)

	// Track names from programmatic entries.
	seen := make(map[string]bool, len(entries))
	for _, entry := range entries {
		seen[entry.name] = true
	}

	// Add config entries that aren't already provided programmatically.
	for _, cfg := range e.config.Stores {
		if seen[cfg.Name] {
			continue
		}
		entries = append(entries, storeEntry{
			name:       cfg.Name,
			driverName: cfg.Driver,
			dsn:        cfg.DSN,
		})
	}

	return entries
}

// resolveStoreDriver creates a driver for a store entry.
func (e *Extension) resolveStoreDriver(entry storeEntry) (driver.Driver, error) {
	if entry.driver != nil {
		return entry.driver, nil
	}
	if entry.driverName == "" || entry.dsn == "" {
		return nil, errors.New("driver and dsn are required")
	}
	return OpenDriver(context.Background(), entry.driverName, entry.dsn)
}

// resolveDefaultName determines the default store name.
func (e *Extension) resolveDefaultName(entries []storeEntry) string {
	if e.defaultStore != "" {
		return e.defaultStore
	}
	if e.config.Default != "" {
		return e.config.Default
	}
	if len(entries) > 0 {
		return entries[0].name
	}
	return ""
}

// registerMultiStoreInDI registers the manager and all stores in the DI container.
func (e *Extension) registerMultiStoreInDI(fapp forge.App) error {
	// Register the StoreManager itself.
	if err := vessel.Provide(fapp.Container(), func() *StoreManager {
		return e.manager
	}); err != nil {
		return fmt.Errorf("kv: register store manager in container: %w", err)
	}

	// Register default *kv.Store (unnamed — backward compatible).
	if err := vessel.Provide(fapp.Container(), func() (*kv.Store, error) {
		return e.manager.Default()
	}); err != nil {
		return fmt.Errorf("kv: register default store in container: %w", err)
	}

	// Register each named store.
	for name, s := range e.manager.All() {
		namedStore := s // capture loop variable
		if err := vessel.ProvideNamed(fapp.Container(), name, func() *kv.Store {
			return namedStore
		}); err != nil {
			return fmt.Errorf("kv: register store %q in container: %w", name, err)
		}
	}

	return nil
}

// Start implements [forge.Extension].
func (e *Extension) Start(_ context.Context) error {
	if e.store == nil {
		return errors.New("kv: extension not initialized")
	}
	e.MarkStarted()
	return nil
}

// Stop gracefully shuts down all KV stores.
func (e *Extension) Stop(_ context.Context) error {
	if e.manager != nil {
		e.manager.Close()
	} else if e.store != nil {
		e.store.Close()
	}
	e.MarkStopped()
	return nil
}

// Health implements [forge.Extension].
func (e *Extension) Health(ctx context.Context) error {
	if e.manager != nil {
		for name, s := range e.manager.All() {
			if err := s.Ping(ctx); err != nil {
				return fmt.Errorf("kv: store %q health check failed: %w", name, err)
			}
		}
		return nil
	}
	if e.store == nil {
		return errors.New("kv: extension not initialized")
	}
	return e.store.Ping(ctx)
}

// --- Config Loading ---

// loadConfiguration loads config from YAML files or programmatic sources.
func (e *Extension) loadConfiguration() error {
	programmaticConfig := e.config
	hasProgrammaticDriver := e.driver != nil || (programmaticConfig.Driver != "" && programmaticConfig.DSN != "")
	hasProgrammaticStores := len(e.stores) > 0

	// Try loading from config file.
	finalConfig, configLoaded := e.tryLoadFromConfigFile()

	if !configLoaded {
		if programmaticConfig.RequireConfig {
			return errors.New("kv: configuration is required but not found in config files; " +
				"ensure 'extensions.grove_kv' or 'grove_kv' key exists in your config")
		}

		finalConfig = e.selectProgrammaticOrDefaultConfig(programmaticConfig, hasProgrammaticDriver || hasProgrammaticStores)
	} else {
		// Config loaded from YAML — merge with programmatic options.
		finalConfig = e.mergeConfigurations(finalConfig, programmaticConfig)
	}

	e.Logger().Debug("kv: configuration loaded",
		forge.F("driver", finalConfig.Driver),
		forge.F("stores", len(finalConfig.Stores)),
	)

	e.config = finalConfig

	if err := e.config.Validate(); err != nil {
		return fmt.Errorf("invalid kv configuration: %w", err)
	}

	return nil
}

// tryLoadFromConfigFile attempts to load config from YAML files.
func (e *Extension) tryLoadFromConfigFile() (Config, bool) {
	cm := e.App().Config()
	var cfg Config

	// Try "extensions.grove_kv" first (namespaced pattern).
	if cm.IsSet("extensions.grove_kv") {
		if err := cm.Bind("extensions.grove_kv", &cfg); err == nil {
			e.Logger().Debug("kv: loaded config from file",
				forge.F("key", "extensions.grove_kv"),
			)
			return cfg, true
		}
		e.Logger().Warn("kv: failed to bind extensions.grove_kv config",
			forge.F("error", "bind failed"),
		)
	}

	// Try legacy "grove_kv" key.
	if cm.IsSet("grove_kv") {
		if err := cm.Bind("grove_kv", &cfg); err == nil {
			e.Logger().Debug("kv: loaded config from file",
				forge.F("key", "grove_kv"),
			)
			return cfg, true
		}
		e.Logger().Warn("kv: failed to bind grove_kv config",
			forge.F("error", "bind failed"),
		)
	}

	return Config{}, false
}

// selectProgrammaticOrDefaultConfig selects between programmatic config and defaults.
func (e *Extension) selectProgrammaticOrDefaultConfig(programmaticConfig Config, hasProgrammaticDriver bool) Config {
	if hasProgrammaticDriver {
		e.Logger().Debug("kv: using programmatic configuration")
		return programmaticConfig
	}

	e.Logger().Debug("kv: using default configuration")
	return DefaultConfig()
}

// mergeConfigurations merges YAML config with programmatic options.
// YAML config takes precedence for Driver/DSN; programmatic fills gaps.
func (e *Extension) mergeConfigurations(yamlConfig, programmaticConfig Config) Config {
	if yamlConfig.Driver == "" && programmaticConfig.Driver != "" {
		yamlConfig.Driver = programmaticConfig.Driver
	}
	if yamlConfig.DSN == "" && programmaticConfig.DSN != "" {
		yamlConfig.DSN = programmaticConfig.DSN
	}

	// Default: YAML takes precedence.
	if yamlConfig.Default == "" && programmaticConfig.Default != "" {
		yamlConfig.Default = programmaticConfig.Default
	}

	return yamlConfig
}

// --- Driver Resolution ---

// resolveDriver creates a driver from config if not set programmatically.
func (e *Extension) resolveDriver() error {
	if e.driver != nil {
		return nil // Already set via WithDriver().
	}

	if e.config.Driver == "" || e.config.DSN == "" {
		return errors.New("kv: no driver configured; use WithDriver() or set driver/dsn in config")
	}

	drv, err := OpenDriver(context.Background(), e.config.Driver, e.config.DSN)
	if err != nil {
		return fmt.Errorf("kv: create driver from config: %w", err)
	}

	e.driver = drv
	return nil
}

// --- Store Initialization ---

// initStore creates the kv.Store and registers hooks (single-store mode).
func (e *Extension) initStore() error {
	s, err := kv.Open(e.driver, e.storeOpts...)
	if err != nil {
		return fmt.Errorf("kv: open: %w", err)
	}
	e.store = s

	for _, h := range e.hooks {
		s.Hooks().AddHook(h.hook, h.scope)
	}

	return nil
}
