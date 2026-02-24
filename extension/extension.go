// Package extension adapts Grove as a Forge extension.
package extension

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/xraph/forge"
	"github.com/xraph/vessel"

	"github.com/xraph/grove"
	"github.com/xraph/grove/crdt"
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
	*forge.BaseExtension

	config Config
	db     *grove.DB
	driver grove.GroveDriver
	groups []*migrate.Group
	hooks  []hookEntry

	// Multi-DB support.
	databases    []databaseEntry
	defaultDB    string
	manager      *DBManager
	dbHooks      map[string][]hookEntry
	dbMigrations map[string][]*migrate.Group

	// CRDT integration.
	crdtPlugin         *crdt.Plugin
	crdtHookScope      hook.Scope
	crdtDatabase       string // named DB for CRDT (multi-DB mode)
	syncer             *crdt.Syncer
	syncControllerOpts []crdt.SyncControllerOption
}

// databaseEntry holds the configuration for a named database
// provided via WithDatabase or WithDatabaseDSN options.
type databaseEntry struct {
	name       string
	driver     grove.GroveDriver
	driverName string // for DSN-based resolution
	dsn        string
}

type hookEntry struct {
	hook  any
	scope hook.Scope
}

// New creates a Grove Forge extension with the given options.
func New(opts ...ExtOption) *Extension {
	e := &Extension{
		BaseExtension: forge.NewBaseExtension(ExtensionName, ExtensionVersion, ExtensionDescription),
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// DB returns the default Grove DB instance (nil until Register is called).
func (e *Extension) DB() *grove.DB { return e.db }

// Manager returns the DBManager for multi-DB mode.
// Returns nil in single-DB mode.
func (e *Extension) Manager() *DBManager { return e.manager }

// MigrationGroups returns all registered migration groups (single-DB mode).
func (e *Extension) MigrationGroups() []*migrate.Group { return e.groups }

// isMultiDB returns true if multiple named databases are configured.
func (e *Extension) isMultiDB() bool {
	return len(e.databases) > 0 || len(e.config.Databases) > 0
}

// Register implements [forge.Extension].
func (e *Extension) Register(fapp forge.App) error {
	// 1. BaseExtension.Register stores app, logger, metrics.
	if err := e.BaseExtension.Register(fapp); err != nil {
		return err
	}

	// 2. Load config: YAML → merge with programmatic → fallback.
	if err := e.loadConfiguration(); err != nil {
		return err
	}

	// 3. Route to single-DB or multi-DB initialization.
	if e.isMultiDB() {
		if err := e.registerMultiDB(fapp); err != nil {
			return err
		}
	} else {
		if err := e.registerSingleDB(fapp); err != nil {
			return err
		}
	}

	// 4. Register CRDT sync controller if enabled and routes not disabled.
	if err := e.registerCRDT(fapp); err != nil {
		return err
	}

	return nil
}

// registerSingleDB handles the original single-database path.
func (e *Extension) registerSingleDB(fapp forge.App) error {
	// Build driver from config if not set via WithDriver().
	if err := e.resolveDriver(); err != nil {
		return err
	}

	// Init DB, register hooks.
	if err := e.initDB(); err != nil {
		return err
	}

	// Register *grove.DB in DI container.
	if err := vessel.Provide(fapp.Container(), func() (*grove.DB, error) {
		return e.db, nil
	}); err != nil {
		return fmt.Errorf("grove: register db in container: %w", err)
	}

	e.Logger().Info("grove extension registered",
		forge.F("driver", e.driver.Name()),
	)
	return nil
}

// registerMultiDB handles the multi-database path.
func (e *Extension) registerMultiDB(fapp forge.App) error {
	mgr := NewDBManager()

	// Merge programmatic entries with config entries.
	entries := e.buildDatabaseEntries()

	// Open each database.
	for _, entry := range entries {
		drv, err := e.resolveDatabaseDriver(entry)
		if err != nil {
			return fmt.Errorf("grove: database %q: %w", entry.name, err)
		}

		db, err := grove.Open(drv)
		if err != nil {
			return fmt.Errorf("grove: open database %q: %w", entry.name, err)
		}

		// Apply global hooks.
		for _, h := range e.hooks {
			db.Hooks().AddHook(h.hook, h.scope)
		}

		// Apply per-DB hooks.
		if dbHooks, ok := e.dbHooks[entry.name]; ok {
			for _, h := range dbHooks {
				db.Hooks().AddHook(h.hook, h.scope)
			}
		}

		mgr.Add(entry.name, db)

		e.Logger().Info("grove: database opened",
			forge.F("name", entry.name),
			forge.F("driver", drv.Name()),
		)
	}

	// Set default database.
	defaultName := e.resolveDefaultName(entries)
	if defaultName != "" {
		if err := mgr.SetDefault(defaultName); err != nil {
			return fmt.Errorf("grove: set default database: %w", err)
		}
	}

	e.manager = mgr

	// Set e.db to the default for backward-compatible DB() accessor.
	defaultDB, err := mgr.Default()
	if err != nil {
		return fmt.Errorf("grove: get default database: %w", err)
	}
	e.db = defaultDB

	// Apply CRDT hooks to the target database.
	if e.crdtPlugin != nil {
		crdtDB, err := e.resolveCRDTDatabase()
		if err != nil {
			return err
		}
		crdtDB.Hooks().AddHook(e.crdtPlugin, e.crdtHookScope)
	}

	// Register in DI.
	if err := e.registerMultiDBInDI(fapp); err != nil {
		return err
	}

	e.Logger().Info("grove extension registered",
		forge.F("mode", "multi-db"),
		forge.F("databases", mgr.Len()),
		forge.F("default", defaultName),
	)
	return nil
}

// buildDatabaseEntries merges programmatic and config-based database entries.
// Programmatic entries take precedence over config entries with the same name.
func (e *Extension) buildDatabaseEntries() []databaseEntry {
	entries := make([]databaseEntry, len(e.databases))
	copy(entries, e.databases)

	// Track names from programmatic entries.
	seen := make(map[string]bool, len(entries))
	for _, entry := range entries {
		seen[entry.name] = true
	}

	// Add config entries that aren't already provided programmatically.
	for _, cfg := range e.config.Databases {
		if seen[cfg.Name] {
			continue
		}
		entries = append(entries, databaseEntry{
			name:       cfg.Name,
			driverName: cfg.Driver,
			dsn:        cfg.DSN,
		})
	}

	return entries
}

// resolveDatabaseDriver creates a driver for a database entry.
func (e *Extension) resolveDatabaseDriver(entry databaseEntry) (grove.GroveDriver, error) {
	if entry.driver != nil {
		return entry.driver, nil
	}
	if entry.driverName == "" || entry.dsn == "" {
		return nil, errors.New("driver and dsn are required")
	}
	return grove.OpenDriver(context.Background(), entry.driverName, entry.dsn)
}

// resolveDefaultName determines the default database name.
func (e *Extension) resolveDefaultName(entries []databaseEntry) string {
	if e.defaultDB != "" {
		return e.defaultDB
	}
	if e.config.Default != "" {
		return e.config.Default
	}
	if len(entries) > 0 {
		return entries[0].name
	}
	return ""
}

// resolveCRDTDatabase returns the database that CRDT hooks should attach to.
func (e *Extension) resolveCRDTDatabase() (*grove.DB, error) {
	if e.crdtDatabase != "" {
		db, err := e.manager.Get(e.crdtDatabase)
		if err != nil {
			return nil, fmt.Errorf("grove: crdt database %q: %w", e.crdtDatabase, err)
		}
		return db, nil
	}
	return e.manager.Default()
}

// registerMultiDBInDI registers the manager and all databases in the DI container.
func (e *Extension) registerMultiDBInDI(fapp forge.App) error {
	// Register the DBManager itself.
	if err := vessel.Provide(fapp.Container(), func() *DBManager {
		return e.manager
	}); err != nil {
		return fmt.Errorf("grove: register db manager in container: %w", err)
	}

	// Register default *grove.DB (unnamed — backward compatible).
	if err := vessel.Provide(fapp.Container(), func() (*grove.DB, error) {
		return e.manager.Default()
	}); err != nil {
		return fmt.Errorf("grove: register default db in container: %w", err)
	}

	// Register each named database.
	for name, db := range e.manager.All() {
		namedDB := db // capture loop variable
		if err := vessel.ProvideNamed(fapp.Container(), name, func() *grove.DB {
			return namedDB
		}); err != nil {
			return fmt.Errorf("grove: register db %q in container: %w", name, err)
		}
	}

	return nil
}

// registerCRDT registers the CRDT sync controller if enabled.
func (e *Extension) registerCRDT(fapp forge.App) error {
	if e.crdtPlugin == nil || e.config.DisableRoutes {
		return nil
	}

	ctrl := crdt.NewSyncController(e.crdtPlugin, e.syncControllerOpts...)
	forgeCtrl := &crdtForgeController{ctrl: ctrl}

	if err := fapp.RegisterController(forgeCtrl); err != nil {
		return fmt.Errorf("grove: register crdt controller: %w", err)
	}

	// Provide CRDT plugin via DI.
	if err := vessel.Provide(fapp.Container(), func() *crdt.Plugin {
		return e.crdtPlugin
	}); err != nil {
		return fmt.Errorf("grove: register crdt plugin in container: %w", err)
	}

	e.Logger().Info("grove: CRDT sync controller registered",
		forge.F("node_id", e.crdtPlugin.NodeID()),
	)
	return nil
}

// Init builds the DB and registers hooks. Can be called standalone outside Forge.
func (e *Extension) Init(_ forge.App) error {
	if e.driver == nil {
		return errors.New("grove: no driver configured; use WithDriver()")
	}
	return e.initDB()
}

// Start implements [forge.Extension].
func (e *Extension) Start(ctx context.Context) error {
	if e.db == nil {
		return errors.New("grove: extension not initialized")
	}

	// Start background syncer if configured.
	if e.syncer != nil {
		go func() {
			if err := e.syncer.Run(ctx); err != nil && ctx.Err() == nil {
				e.Logger().Error("grove: CRDT syncer stopped", forge.F("error", err.Error()))
			}
		}()
		e.Logger().Info("grove: CRDT background syncer started")
	}

	e.MarkStarted()
	return nil
}

// Stop gracefully shuts down all Grove databases.
func (e *Extension) Stop(_ context.Context) error {
	if e.manager != nil {
		e.manager.Close()
	} else if e.db != nil {
		e.db.Close()
	}
	e.MarkStopped()
	return nil
}

// Health implements [forge.Extension].
func (e *Extension) Health(_ context.Context) error {
	if e.manager != nil {
		for name, db := range e.manager.All() {
			if db == nil {
				return fmt.Errorf("grove: database %q is nil", name)
			}
		}
		return nil
	}
	if e.db == nil {
		return errors.New("grove: extension not initialized")
	}
	return nil
}

// --- Config Loading (mirrors forge database extension pattern) ---

// loadConfiguration loads config from YAML files or programmatic sources.
func (e *Extension) loadConfiguration() error {
	programmaticConfig := e.config
	hasProgrammaticDriver := e.driver != nil || (programmaticConfig.Driver != "" && programmaticConfig.DSN != "")
	hasProgrammaticDBs := len(e.databases) > 0

	// Try loading from config file.
	finalConfig, configLoaded := e.tryLoadFromConfigFile()

	if !configLoaded {
		if programmaticConfig.RequireConfig {
			return errors.New("grove: configuration is required but not found in config files; " +
				"ensure 'extensions.grove' or 'grove' key exists in your config")
		}

		finalConfig = e.selectProgrammaticOrDefaultConfig(programmaticConfig, hasProgrammaticDriver || hasProgrammaticDBs)
	} else {
		// Config loaded from YAML — merge with programmatic options.
		finalConfig = e.mergeConfigurations(finalConfig, programmaticConfig)
	}

	e.Logger().Debug("grove: configuration loaded",
		forge.F("driver", finalConfig.Driver),
		forge.F("disable_routes", finalConfig.DisableRoutes),
		forge.F("disable_migrate", finalConfig.DisableMigrate),
		forge.F("databases", len(finalConfig.Databases)),
	)

	e.config = finalConfig

	if err := e.config.Validate(); err != nil {
		return fmt.Errorf("invalid grove configuration: %w", err)
	}

	return nil
}

// tryLoadFromConfigFile attempts to load config from YAML files.
func (e *Extension) tryLoadFromConfigFile() (Config, bool) {
	cm := e.App().Config()
	var cfg Config

	// Try "extensions.grove" first (namespaced pattern).
	if cm.IsSet("extensions.grove") {
		if err := cm.Bind("extensions.grove", &cfg); err == nil {
			e.Logger().Debug("grove: loaded config from file",
				forge.F("key", "extensions.grove"),
			)
			return cfg, true
		}
		e.Logger().Warn("grove: failed to bind extensions.grove config",
			forge.F("error", "bind failed"),
		)
	}

	// Try legacy "grove" key.
	if cm.IsSet("grove") {
		if err := cm.Bind("grove", &cfg); err == nil {
			e.Logger().Debug("grove: loaded config from file",
				forge.F("key", "grove"),
			)
			return cfg, true
		}
		e.Logger().Warn("grove: failed to bind grove config",
			forge.F("error", "bind failed"),
		)
	}

	return Config{}, false
}

// selectProgrammaticOrDefaultConfig selects between programmatic config and defaults.
func (e *Extension) selectProgrammaticOrDefaultConfig(programmaticConfig Config, hasProgrammaticDriver bool) Config {
	if hasProgrammaticDriver {
		e.Logger().Debug("grove: using programmatic configuration")
		return programmaticConfig
	}

	e.Logger().Debug("grove: using default configuration")
	return DefaultConfig()
}

// mergeConfigurations merges YAML config with programmatic options.
// YAML config takes precedence for Driver/DSN; programmatic fills gaps.
func (e *Extension) mergeConfigurations(yamlConfig, programmaticConfig Config) Config {
	// YAML takes precedence for driver/DSN.
	if yamlConfig.Driver == "" && programmaticConfig.Driver != "" {
		yamlConfig.Driver = programmaticConfig.Driver
	}
	if yamlConfig.DSN == "" && programmaticConfig.DSN != "" {
		yamlConfig.DSN = programmaticConfig.DSN
	}

	// Programmatic bool flags fill in if not set in YAML.
	// Note: bool zero-value is false, so programmatic true overrides YAML false.
	if programmaticConfig.DisableRoutes {
		yamlConfig.DisableRoutes = true
	}
	if programmaticConfig.DisableMigrate {
		yamlConfig.DisableMigrate = true
	}

	// BasePath: YAML takes precedence.
	if yamlConfig.BasePath == "" && programmaticConfig.BasePath != "" {
		yamlConfig.BasePath = programmaticConfig.BasePath
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
		return errors.New("grove: no driver configured; use WithDriver() or set driver/dsn in config")
	}

	drv, err := grove.OpenDriver(context.Background(), e.config.Driver, e.config.DSN)
	if err != nil {
		return fmt.Errorf("grove: create driver from config: %w", err)
	}

	e.driver = drv
	return nil
}

// --- DB Initialization ---

// initDB creates the grove.DB and registers hooks (single-DB mode).
func (e *Extension) initDB() error {
	db, err := grove.Open(e.driver)
	if err != nil {
		return fmt.Errorf("grove: open: %w", err)
	}
	e.db = db

	for _, h := range e.hooks {
		db.Hooks().AddHook(h.hook, h.scope)
	}

	// In single-DB mode, CRDT hooks are applied directly.
	if e.crdtPlugin != nil {
		db.Hooks().AddHook(e.crdtPlugin, e.crdtHookScope)
	}

	return nil
}

// --- CRDT Forge Controller ---

// crdtForgeController adapts the crdt.SyncController as a forge.Controller.
// It registers sync routes (pull, push, stream) on the Forge router.
type crdtForgeController struct {
	ctrl *crdt.SyncController
}

// Ensure compile-time interface compliance.
var _ forge.Controller = (*crdtForgeController)(nil)

// Name implements forge.Controller.
func (c *crdtForgeController) Name() string { return "crdt-sync" }

// Routes implements forge.Controller. It registers CRDT sync routes
// on the Forge router with proper request/response schemas and tags.
func (c *crdtForgeController) Routes(r forge.Router) error {
	sync := r.Group("/sync")

	// POST /sync/pull — remote nodes pull changes from this node.
	if err := sync.POST("/pull", c.handlePull,
		forge.WithName("crdt.pull"),
		forge.WithTags("crdt", "sync"),
	); err != nil {
		return fmt.Errorf("crdt: register pull route: %w", err)
	}

	// POST /sync/push — remote nodes push changes to this node.
	if err := sync.POST("/push", c.handlePush,
		forge.WithName("crdt.push"),
		forge.WithTags("crdt", "sync"),
	); err != nil {
		return fmt.Errorf("crdt: register push route: %w", err)
	}

	// GET /sync/stream — SSE stream of real-time changes.
	if err := sync.EventStream("/stream", c.handleStream,
		forge.WithName("crdt.stream"),
		forge.WithTags("crdt", "sync", "streaming"),
	); err != nil {
		return fmt.Errorf("crdt: register stream route: %w", err)
	}

	return nil
}

// handlePull handles POST /sync/pull using Forge context.
func (c *crdtForgeController) handlePull(ctx forge.Context) error {
	var req crdt.PullRequest
	if err := ctx.Bind(&req); err != nil {
		return ctx.JSON(400, map[string]string{"error": fmt.Sprintf("invalid request: %v", err)})
	}

	resp, err := c.ctrl.HandlePull(ctx.Request().Context(), &req)
	if err != nil {
		return ctx.JSON(500, map[string]string{"error": err.Error()})
	}

	return ctx.JSON(200, resp)
}

// handlePush handles POST /sync/push using Forge context.
func (c *crdtForgeController) handlePush(ctx forge.Context) error {
	var req crdt.PushRequest
	if err := ctx.Bind(&req); err != nil {
		return ctx.JSON(400, map[string]string{"error": fmt.Sprintf("invalid request: %v", err)})
	}

	resp, err := c.ctrl.HandlePush(ctx.Request().Context(), &req)
	if err != nil {
		return ctx.JSON(500, map[string]string{"error": err.Error()})
	}

	return ctx.JSON(200, resp)
}

// handleStream handles GET /sync/stream using Forge SSE streaming.
func (c *crdtForgeController) handleStream(ctx forge.Context, stream forge.Stream) error {
	// Parse query params.
	tablesParam := ctx.Query("tables")
	var tables []string
	if tablesParam != "" {
		for _, t := range splitAndTrim(tablesParam, ",") {
			if t != "" {
				tables = append(tables, t)
			}
		}
	}

	// Parse since HLC from query params.
	var since crdt.HLC
	if ts := ctx.Query("since_ts"); ts != "" {
		fmt.Sscanf(ts, "%d", &since.Timestamp)
	}
	if cnt := ctx.Query("since_count"); cnt != "" {
		fmt.Sscanf(cnt, "%d", &since.Counter)
	}
	since.NodeID = ctx.Query("since_node")

	// Start streaming changes.
	ch, err := c.ctrl.StreamChangesSince(stream.Context(), tables, since)
	if err != nil {
		return stream.Send("error", []byte(err.Error()))
	}

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case changes, ok := <-ch:
			if !ok {
				return nil // Channel closed.
			}
			data, err := json.Marshal(changes)
			if err != nil {
				continue
			}
			if err := stream.Send("changes", data); err != nil {
				return err
			}
		}
	}
}

// splitAndTrim splits a string by sep and trims whitespace from each part.
func splitAndTrim(s, sep string) []string {
	parts := strings.Split(s, sep)
	var result []string
	for _, p := range parts {
		trimmed := strings.TrimSpace(p)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}
