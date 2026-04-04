// Package extension adapts Grove as a Forge extension.
package extension

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
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
	syncController     *crdt.SyncController // initialized during registerCRDT
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
	return e.registerCRDT(fapp)
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
	e.syncController = ctrl
	forgeCtrl := &crdtForgeController{ctrl: ctrl, basePath: e.config.BasePath}

	if err := fapp.RegisterController(forgeCtrl); err != nil {
		return fmt.Errorf("grove: register crdt controller: %w", err)
	}

	// Provide CRDT plugin via DI.
	if err := vessel.Provide(fapp.Container(), func() *crdt.Plugin {
		return e.crdtPlugin
	}); err != nil {
		return fmt.Errorf("grove: register crdt plugin in container: %w", err)
	}

	// Provide SyncController via DI.
	if err := vessel.Provide(fapp.Container(), func() *crdt.SyncController {
		return ctrl
	}); err != nil {
		return fmt.Errorf("grove: register crdt controller in container: %w", err)
	}

	// Provide RoomManager via DI if enabled.
	if ctrl.Rooms() != nil {
		if err := vessel.Provide(fapp.Container(), func() *crdt.RoomManager {
			return ctrl.Rooms()
		}); err != nil {
			return fmt.Errorf("grove: register room manager in container: %w", err)
		}
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

// Stop gracefully shuts down all Grove databases and CRDT subsystems.
func (e *Extension) Stop(_ context.Context) error {
	// Clean up CRDT subsystems.
	if e.syncController != nil {
		e.syncController.Close()
	}

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
	ctrl     *crdt.SyncController
	basePath string // URL prefix for routes (default: "/sync")
}

// Ensure compile-time interface compliance.
var _ forge.Controller = (*crdtForgeController)(nil)

// Name implements forge.Controller.
func (c *crdtForgeController) Name() string { return "crdt-sync" }

// Routes implements forge.Controller. It registers CRDT sync routes
// on the Forge router with proper request/response schemas and tags.
// The base path defaults to "/sync" but can be overridden via WithBasePath.
func (c *crdtForgeController) Routes(r forge.Router) error {
	base := c.basePath
	if base == "" {
		base = "/sync"
	}
	sync := r.Group(base)

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

	// Presence routes (only registered when presence is enabled).
	if c.ctrl.Presence() != nil {
		// POST /sync/presence — update presence for a topic.
		if err := sync.POST("/presence", c.handlePresenceUpdate,
			forge.WithName("crdt.presence.update"),
			forge.WithTags("crdt", "sync", "presence"),
		); err != nil {
			return fmt.Errorf("crdt: register presence update route: %w", err)
		}

		// GET /sync/presence — get current presence for a topic.
		if err := sync.GET("/presence", c.handleGetPresence,
			forge.WithName("crdt.presence.get"),
			forge.WithTags("crdt", "sync", "presence"),
		); err != nil {
			return fmt.Errorf("crdt: register presence get route: %w", err)
		}
	}

	// Time-travel routes (only registered when time-travel is enabled).
	if c.ctrl.TimeTravelEnabled() {
		if err := sync.GET("/history", c.handleHistory,
			forge.WithName("crdt.history"),
			forge.WithTags("crdt", "sync", "timetravel"),
		); err != nil {
			return fmt.Errorf("crdt: register history route: %w", err)
		}

		if err := sync.POST("/history", c.handleHistory,
			forge.WithName("crdt.history.post"),
			forge.WithTags("crdt", "sync", "timetravel"),
		); err != nil {
			return fmt.Errorf("crdt: register history post route: %w", err)
		}

		if err := sync.GET("/field-history", c.handleFieldHistory,
			forge.WithName("crdt.field-history"),
			forge.WithTags("crdt", "sync", "timetravel"),
		); err != nil {
			return fmt.Errorf("crdt: register field-history route: %w", err)
		}
	}

	// Room management routes (only registered when room manager is enabled).
	if c.ctrl.Rooms() != nil {
		rooms := sync.Group("/rooms")

		if err := rooms.GET("", c.handleListRooms,
			forge.WithName("crdt.rooms.list"),
			forge.WithTags("crdt", "sync", "rooms"),
		); err != nil {
			return fmt.Errorf("crdt: register rooms list route: %w", err)
		}

		if err := rooms.POST("", c.handleCreateRoom,
			forge.WithName("crdt.rooms.create"),
			forge.WithTags("crdt", "sync", "rooms"),
		); err != nil {
			return fmt.Errorf("crdt: register rooms create route: %w", err)
		}

		if err := rooms.GET("/:id", c.handleGetRoom,
			forge.WithName("crdt.rooms.get"),
			forge.WithTags("crdt", "sync", "rooms"),
		); err != nil {
			return fmt.Errorf("crdt: register rooms get route: %w", err)
		}

		if err := rooms.POST("/:id/join", c.handleJoinRoom,
			forge.WithName("crdt.rooms.join"),
			forge.WithTags("crdt", "sync", "rooms"),
		); err != nil {
			return fmt.Errorf("crdt: register rooms join route: %w", err)
		}

		if err := rooms.POST("/:id/leave", c.handleLeaveRoom,
			forge.WithName("crdt.rooms.leave"),
			forge.WithTags("crdt", "sync", "rooms"),
		); err != nil {
			return fmt.Errorf("crdt: register rooms leave route: %w", err)
		}

		if err := rooms.GET("/:id/participants", c.handleGetParticipants,
			forge.WithName("crdt.rooms.participants"),
			forge.WithTags("crdt", "sync", "rooms"),
		); err != nil {
			return fmt.Errorf("crdt: register rooms participants route: %w", err)
		}

		if err := rooms.POST("/:id/cursor", c.handleUpdateCursor,
			forge.WithName("crdt.rooms.cursor"),
			forge.WithTags("crdt", "sync", "rooms", "cursor"),
		); err != nil {
			return fmt.Errorf("crdt: register rooms cursor route: %w", err)
		}

		if err := rooms.POST("/:id/typing", c.handleUpdateTyping,
			forge.WithName("crdt.rooms.typing"),
			forge.WithTags("crdt", "sync", "rooms", "presence"),
		); err != nil {
			return fmt.Errorf("crdt: register rooms typing route: %w", err)
		}

		if err := rooms.PUT("/:id/metadata", c.handleUpdateRoomMetadata,
			forge.WithName("crdt.rooms.metadata"),
			forge.WithTags("crdt", "sync", "rooms"),
		); err != nil {
			return fmt.Errorf("crdt: register rooms metadata route: %w", err)
		}
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

// handlePresenceUpdate handles POST /sync/presence using Forge context.
func (c *crdtForgeController) handlePresenceUpdate(ctx forge.Context) error {
	var update crdt.PresenceUpdate
	if err := ctx.Bind(&update); err != nil {
		return ctx.JSON(400, map[string]string{"error": fmt.Sprintf("invalid request: %v", err)})
	}

	event, err := c.ctrl.HandlePresenceUpdate(ctx.Request().Context(), &update)
	if err != nil {
		return ctx.JSON(500, map[string]string{"error": err.Error()})
	}

	return ctx.JSON(200, event)
}

// handleGetPresence handles GET /sync/presence using Forge context.
func (c *crdtForgeController) handleGetPresence(ctx forge.Context) error {
	topic := ctx.Query("topic")
	if topic == "" {
		return ctx.JSON(400, map[string]string{"error": "missing topic query parameter"})
	}

	snapshot, err := c.ctrl.HandleGetPresence(ctx.Request().Context(), topic)
	if err != nil {
		return ctx.JSON(500, map[string]string{"error": err.Error()})
	}

	return ctx.JSON(200, snapshot)
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
		if v, err := strconv.ParseInt(ts, 10, 64); err == nil {
			since.Timestamp = v
		}
	}
	if cnt := ctx.Query("since_count"); cnt != "" {
		if v, err := strconv.ParseUint(cnt, 10, 32); err == nil {
			since.Counter = uint32(v)
		}
	}
	since.NodeID = ctx.Query("since_node")

	// Parse node_id for presence cleanup on disconnect.
	nodeID := ctx.Query("node_id")

	// Start streaming changes.
	ch, err := c.ctrl.StreamChangesSince(stream.Context(), tables, since)
	if err != nil {
		return stream.Send("error", []byte(err.Error()))
	}

	// Get presence channel (nil if presence is disabled).
	presenceCh := c.ctrl.PresenceChannel()

	// Clean up presence on disconnect.
	defer func() {
		if nodeID != "" && c.ctrl.Presence() != nil {
			c.ctrl.Presence().RemoveNode(nodeID)
		}
	}()

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
		case event, ok := <-presenceCh:
			if !ok {
				continue // Channel closed, presence disabled.
			}
			data, err := crdt.MarshalPresenceEvent(event)
			if err != nil {
				continue
			}
			if err := stream.Send("presence", data); err != nil {
				return err
			}
		}
	}
}

// --- Time-Travel Handlers ---

// handleHistory handles GET/POST /sync/history using Forge context.
func (c *crdtForgeController) handleHistory(ctx forge.Context) error {
	var req crdt.HistoryRequest
	if ctx.Request().Method == "GET" {
		req.Table = ctx.Query("table")
		req.PK = ctx.Query("pk")
		if ts := ctx.Query("at_ts"); ts != "" {
			if v, err := strconv.ParseInt(ts, 10, 64); err == nil {
				req.AtHLC.Timestamp = v
			}
		}
		if cnt := ctx.Query("at_c"); cnt != "" {
			if v, err := strconv.ParseUint(cnt, 10, 32); err == nil {
				req.AtHLC.Counter = uint32(v)
			}
		}
		req.AtHLC.NodeID = ctx.Query("at_node")
	} else {
		if err := ctx.Bind(&req); err != nil {
			return ctx.JSON(400, map[string]string{"error": fmt.Sprintf("invalid request: %v", err)})
		}
	}

	if req.Table == "" || req.PK == "" {
		return ctx.JSON(400, map[string]string{"error": "table and pk are required"})
	}

	resp, err := c.ctrl.HandleHistory(ctx.Request().Context(), &req)
	if err != nil {
		return ctx.JSON(500, map[string]string{"error": err.Error()})
	}
	return ctx.JSON(200, resp)
}

// handleFieldHistory handles GET /sync/field-history using Forge context.
func (c *crdtForgeController) handleFieldHistory(ctx forge.Context) error {
	var req crdt.FieldHistoryRequest
	req.Table = ctx.Query("table")
	req.PK = ctx.Query("pk")
	req.Field = ctx.Query("field")
	if limit := ctx.Query("limit"); limit != "" {
		if v, err := strconv.Atoi(limit); err == nil {
			req.Limit = v
		}
	}

	if req.Table == "" || req.PK == "" || req.Field == "" {
		return ctx.JSON(400, map[string]string{"error": "table, pk, and field are required"})
	}

	resp, err := c.ctrl.HandleFieldHistory(ctx.Request().Context(), &req)
	if err != nil {
		return ctx.JSON(500, map[string]string{"error": err.Error()})
	}
	return ctx.JSON(200, resp)
}

// --- Room Handlers ---

// handleListRooms handles GET /sync/rooms.
func (c *crdtForgeController) handleListRooms(ctx forge.Context) error {
	rm := c.ctrl.Rooms()
	roomType := ctx.Query("type")
	var rooms []crdt.RoomInfo
	if roomType != "" {
		rooms = rm.ListRoomsByType(roomType)
	} else {
		rooms = rm.ListRooms()
	}
	return ctx.JSON(200, rooms)
}

// handleCreateRoom handles POST /sync/rooms.
func (c *crdtForgeController) handleCreateRoom(ctx forge.Context) error {
	rm := c.ctrl.Rooms()
	var req struct {
		ID              string          `json:"id"`
		Type            string          `json:"type"`
		Metadata        json.RawMessage `json:"metadata,omitempty"`
		MaxParticipants int             `json:"max_participants,omitempty"`
		CreatedBy       string          `json:"created_by,omitempty"`
	}
	if err := ctx.Bind(&req); err != nil {
		return ctx.JSON(400, map[string]string{"error": fmt.Sprintf("invalid request: %v", err)})
	}
	if req.ID == "" {
		return ctx.JSON(400, map[string]string{"error": "id is required"})
	}

	var opts []crdt.RoomOption
	if req.Metadata != nil {
		opts = append(opts, func(r *crdt.Room) { r.Metadata = req.Metadata })
	}
	if req.MaxParticipants > 0 {
		opts = append(opts, crdt.WithMaxParticipants(req.MaxParticipants))
	}
	if req.CreatedBy != "" {
		opts = append(opts, crdt.WithRoomCreator(req.CreatedBy))
	}

	room, err := rm.CreateRoom(ctx.Request().Context(), req.ID, req.Type, opts...)
	if err != nil {
		return ctx.JSON(500, map[string]string{"error": err.Error()})
	}
	return ctx.JSON(201, room)
}

// handleGetRoom handles GET /sync/rooms/:id.
func (c *crdtForgeController) handleGetRoom(ctx forge.Context) error {
	rm := c.ctrl.Rooms()
	id := ctx.Param("id")
	info := rm.GetRoomInfo(id)
	if info == nil {
		return ctx.JSON(404, map[string]string{"error": "room not found"})
	}
	return ctx.JSON(200, info)
}

// handleJoinRoom handles POST /sync/rooms/:id/join.
func (c *crdtForgeController) handleJoinRoom(ctx forge.Context) error {
	rm := c.ctrl.Rooms()
	id := ctx.Param("id")
	var req struct {
		NodeID string          `json:"node_id"`
		Data   json.RawMessage `json:"data,omitempty"`
	}
	if err := ctx.Bind(&req); err != nil {
		return ctx.JSON(400, map[string]string{"error": fmt.Sprintf("invalid request: %v", err)})
	}
	if req.NodeID == "" {
		return ctx.JSON(400, map[string]string{"error": "node_id is required"})
	}

	if err := rm.JoinRoom(ctx.Request().Context(), id, req.NodeID, req.Data); err != nil {
		return ctx.JSON(409, map[string]string{"error": err.Error()})
	}

	info := rm.GetRoomInfo(id)
	return ctx.JSON(200, info)
}

// handleLeaveRoom handles POST /sync/rooms/:id/leave.
func (c *crdtForgeController) handleLeaveRoom(ctx forge.Context) error {
	rm := c.ctrl.Rooms()
	id := ctx.Param("id")
	var req struct {
		NodeID string `json:"node_id"`
	}
	if err := ctx.Bind(&req); err != nil {
		return ctx.JSON(400, map[string]string{"error": fmt.Sprintf("invalid request: %v", err)})
	}
	if req.NodeID == "" {
		return ctx.JSON(400, map[string]string{"error": "node_id is required"})
	}

	rm.LeaveRoom(ctx.Request().Context(), id, req.NodeID)
	return ctx.NoContent(204)
}

// handleGetParticipants handles GET /sync/rooms/:id/participants.
func (c *crdtForgeController) handleGetParticipants(ctx forge.Context) error {
	rm := c.ctrl.Rooms()
	id := ctx.Param("id")
	participants := rm.GetDocumentParticipants("", id)
	// Use presence.Get directly since the room ID IS the topic.
	if rm.GetRoom(id) != nil {
		participants = c.ctrl.Presence().Get(id)
	}
	if participants == nil {
		participants = []crdt.PresenceState{}
	}
	return ctx.JSON(200, participants)
}

// handleUpdateCursor handles POST /sync/rooms/:id/cursor.
func (c *crdtForgeController) handleUpdateCursor(ctx forge.Context) error {
	rm := c.ctrl.Rooms()
	id := ctx.Param("id")
	var req struct {
		NodeID string              `json:"node_id"`
		Cursor crdt.CursorPosition `json:"cursor"`
	}
	if err := ctx.Bind(&req); err != nil {
		return ctx.JSON(400, map[string]string{"error": fmt.Sprintf("invalid request: %v", err)})
	}
	rm.UpdateCursor(id, req.NodeID, req.Cursor)
	return ctx.NoContent(204)
}

// handleUpdateTyping handles POST /sync/rooms/:id/typing.
func (c *crdtForgeController) handleUpdateTyping(ctx forge.Context) error {
	rm := c.ctrl.Rooms()
	id := ctx.Param("id")
	var req struct {
		NodeID   string `json:"node_id"`
		IsTyping bool   `json:"is_typing"`
	}
	if err := ctx.Bind(&req); err != nil {
		return ctx.JSON(400, map[string]string{"error": fmt.Sprintf("invalid request: %v", err)})
	}
	rm.UpdateTypingStatus(id, req.NodeID, req.IsTyping)
	return ctx.NoContent(204)
}

// handleUpdateRoomMetadata handles PUT /sync/rooms/:id/metadata.
func (c *crdtForgeController) handleUpdateRoomMetadata(ctx forge.Context) error {
	rm := c.ctrl.Rooms()
	id := ctx.Param("id")
	var metadata json.RawMessage
	if err := ctx.Bind(&metadata); err != nil {
		return ctx.JSON(400, map[string]string{"error": fmt.Sprintf("invalid request: %v", err)})
	}
	if err := rm.SetRoomMetadata(id, metadata); err != nil {
		return ctx.JSON(404, map[string]string{"error": err.Error()})
	}
	return ctx.NoContent(204)
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
