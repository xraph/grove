// Package extension adapts Grove as a Forge extension.
package extension

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
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
	config Config
	db     *grove.DB
	logger *slog.Logger
	driver grove.GroveDriver
	groups []*migrate.Group
	hooks  []hookEntry

	// CRDT integration.
	crdtPlugin         *crdt.Plugin
	syncer             *crdt.Syncer
	syncControllerOpts []crdt.SyncControllerOption
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

	// Register CRDT sync controller if CRDT is enabled.
	if e.crdtPlugin != nil {
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

		e.logger.Info("grove: CRDT sync controller registered",
			slog.String("node_id", e.crdtPlugin.NodeID()),
		)
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
func (e *Extension) Start(ctx context.Context) error {
	if e.db == nil {
		return errors.New("grove: extension not initialized")
	}

	// Start background syncer if configured.
	if e.syncer != nil {
		go func() {
			if err := e.syncer.Run(ctx); err != nil && ctx.Err() == nil {
				e.logger.Error("grove: CRDT syncer stopped", slog.String("error", err.Error()))
			}
		}()
		e.logger.Info("grove: CRDT background syncer started")
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
