// Package plugin provides the interface for Grove plugins and a registry for managing them.
package plugin

import (
	"context"
	"fmt"
)

// Plugin is the interface that all Grove plugins must implement.
type Plugin interface {
	// Name returns the plugin identifier.
	Name() string
	// Init is called once when the plugin is registered with a DB.
	Init(ctx context.Context, db any) error
}

// WithHooks is an optional interface for plugins that register hooks.
type WithHooks interface {
	Plugin
	// RegisterHooks is called to let the plugin register its hooks.
	RegisterHooks(hookEngine any) error
}

// WithMigrations is an optional interface for plugins that provide migrations.
type WithMigrations interface {
	Plugin
	// MigrationGroup returns the plugin's migration group.
	MigrationGroup() any
}

// Registry manages registered plugins.
type Registry struct {
	plugins []Plugin
}

// NewRegistry creates a new plugin registry.
func NewRegistry() *Registry {
	return &Registry{}
}

// Register adds a plugin to the registry.
func (r *Registry) Register(p Plugin) {
	r.plugins = append(r.plugins, p)
}

// Plugins returns all registered plugins.
func (r *Registry) Plugins() []Plugin {
	result := make([]Plugin, len(r.plugins))
	copy(result, r.plugins)
	return result
}

// Get returns a plugin by name, or nil if not found.
func (r *Registry) Get(name string) Plugin {
	for _, p := range r.plugins {
		if p.Name() == name {
			return p
		}
	}
	return nil
}

// InitAll initializes all registered plugins.
func (r *Registry) InitAll(ctx context.Context, db any) error {
	for _, p := range r.plugins {
		if err := p.Init(ctx, db); err != nil {
			return fmt.Errorf("plugin %s: init: %w", p.Name(), err)
		}
	}
	return nil
}
