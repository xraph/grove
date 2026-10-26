package plugin

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakePlugin struct {
	name    string
	initErr error
	initDB  any
	inits   int
}

func (p *fakePlugin) Name() string { return p.name }

func (p *fakePlugin) Init(_ context.Context, db any) error {
	p.inits++
	p.initDB = db

	return p.initErr
}

func TestRegistry_RegisterGetPlugins(t *testing.T) {
	r := NewRegistry()
	assert.Empty(t, r.Plugins())

	a := &fakePlugin{name: "a"}
	b := &fakePlugin{name: "b"}
	r.Register(a)
	r.Register(b)

	assert.Same(t, a, r.Get("a"))
	assert.Same(t, b, r.Get("b"))
	assert.Nil(t, r.Get("missing"))

	plugins := r.Plugins()
	require.Len(t, plugins, 2)
	assert.Equal(t, "a", plugins[0].Name())

	// Plugins returns a copy.
	plugins[0] = b
	assert.Same(t, a, r.Plugins()[0])
}

func TestRegistry_InitAllPassesDB(t *testing.T) {
	r := NewRegistry()
	a := &fakePlugin{name: "a"}
	b := &fakePlugin{name: "b"}
	r.Register(a)
	r.Register(b)

	db := struct{ id int }{7}
	require.NoError(t, r.InitAll(t.Context(), db))

	assert.Equal(t, 1, a.inits)
	assert.Equal(t, db, a.initDB)
	assert.Equal(t, 1, b.inits)
}

func TestRegistry_InitAllStopsAtFirstError(t *testing.T) {
	initErr := errors.New("no schema")
	r := NewRegistry()
	bad := &fakePlugin{name: "bad", initErr: initErr}
	after := &fakePlugin{name: "after"}
	r.Register(bad)
	r.Register(after)

	err := r.InitAll(t.Context(), nil)

	require.ErrorIs(t, err, initErr)
	assert.Contains(t, err.Error(), "plugin bad: init")
	assert.Zero(t, after.inits, "plugins after the failing one are not initialised")
}
