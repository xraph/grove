package migrate

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func noopFunc(_ context.Context, _ *DB) error { return nil }

func TestParseMigrationFile(t *testing.T) {
	tests := []struct {
		name        string
		path        string
		wantVersion string
		wantName    string
		wantErr     bool
	}{
		{
			name:        "version and name",
			path:        "/x/20240102030405_create_widgets.go",
			wantVersion: "20240102030405",
			wantName:    "create_widgets",
		},
		{
			name:        "relative path",
			path:        "migrations/20240102030405_add_index.go",
			wantVersion: "20240102030405",
			wantName:    "add_index",
		},
		{name: "no version", path: "/x/migrations.go", wantErr: true},
		{name: "short version", path: "/x/2024_create.go", wantErr: true},
		{name: "empty name", path: "/x/20240102030405_.go", wantErr: true},
		{name: "not a go file", path: "/x/20240102030405_create.sql", wantErr: true},
		{name: "version only", path: "/x/20240102030405.go", wantErr: true},
		{name: "empty path", path: "", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			version, name, err := parseMigrationFile(tt.path)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "migrate: ")
				assert.Empty(t, version)
				assert.Empty(t, name)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantVersion, version)
			assert.Equal(t, tt.wantName, name)
		})
	}
}

func TestDefine_setsFields(t *testing.T) {
	m := Define("20240102030405", "create_widgets", noopFunc, noopFunc, Comment("widgets table"))

	assert.Equal(t, "20240102030405", m.Version)
	assert.Equal(t, "create_widgets", m.Name)
	assert.Equal(t, "widgets table", m.Comment)
	assert.True(t, m.Transactional)
	assert.Empty(t, m.Group, "the group sets this on registration")
	assert.NotNil(t, m.Up)
	assert.NotNil(t, m.Down)
}

func TestDefine_noTransaction(t *testing.T) {
	m := Define("20240102030405", "create_widgets", noopFunc, noopFunc, NoTransaction())
	assert.False(t, m.Transactional)

	m = Define("20240102030405", "create_widgets", noopFunc, noopFunc, Comment("x"))
	assert.Equal(t, "x", m.Comment)
	assert.True(t, m.Transactional, "an option other than NoTransaction leaves it on")
}

func TestDefine_optionsApplyInOrder(t *testing.T) {
	m := Define("20240102030405", "n", noopFunc, nil, NoTransaction(), Comment("first"), Comment("second"))
	assert.Equal(t, "second", m.Comment)
	assert.False(t, m.Transactional)
}

func TestDefine_nilFuncsStayNil(t *testing.T) {
	m := Define("20240102030405", "n", nil, nil)

	// The migrator checks Up for nil and reports a clean error, which it can
	// only do if Define didn't wrap the missing function in a closure.
	assert.Nil(t, m.Up)
	assert.Nil(t, m.Down)
}

func TestDefine_upReceivesDB(t *testing.T) {
	fake := &fakeExecutor{}
	var got *DB
	up := func(_ context.Context, db *DB) error {
		got = db
		return nil
	}

	m := Define("20240102030405", "n", up, nil)
	require.NoError(t, m.Up(context.Background(), fake))

	require.NotNil(t, got)
	assert.Same(t, fake, got.Executor())
}

func TestDefine_downReceivesDB(t *testing.T) {
	fake := &fakeExecutor{}
	var got *DB
	down := func(_ context.Context, db *DB) error {
		got = db
		return nil
	}

	m := Define("20240102030405", "n", noopFunc, down)
	require.NoError(t, m.Down(context.Background(), fake))

	require.NotNil(t, got)
	assert.Same(t, fake, got.Executor())
}

func TestDefine_passesErrorsThrough(t *testing.T) {
	boom := errors.New("boom")
	m := Define("20240102030405", "n", func(_ context.Context, _ *DB) error { return boom }, nil)

	err := m.Up(context.Background(), &fakeExecutor{})
	assert.ErrorIs(t, err, boom)
}

func TestAdd_badFileNamePanics(t *testing.T) {
	g := NewGroup("core")

	assert.PanicsWithValue(t,
		"migrate: Add called from migrations.go; name the file <14 digit version>_<name>.go",
		func() { addFromFile(g, "/x/migrations.go", noopFunc, nil) })
	assert.Empty(t, g.Migrations())
}

func TestAdd_registersFromFileName(t *testing.T) {
	g := NewGroup("core")

	addFromFile(g, "/x/20240102030405_create_widgets.go", noopFunc, noopFunc, Comment("c"), NoTransaction())

	ms := g.Migrations()
	require.Len(t, ms, 1)
	assert.Equal(t, "20240102030405", ms[0].Version)
	assert.Equal(t, "create_widgets", ms[0].Name)
	assert.Equal(t, "core", ms[0].Group)
	assert.Equal(t, "c", ms[0].Comment)
	assert.False(t, ms[0].Transactional)
}

func TestAdd_usesTheCallerFile(t *testing.T) {
	// This file is add_test.go, which carries no version, so Add must name
	// it in the panic. That proves Add reads its caller and not its own file.
	g := NewGroup("core")

	assert.PanicsWithValue(t,
		"migrate: Add called from add_test.go; name the file <14 digit version>_<name>.go",
		func() { g.Add(noopFunc, nil) })
}

func TestAdd_duplicateVersionPanics(t *testing.T) {
	g := NewGroup("core")
	g.MustRegister(Define("20240102030405", "first", noopFunc, nil))

	var recovered any
	func() {
		defer func() { recovered = recover() }()
		g.MustRegister(Define("20240102030405", "second", noopFunc, nil))
	}()

	err, ok := recovered.(error)
	require.True(t, ok, "want an error panic, got %v", recovered)
	assert.ErrorContains(t, err, "migrate: duplicate version")
	assert.Len(t, g.Migrations(), 1)
}
