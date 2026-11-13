package extension

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	forgetesting "github.com/xraph/forge/testing"

	"github.com/xraph/grove"
)

type resourceDriver struct{ closes int }

func (d *resourceDriver) Name() string               { return "resource" }
func (d *resourceDriver) Ping(context.Context) error { return nil }
func (d *resourceDriver) Close() error               { d.closes++; return nil }

func TestRegisterFailureReleasesDatabases(t *testing.T) {
	for _, mode := range []string{"later open fails", "default missing"} {
		t.Run(mode, func(t *testing.T) {
			var opened []*resourceDriver
			failure := errors.New("database unavailable")
			ext := New(
				WithDatabaseDSN("primary", "resource", "first"),
				WithDatabaseDSN("analytics", "resource", "second"),
				WithDriverFactory("resource", func(_ context.Context, dsn string) (grove.GroveDriver, error) {
					if mode == "later open fails" && dsn == "second" {
						return nil, failure
					}
					d := &resourceDriver{}
					opened = append(opened, d)
					return d, nil
				}),
			)
			if mode == "default missing" {
				WithDefaultDatabase("missing")(ext)
			}
			app := forgetesting.NewTestApp("resource-test", "0.1.0")
			require.Error(t, ext.Register(app))
			require.NotEmpty(t, opened)
			for _, d := range opened {
				require.Equal(t, 1, d.closes, "already opened pools must be closed on registration failure")
			}
			require.Nil(t, ext.DB())
			require.Nil(t, ext.Manager())
		})
	}
}

func TestRegisterRejectsDuplicateNamesBeforeOpening(t *testing.T) {
	calls := 0
	ext := New(
		WithDatabaseDSN("same", "resource", "first"),
		WithDatabaseDSN("same", "resource", "second"),
		WithDriverFactory("resource", func(context.Context, string) (grove.GroveDriver, error) { calls++; return &resourceDriver{}, nil }),
	)
	app := forgetesting.NewTestApp("duplicate-test", "0.1.0")
	require.ErrorContains(t, ext.Register(app), "duplicate database name")
	require.Zero(t, calls)
}
func TestRegisterPreservesExistingDatabase(t *testing.T) {
	d := &resourceDriver{}
	ext := New(WithDriver(d))
	app := forgetesting.NewTestApp("repeat-test", "0.1.0")
	require.NoError(t, ext.Register(app))
	existing := ext.DB()
	require.ErrorContains(t, ext.Register(app), "already registered")
	require.Same(t, existing, ext.DB())
	require.Zero(t, d.closes)
	require.NoError(t, ext.Stop(t.Context()))
	require.Equal(t, 1, d.closes)
}
