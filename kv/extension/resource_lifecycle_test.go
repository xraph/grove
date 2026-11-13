package extension

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	forgetesting "github.com/xraph/forge/testing"
	"github.com/xraph/vessel"

	"github.com/xraph/grove/kv"
	"github.com/xraph/grove/kv/driver"
	"github.com/xraph/grove/kv/kvtest"
)

type resourceDriver struct {
	*kvtest.MockDriver
	closes   int
	closeErr error
}

func (d *resourceDriver) Close() error { d.closes++; return d.closeErr }

func TestRegisterFailureReleasesStores(t *testing.T) {
	for _, mode := range []string{"later open fails", "default missing", "DI conflict"} {
		t.Run(mode, func(t *testing.T) {
			d := &resourceDriver{MockDriver: kvtest.NewMockDriver()}
			ext := New(WithStore("primary", d))
			app := forgetesting.NewTestApp("resource-test", "0.1.0")
			switch mode {
			case "later open fails":
				WithStoreDSN("secondary", "unregistered_resource_driver", "unused")(ext)
			case "default missing":
				WithDefaultStore("missing")(ext)
			case "DI conflict":
				require.NoError(t, vessel.Provide(app.Container(), func() *StoreManager { return NewStoreManager() }))
			}
			require.Error(t, ext.Register(app))
			require.Equal(t, 1, d.closes)
			require.Nil(t, ext.Store())
			require.Nil(t, ext.Manager())
		})
	}
}

func TestRegisterSingleStoreFailureReleasesStore(t *testing.T) {
	closeErr := errors.New("close failed")
	d := &resourceDriver{MockDriver: kvtest.NewMockDriver(), closeErr: closeErr}
	ext := New(WithDriver(d))
	app := forgetesting.NewTestApp("single-resource-test", "0.1.0")
	require.NoError(t, vessel.Provide(app.Container(), func() *kv.Store { return nil }))
	require.ErrorIs(t, ext.Register(app), closeErr)
	require.Equal(t, 1, d.closes)
	require.Nil(t, ext.Store())
}

func TestRegisterRejectsInvalidNamesBeforeOpening(t *testing.T) {
	for _, names := range [][]string{{"same", "same"}, {""}} {
		t.Run(names[0], func(t *testing.T) {
			calls := 0
			name := "resource_" + t.Name()
			kv.RegisterDriver(name, func() driver.Driver { calls++; return kvtest.NewMockDriver() })
			ext := New()
			for _, storeName := range names {
				WithStoreDSN(storeName, name, "unused")(ext)
			}
			app := forgetesting.NewTestApp("invalid-resource-test", "0.1.0")
			require.Error(t, ext.Register(app))
			require.Zero(t, calls)
		})
	}
}

func TestRegisterPreservesExistingStore(t *testing.T) {
	d := &resourceDriver{MockDriver: kvtest.NewMockDriver()}
	ext := New(WithDriver(d))
	app := forgetesting.NewTestApp("repeat-resource-test", "0.1.0")
	require.NoError(t, ext.Register(app))
	existing := ext.Store()
	require.ErrorContains(t, ext.Register(app), "already registered")
	require.Same(t, existing, ext.Store())
	require.Zero(t, d.closes)
	require.NoError(t, ext.Stop(context.Background()))
	require.Equal(t, 1, d.closes)
}
