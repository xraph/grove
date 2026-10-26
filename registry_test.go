package grove

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRegisterDriver_OpenDriverUsesFactory(t *testing.T) {
	const name = "grove-registry-test"

	var gotDSN string
	RegisterDriver(name, func(_ context.Context, dsn string) (GroveDriver, error) {
		gotDSN = dsn

		return &minimalDriver{name: name}, nil
	})

	drv, err := OpenDriver(t.Context(), name, "mem://one")
	require.NoError(t, err)
	assert.Equal(t, name, drv.Name())
	assert.Equal(t, "mem://one", gotDSN)
	assert.Contains(t, Drivers(), name)
}

func TestRegisterDriver_LaterRegistrationWins(t *testing.T) {
	const name = "grove-registry-overwrite"

	RegisterDriver(name, func(context.Context, string) (GroveDriver, error) {
		return &minimalDriver{name: "first"}, nil
	})
	RegisterDriver(name, func(context.Context, string) (GroveDriver, error) {
		return &minimalDriver{name: "second"}, nil
	})

	drv, err := OpenDriver(t.Context(), name, "")
	require.NoError(t, err)
	assert.Equal(t, "second", drv.Name())
}

func TestOpenDriver_PropagatesFactoryError(t *testing.T) {
	const name = "grove-registry-failing"

	factoryErr := errors.New("dial refused")
	RegisterDriver(name, func(context.Context, string) (GroveDriver, error) {
		return nil, factoryErr
	})

	_, err := OpenDriver(t.Context(), name, "x")
	require.ErrorIs(t, err, factoryErr)
}

func TestOpenDriver_UnknownName(t *testing.T) {
	_, err := OpenDriver(t.Context(), "no-such-driver", "x")

	require.Error(t, err)
	assert.Contains(t, err.Error(), `unknown driver "no-such-driver"`)
	assert.NotContains(t, Drivers(), "no-such-driver")
}
