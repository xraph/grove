package kvtest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMockDriverConformance(t *testing.T) {
	drv := NewMockDriver()
	err := drv.Open(context.Background(), "mock://")
	require.NoError(t, err)

	RunConformanceSuite(t, drv)
}
