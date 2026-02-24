package driver_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xraph/grove/kv/driver"
)

func TestDriverInfo_Has_SingleCap(t *testing.T) {
	info := driver.DriverInfo{Capabilities: driver.CapTTL}
	assert.True(t, info.Has(driver.CapTTL))
}

func TestDriverInfo_Has_MultipleCaps(t *testing.T) {
	info := driver.DriverInfo{Capabilities: driver.CapTTL | driver.CapCAS}
	assert.True(t, info.Has(driver.CapTTL))
	assert.True(t, info.Has(driver.CapCAS))
}

func TestDriverInfo_Has_MissingCap(t *testing.T) {
	info := driver.DriverInfo{Capabilities: driver.CapTTL}
	assert.False(t, info.Has(driver.CapScan))
}

func TestDriverInfo_Has_NoCaps(t *testing.T) {
	info := driver.DriverInfo{Capabilities: 0}
	assert.False(t, info.Has(driver.CapTTL))
}

func TestCapability_AllFlags(t *testing.T) {
	caps := []struct {
		name string
		cap  driver.Capability
	}{
		{"CapTTL", driver.CapTTL},
		{"CapCAS", driver.CapCAS},
		{"CapScan", driver.CapScan},
		{"CapBatch", driver.CapBatch},
		{"CapPubSub", driver.CapPubSub},
		{"CapStreams", driver.CapStreams},
		{"CapTransaction", driver.CapTransaction},
		{"CapSortedSet", driver.CapSortedSet},
		{"CapCRDT", driver.CapCRDT},
	}

	seen := make(map[driver.Capability]string)
	for _, tc := range caps {
		t.Run(tc.name, func(t *testing.T) {
			// Each capability must be a unique power of 2.
			assert.NotZero(t, tc.cap, "capability %s should be non-zero", tc.name)
			assert.Equal(t, driver.Capability(0), tc.cap&(tc.cap-1),
				"capability %s should be a power of 2", tc.name)

			if prev, exists := seen[tc.cap]; exists {
				t.Fatalf("capability %s has same value as %s", tc.name, prev)
			}
			seen[tc.cap] = tc.name
		})
	}
}
