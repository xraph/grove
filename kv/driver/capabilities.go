package driver

// Capability is a bitflag indicating what a driver supports.
type Capability uint64

const (
	CapTTL         Capability = 1 << iota // Time-to-live on keys
	CapCAS                                // Compare-and-swap (SetNX/SetXX)
	CapScan                               // Key scanning / iteration
	CapBatch                              // Multi-key MGet/MSet
	CapPubSub                             // Publish / Subscribe
	CapStreams                            // Event streaming (e.g., Redis Streams)
	CapTransaction                        // Multi-key transactions
	CapSortedSet                          // Sorted sets / leaderboards
	CapCRDT                               // CRDT-native support
)

// DriverInfo describes a driver's metadata and capabilities.
type DriverInfo struct {
	Name         string
	Version      string
	Capabilities Capability
}

// Has returns true if the driver info includes the given capability.
func (i DriverInfo) Has(cap Capability) bool {
	return i.Capabilities&cap != 0
}
