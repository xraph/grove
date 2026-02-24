package clickhousedriver

import "github.com/xraph/grove/driver"

// WithNativeProtocol signals the driver to use ClickHouse's native TCP protocol
// instead of the HTTP interface. This is a hint stored in DriverOptions.Extra;
// the actual protocol selection happens in the DSN or driver configuration.
func WithNativeProtocol() driver.Option {
	return func(o *driver.DriverOptions) {
		if o.Extra == nil {
			o.Extra = make(map[string]any)
		}
		o.Extra["ch_native_protocol"] = true
	}
}

// WithCompression enables LZ4 compression for ClickHouse queries and inserts.
// This is a hint stored in DriverOptions.Extra.
func WithCompression() driver.Option {
	return func(o *driver.DriverOptions) {
		if o.Extra == nil {
			o.Extra = make(map[string]any)
		}
		o.Extra["ch_compression"] = true
	}
}

// WithBatchSize sets the preferred batch size for bulk insert operations.
// This is a hint stored in DriverOptions.Extra.
func WithBatchSize(n int) driver.Option {
	return func(o *driver.DriverOptions) {
		if o.Extra == nil {
			o.Extra = make(map[string]any)
		}
		o.Extra["ch_batch_size"] = n
	}
}
