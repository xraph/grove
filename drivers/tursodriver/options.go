package tursodriver

import (
	"time"

	"github.com/xraph/grove/driver"
)

// tursoOpts holds Turso-specific connection options.
type tursoOpts struct {
	authToken           string
	syncInterval        time.Duration
	embeddedReplicaPath string
}

// WithAuthToken sets the authentication token for Turso cloud.
func WithAuthToken(token string) driver.Option {
	return func(o *driver.DriverOptions) {
		if o.Extra == nil {
			o.Extra = make(map[string]any)
		}
		o.Extra["turso_auth_token"] = token
	}
}

// WithSyncInterval configures embedded replica sync interval.
func WithSyncInterval(d time.Duration) driver.Option {
	return func(o *driver.DriverOptions) {
		if o.Extra == nil {
			o.Extra = make(map[string]any)
		}
		o.Extra["turso_sync_interval"] = d
	}
}

// WithEmbeddedReplica enables embedded replica mode with a local path.
func WithEmbeddedReplica(localPath string) driver.Option {
	return func(o *driver.DriverOptions) {
		if o.Extra == nil {
			o.Extra = make(map[string]any)
		}
		o.Extra["turso_embedded_replica_path"] = localPath
	}
}

// extractTursoOpts extracts Turso-specific options from the generic driver options.
func extractTursoOpts(dopts *driver.DriverOptions) tursoOpts {
	var t tursoOpts
	if dopts == nil || dopts.Extra == nil {
		return t
	}
	if v, ok := dopts.Extra["turso_auth_token"].(string); ok {
		t.authToken = v
	}
	if v, ok := dopts.Extra["turso_sync_interval"].(time.Duration); ok {
		t.syncInterval = v
	}
	if v, ok := dopts.Extra["turso_embedded_replica_path"].(string); ok {
		t.embeddedReplicaPath = v
	}
	return t
}
