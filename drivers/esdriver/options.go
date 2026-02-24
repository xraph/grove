package esdriver

import "net/http"

// EsOption configures the Elasticsearch driver during Open.
type EsOption func(*esOptions)

// esOptions holds Elasticsearch-specific configuration.
type esOptions struct {
	Addresses  []string          // Override parsed addresses
	Username   string            // Basic auth username
	Password   string            // Basic auth password
	APIKey     string            // API key authentication
	CloudID    string            // Elastic Cloud ID
	CACert     []byte            // CA certificate for TLS
	Refresh    string            // Refresh policy: "true", "false", "wait_for"
	MaxRetries int               // Maximum number of retries
	Transport  http.RoundTripper // Custom HTTP transport
}

func defaultEsOptions() *esOptions {
	return &esOptions{}
}

func (o *esOptions) apply(opts []EsOption) {
	for _, fn := range opts {
		fn(o)
	}
}

// WithBasicAuth sets the username and password for HTTP basic authentication.
func WithBasicAuth(username, password string) EsOption {
	return func(o *esOptions) {
		o.Username = username
		o.Password = password
	}
}

// WithAPIKey sets the API key for authentication.
func WithAPIKey(key string) EsOption {
	return func(o *esOptions) {
		o.APIKey = key
	}
}

// WithCloudID sets the Elastic Cloud ID for connecting to Elastic Cloud.
func WithCloudID(id string) EsOption {
	return func(o *esOptions) {
		o.CloudID = id
	}
}

// WithCACert sets the CA certificate for verifying the server's TLS certificate.
func WithCACert(cert []byte) EsOption {
	return func(o *esOptions) {
		o.CACert = cert
	}
}

// WithRefresh sets the default refresh policy for write operations.
// Valid values: "true" (immediate), "false" (default), "wait_for" (wait until visible).
func WithRefresh(policy string) EsOption {
	return func(o *esOptions) {
		o.Refresh = policy
	}
}

// WithMaxRetries sets the maximum number of retries for failed requests.
func WithMaxRetries(n int) EsOption {
	return func(o *esOptions) {
		o.MaxRetries = n
	}
}

// WithTransport sets a custom HTTP transport for the Elasticsearch client.
func WithTransport(t http.RoundTripper) EsOption {
	return func(o *esOptions) {
		o.Transport = t
	}
}

// WithAddresses overrides the addresses parsed from the connection string.
func WithAddresses(addrs ...string) EsOption {
	return func(o *esOptions) {
		o.Addresses = addrs
	}
}
