package esdriver

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"

	"github.com/xraph/grove/driver"
)

// Keep the actual HTTP transport so Close can release its idle connections.
// The Elasticsearch SDK otherwise creates a private transport we cannot close.
func newTransport(o *esOptions) (http.RoundTripper, *http.Transport, error) {
	transport := o.Transport
	var owned *http.Transport
	if transport == nil {
		base, ok := http.DefaultTransport.(*http.Transport)
		if !ok {
			return nil, nil, fmt.Errorf("esdriver: cannot clone http.DefaultTransport")
		}
		owned = base.Clone()
		if owned.MaxConnsPerHost <= 0 {
			owned.MaxConnsPerHost = driver.DefaultDriverOptions().PoolSize
		}
		transport = owned
	}
	if o.PoolSize > 0 {
		base, ok := transport.(*http.Transport)
		if !ok {
			return nil, nil, fmt.Errorf("esdriver: cannot set pool size for transport of type %T", transport)
		}
		if owned == nil {
			owned = base.Clone()
		}
		owned.MaxConnsPerHost = o.PoolSize
		transport = owned
	}
	if o.CACert != nil {
		base, ok := transport.(*http.Transport)
		if !ok {
			return nil, nil, fmt.Errorf("esdriver: cannot set CA certificate for transport of type %T", transport)
		}
		if owned == nil {
			owned = base.Clone()
		}
		if owned.TLSClientConfig == nil {
			owned.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
		}
		owned.TLSClientConfig.RootCAs = x509.NewCertPool()
		if !owned.TLSClientConfig.RootCAs.AppendCertsFromPEM(o.CACert) {
			return nil, nil, fmt.Errorf("esdriver: unable to add CA certificate")
		}
		transport = owned
	}
	if owned != nil && owned.MaxConnsPerHost > 0 {
		idle := owned.MaxIdleConnsPerHost
		if idle == 0 {
			idle = http.DefaultMaxIdleConnsPerHost
		}
		if idle > owned.MaxConnsPerHost {
			owned.MaxIdleConnsPerHost = owned.MaxConnsPerHost
		}
	}
	return transport, owned, nil
}
