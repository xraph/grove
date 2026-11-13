package esdriver

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
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
	return transport, owned, nil
}
