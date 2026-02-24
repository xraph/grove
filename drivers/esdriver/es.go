// Package esdriver implements an Elasticsearch driver for the Grove ORM.
//
// Unlike the SQL-based drivers (pgdriver, mysqldriver), this driver uses
// Elasticsearch-native JSON operations (Search, Index, Update, Delete,
// Bulk, Aggregate) instead of SQL query builders. It implements
// grove.GroveDriver and the adapter interface (queryBuilder) so that it
// integrates with the top-level grove.DB handle.
//
// Usage:
//
//	esdb := esdriver.New()
//	err := esdb.Open(ctx, "http://localhost:9200")
//	db, err := grove.Open(esdb)
//
//	// Typed access via Unwrap:
//	es := esdriver.Unwrap(db)
//	es.NewSearch(&users).Match("name", "alice").Scan(ctx)
package esdriver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"

	"github.com/xraph/grove/hook"
)

// ElasticDB implements grove.GroveDriver for Elasticsearch using the
// official Go client github.com/elastic/go-elasticsearch/v8.
// It also implements the grove adapter interface (queryBuilder) for
// integration with grove.DB.
type ElasticDB struct {
	client       *elasticsearch.Client
	defaultIndex string
	refresh      string // "true", "false", "wait_for"
	hooks        *hook.Engine
}

// New creates a new unconnected Elasticsearch driver. Call Open to
// establish a connection to the Elasticsearch cluster.
func New() *ElasticDB {
	return &ElasticDB{}
}

// Name returns the driver identifier.
func (db *ElasticDB) Name() string { return "elasticsearch" }

// Open connects to Elasticsearch using the given address(es). Addresses
// can be comma-separated (e.g. "http://node1:9200,http://node2:9200")
// or a single URL.
//
//	esdb := esdriver.New()
//	err := esdb.Open(ctx, "http://localhost:9200")
func (db *ElasticDB) Open(ctx context.Context, addresses string, opts ...EsOption) error {
	o := defaultEsOptions()
	o.apply(opts)

	addrs := o.Addresses
	if len(addrs) == 0 {
		addrs = parseAddresses(addresses)
	}

	cfg := elasticsearch.Config{
		Addresses: addrs,
	}

	if o.Username != "" {
		cfg.Username = o.Username
		cfg.Password = o.Password
	}
	if o.APIKey != "" {
		cfg.APIKey = o.APIKey
	}
	if o.CloudID != "" {
		cfg.CloudID = o.CloudID
	}
	if o.CACert != nil {
		cfg.CACert = o.CACert
	}
	if o.MaxRetries > 0 {
		cfg.MaxRetries = o.MaxRetries
	}
	if o.Transport != nil {
		cfg.Transport = o.Transport
	}

	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return fmt.Errorf("esdriver: new client: %w", err)
	}

	// Verify connectivity.
	res, err := client.Info(client.Info.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("esdriver: ping: %w", err)
	}
	defer func() { _ = res.Body.Close() }()

	if res.IsError() {
		return fmt.Errorf("esdriver: ping: %s", res.Status())
	}

	db.client = client
	db.refresh = o.Refresh
	return nil
}

// Close is a no-op for Elasticsearch since the HTTP client does not
// require explicit cleanup.
func (db *ElasticDB) Close() error {
	return nil
}

// Ping verifies that the Elasticsearch cluster is reachable.
func (db *ElasticDB) Ping(ctx context.Context) error {
	if db.client == nil {
		return fmt.Errorf("esdriver: client is not initialized; call Open first")
	}
	res, err := db.client.Info(db.client.Info.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("esdriver: ping: %w", err)
	}
	defer func() { _ = res.Body.Close() }()

	if res.IsError() {
		return fmt.Errorf("esdriver: ping: %s", res.Status())
	}
	return nil
}

// SetHooks attaches a hook engine for lifecycle hooks.
func (db *ElasticDB) SetHooks(engine *hook.Engine) {
	db.hooks = engine
}

// Client returns the underlying elasticsearch.Client.
func (db *ElasticDB) Client() *elasticsearch.Client {
	return db.client
}

// DefaultIndex returns the default index name, if set.
func (db *ElasticDB) DefaultIndex() string {
	return db.defaultIndex
}

// SetDefaultIndex sets the default index used when no index is specified
// on a query builder.
func (db *ElasticDB) SetDefaultIndex(name string) {
	db.defaultIndex = name
}

// ---------------------------------------------------------------------------
// Grove adapter interface implementations
// ---------------------------------------------------------------------------

// GroveSelect is the adapter method for grove.DB.NewSelect().
func (db *ElasticDB) GroveSelect(model ...any) any { return db.NewSearch(model...) }

// GroveInsert is the adapter method for grove.DB.NewInsert().
func (db *ElasticDB) GroveInsert(model any) any { return db.NewInsert(model) }

// GroveUpdate is the adapter method for grove.DB.NewUpdate().
func (db *ElasticDB) GroveUpdate(model any) any { return db.NewUpdate(model) }

// GroveDelete is the adapter method for grove.DB.NewDelete().
func (db *ElasticDB) GroveDelete(model any) any { return db.NewDelete(model) }

// ---------------------------------------------------------------------------
// Index Management
// ---------------------------------------------------------------------------

// CreateIndex creates an Elasticsearch index with the given mapping.
func (db *ElasticDB) CreateIndex(ctx context.Context, name string, mapping M) error {
	var body io.Reader
	if mapping != nil {
		data, err := json.Marshal(mapping)
		if err != nil {
			return fmt.Errorf("esdriver: marshal mapping: %w", err)
		}
		body = bytes.NewReader(data)
	}

	res, err := db.client.Indices.Create(
		name,
		db.client.Indices.Create.WithContext(ctx),
		db.client.Indices.Create.WithBody(body),
	)
	if err != nil {
		return fmt.Errorf("esdriver: create index: %w", err)
	}
	defer func() { _ = res.Body.Close() }()

	if res.IsError() {
		return decodeError(res)
	}
	return nil
}

// DeleteIndex deletes an Elasticsearch index.
func (db *ElasticDB) DeleteIndex(ctx context.Context, name string) error {
	res, err := db.client.Indices.Delete(
		[]string{name},
		db.client.Indices.Delete.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("esdriver: delete index: %w", err)
	}
	defer func() { _ = res.Body.Close() }()

	if res.IsError() {
		return decodeError(res)
	}
	return nil
}

// IndexExists checks whether an Elasticsearch index exists.
func (db *ElasticDB) IndexExists(ctx context.Context, name string) (bool, error) {
	res, err := db.client.Indices.Exists(
		[]string{name},
		db.client.Indices.Exists.WithContext(ctx),
	)
	if err != nil {
		return false, fmt.Errorf("esdriver: index exists: %w", err)
	}
	defer func() { _ = res.Body.Close() }()

	return res.StatusCode == 200, nil
}

// PutMapping updates the field mapping for an index.
func (db *ElasticDB) PutMapping(ctx context.Context, index string, mapping M) error {
	data, err := json.Marshal(mapping)
	if err != nil {
		return fmt.Errorf("esdriver: marshal mapping: %w", err)
	}

	res, err := db.client.Indices.PutMapping(
		[]string{index},
		bytes.NewReader(data),
		db.client.Indices.PutMapping.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("esdriver: put mapping: %w", err)
	}
	defer func() { _ = res.Body.Close() }()

	if res.IsError() {
		return decodeError(res)
	}
	return nil
}

// Refresh forces a refresh on the given indices, making recent writes searchable.
func (db *ElasticDB) Refresh(ctx context.Context, indices ...string) error {
	res, err := db.client.Indices.Refresh(
		db.client.Indices.Refresh.WithContext(ctx),
		db.client.Indices.Refresh.WithIndex(indices...),
	)
	if err != nil {
		return fmt.Errorf("esdriver: refresh: %w", err)
	}
	defer func() { _ = res.Body.Close() }()

	if res.IsError() {
		return decodeError(res)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// parseAddresses splits a comma-separated address string into a slice.
func parseAddresses(addresses string) []string {
	if addresses == "" {
		return []string{"http://localhost:9200"}
	}
	parts := strings.Split(addresses, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			result = append(result, p)
		}
	}
	if len(result) == 0 {
		return []string{"http://localhost:9200"}
	}
	return result
}

// decodeError parses an Elasticsearch error response and returns a
// formatted error.
func decodeError(res *esapi.Response) error {
	var errResp esErrorResponse
	body, readErr := io.ReadAll(res.Body)
	if readErr != nil {
		return fmt.Errorf("esdriver: [%s] failed to read error body: %w", res.Status(), readErr)
	}
	if err := json.Unmarshal(body, &errResp); err == nil && errResp.Error.Type != "" {
		return fmt.Errorf("esdriver: [%d] %s: %s", errResp.Status, errResp.Error.Type, errResp.Error.Reason)
	}
	return fmt.Errorf("esdriver: [%s] %s", res.Status(), string(body))
}

// decodeResponse reads and decodes a JSON response body into dest.
// It checks for errors first and returns a formatted error if the
// response indicates failure.
func decodeResponse(res *esapi.Response, dest any) error {
	defer func() { _ = res.Body.Close() }()

	if res.IsError() {
		return decodeError(res)
	}

	if dest == nil {
		return nil
	}

	return json.NewDecoder(res.Body).Decode(dest)
}

// refreshOption returns the refresh query parameter for write operations.
// It uses the query-level override if set, otherwise the driver default.
func (db *ElasticDB) refreshOption(override string) string {
	if override != "" {
		return override
	}
	return db.refresh
}

// resolveIndex returns the index to use: query-level override > model-derived > driver default.
func (db *ElasticDB) resolveIndex(queryIndex string) string {
	if queryIndex != "" {
		return queryIndex
	}
	return db.defaultIndex
}
