package esdriver

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// EsCursor provides deep pagination over Elasticsearch search results
// using the scroll API.
type EsCursor struct {
	db        *ElasticDB
	ctx       context.Context
	keepAlive time.Duration
	scrollID  string
	hits      []Hit
	pos       int
	size      int
	done      bool
	err       error
}

// Next advances the cursor to the next hit. Returns false when there
// are no more results or an error occurred.
func (c *EsCursor) Next() bool {
	if c.err != nil || c.done {
		return false
	}
	if err := c.ctx.Err(); err != nil {
		c.err = err
		return false
	}

	c.pos++
	if c.pos < len(c.hits) {
		return true
	}

	// Fetch the next page.
	if c.scrollID == "" || len(c.hits) < c.size {
		c.done = true
		return false
	}

	res, err := c.db.client.Scroll(
		c.db.client.Scroll.WithContext(c.ctx),
		c.db.client.Scroll.WithScrollID(c.scrollID),
		c.db.client.Scroll.WithScroll(c.keepAlive),
	)
	if err != nil {
		c.err = fmt.Errorf("esdriver: scroll: %w", err)
		return false
	}

	var result SearchResult
	if err := decodeResponse(res, &result); err != nil {
		c.err = err
		return false
	}

	// Even an empty final response can rotate the server's scroll ID.
	if result.ScrollID != "" {
		c.scrollID = result.ScrollID
	}
	if len(result.Hits.Hits) == 0 {
		c.done = true
		return false
	}

	c.hits = result.Hits.Hits
	c.pos = 0
	return true
}

// Hit returns the current hit. Must be called after a successful Next().
func (c *EsCursor) Hit() *Hit {
	if c.pos < 0 || c.pos >= len(c.hits) {
		return nil
	}
	return &c.hits[c.pos]
}

// Decode decodes the current hit's _source into dest.
func (c *EsCursor) Decode(dest any) error {
	hit := c.Hit()
	if hit == nil {
		return fmt.Errorf("esdriver: no current hit")
	}
	return json.Unmarshal(hit.Source, dest)
}

// Close clears the scroll context on the Elasticsearch server.
func (c *EsCursor) Close() error {
	c.done = true
	if c.scrollID == "" {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(c.ctx), 5*time.Second)
	defer cancel()

	res, err := c.db.client.ClearScroll(
		c.db.client.ClearScroll.WithContext(ctx),
		c.db.client.ClearScroll.WithScrollID(c.scrollID),
	)
	if err != nil {
		return fmt.Errorf("esdriver: clear scroll: %w", err)
	}
	defer func() { _ = res.Body.Close() }()
	if res.IsError() {
		return decodeError(res)
	}

	c.scrollID = ""
	return nil
}

// Err returns the first error encountered during iteration.
func (c *EsCursor) Err() error {
	return c.err
}
