package memcacheddriver

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/bradfitz/gomemcache/memcache"

	"github.com/xraph/grove/kv"
)

// Each request owns a client until its command and cancellation cleanup finish.
// This lets us interrupt its sockets without canceling another request.
type requestClient struct {
	db     *MemcachedDB
	client *memcache.Client
	mu     sync.Mutex
	ctx    context.Context
	closed bool
	conns  map[*ownedConn]struct{}
}

func (db *MemcachedDB) newRequestClient(selector memcache.ServerSelector) *requestClient {
	c := &requestClient{db: db, client: memcache.NewFromSelector(selector), conns: make(map[*ownedConn]struct{})}
	c.client.DialContext = c.dial
	c.client.MaxIdleConns = 1
	return c
}

func (c *requestClient) dial(ctx context.Context, network, address string) (net.Conn, error) {
	c.mu.Lock()
	if c.ctx != nil {
		ctx = c.ctx
	}
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return nil, kv.ErrStoreClosed
	}
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(c.db.shutdown, cancel)
	defer func() { stop(); cancel() }()
	dialer := &net.Dialer{Timeout: c.db.opts.DialTimeout}
	var conn net.Conn
	var err error
	if c.db.opts.TLSConfig != nil {
		tlsDialer := &tls.Dialer{NetDialer: dialer, Config: c.db.opts.TLSConfig}
		conn, err = tlsDialer.DialContext(ctx, network, address)
	} else {
		conn, err = dialer.DialContext(ctx, network, address)
	}
	if err != nil {
		return nil, err
	}
	owned := &ownedConn{Conn: conn, owner: c}
	c.mu.Lock()
	if c.closed || ctx.Err() != nil {
		c.mu.Unlock()
		conn.Close()
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return nil, kv.ErrStoreClosed
	}
	c.conns[owned] = struct{}{}
	c.mu.Unlock()
	return owned, nil
}

func (c *requestClient) closeConnections(shutdown bool) error {
	c.mu.Lock()
	c.closed = c.closed || shutdown
	conns := make([]*ownedConn, 0, len(c.conns))
	for conn := range c.conns {
		conns = append(conns, conn)
	}
	c.mu.Unlock()
	var err error
	for _, conn := range conns {
		err = errors.Join(err, conn.Close())
	}
	return errors.Join(err, c.client.Close())
}

func withClient[T any](ctx context.Context, db *MemcachedDB, fn func(*memcache.Client) (T, error)) (value T, err error) {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return value, ctxErr
	}
	c, err := db.acquireClient(ctx)
	if err != nil {
		return value, err
	}
	defer db.releaseClient(c)
	shutdown := db.shutdown
	if ctxErr := ctx.Err(); ctxErr != nil {
		return value, ctxErr
	}
	if shutdown.Err() != nil {
		return value, kv.ErrStoreClosed
	}
	c.mu.Lock()
	c.ctx = ctx
	c.mu.Unlock()
	canceled := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		_ = c.closeConnections(false) //nolint:errcheck // closing sockets interrupts the canceled operation
		close(canceled)
	})
	finished := false
	defer func() {
		if !stop() {
			<-canceled
		}
		if !finished || ctx.Err() != nil || shutdown.Err() != nil {
			_ = c.closeConnections(false) //nolint:errcheck // discard sockets after cancellation, shutdown, or panic
		}
		c.mu.Lock()
		c.ctx = nil
		c.mu.Unlock()
		if ctx.Err() != nil {
			err = ctx.Err()
		} else if shutdown.Err() != nil {
			err = kv.ErrStoreClosed
		}
	}()
	value, err = fn(c.client)
	finished = true
	return value, err
}

func (db *MemcachedDB) acquireClient(ctx context.Context) (*requestClient, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		db.mu.Lock()
		if db.shutdown == nil || db.shutdown.Err() != nil {
			db.mu.Unlock()
			return nil, kv.ErrStoreClosed
		}
		if n := len(db.idle); n > 0 {
			c := db.idle[n-1]
			db.idle = db.idle[:n-1]
			db.mu.Unlock()
			return c, nil
		}
		if db.managed < db.opts.PoolSize {
			c := db.newRequestClient(db.selector)
			db.clients[c] = struct{}{}
			db.managed++
			db.mu.Unlock()
			return c, nil
		}
		changed, done := db.changed, db.shutdown.Done()
		db.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-done:
			return nil, kv.ErrStoreClosed
		case <-changed:
		}
	}
}

func (db *MemcachedDB) releaseClient(c *requestClient) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.shutdown.Err() != nil || len(db.idle) >= db.idleLimit {
		_ = c.closeConnections(true) //nolint:errcheck // retire excess or shut-down clients
		delete(db.clients, c)
		db.managed--
	} else {
		db.idle = append(db.idle, c)
	}
	close(db.changed)
	db.changed = make(chan struct{})
}

type ownedConn struct {
	net.Conn
	owner *requestClient
	once  sync.Once
	err   error
}

func (c *ownedConn) Close() error {
	c.once.Do(func() {
		c.err = c.Conn.Close()
		c.owner.mu.Lock()
		delete(c.owner.conns, c)
		c.owner.mu.Unlock()
	})
	return c.err
}

func (c *ownedConn) deadline(timeout time.Duration) time.Time {
	var deadline time.Time
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
	}
	c.owner.mu.Lock()
	defer c.owner.mu.Unlock()
	if ctx := c.owner.ctx; ctx != nil {
		if d, ok := ctx.Deadline(); ok && (deadline.IsZero() || d.Before(deadline)) {
			deadline = d
		}
	}
	return deadline
}

func (c *ownedConn) Read(p []byte) (int, error) {
	if err := c.requestError(); err != nil {
		return 0, err
	}
	if err := c.SetReadDeadline(c.deadline(c.owner.db.opts.ReadTimeout)); err != nil {
		return 0, err
	}
	return c.Conn.Read(p)
}

func (c *ownedConn) Write(p []byte) (int, error) {
	if err := c.requestError(); err != nil {
		return 0, err
	}
	if err := c.SetWriteDeadline(c.deadline(c.owner.db.opts.WriteTimeout)); err != nil {
		return 0, err
	}
	return c.Conn.Write(p)
}

func (c *ownedConn) requestError() error {
	c.owner.mu.Lock()
	defer c.owner.mu.Unlock()
	if c.owner.ctx != nil && c.owner.ctx.Err() != nil {
		return c.owner.ctx.Err()
	}
	if c.owner.closed {
		return kv.ErrStoreClosed
	}
	return nil
}
