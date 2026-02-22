package pgdriver

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xraph/grove/internal/safe"
)

// Notification represents a PostgreSQL NOTIFY message.
type Notification struct {
	Channel string
	Payload string
	PID     uint32 // Backend PID that sent the notification
}

// Listener manages PostgreSQL LISTEN/NOTIFY subscriptions.
// It holds a dedicated connection from the pool for receiving notifications.
type Listener struct {
	db       *PgDB
	conn     *pgxpool.Conn
	handlers map[string][]func(*Notification)
	mu       sync.RWMutex
	done     chan struct{}
	once     sync.Once
}

// NewListener creates a new Listener associated with the given PgDB.
// The listener is not started and has no subscriptions. Call Start to begin
// receiving notifications, Listen to subscribe to channels, and
// OnNotification to register handlers.
func (db *PgDB) NewListener() *Listener {
	return &Listener{
		db:       db,
		handlers: make(map[string][]func(*Notification)),
		done:     make(chan struct{}),
	}
}

// Listen subscribes to a PostgreSQL notification channel by executing
// LISTEN on the dedicated connection. The listener must be started
// (via Start) before calling Listen.
func (l *Listener) Listen(ctx context.Context, channel string) error {
	l.mu.RLock()
	conn := l.conn
	l.mu.RUnlock()

	if conn == nil {
		return fmt.Errorf("pgdriver: listener not started; call Start first")
	}

	query := "LISTEN " + safe.QuoteIdent(channel)
	_, err := conn.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("pgdriver: listen %q: %w", channel, err)
	}
	return nil
}

// Unlisten unsubscribes from a PostgreSQL notification channel by executing
// UNLISTEN on the dedicated connection. The listener must be started
// (via Start) before calling Unlisten.
func (l *Listener) Unlisten(ctx context.Context, channel string) error {
	l.mu.RLock()
	conn := l.conn
	l.mu.RUnlock()

	if conn == nil {
		return fmt.Errorf("pgdriver: listener not started; call Start first")
	}

	query := "UNLISTEN " + safe.QuoteIdent(channel)
	_, err := conn.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("pgdriver: unlisten %q: %w", channel, err)
	}

	// Remove handlers for this channel.
	l.mu.Lock()
	delete(l.handlers, channel)
	l.mu.Unlock()

	return nil
}

// Notify sends a notification on the given channel with the specified payload.
// It uses a connection from the pool (not the dedicated listener connection)
// so that notifications can be sent independently of the listener.
func (l *Listener) Notify(ctx context.Context, channel, payload string) error {
	_, err := l.db.pool.Exec(ctx, "SELECT pg_notify($1, $2)", channel, payload)
	if err != nil {
		return fmt.Errorf("pgdriver: notify %q: %w", channel, err)
	}
	return nil
}

// OnNotification registers a handler function that will be called whenever a
// notification arrives on the specified channel. Multiple handlers can be
// registered for the same channel and they will all be invoked.
func (l *Listener) OnNotification(channel string, handler func(*Notification)) {
	l.mu.Lock()
	l.handlers[channel] = append(l.handlers[channel], handler)
	l.mu.Unlock()
}

// Start acquires a dedicated connection from the pool and begins listening for
// notifications in a background goroutine. The goroutine runs until the
// context is cancelled or Close is called.
func (l *Listener) Start(ctx context.Context) error {
	conn, err := l.db.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("pgdriver: acquire listener conn: %w", err)
	}

	l.mu.Lock()
	l.conn = conn
	l.mu.Unlock()

	go l.listen(ctx)

	return nil
}

// listen is the internal loop that waits for PostgreSQL notifications and
// dispatches them to the registered handlers.
func (l *Listener) listen(ctx context.Context) {
	for {
		notification, err := l.conn.Conn().WaitForNotification(ctx)
		if err != nil {
			// Check if the listener was closed or the context was cancelled.
			select {
			case <-l.done:
				return
			case <-ctx.Done():
				return
			default:
				// Unexpected error; exit the loop to avoid a tight spin.
				return
			}
		}

		n := &Notification{
			Channel: notification.Channel,
			Payload: notification.Payload,
			PID:     notification.PID,
		}

		l.mu.RLock()
		handlers := l.handlers[n.Channel]
		l.mu.RUnlock()

		for _, h := range handlers {
			h(n)
		}
	}
}

// Close stops the listener goroutine and releases the dedicated connection
// back to the pool. Close is safe to call multiple times.
func (l *Listener) Close() error {
	l.once.Do(func() {
		close(l.done)

		l.mu.Lock()
		conn := l.conn
		l.conn = nil
		l.mu.Unlock()

		if conn != nil {
			conn.Release()
		}
	})
	return nil
}

// Listen is a convenience method that creates a Listener, starts it,
// subscribes to the given channel, and registers the handler. It returns the
// Listener so the caller can close it when done.
func (db *PgDB) Listen(ctx context.Context, channel string, handler func(*Notification)) (*Listener, error) {
	l := db.NewListener()

	if err := l.Start(ctx); err != nil {
		return nil, err
	}

	l.OnNotification(channel, handler)

	if err := l.Listen(ctx, channel); err != nil {
		l.Close()
		return nil, err
	}

	return l, nil
}
