package pgdriver

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xraph/grove/internal/safe"
)

// Notification represents a PostgreSQL NOTIFY message.
type Notification struct {
	Channel string
	Payload string
	PID     uint32 // Backend PID that sent the notification
}

// listenCmd is a LISTEN/UNLISTEN statement queued for execution by the
// listen goroutine. The dedicated connection is not concurrency-safe, so
// only that goroutine may touch it; other goroutines queue commands here
// and wait on reply.
type listenCmd struct {
	sql   string
	reply chan error // buffered (capacity 1) so the goroutine never blocks
}

// Listener manages PostgreSQL LISTEN/NOTIFY subscriptions.
// It holds a dedicated connection from the pool for receiving notifications.
type Listener struct {
	db       *PgDB
	handlers map[string][]func(*Notification)
	mu       sync.RWMutex // guards handlers, conn, started

	conn    *pgxpool.Conn
	started bool

	// cmdMu guards pending and wake. Each loop iteration installs a fresh
	// wake func and drains pending in a single critical section, so every
	// queued command is either picked up by that drain or its sender sees
	// the freshly installed wake func and interrupts the wait.
	cmdMu   sync.Mutex
	pending []*listenCmd
	wake    context.CancelFunc

	done    chan struct{} // closed by Close
	stopped chan struct{} // closed when the listen goroutine exits
	once    sync.Once
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
		stopped:  make(chan struct{}),
	}
}

// Listen subscribes to a PostgreSQL notification channel. The LISTEN
// statement is executed by the listen goroutine, which owns the dedicated
// connection. The listener must be started (via Start) before calling Listen.
func (l *Listener) Listen(ctx context.Context, channel string) error {
	if err := l.exec(ctx, "LISTEN "+safe.QuoteIdent(channel)); err != nil {
		return fmt.Errorf("pgdriver: listen %q: %w", channel, err)
	}
	return nil
}

// Unlisten unsubscribes from a PostgreSQL notification channel. The UNLISTEN
// statement is executed by the listen goroutine, which owns the dedicated
// connection. The listener must be started (via Start) before calling
// Unlisten.
func (l *Listener) Unlisten(ctx context.Context, channel string) error {
	if err := l.exec(ctx, "UNLISTEN "+safe.QuoteIdent(channel)); err != nil {
		return fmt.Errorf("pgdriver: unlisten %q: %w", channel, err)
	}

	// Remove handlers for this channel.
	l.mu.Lock()
	delete(l.handlers, channel)
	l.mu.Unlock()

	return nil
}

// exec queues a statement for the listen goroutine, wakes it out of
// WaitForNotification, and waits for the execution result.
func (l *Listener) exec(ctx context.Context, sql string) error {
	l.mu.RLock()
	started := l.started
	l.mu.RUnlock()
	if !started {
		return fmt.Errorf("listener not started; call Start first")
	}
	select {
	case <-l.stopped:
		return fmt.Errorf("listener stopped")
	default:
	}

	cmd := &listenCmd{sql: sql, reply: make(chan error, 1)}

	l.cmdMu.Lock()
	l.pending = append(l.pending, cmd)
	wake := l.wake
	l.cmdMu.Unlock()

	// Interrupt the goroutine's current WaitForNotification so it drains
	// the queue. A nil or already-spent wake func means the goroutine is
	// not parked and will drain the queue before its next wait.
	if wake != nil {
		wake()
	}

	select {
	case err := <-cmd.reply:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-l.stopped:
		// Prefer the actual result if the command ran before the
		// goroutine exited.
		select {
		case err := <-cmd.reply:
			return err
		default:
		}
		return fmt.Errorf("listener stopped")
	}
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
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.started {
		return fmt.Errorf("pgdriver: listener already started")
	}
	select {
	case <-l.done:
		return fmt.Errorf("pgdriver: listener closed")
	default:
	}

	conn, err := l.db.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("pgdriver: acquire listener conn: %w", err)
	}

	l.conn = conn
	l.started = true

	go l.listen(ctx, conn)

	return nil
}

// listen is the internal loop that waits for PostgreSQL notifications and
// dispatches them to the registered handlers. It has exclusive use of conn:
// LISTEN/UNLISTEN statements queued by exec are run here, between waits,
// because executing them concurrently with WaitForNotification would race
// on the connection ("conn busy").
func (l *Listener) listen(ctx context.Context, conn *pgxpool.Conn) {
	defer func() {
		close(l.stopped)
		l.mu.Lock()
		l.conn = nil
		l.mu.Unlock()
		conn.Release()
	}()

	for {
		select {
		case <-l.done:
			return
		case <-ctx.Done():
			return
		default:
		}

		// Install the wake func and take the pending snapshot in one
		// critical section; see the cmdMu field comment.
		waitCtx, cancel := context.WithCancel(ctx)
		l.cmdMu.Lock()
		l.wake = cancel
		pending := l.pending
		l.pending = nil
		l.cmdMu.Unlock()

		for _, cmd := range pending {
			_, err := conn.Exec(ctx, cmd.sql)
			cmd.reply <- err
		}

		notification, err := conn.Conn().WaitForNotification(waitCtx)
		cancel()
		if err != nil {
			// Check if the listener was closed or the context was cancelled.
			select {
			case <-l.done:
				return
			case <-ctx.Done():
				return
			default:
			}
			if waitCtx.Err() != nil || pgconn.Timeout(err) {
				// Woken by exec to process queued commands. The
				// interrupt is delivered as a read-deadline
				// timeout, which pgconn does not treat as fatal,
				// so the connection remains usable.
				continue
			}
			// Unexpected error; exit the loop to avoid a tight spin.
			return
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

// Done returns a channel that is closed when the listen goroutine exits —
// after Close, context cancellation, or a fatal connection error. Consumers
// that need the listener to survive connection loss can watch Done and
// build a replacement (the listener does not reconnect by itself). For a
// listener that was never started, the channel never closes.
func (l *Listener) Done() <-chan struct{} {
	return l.stopped
}

// Close stops the listener goroutine, which releases the dedicated connection
// back to the pool as it exits. Close is safe to call multiple times and
// returns without waiting for the goroutine to finish; pool shutdown blocks
// until the connection is released, which happens promptly after the wake.
func (l *Listener) Close() error {
	l.once.Do(func() {
		close(l.done)

		l.cmdMu.Lock()
		wake := l.wake
		l.cmdMu.Unlock()
		if wake != nil {
			wake()
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
		_ = l.Close()
		return nil, err
	}

	return l, nil
}
