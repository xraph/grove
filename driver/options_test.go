package driver

import (
	"log/slog"
	"os"
	"testing"
	"time"
)

func TestDefaultDriverOptions(t *testing.T) {
	opts := DefaultDriverOptions()
	if opts.PoolSize != 10 {
		t.Errorf("expected default PoolSize 10, got %d", opts.PoolSize)
	}
	if opts.QueryTimeout != 30*time.Second {
		t.Errorf("expected default QueryTimeout 30s, got %v", opts.QueryTimeout)
	}
	if opts.Logger == nil {
		t.Error("expected non-nil default Logger")
	}
}

func TestWithPoolSize(t *testing.T) {
	opts := ApplyOptions([]Option{WithPoolSize(25)})
	if opts.PoolSize != 25 {
		t.Errorf("expected PoolSize 25, got %d", opts.PoolSize)
	}
	// Other defaults should remain.
	if opts.QueryTimeout != 30*time.Second {
		t.Errorf("expected default QueryTimeout 30s, got %v", opts.QueryTimeout)
	}
}

func TestWithQueryTimeout(t *testing.T) {
	opts := ApplyOptions([]Option{WithQueryTimeout(5 * time.Second)})
	if opts.QueryTimeout != 5*time.Second {
		t.Errorf("expected QueryTimeout 5s, got %v", opts.QueryTimeout)
	}
	// Other defaults should remain.
	if opts.PoolSize != 10 {
		t.Errorf("expected default PoolSize 10, got %d", opts.PoolSize)
	}
}

func TestWithLogger(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	opts := ApplyOptions([]Option{WithLogger(logger)})
	if opts.Logger != logger {
		t.Error("expected Logger to be the one provided")
	}
}

func TestApplyOptionsMultiple(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	opts := ApplyOptions([]Option{
		WithPoolSize(50),
		WithQueryTimeout(10 * time.Second),
		WithLogger(logger),
	})

	if opts.PoolSize != 50 {
		t.Errorf("expected PoolSize 50, got %d", opts.PoolSize)
	}
	if opts.QueryTimeout != 10*time.Second {
		t.Errorf("expected QueryTimeout 10s, got %v", opts.QueryTimeout)
	}
	if opts.Logger != logger {
		t.Error("expected Logger to match provided logger")
	}
}

func TestApplyOptionsEmpty(t *testing.T) {
	opts := ApplyOptions(nil)
	if opts.PoolSize != 10 {
		t.Errorf("expected default PoolSize 10, got %d", opts.PoolSize)
	}
	if opts.QueryTimeout != 30*time.Second {
		t.Errorf("expected default QueryTimeout 30s, got %v", opts.QueryTimeout)
	}
	if opts.Logger == nil {
		t.Error("expected non-nil default Logger")
	}
}

func TestApplyOptionsLastWins(t *testing.T) {
	opts := ApplyOptions([]Option{
		WithPoolSize(5),
		WithPoolSize(20),
	})
	if opts.PoolSize != 20 {
		t.Errorf("expected last PoolSize to win (20), got %d", opts.PoolSize)
	}
}

func TestIsolationLevelString(t *testing.T) {
	tests := []struct {
		level IsolationLevel
		want  string
	}{
		{LevelDefault, "Default"},
		{LevelReadUncommitted, "Read Uncommitted"},
		{LevelReadCommitted, "Read Committed"},
		{LevelRepeatableRead, "Repeatable Read"},
		{LevelSerializable, "Serializable"},
		{IsolationLevel(99), "Unknown"},
	}

	for _, tt := range tests {
		got := tt.level.String()
		if got != tt.want {
			t.Errorf("IsolationLevel(%d).String() = %q, want %q", tt.level, got, tt.want)
		}
	}
}
