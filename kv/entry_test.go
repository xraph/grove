package kv_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/xraph/grove/kv"
)

func TestEntry_HasTTL(t *testing.T) {
	tests := []struct {
		name string
		ttl  time.Duration
		want bool
	}{
		{name: "positive TTL", ttl: 5 * time.Second, want: true},
		{name: "zero TTL", ttl: 0, want: false},
		{name: "negative TTL", ttl: -1 * time.Second, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry := kv.Entry[string]{
				Key:   "test-key",
				Value: "test-value",
				TTL:   tt.ttl,
			}
			assert.Equal(t, tt.want, entry.HasTTL())
		})
	}
}
