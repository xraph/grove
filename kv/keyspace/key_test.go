package keyspace_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xraph/grove/kv/keyspace"
)

func TestComposeKey(t *testing.T) {
	result := keyspace.ComposeKey(":", "users", "123")
	assert.Equal(t, "users:123", result)
}

func TestComposeKey_CustomSeparator(t *testing.T) {
	result := keyspace.ComposeKey("/", "a", "b")
	assert.Equal(t, "a/b", result)
}

func TestComposeKey_SingleSegment(t *testing.T) {
	result := keyspace.ComposeKey(":", "solo")
	assert.Equal(t, "solo", result)
}

func TestParseKey(t *testing.T) {
	result := keyspace.ParseKey("users:123", ":")
	assert.Equal(t, []string{"users", "123"}, result)
}

func TestParseKey_Multiple(t *testing.T) {
	result := keyspace.ParseKey("a:b:c", ":")
	assert.Equal(t, []string{"a", "b", "c"}, result)
}

func TestJoin(t *testing.T) {
	result := keyspace.Join("users", "123")
	assert.Equal(t, "users:123", result)
}

func TestJoin_Single(t *testing.T) {
	result := keyspace.Join("solo")
	assert.Equal(t, "solo", result)
}
