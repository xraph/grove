package middleware_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/grove/kv/middleware"
)

func TestCompressHook_RoundTrip(t *testing.T) {
	h := middleware.NewCompress(middleware.Gzip)

	original := bytes.Repeat([]byte("compress me please "), 50)
	compressed, err := h.Compress(original)
	require.NoError(t, err)

	decompressed, err := h.Decompress(compressed)
	require.NoError(t, err)
	assert.Equal(t, original, decompressed)
}

func TestCompressHook_BelowThreshold(t *testing.T) {
	h := middleware.NewCompress(middleware.Gzip) // default threshold: 256

	small := []byte("short")
	result, err := h.Compress(small)
	require.NoError(t, err)
	assert.Equal(t, small, result, "data below threshold should be returned unchanged")
}

func TestCompressHook_AboveThreshold(t *testing.T) {
	h := middleware.NewCompress(middleware.Gzip) // default threshold: 256

	large := bytes.Repeat([]byte("abcdefghijklmnop"), 30) // 480 bytes
	compressed, err := h.Compress(large)
	require.NoError(t, err)

	assert.Less(t, len(compressed), len(large), "compressed data should be shorter")
	assert.Equal(t, byte(0x1F), compressed[0], "compressed data should start with magic byte")
}

func TestCompressHook_MagicByte(t *testing.T) {
	h := middleware.NewCompress(middleware.Gzip, 0) // threshold 0: compress everything

	data := []byte("any data at all")
	compressed, err := h.Compress(data)
	require.NoError(t, err)
	require.NotEmpty(t, compressed)

	assert.Equal(t, byte(0x1F), compressed[0], "compressed data must start with 0x1F magic byte")
}

func TestCompressHook_DecompressUncompressed(t *testing.T) {
	h := middleware.NewCompress(middleware.Gzip)

	plain := []byte("no magic byte here")
	result, err := h.Decompress(plain)
	require.NoError(t, err)
	assert.Equal(t, plain, result, "data without magic byte should be returned unchanged")
}

func TestCompressHook_CustomThreshold(t *testing.T) {
	h := middleware.NewCompress(middleware.Gzip, 64)

	// Below custom threshold (64 bytes).
	small := bytes.Repeat([]byte("x"), 50)
	result, err := h.Compress(small)
	require.NoError(t, err)
	assert.Equal(t, small, result, "data below custom threshold should not be compressed")

	// Above custom threshold.
	large := bytes.Repeat([]byte("x"), 100)
	compressed, err := h.Compress(large)
	require.NoError(t, err)
	assert.Equal(t, byte(0x1F), compressed[0], "data above custom threshold should be compressed")
}

func TestCompressHook_DefaultThreshold(t *testing.T) {
	h := middleware.NewCompress(middleware.Gzip)

	// Exactly at default threshold boundary: 255 bytes should not compress.
	below := bytes.Repeat([]byte("a"), 255)
	result, err := h.Compress(below)
	require.NoError(t, err)
	assert.Equal(t, below, result, "255 bytes should be below the 256-byte default threshold")

	// At threshold: 256 bytes should compress.
	atThreshold := bytes.Repeat([]byte("a"), 256)
	compressed, err := h.Compress(atThreshold)
	require.NoError(t, err)
	assert.Equal(t, byte(0x1F), compressed[0], "256 bytes should meet the default threshold and be compressed")
}

func TestCompressHook_LargePayload(t *testing.T) {
	h := middleware.NewCompress(middleware.Gzip)

	payload := bytes.Repeat([]byte("large payload data "), 512) // ~10KB
	compressed, err := h.Compress(payload)
	require.NoError(t, err)
	assert.Less(t, len(compressed), len(payload), "10KB payload should compress significantly")

	decompressed, err := h.Decompress(compressed)
	require.NoError(t, err)
	assert.Equal(t, payload, decompressed, "decompressed data must match the original 10KB payload")
}
