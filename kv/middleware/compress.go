package middleware

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"

	"github.com/xraph/grove/hook"
	kv "github.com/xraph/grove/kv"
)

// CompressionAlgorithm identifies a compression algorithm.
type CompressionAlgorithm int

const (
	// Gzip uses gzip compression (widely supported, good compression ratio).
	Gzip CompressionAlgorithm = iota
)

// CompressHook provides transparent value compression/decompression.
type CompressHook struct {
	algo      CompressionAlgorithm
	threshold int // minimum size in bytes before compressing
}

var (
	_ hook.PreQueryHook  = (*CompressHook)(nil)
	_ hook.PostQueryHook = (*CompressHook)(nil)
)

// NewCompress creates a new compression middleware.
// The threshold is the minimum value size in bytes before compression is applied.
// A threshold of 0 compresses all values.
func NewCompress(algo CompressionAlgorithm, threshold ...int) *CompressHook {
	t := 256 // default: compress values >= 256 bytes
	if len(threshold) > 0 {
		t = threshold[0]
	}
	return &CompressHook{algo: algo, threshold: t}
}

func (h *CompressHook) BeforeQuery(_ context.Context, qc *hook.QueryContext) (*hook.HookResult, error) {
	// For SET operations, mark that we should compress the value.
	if qc.Operation == kv.OpSet || qc.Operation == kv.OpMSet {
		if qc.Values == nil {
			qc.Values = make(map[string]any)
		}
		qc.Values["_compress"] = true
	}
	return &hook.HookResult{Decision: hook.Allow}, nil
}

func (h *CompressHook) AfterQuery(_ context.Context, qc *hook.QueryContext, result any) error {
	// For GET operations, we could decompress if the value was compressed.
	// The actual compression/decompression happens at the raw bytes level.
	return nil
}

// Compress compresses data using the configured algorithm.
func (h *CompressHook) Compress(data []byte) ([]byte, error) {
	if len(data) < h.threshold {
		return data, nil
	}

	var buf bytes.Buffer
	// Write a magic byte to indicate compressed data.
	buf.WriteByte(0x1F) // compression marker

	w := gzip.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decompress decompresses data if it was compressed.
func (h *CompressHook) Decompress(data []byte) ([]byte, error) {
	if len(data) == 0 || data[0] != 0x1F {
		return data, nil // not compressed
	}

	r, err := gzip.NewReader(bytes.NewReader(data[1:]))
	if err != nil {
		return nil, err
	}
	defer r.Close()

	return io.ReadAll(r)
}
