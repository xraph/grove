package middleware_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/xraph/grove/kv/middleware"
)

func BenchmarkCompress(b *testing.B) {
	hook := middleware.NewCompress(middleware.Gzip, 0)
	for _, size := range []int{256, 1024, 10240, 102400} {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			data := bytes.Repeat([]byte("a"), size)
			b.ResetTimer()
			b.SetBytes(int64(size))
			for i := 0; i < b.N; i++ {
				_, _ = hook.Compress(data)
			}
		})
	}
}

func BenchmarkDecompress(b *testing.B) {
	hook := middleware.NewCompress(middleware.Gzip, 0)
	for _, size := range []int{256, 1024, 10240, 102400} {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			data := bytes.Repeat([]byte("a"), size)
			compressed, _ := hook.Compress(data)
			b.ResetTimer()
			b.SetBytes(int64(size))
			for i := 0; i < b.N; i++ {
				_, _ = hook.Decompress(compressed)
			}
		})
	}
}
