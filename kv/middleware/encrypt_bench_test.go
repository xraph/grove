package middleware_test

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"testing"

	"github.com/xraph/grove/kv/middleware"
)

func BenchmarkEncrypt(b *testing.B) {
	keySizes := []struct {
		name string
		size int
	}{
		{"AES128", 16},
		{"AES256", 32},
	}
	dataSizes := []int{64, 1024, 10240}

	for _, ks := range keySizes {
		key := make([]byte, ks.size)
		_, _ = rand.Read(key)
		hook, _ := middleware.NewEncrypt(key)

		for _, ds := range dataSizes {
			b.Run(fmt.Sprintf("%s/%dB", ks.name, ds), func(b *testing.B) {
				data := bytes.Repeat([]byte("x"), ds)
				b.ResetTimer()
				b.SetBytes(int64(ds))
				for i := 0; i < b.N; i++ {
					_, _ = hook.Encrypt(data)
				}
			})
		}
	}
}

func BenchmarkDecrypt(b *testing.B) {
	keySizes := []struct {
		name string
		size int
	}{
		{"AES128", 16},
		{"AES256", 32},
	}
	dataSizes := []int{64, 1024, 10240}

	for _, ks := range keySizes {
		key := make([]byte, ks.size)
		_, _ = rand.Read(key)
		hook, _ := middleware.NewEncrypt(key)

		for _, ds := range dataSizes {
			b.Run(fmt.Sprintf("%s/%dB", ks.name, ds), func(b *testing.B) {
				data := bytes.Repeat([]byte("x"), ds)
				encrypted, _ := hook.Encrypt(data)
				b.ResetTimer()
				b.SetBytes(int64(ds))
				for i := 0; i < b.N; i++ {
					_, _ = hook.Decrypt(encrypted)
				}
			})
		}
	}
}
