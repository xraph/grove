package codec_test

import (
	"testing"

	"github.com/xraph/grove/kv/codec"
)

type benchUser struct {
	Name  string `json:"name" msgpack:"name"`
	Email string `json:"email" msgpack:"email"`
	Age   int    `json:"age" msgpack:"age"`
}

func BenchmarkCodec_Encode(b *testing.B) {
	codecs := []struct {
		name  string
		codec codec.Codec
	}{
		{"JSON", codec.JSON()},
		{"MsgPack", codec.MsgPack()},
		{"Gob", codec.Gob{}},
	}

	small := benchUser{Name: "Alice", Email: "alice@example.com", Age: 30}
	medium := benchUser{Name: "Bob", Email: string(make([]byte, 1024)), Age: 25}
	large := benchUser{Name: "Charlie", Email: string(make([]byte, 10240)), Age: 35}

	payloads := []struct {
		name string
		val  any
	}{
		{"Small", small},
		{"Medium", medium},
		{"Large", large},
	}

	for _, c := range codecs {
		for _, p := range payloads {
			b.Run(c.name+"/"+p.name, func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					_, _ = c.codec.Encode(p.val)
				}
			})
		}
	}
}

func BenchmarkCodec_Decode(b *testing.B) {
	codecs := []struct {
		name  string
		codec codec.Codec
	}{
		{"JSON", codec.JSON()},
		{"MsgPack", codec.MsgPack()},
		{"Gob", codec.Gob{}},
	}

	small := benchUser{Name: "Alice", Email: "alice@example.com", Age: 30}
	medium := benchUser{Name: "Bob", Email: string(make([]byte, 1024)), Age: 25}
	large := benchUser{Name: "Charlie", Email: string(make([]byte, 10240)), Age: 35}

	payloads := []struct {
		name string
		val  any
	}{
		{"Small", small},
		{"Medium", medium},
		{"Large", large},
	}

	for _, c := range codecs {
		for _, p := range payloads {
			b.Run(c.name+"/"+p.name, func(b *testing.B) {
				data, _ := c.codec.Encode(p.val)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					var dest benchUser
					_ = c.codec.Decode(data, &dest)
				}
			})
		}
	}
}
