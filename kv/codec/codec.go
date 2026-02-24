// Package codec provides serialization codecs for KV values.
//
// Codecs handle the marshalling and unmarshalling of Go types to and from
// byte slices for storage in KV backends. The default codec is JSON.
package codec

// Codec serializes and deserializes values for KV storage.
type Codec interface {
	// Encode serializes v into bytes.
	Encode(v any) ([]byte, error)

	// Decode deserializes data into v. v must be a pointer.
	Decode(data []byte, v any) error

	// Name returns the codec identifier (e.g., "json", "msgpack", "protobuf").
	Name() string
}
