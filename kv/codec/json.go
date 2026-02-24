package codec

import "encoding/json"

type jsonCodec struct{}

// JSON returns a codec that uses encoding/json for serialization.
// This is the default codec used by the Store when none is specified.
func JSON() Codec { return &jsonCodec{} }

func (c *jsonCodec) Name() string { return "json" }

func (c *jsonCodec) Encode(v any) ([]byte, error) {
	return json.Marshal(v)
}

func (c *jsonCodec) Decode(data []byte, v any) error {
	return json.Unmarshal(data, v)
}
