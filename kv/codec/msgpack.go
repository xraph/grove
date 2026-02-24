package codec

import "github.com/vmihailenco/msgpack/v5"

type msgpackCodec struct{}

// MsgPack returns a codec that uses MessagePack for serialization.
// MsgPack is more compact than JSON and faster to encode/decode.
func MsgPack() Codec { return &msgpackCodec{} }

func (c *msgpackCodec) Name() string { return "msgpack" }

func (c *msgpackCodec) Encode(v any) ([]byte, error) {
	return msgpack.Marshal(v)
}

func (c *msgpackCodec) Decode(data []byte, v any) error {
	return msgpack.Unmarshal(data, v)
}
