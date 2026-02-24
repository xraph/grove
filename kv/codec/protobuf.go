package codec

import (
	"fmt"

	"google.golang.org/protobuf/proto"
)

// Protobuf implements Codec using Protocol Buffers.
// Values must implement proto.Message.
type Protobuf struct{}

// Encode serialises v using Protocol Buffers.
func (Protobuf) Encode(v any) ([]byte, error) {
	msg, ok := v.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("protobuf: value does not implement proto.Message")
	}
	return proto.Marshal(msg)
}

// Decode deserialises data into v using Protocol Buffers.
func (Protobuf) Decode(data []byte, v any) error {
	msg, ok := v.(proto.Message)
	if !ok {
		return fmt.Errorf("protobuf: target does not implement proto.Message")
	}
	return proto.Unmarshal(data, msg)
}

// Name returns the codec name.
func (Protobuf) Name() string { return "protobuf" }
