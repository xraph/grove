package codec_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/grove/kv/codec"
)

type sample struct {
	Name  string `json:"name" msgpack:"name"`
	Count int    `json:"count" msgpack:"count"`
}

func TestJSON_RoundTrip(t *testing.T) {
	c := codec.JSON()
	original := sample{Name: "alice", Count: 42}

	data, err := c.Encode(original)
	require.NoError(t, err)

	var decoded sample
	err = c.Decode(data, &decoded)
	require.NoError(t, err)
	assert.Equal(t, original, decoded)
}

func TestJSON_Name(t *testing.T) {
	assert.Equal(t, "json", codec.JSON().Name())
}

func TestJSON_DecodeError(t *testing.T) {
	c := codec.JSON()
	var decoded sample
	err := c.Decode([]byte("{invalid json!"), &decoded)
	assert.Error(t, err)
}

func TestMsgPack_RoundTrip(t *testing.T) {
	c := codec.MsgPack()
	original := sample{Name: "bob", Count: 99}

	data, err := c.Encode(original)
	require.NoError(t, err)

	var decoded sample
	err = c.Decode(data, &decoded)
	require.NoError(t, err)
	assert.Equal(t, original, decoded)
}

func TestMsgPack_Name(t *testing.T) {
	assert.Equal(t, "msgpack", codec.MsgPack().Name())
}

func TestMsgPack_DecodeError(t *testing.T) {
	c := codec.MsgPack()
	var decoded sample
	err := c.Decode([]byte{0xff, 0xfe, 0xfd}, &decoded)
	assert.Error(t, err)
}

func TestGob_RoundTrip(t *testing.T) {
	c := codec.Gob{}
	original := sample{Name: "carol", Count: 7}

	data, err := c.Encode(original)
	require.NoError(t, err)

	var decoded sample
	err = c.Decode(data, &decoded)
	require.NoError(t, err)
	assert.Equal(t, original, decoded)
}

func TestGob_Name(t *testing.T) {
	assert.Equal(t, "gob", codec.Gob{}.Name())
}

func TestGob_DecodeError(t *testing.T) {
	c := codec.Gob{}
	var decoded sample
	err := c.Decode([]byte{0x00, 0x01, 0x02}, &decoded)
	assert.Error(t, err)
}

func TestProtobuf_Name(t *testing.T) {
	assert.Equal(t, "protobuf", codec.Protobuf{}.Name())
}

func TestProtobuf_EncodeNonMessage(t *testing.T) {
	c := codec.Protobuf{}
	_, err := c.Encode("not a proto")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "proto.Message")
}

func TestProtobuf_DecodeNonMessage(t *testing.T) {
	c := codec.Protobuf{}
	s := "not a proto"
	err := c.Decode([]byte{}, &s)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "proto.Message")
}

func TestCodec_RoundTrip_String(t *testing.T) {
	c := codec.JSON()
	original := "hello world"

	data, err := c.Encode(original)
	require.NoError(t, err)

	var decoded string
	err = c.Decode(data, &decoded)
	require.NoError(t, err)
	assert.Equal(t, original, decoded)
}

func TestCodec_RoundTrip_Map(t *testing.T) {
	c := codec.JSON()
	original := map[string]any{
		"key1": "value1",
		"key2": float64(42),
	}

	data, err := c.Encode(original)
	require.NoError(t, err)

	var decoded map[string]any
	err = c.Decode(data, &decoded)
	require.NoError(t, err)
	assert.Equal(t, original, decoded)
}
