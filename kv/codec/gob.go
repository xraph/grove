package codec

import (
	"bytes"
	"encoding/gob"
)

// Gob implements Codec using Go's gob encoding.
// Types must be registered with gob.Register if they are interface values.
type Gob struct{}

// Encode serialises v using gob encoding.
func (Gob) Encode(v any) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode deserialises data into v using gob encoding.
func (Gob) Decode(data []byte, v any) error {
	return gob.NewDecoder(bytes.NewReader(data)).Decode(v)
}

// Name returns the codec name.
func (Gob) Name() string { return "gob" }
