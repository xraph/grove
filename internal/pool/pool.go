// Package pool provides a sync.Pool-based byte buffer pool for minimizing
// allocations during query building. Buffers are pre-allocated with a 256-byte
// capacity and reused across query construction cycles.
package pool

import (
	"sync"
)

// Buffer is a pooled byte buffer for building query strings.
type Buffer struct {
	B []byte
}

var bufferPool = sync.Pool{
	New: func() any {
		return &Buffer{B: make([]byte, 0, 256)}
	},
}

// GetBuffer returns a Buffer from the pool. The returned buffer is empty but
// may have pre-allocated capacity from a previous use.
func GetBuffer() *Buffer {
	return bufferPool.Get().(*Buffer)
}

// PutBuffer returns a Buffer to the pool after resetting it. The buffer must
// not be used after being returned.
func PutBuffer(buf *Buffer) {
	if buf == nil {
		return
	}
	// Guard against retaining very large buffers that could waste memory.
	// If the buffer grew beyond 64 KiB, let it be garbage collected instead
	// of returning it to the pool.
	if cap(buf.B) > 65536 {
		return
	}
	buf.Reset()
	bufferPool.Put(buf)
}

// WriteString appends a string to the buffer.
func (b *Buffer) WriteString(s string) {
	b.B = append(b.B, s...)
}

// WriteByte appends a single byte to the buffer.
func (b *Buffer) WriteByte(c byte) error {
	b.B = append(b.B, c)
	return nil
}

// Write appends p to the buffer. It always returns len(p), nil.
func (b *Buffer) Write(p []byte) (int, error) {
	b.B = append(b.B, p...)
	return len(p), nil
}

// String returns the buffer contents as a string.
func (b *Buffer) String() string {
	return string(b.B)
}

// Len returns the number of bytes in the buffer.
func (b *Buffer) Len() int {
	return len(b.B)
}

// Reset clears the buffer contents while retaining the underlying capacity.
func (b *Buffer) Reset() {
	b.B = b.B[:0]
}

// Bytes returns the byte slice of the buffer contents.
func (b *Buffer) Bytes() []byte {
	return b.B
}
