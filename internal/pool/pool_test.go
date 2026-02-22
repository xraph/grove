package pool

import (
	"bytes"
	"testing"
)

func TestGetPutBuffer(t *testing.T) {
	buf := GetBuffer()
	if buf == nil {
		t.Fatal("GetBuffer returned nil")
	}
	if buf.Len() != 0 {
		t.Errorf("new buffer Len() = %d, want 0", buf.Len())
	}

	buf.WriteString("hello")
	if buf.String() != "hello" {
		t.Errorf("after WriteString, got %q, want %q", buf.String(), "hello")
	}

	PutBuffer(buf)
}

func TestBufferReuse(t *testing.T) {
	buf := GetBuffer()
	buf.WriteString("first")
	PutBuffer(buf)

	// Get another buffer from the pool; it should be reset.
	buf2 := GetBuffer()
	if buf2.Len() != 0 {
		t.Errorf("reused buffer Len() = %d, want 0", buf2.Len())
	}
	if buf2.String() != "" {
		t.Errorf("reused buffer String() = %q, want %q", buf2.String(), "")
	}
	PutBuffer(buf2)
}

func TestBufferWriteString(t *testing.T) {
	buf := GetBuffer()
	defer PutBuffer(buf)

	buf.WriteString("SELECT ")
	buf.WriteString("* FROM users")

	want := "SELECT * FROM users"
	if got := buf.String(); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestBufferWriteByte(t *testing.T) {
	buf := GetBuffer()
	defer PutBuffer(buf)

	if err := buf.WriteByte('('); err != nil {
		t.Fatalf("WriteByte returned error: %v", err)
	}
	buf.WriteString("id")
	if err := buf.WriteByte(')'); err != nil {
		t.Fatalf("WriteByte returned error: %v", err)
	}

	want := "(id)"
	if got := buf.String(); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestBufferWrite(t *testing.T) {
	buf := GetBuffer()
	defer PutBuffer(buf)

	data := []byte("WHERE id = $1")
	n, err := buf.Write(data)
	if err != nil {
		t.Fatalf("Write returned error: %v", err)
	}
	if n != len(data) {
		t.Errorf("Write returned n = %d, want %d", n, len(data))
	}
	if got := buf.String(); got != "WHERE id = $1" {
		t.Errorf("got %q, want %q", got, "WHERE id = $1")
	}
}

func TestBufferBytes(t *testing.T) {
	buf := GetBuffer()
	defer PutBuffer(buf)

	buf.WriteString("test")
	if !bytes.Equal(buf.Bytes(), []byte("test")) {
		t.Errorf("Bytes() = %v, want %v", buf.Bytes(), []byte("test"))
	}
}

func TestBufferLen(t *testing.T) {
	buf := GetBuffer()
	defer PutBuffer(buf)

	if buf.Len() != 0 {
		t.Errorf("empty buffer Len() = %d, want 0", buf.Len())
	}

	buf.WriteString("abc")
	if buf.Len() != 3 {
		t.Errorf("after WriteString(\"abc\"), Len() = %d, want 3", buf.Len())
	}
}

func TestBufferReset(t *testing.T) {
	buf := GetBuffer()
	defer PutBuffer(buf)

	buf.WriteString("some data here")
	capBefore := cap(buf.B)

	buf.Reset()
	if buf.Len() != 0 {
		t.Errorf("after Reset, Len() = %d, want 0", buf.Len())
	}
	if buf.String() != "" {
		t.Errorf("after Reset, String() = %q, want %q", buf.String(), "")
	}
	// Capacity should be preserved after reset.
	if cap(buf.B) != capBefore {
		t.Errorf("after Reset, cap = %d, want %d", cap(buf.B), capBefore)
	}
}

func TestBufferStringBuilding(t *testing.T) {
	tests := []struct {
		name    string
		buildFn func(buf *Buffer)
		want    string
	}{
		{
			name: "simple SELECT",
			buildFn: func(buf *Buffer) {
				buf.WriteString("SELECT * FROM users")
			},
			want: "SELECT * FROM users",
		},
		{
			name: "SELECT with WHERE",
			buildFn: func(buf *Buffer) {
				buf.WriteString("SELECT ")
				buf.WriteString("id, name ")
				buf.WriteString("FROM users ")
				buf.WriteString("WHERE id = $1")
			},
			want: "SELECT id, name FROM users WHERE id = $1",
		},
		{
			name: "INSERT statement",
			buildFn: func(buf *Buffer) {
				buf.WriteString("INSERT INTO users ")
				_ = buf.WriteByte('(')
				buf.WriteString("name, email")
				_ = buf.WriteByte(')')
				buf.WriteString(" VALUES ")
				_ = buf.WriteByte('(')
				buf.WriteString("$1, $2")
				_ = buf.WriteByte(')')
			},
			want: "INSERT INTO users (name, email) VALUES ($1, $2)",
		},
		{
			name: "mixed Write and WriteString",
			buildFn: func(buf *Buffer) {
				buf.WriteString("DELETE FROM ")
				_, _ = buf.Write([]byte("users"))
				buf.WriteString(" WHERE ")
				_, _ = buf.Write([]byte("id = $1"))
			},
			want: "DELETE FROM users WHERE id = $1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := GetBuffer()
			defer PutBuffer(buf)

			tt.buildFn(buf)

			if got := buf.String(); got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestPutBufferNil(t *testing.T) {
	// Should not panic.
	PutBuffer(nil)
}

func TestPutBufferLargeNotReturned(t *testing.T) {
	buf := GetBuffer()
	// Grow the buffer beyond the 64 KiB threshold.
	large := make([]byte, 70000)
	buf.B = append(buf.B, large...)
	PutBuffer(buf)

	// Get a new buffer. It should be fresh, not the oversized one.
	buf2 := GetBuffer()
	defer PutBuffer(buf2)
	if cap(buf2.B) > 65536 {
		t.Errorf("pool returned oversized buffer with cap %d", cap(buf2.B))
	}
}

func BenchmarkBufferPool(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf := GetBuffer()
		buf.WriteString("SELECT ")
		buf.WriteString("id, name, email ")
		buf.WriteString("FROM users ")
		buf.WriteString("WHERE id = $1 ")
		buf.WriteString("AND deleted_at IS NULL ")
		buf.WriteString("ORDER BY created_at DESC ")
		buf.WriteString("LIMIT 10")
		_ = buf.String()
		PutBuffer(buf)
	}
}

func BenchmarkBufferPoolParallel(b *testing.B) {
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			buf := GetBuffer()
			buf.WriteString("SELECT * FROM users WHERE id = $1")
			_ = buf.String()
			PutBuffer(buf)
		}
	})
}
