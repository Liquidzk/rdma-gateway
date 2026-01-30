package rdma

/*
#include <string.h>
*/
import "C"

import (
	"fmt"
	"io"
	"unsafe"
)

// CBufferReader provides a streaming reader over a C-allocated buffer.
// It avoids allocating a full Go slice and copies into p on each Read.
type CBufferReader struct {
	base uint64
	size int
	off  int
}

func NewCBufferReader(base uint64, size int) (*CBufferReader, error) {
	if base == 0 || size <= 0 {
		return nil, fmt.Errorf("invalid buffer")
	}
	return &CBufferReader{base: base, size: size}, nil
}

func (r *CBufferReader) Read(p []byte) (int, error) {
	if r.off >= r.size {
		return 0, io.EOF
	}
	if len(p) == 0 {
		return 0, nil
	}
	remain := r.size - r.off
	n := len(p)
	if n > remain {
		n = remain
	}
	src := unsafe.Pointer(uintptr(r.base + uint64(r.off)))
	dst := unsafe.Pointer(&p[0])
	C.memcpy(dst, src, C.size_t(n))
	r.off += n
	return n, nil
}

// CBufferWriter writes into a C-allocated buffer.
// It copies from p into the C buffer on each Write.
type CBufferWriter struct {
	base uint64
	size int
	off  int
}

func NewCBufferWriter(base uint64, size int) (*CBufferWriter, error) {
	if base == 0 || size <= 0 {
		return nil, fmt.Errorf("invalid buffer")
	}
	return &CBufferWriter{base: base, size: size}, nil
}

func (w *CBufferWriter) Write(p []byte) (int, error) {
	if w.off >= w.size {
		return 0, io.ErrShortWrite
	}
	if len(p) == 0 {
		return 0, nil
	}
	remain := w.size - w.off
	n := len(p)
	if n > remain {
		n = remain
	}
	dst := unsafe.Pointer(uintptr(w.base + uint64(w.off)))
	src := unsafe.Pointer(&p[0])
	C.memcpy(dst, src, C.size_t(n))
	w.off += n
	if n < len(p) {
		return n, io.ErrShortWrite
	}
	return n, nil
}

// UnsafeSliceFromAddr exposes a C-allocated buffer as a Go byte slice.
// Use with extreme care: the caller must ensure the C buffer stays valid
// for the lifetime of the returned slice and no concurrent mutation occurs.
func UnsafeSliceFromAddr(addr uint64, length int) ([]byte, error) {
	if addr == 0 || length <= 0 {
		return nil, fmt.Errorf("invalid addr/length")
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(uintptr(addr))), length), nil
}
