package rdma

/*
#cgo LDFLAGS: -lrdmacm -libverbs
#include "rdma.h"
#include <infiniband/verbs.h>
#include <stdlib.h>
#include <string.h>
*/
import "C"

import (
	"fmt"
	"sync"
	"time"
	"unsafe"
)

type Listener struct {
	h *C.struct_rdma_listener
}

type Conn struct {
	h   *C.struct_rdma_conn
	mu  sync.Mutex
	mrs map[uintptr]*C.struct_ibv_mr
	buf map[uintptr][]byte
}

type MR struct {
	Base   uint64
	RKey   uint32
	Size   uint64
	Handle uintptr
}

type OpType int

const (
	OpUnknown OpType = iota
	OpSend
	OpRecv
	OpRdmaRead
	OpRdmaWrite
)

func Listen(bindIP string, port int) (*Listener, error) {
	cIP := C.CString(bindIP)
	defer C.free(unsafe.Pointer(cIP))

	var out *C.struct_rdma_listener
	if C.rdma_listen_create(cIP, C.int(port), &out) != 0 {
		return nil, fmt.Errorf("rdma_listen_create: %s", C.GoString(C.rdma_last_error()))
	}
	return &Listener{h: out}, nil
}

func (l *Listener) Accept() (*Conn, error) {
	if l == nil || l.h == nil {
		return nil, fmt.Errorf("listener is nil")
	}
	var out *C.struct_rdma_conn
	if C.rdma_listen_accept(l.h, &out) != 0 {
		return nil, fmt.Errorf("rdma_listen_accept: %s", C.GoString(C.rdma_last_error()))
	}
	return &Conn{h: out, mrs: make(map[uintptr]*C.struct_ibv_mr), buf: make(map[uintptr][]byte)}, nil
}

func (l *Listener) Close() error {
	if l == nil || l.h == nil {
		return nil
	}
	if C.rdma_listen_close(l.h) != 0 {
		return fmt.Errorf("rdma_listen_close: %s", C.GoString(C.rdma_last_error()))
	}
	l.h = nil
	return nil
}

func Dial(serverIP string, port int) (*Conn, error) {
	cIP := C.CString(serverIP)
	defer C.free(unsafe.Pointer(cIP))

	var out *C.struct_rdma_conn
	if C.rdma_dial(cIP, C.int(port), &out) != 0 {
		return nil, fmt.Errorf("rdma_dial: %s", C.GoString(C.rdma_last_error()))
	}
	return &Conn{h: out, mrs: make(map[uintptr]*C.struct_ibv_mr), buf: make(map[uintptr][]byte)}, nil
}

func (c *Conn) Close() error {
	if c == nil || c.h == nil {
		return nil
	}
	c.mu.Lock()
	for _, mr := range c.mrs {
		_ = C.rdma_dereg_mr(mr)
	}
	c.mrs = nil
	c.buf = nil
	c.mu.Unlock()

	if C.rdma_conn_close(c.h) != 0 {
		return fmt.Errorf("rdma_conn_close: %s", C.GoString(C.rdma_last_error()))
	}
	c.h = nil
	return nil
}

func AllocBuffer(size int) ([]byte, error) {
	if size <= 0 {
		return nil, fmt.Errorf("invalid buffer size")
	}
	ptr := C.malloc(C.size_t(size))
	if ptr == nil {
		return nil, fmt.Errorf("malloc failed")
	}
	buf := unsafe.Slice((*byte)(ptr), size)
	return buf, nil
}

func FreeBuffer(buf []byte) {
	if len(buf) == 0 {
		return
	}
	C.free(unsafe.Pointer(&buf[0]))
}

func (c *Conn) ensureMR(buf []byte) (*C.struct_ibv_mr, uintptr, error) {
	if len(buf) == 0 {
		return nil, 0, fmt.Errorf("buffer is empty")
	}
	ptr := uintptr(unsafe.Pointer(&buf[0]))
	c.mu.Lock()
	defer c.mu.Unlock()
	if mr, ok := c.mrs[ptr]; ok {
		return mr, ptr, nil
	}
	var out *C.struct_ibv_mr
	if C.rdma_reg_mr(c.h, unsafe.Pointer(&buf[0]), C.size_t(len(buf)), C.IBV_ACCESS_LOCAL_WRITE, &out) != 0 {
		return nil, 0, fmt.Errorf("rdma_reg_mr: %s", C.GoString(C.rdma_last_error()))
	}
	c.mrs[ptr] = out
	c.buf[ptr] = buf
	return out, ptr, nil
}

func (c *Conn) DeregisterBuffer(buf []byte) error {
	if c == nil || c.h == nil || len(buf) == 0 {
		return nil
	}
	ptr := uintptr(unsafe.Pointer(&buf[0]))
	c.mu.Lock()
	mr := c.mrs[ptr]
	delete(c.mrs, ptr)
	delete(c.buf, ptr)
	c.mu.Unlock()
	if mr == nil {
		return nil
	}
	if C.rdma_dereg_mr(mr) != 0 {
		return fmt.Errorf("rdma_dereg_mr: %s", C.GoString(C.rdma_last_error()))
	}
	return nil
}

func (c *Conn) PostRecv(buf []byte) error {
	if c == nil || c.h == nil {
		return fmt.Errorf("conn is nil")
	}
	mr, _, err := c.ensureMR(buf)
	if err != nil {
		return err
	}
	if C.rdma_post_recv(c.h, mr, unsafe.Pointer(&buf[0]), C.size_t(len(buf))) != 0 {
		return fmt.Errorf("rdma_post_recv: %s", C.GoString(C.rdma_last_error()))
	}
	return nil
}

func (c *Conn) Send(buf []byte) error {
	if c == nil || c.h == nil {
		return fmt.Errorf("conn is nil")
	}
	mr, _, err := c.ensureMR(buf)
	if err != nil {
		return err
	}
	if C.rdma_post_send(c.h, mr, unsafe.Pointer(&buf[0]), C.size_t(len(buf))) != 0 {
		return fmt.Errorf("rdma_post_send: %s", C.GoString(C.rdma_last_error()))
	}
	return nil
}

func (c *Conn) Poll(timeoutMs int) (OpType, []byte, int, error) {
	if c == nil || c.h == nil {
		return OpUnknown, nil, 0, fmt.Errorf("conn is nil")
	}

	deadline := time.Time{}
	if timeoutMs >= 0 {
		deadline = time.Now().Add(time.Duration(timeoutMs) * time.Millisecond)
	}

	for {
		var opcode C.int
		var length C.size_t
		var wrID C.uint64_t
		rc := C.rdma_poll(c.h, &opcode, &length, &wrID)
		if rc == 0 {
			op := mapOp(int(opcode))
			buf := c.lookupBuf(uintptr(wrID))
			return op, buf, int(length), nil
		}
		if rc < 0 {
			return OpUnknown, nil, 0, fmt.Errorf("rdma_poll: %s", C.GoString(C.rdma_last_error()))
		}
		if timeoutMs == 0 {
			return OpUnknown, nil, 0, nil
		}
		if timeoutMs > 0 && time.Now().After(deadline) {
			return OpUnknown, nil, 0, nil
		}
		time.Sleep(1 * time.Millisecond)
	}
}

func (c *Conn) lookupBuf(ptr uintptr) []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf[ptr]
}

func (c *Conn) RdmaWrite(remoteAddr uint64, rkey uint32, local []byte) error {
	if c == nil || c.h == nil {
		return fmt.Errorf("conn is nil")
	}
	if err := c.PostRdmaWrite(remoteAddr, rkey, local); err != nil {
		return err
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		op, _, _, err := c.Poll(200)
		if err != nil {
			return err
		}
		if op == OpRdmaWrite {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("rdma write timeout")
		}
	}
}

func (c *Conn) PostRdmaWrite(remoteAddr uint64, rkey uint32, local []byte) error {
	if c == nil || c.h == nil {
		return fmt.Errorf("conn is nil")
	}
	mr, _, err := c.ensureMR(local)
	if err != nil {
		return err
	}
	if C.rdma_post_write(c.h, mr, unsafe.Pointer(&local[0]), C.size_t(len(local)), C.uint64_t(remoteAddr), C.uint32_t(rkey)) != 0 {
		return fmt.Errorf("rdma_post_write: %s", C.GoString(C.rdma_last_error()))
	}
	return nil
}

func (c *Conn) RdmaRead(remoteAddr uint64, rkey uint32, local []byte) error {
	if c == nil || c.h == nil {
		return fmt.Errorf("conn is nil")
	}
	if err := c.PostRdmaRead(remoteAddr, rkey, local); err != nil {
		return err
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		op, _, _, err := c.Poll(200)
		if err != nil {
			return err
		}
		if op == OpRdmaRead {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("rdma read timeout")
		}
	}
}

func (c *Conn) PostRdmaRead(remoteAddr uint64, rkey uint32, local []byte) error {
	if c == nil || c.h == nil {
		return fmt.Errorf("conn is nil")
	}
	mr, _, err := c.ensureMR(local)
	if err != nil {
		return err
	}
	if C.rdma_post_read(c.h, mr, unsafe.Pointer(&local[0]), C.size_t(len(local)), C.uint64_t(remoteAddr), C.uint32_t(rkey)) != 0 {
		return fmt.Errorf("rdma_post_read: %s", C.GoString(C.rdma_last_error()))
	}
	return nil
}

func (c *Conn) AllocAndRegisterMR(sizeBytes uint64) (*MR, error) {
	if c == nil || c.h == nil {
		return nil, fmt.Errorf("conn is nil")
	}
	if sizeBytes == 0 {
		return nil, fmt.Errorf("invalid size")
	}
	var out *C.struct_rdma_mr
	access := C.IBV_ACCESS_LOCAL_WRITE | C.IBV_ACCESS_REMOTE_READ | C.IBV_ACCESS_REMOTE_WRITE
	if C.rdma_alloc_and_reg_mr(c.h, C.size_t(sizeBytes), C.int(access), &out) != 0 {
		return nil, fmt.Errorf("rdma_alloc_and_reg_mr: %s", C.GoString(C.rdma_last_error()))
	}
	base := uint64(uintptr(C.rdma_mr_addr(out)))
	size := uint64(C.rdma_mr_size(out))
	rkey := uint32(C.rdma_mr_rkey(out))
	return &MR{
		Base:   base,
		RKey:   rkey,
		Size:   size,
		Handle: uintptr(unsafe.Pointer(out)),
	}, nil
}

func (c *Conn) DeregMR(mr *MR) error {
	if mr == nil || mr.Handle == 0 {
		return nil
	}
	if C.rdma_mr_free((*C.struct_rdma_mr)(unsafe.Pointer(mr.Handle))) != 0 {
		return fmt.Errorf("rdma_mr_free: %s", C.GoString(C.rdma_last_error()))
	}
	mr.Handle = 0
	return nil
}

func CopyToAddr(addr uint64, data []byte) error {
	if addr == 0 || len(data) == 0 {
		return fmt.Errorf("invalid addr/data")
	}
	C.memcpy(unsafe.Pointer(uintptr(addr)), unsafe.Pointer(&data[0]), C.size_t(len(data)))
	return nil
}

func GoBytesFromAddr(addr uint64, length int) ([]byte, error) {
	if addr == 0 || length <= 0 {
		return nil, fmt.Errorf("invalid addr/length")
	}
	b := C.GoBytes(unsafe.Pointer(uintptr(addr)), C.int(length))
	return b, nil
}

func mapOp(opcode int) OpType {
	switch opcode {
	case C.IBV_WC_SEND:
		return OpSend
	case C.IBV_WC_RECV:
		return OpRecv
	case C.IBV_WC_RDMA_READ:
		return OpRdmaRead
	case C.IBV_WC_RDMA_WRITE:
		return OpRdmaWrite
	default:
		return OpUnknown
	}
}
