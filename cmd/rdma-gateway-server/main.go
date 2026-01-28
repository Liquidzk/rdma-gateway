package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"time"
	"unsafe"

	"rdma_gateway_go/internal/backend"
	"rdma_gateway_go/internal/bufpool"
	"rdma_gateway_go/internal/proto"
	"rdma_gateway_go/internal/rdma"
)

const (
	ctrlBufSize = 4096
	recvDepth   = 64
)

type putState struct {
	lease  bufpool.Lease
	bucket string
	key    string
	objLen uint32
}

type getState struct {
	lease bufpool.Lease
}

type serverSession struct {
	conn    *rdma.Conn
	pool    *bufpool.Pool
	backend *backend.MinioBackend

	recvCh chan []byte
	sendCh chan []byte
	errCh  chan error

	sendMu   sync.Mutex
	sendBufs map[uintptr][]byte

	stateMu  sync.Mutex
	putState map[uint32]putState
	getState map[uint32]getState

	workCh chan func()
}

func main() {
	fs := flag.NewFlagSet("rdma-gateway-server", flag.ContinueOnError)
	fs.SetOutput(os.Stdout)

	bindIP := fs.String("bind-ip", "0.0.0.0", "IP address to bind")
	port := fs.Int("port", 7471, "Port to listen on")
	minioEndpoint := fs.String("minio-endpoint", "http://127.0.0.1:9000", "MinIO endpoint URL")
	accessKey := fs.String("access-key", "", "MinIO access key")
	secretKey := fs.String("secret-key", "", "MinIO secret key")

	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "Usage: %s [flags]\n\nFlags:\n", fs.Name())
		fs.PrintDefaults()
	}

	if err := fs.Parse(os.Args[1:]); err != nil {
		if err == flag.ErrHelp {
			return
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	fmt.Printf("rdma-gateway-server starting (bind %s:%d, minio %s)\n", *bindIP, *port, *minioEndpoint)
	if *accessKey == "" || *secretKey == "" {
		fmt.Println("warning: MinIO credentials not set; pass --access-key and --secret-key")
	}

	backendClient, err := backend.NewMinio(*minioEndpoint, *accessKey, *secretKey)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	listener, err := rdma.Listen(*bindIP, *port)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer listener.Close()

	conn, err := listener.Accept()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer conn.Close()

	fmt.Println("ESTABLISHED")

	if err := serverHello(conn); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	mr, err := conn.AllocAndRegisterMR(64 * 1024 * 1024)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer conn.DeregMR(mr)

	pool, err := bufpool.New(mr, 8*1024*1024)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("MR base=0x%x rkey=0x%x size=%d\n", mr.Base, mr.RKey, mr.Size)

	s := newServerSession(conn, pool, backendClient)
	s.run()

	if err := <-s.errCh; err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func serverHello(conn *rdma.Conn) error {
	recvBuf, err := rdma.AllocBuffer(ctrlBufSize)
	if err != nil {
		return err
	}

	if err := conn.PostRecv(recvBuf); err != nil {
		_ = conn.DeregisterBuffer(recvBuf)
		return err
	}

	var hdr proto.MsgHdr
	recvDone := false
	deadline := time.Now().Add(5 * time.Second)
	for !recvDone {
		op, buf, n, err := conn.Poll(200)
		if err != nil {
			_ = conn.DeregisterBuffer(recvBuf)
			rdma.FreeBuffer(recvBuf)
			return err
		}
		if op == rdma.OpRecv {
			if n < proto.HeaderLen {
				_ = conn.DeregisterBuffer(recvBuf)
				rdma.FreeBuffer(recvBuf)
				return fmt.Errorf("unexpected completion")
			}
			h, err := proto.ReadHeader(buf[:proto.HeaderLen])
			if err != nil {
				_ = conn.DeregisterBuffer(recvBuf)
				rdma.FreeBuffer(recvBuf)
				return err
			}
			if h.Magic != proto.Magic || h.Version != proto.Version || h.Type != proto.MsgHello {
				_ = conn.DeregisterBuffer(recvBuf)
				rdma.FreeBuffer(recvBuf)
				return fmt.Errorf("invalid hello")
			}
			hdr = h
			recvDone = true
		}
		if time.Now().After(deadline) {
			_ = conn.DeregisterBuffer(recvBuf)
			rdma.FreeBuffer(recvBuf)
			return fmt.Errorf("hello timeout")
		}
	}

	sendBuf, err := rdma.AllocBuffer(ctrlBufSize)
	if err != nil {
		_ = conn.DeregisterBuffer(recvBuf)
		rdma.FreeBuffer(recvBuf)
		return err
	}

	ack := proto.MsgHdr{
		Magic:   proto.Magic,
		Version: proto.Version,
		Type:    proto.MsgHelloAck,
		ReqID:   hdr.ReqID,
		BodyLen: 0,
	}
	if err := proto.WriteHeader(sendBuf[:proto.HeaderLen], ack); err != nil {
		_ = conn.DeregisterBuffer(sendBuf)
		_ = conn.DeregisterBuffer(recvBuf)
		rdma.FreeBuffer(sendBuf)
		rdma.FreeBuffer(recvBuf)
		return err
	}
	if err := conn.Send(sendBuf[:proto.HeaderLen]); err != nil {
		_ = conn.DeregisterBuffer(sendBuf)
		_ = conn.DeregisterBuffer(recvBuf)
		rdma.FreeBuffer(sendBuf)
		rdma.FreeBuffer(recvBuf)
		return err
	}

	sendDone := false
	deadline := time.Now().Add(5 * time.Second)
	for !sendDone {
		op, buf, _, err := conn.Poll(200)
		if err != nil {
			return err
		}
		if op == rdma.OpSend {
			if buf != nil && len(buf) > 0 && &buf[0] == &sendBuf[0] {
				sendDone = true
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("hello timeout")
		}
	}
	_ = conn.DeregisterBuffer(sendBuf)
	_ = conn.DeregisterBuffer(recvBuf)
	rdma.FreeBuffer(sendBuf)
	rdma.FreeBuffer(recvBuf)
	return nil
}

func newServerSession(conn *rdma.Conn, pool *bufpool.Pool, backendClient *backend.MinioBackend) *serverSession {
	s := &serverSession{
		conn:     conn,
		pool:     pool,
		backend:  backendClient,
		recvCh:   make(chan []byte, 256),
		sendCh:   make(chan []byte, 256),
		errCh:    make(chan error, 1),
		sendBufs: make(map[uintptr][]byte),
		putState: make(map[uint32]putState),
		getState: make(map[uint32]getState),
		workCh:   make(chan func(), 256),
	}
	return s
}

func (s *serverSession) run() {
	for i := 0; i < recvDepth; i++ {
		buf, err := rdma.AllocBuffer(ctrlBufSize)
		if err != nil {
			s.errCh <- err
			return
		}
		if err := s.conn.PostRecv(buf); err != nil {
			s.errCh <- err
			return
		}
	}

	for i := 0; i < runtime.NumCPU(); i++ {
		go s.workerLoop()
	}

	go s.sendLoop()
	go s.pollLoop()
	go s.dispatchLoop()
}

func (s *serverSession) sendLoop() {
	for buf := range s.sendCh {
		if err := s.conn.Send(buf); err != nil {
			s.freeSend(buf)
			s.errCh <- err
			return
		}
	}
}

func (s *serverSession) pollLoop() {
	for {
		op, buf, n, err := s.conn.Poll(100)
		if err != nil {
			s.errCh <- err
			return
		}
		switch op {
		case rdma.OpRecv:
			msg := make([]byte, n)
			copy(msg, buf[:n])
			s.recvCh <- msg
			if err := s.conn.PostRecv(buf); err != nil {
				s.errCh <- err
				return
			}
		case rdma.OpSend:
			s.freeSend(buf)
		case rdma.OpUnknown:
			continue
		}
	}
}

func (s *serverSession) dispatchLoop() {
	for msg := range s.recvCh {
		hdr, err := proto.ReadHeader(msg)
		if err != nil {
			continue
		}
		switch hdr.Type {
		case proto.MsgPutReq:
			req, err := proto.DecodePutReq(msg)
			if err != nil {
				continue
			}
			s.handlePutReq(req)
		case proto.MsgPutCommit:
			commit, err := proto.DecodePutCommit(msg)
			if err != nil {
				continue
			}
			s.handlePutCommit(commit)
		case proto.MsgGetReq:
			req, err := proto.DecodeGetReq(msg)
			if err != nil {
				continue
			}
			s.handleGetReq(req)
		case proto.MsgGetDone:
			done, err := proto.DecodeGetDone(msg)
			if err != nil {
				continue
			}
			s.handleGetDone(done)
		}
	}
}

func (s *serverSession) handlePutReq(req proto.PutReq) {
	lease, ok := s.pool.Acquire()
	if !ok || lease.MaxLen < uint64(req.ObjLen) {
		s.sendPutDone(req.ReqID, 1)
		if ok {
			_ = s.pool.Release(lease.Token)
		}
		return
	}

	s.stateMu.Lock()
	s.putState[req.ReqID] = putState{lease: lease, bucket: req.Bucket, key: req.Key, objLen: req.ObjLen}
	s.stateMu.Unlock()

	msg := proto.PutLease{ReqID: req.ReqID, Token: lease.Token, Addr: lease.Addr, RKey: lease.RKey, MaxLen: uint32(lease.MaxLen)}
	buf := s.allocSendBuf()
	if buf == nil {
		_ = s.pool.Release(lease.Token)
		s.clearPut(req.ReqID)
		return
	}
	wlen, err := proto.EncodePutLease(buf, msg)
	if err != nil {
		rdma.FreeBuffer(buf)
		_ = s.pool.Release(lease.Token)
		s.clearPut(req.ReqID)
		return
	}
	s.send(buf[:wlen])
}

func (s *serverSession) handlePutCommit(commit proto.PutCommit) {
	s.stateMu.Lock()
	st, ok := s.putState[commit.ReqID]
	s.stateMu.Unlock()
	if !ok || st.lease.Token != commit.Token {
		s.sendPutDone(commit.ReqID, 1)
		return
	}
	if commit.DataLen > uint32(st.lease.MaxLen) {
		s.sendPutDone(commit.ReqID, 1)
		return
	}

	s.workCh <- func() {
		payload, err := rdma.GoBytesFromAddr(st.lease.Addr, int(commit.DataLen))
		if err != nil {
			s.sendPutDone(commit.ReqID, 1)
			_ = s.pool.Release(st.lease.Token)
			s.clearPut(commit.ReqID)
			return
		}
		sum := sha256.Sum256(payload)
		if err := s.backend.Put(context.Background(), commit.Bucket, commit.Key, bytes.NewReader(payload), int64(commit.DataLen)); err != nil {
			s.sendPutDone(commit.ReqID, 1)
			_ = s.pool.Release(st.lease.Token)
			s.clearPut(commit.ReqID)
			return
		}
		fmt.Printf("PUT sha256=%s bucket=%s key=%s len=%d\n", hex.EncodeToString(sum[:]), commit.Bucket, commit.Key, commit.DataLen)
		s.sendPutDone(commit.ReqID, 0)
		_ = s.pool.Release(st.lease.Token)
		s.clearPut(commit.ReqID)
	}
}

func (s *serverSession) handleGetReq(req proto.GetReq) {
	s.workCh <- func() {
		obj, size, err := s.backend.Get(context.Background(), req.Bucket, req.Key)
		if err != nil {
			return
		}
		defer obj.Close()
		if size <= 0 {
			return
		}
		data, err := io.ReadAll(obj)
		if err != nil {
			return
		}
		if int64(len(data)) != size {
			return
		}
		lease, ok := s.pool.Acquire()
		if !ok || lease.MaxLen < uint64(size) {
			if ok {
				_ = s.pool.Release(lease.Token)
			}
			return
		}
		if err := rdma.CopyToAddr(lease.Addr, data); err != nil {
			_ = s.pool.Release(lease.Token)
			return
		}

		s.stateMu.Lock()
		s.getState[req.ReqID] = getState{lease: lease}
		s.stateMu.Unlock()

		ready := proto.GetReady{ReqID: req.ReqID, Token: lease.Token, Addr: lease.Addr, RKey: lease.RKey, DataLen: uint32(size)}
		buf := s.allocSendBuf()
		if buf == nil {
			_ = s.pool.Release(lease.Token)
			s.clearGet(req.ReqID)
			return
		}
		wlen, err := proto.EncodeGetReady(buf, ready)
		if err != nil {
			rdma.FreeBuffer(buf)
			_ = s.pool.Release(lease.Token)
			s.clearGet(req.ReqID)
			return
		}
		s.send(buf[:wlen])
	}
}

func (s *serverSession) handleGetDone(done proto.GetDone) {
	s.stateMu.Lock()
	st, ok := s.getState[done.ReqID]
	s.stateMu.Unlock()
	if !ok || st.lease.Token != done.Token {
		return
	}
	_ = s.pool.Release(st.lease.Token)
	s.clearGet(done.ReqID)
}

func (s *serverSession) workerLoop() {
	for task := range s.workCh {
		task()
	}
}

func (s *serverSession) sendPutDone(reqID uint32, status uint16) {
	buf := s.allocSendBuf()
	if buf == nil {
		return
	}
	wlen, err := proto.EncodePutDone(buf, proto.PutDone{ReqID: reqID, Status: status})
	if err != nil {
		rdma.FreeBuffer(buf)
		return
	}
	s.send(buf[:wlen])
}

func (s *serverSession) allocSendBuf() []byte {
	buf, err := rdma.AllocBuffer(ctrlBufSize)
	if err != nil {
		s.errCh <- err
		return nil
	}
	return buf
}

func (s *serverSession) send(buf []byte) {
	ptr := uintptr(unsafe.Pointer(&buf[0]))
	s.sendMu.Lock()
	s.sendBufs[ptr] = buf
	s.sendMu.Unlock()
	s.sendCh <- buf
}

func (s *serverSession) freeSend(buf []byte) {
	ptr := uintptr(unsafe.Pointer(&buf[0]))
	s.sendMu.Lock()
	b := s.sendBufs[ptr]
	delete(s.sendBufs, ptr)
	s.sendMu.Unlock()
	if b != nil {
		_ = s.conn.DeregisterBuffer(b)
		rdma.FreeBuffer(b)
	}
}

func (s *serverSession) clearPut(reqID uint32) {
	s.stateMu.Lock()
	delete(s.putState, reqID)
	s.stateMu.Unlock()
}

func (s *serverSession) clearGet(reqID uint32) {
	s.stateMu.Lock()
	delete(s.getState, reqID)
	s.stateMu.Unlock()
}
