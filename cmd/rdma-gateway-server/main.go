package main

import (
	"bytes"
	"context"
	"encoding/json"
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
	timing  bool

	recvCh chan []byte
	sendCh chan []byte
	errCh  chan error

	sendMu   sync.Mutex
	sendBufs map[uintptr][]byte

	stateMu  sync.Mutex
	putState map[uint32]putState
	getState map[uint32]getState

	workCh chan func()

	poolReady chan struct{}

	timeMu   sync.Mutex
	putTimes map[uint32]*putTiming
	getTimes map[uint32]*getTiming
}

type putTiming struct {
	S0 int64
	S1 int64
	S2 int64
	S3 int64
	S4 int64
	S5 int64
}

type getTiming struct {
	S0 int64
	S1 int64
	S2 int64
	S3 int64
	S4 int64
}

type timingServerPut struct {
	Side string `json:"side"`
	Op   string `json:"op"`
	Req  uint32 `json:"req_id"`

	S0 int64 `json:"s0_recv_req_ns"`
	S1 int64 `json:"s1_send_lease_ns"`
	S2 int64 `json:"s2_recv_commit_ns"`
	S3 int64 `json:"s3_minio_start_ns"`
	S4 int64 `json:"s4_minio_done_ns"`
	S5 int64 `json:"s5_send_done_ns"`

	TotalNS int64 `json:"total_ns"`
}

type timingServerGet struct {
	Side string `json:"side"`
	Op   string `json:"op"`
	Req  uint32 `json:"req_id"`

	S0 int64 `json:"s0_recv_req_ns"`
	S1 int64 `json:"s1_minio_start_ns"`
	S2 int64 `json:"s2_minio_done_ns"`
	S3 int64 `json:"s3_send_ready_ns"`
	S4 int64 `json:"s4_recv_done_ns"`

	TotalNS int64 `json:"total_ns"`
}

func main() {
	fs := flag.NewFlagSet("rdma-gateway-server", flag.ContinueOnError)
	fs.SetOutput(os.Stdout)

	bindIP := fs.String("bind-ip", "0.0.0.0", "IP address to bind")
	port := fs.Int("port", 7471, "Port to listen on")
	minioEndpoint := fs.String("minio-endpoint", "http://127.0.0.1:9000", "MinIO endpoint URL")
	accessKey := fs.String("access-key", "", "MinIO access key")
	secretKey := fs.String("secret-key", "", "MinIO secret key")
	timing := fs.Bool("timing", false, "Emit per-request timing JSON")

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

	s := newServerSession(conn, backendClient, *timing)
	s.run()

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
	s.setPool(pool)

	if err := <-s.errCh; err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func newServerSession(conn *rdma.Conn, backendClient *backend.MinioBackend, timing bool) *serverSession {
	s := &serverSession{
		conn:      conn,
		backend:   backendClient,
		timing:    timing,
		recvCh:    make(chan []byte, 256),
		sendCh:    make(chan []byte, 256),
		errCh:     make(chan error, 1),
		sendBufs:  make(map[uintptr][]byte),
		putState:  make(map[uint32]putState),
		getState:  make(map[uint32]getState),
		workCh:    make(chan func(), 256),
		poolReady: make(chan struct{}),
		putTimes:  make(map[uint32]*putTiming),
		getTimes:  make(map[uint32]*getTiming),
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

func (s *serverSession) setPool(pool *bufpool.Pool) {
	s.pool = pool
	close(s.poolReady)
}

func (s *serverSession) waitPool() *bufpool.Pool {
	<-s.poolReady
	return s.pool
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
		fmt.Printf("CTRL recv type=%d req=%d len=%d\n", hdr.Type, hdr.ReqID, len(msg))
		switch hdr.Type {
		case proto.MsgHello:
			s.handleHello(hdr)
		case proto.MsgPutReq:
			s.markPutReq(hdr.ReqID)
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
			s.markPutCommit(commit.ReqID)
			fmt.Printf("PUT commit req=%d len=%d\n", commit.ReqID, commit.DataLen)
			s.handlePutCommit(commit)
		case proto.MsgGetReq:
			s.markGetReq(hdr.ReqID)
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
			s.markGetDone(done.ReqID)
			s.handleGetDone(done)
		}
	}
}

func (s *serverSession) handleHello(hdr proto.MsgHdr) {
	buf := s.allocSendBuf()
	if buf == nil {
		return
	}
	ack := proto.MsgHdr{
		Magic:   proto.Magic,
		Version: proto.Version,
		Type:    proto.MsgHelloAck,
		ReqID:   hdr.ReqID,
		BodyLen: 0,
	}
	if err := proto.WriteHeader(buf[:proto.HeaderLen], ack); err != nil {
		rdma.FreeBuffer(buf)
		return
	}
	s.send(buf[:proto.HeaderLen])
}

func (s *serverSession) handlePutReq(req proto.PutReq) {
	pool := s.waitPool()
	lease, ok := pool.Acquire()
	if !ok || lease.MaxLen < uint64(req.ObjLen) {
		s.sendPutDone(req.ReqID, 1)
		if ok {
			_ = pool.Release(lease.Token)
		}
		return
	}

	s.stateMu.Lock()
	s.putState[req.ReqID] = putState{lease: lease, bucket: req.Bucket, key: req.Key, objLen: req.ObjLen}
	s.stateMu.Unlock()

	fmt.Printf("PUT lease req=%d addr=0x%x rkey=0x%x max=%d\\n", req.ReqID, lease.Addr, lease.RKey, lease.MaxLen)

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
	s.markPutLease(req.ReqID)
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
		start := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		s.markPutMinioStart(commit.ReqID)
		payload, err := rdma.UnsafeSliceFromAddr(st.lease.Addr, int(commit.DataLen))
		if err != nil {
			s.sendPutDone(commit.ReqID, 1)
			_ = s.pool.Release(st.lease.Token)
			s.clearPut(commit.ReqID)
			return
		}
		if err := s.backend.Put(ctx, commit.Bucket, commit.Key, bytes.NewReader(payload), int64(commit.DataLen)); err != nil {
			fmt.Printf("PUT backend error req=%d err=%v\n", commit.ReqID, err)
			s.markPutMinioDone(commit.ReqID)
			s.sendPutDone(commit.ReqID, 1)
			_ = s.waitPool().Release(st.lease.Token)
			s.clearPut(commit.ReqID)
			return
		}
		s.markPutMinioDone(commit.ReqID)
		fmt.Printf("PUT bucket=%s key=%s len=%d took=%s\n", commit.Bucket, commit.Key, commit.DataLen, time.Since(start))
		s.sendPutDone(commit.ReqID, 0)
		_ = s.waitPool().Release(st.lease.Token)
		s.clearPut(commit.ReqID)
	}
}

func (s *serverSession) handleGetReq(req proto.GetReq) {
	s.workCh <- func() {
		pool := s.waitPool()
		s.markGetMinioStart(req.ReqID)
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
		s.markGetMinioDone(req.ReqID)
		lease, ok := pool.Acquire()
		if !ok || lease.MaxLen < uint64(size) {
			if ok {
				_ = pool.Release(lease.Token)
			}
			return
		}
		if err := rdma.CopyToAddr(lease.Addr, data); err != nil {
			_ = pool.Release(lease.Token)
			return
		}

		s.stateMu.Lock()
		s.getState[req.ReqID] = getState{lease: lease}
		s.stateMu.Unlock()

		ready := proto.GetReady{ReqID: req.ReqID, Token: lease.Token, Addr: lease.Addr, RKey: lease.RKey, DataLen: uint32(size)}
		buf := s.allocSendBuf()
		if buf == nil {
			_ = pool.Release(lease.Token)
			s.clearGet(req.ReqID)
			return
		}
		wlen, err := proto.EncodeGetReady(buf, ready)
		if err != nil {
			rdma.FreeBuffer(buf)
			_ = pool.Release(lease.Token)
			s.clearGet(req.ReqID)
			return
		}
		s.markGetReady(req.ReqID)
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
	s.markGetDone(done.ReqID)
	_ = s.waitPool().Release(st.lease.Token)
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
	s.markPutDone(reqID)
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

func (s *serverSession) emitJSON(v any) {
	if !s.timing {
		return
	}
	b, err := json.Marshal(v)
	if err != nil {
		return
	}
	fmt.Println(string(b))
}

func (s *serverSession) markPutReq(reqID uint32) {
	if !s.timing {
		return
	}
	s.timeMu.Lock()
	defer s.timeMu.Unlock()
	t := s.putTimes[reqID]
	if t == nil {
		t = &putTiming{}
		s.putTimes[reqID] = t
	}
	if t.S0 == 0 {
		t.S0 = time.Now().UnixNano()
	}
}

func (s *serverSession) markPutLease(reqID uint32) {
	if !s.timing {
		return
	}
	s.timeMu.Lock()
	defer s.timeMu.Unlock()
	t := s.putTimes[reqID]
	if t == nil {
		t = &putTiming{}
		s.putTimes[reqID] = t
	}
	if t.S1 == 0 {
		t.S1 = time.Now().UnixNano()
	}
}

func (s *serverSession) markPutCommit(reqID uint32) {
	if !s.timing {
		return
	}
	s.timeMu.Lock()
	defer s.timeMu.Unlock()
	t := s.putTimes[reqID]
	if t == nil {
		t = &putTiming{}
		s.putTimes[reqID] = t
	}
	if t.S2 == 0 {
		t.S2 = time.Now().UnixNano()
	}
}

func (s *serverSession) markPutMinioStart(reqID uint32) {
	if !s.timing {
		return
	}
	s.timeMu.Lock()
	defer s.timeMu.Unlock()
	t := s.putTimes[reqID]
	if t == nil {
		t = &putTiming{}
		s.putTimes[reqID] = t
	}
	if t.S3 == 0 {
		t.S3 = time.Now().UnixNano()
	}
}

func (s *serverSession) markPutMinioDone(reqID uint32) {
	if !s.timing {
		return
	}
	s.timeMu.Lock()
	defer s.timeMu.Unlock()
	t := s.putTimes[reqID]
	if t == nil {
		t = &putTiming{}
		s.putTimes[reqID] = t
	}
	if t.S4 == 0 {
		t.S4 = time.Now().UnixNano()
	}
}

func (s *serverSession) markPutDone(reqID uint32) {
	if !s.timing {
		return
	}
	var out timingServerPut
	s.timeMu.Lock()
	t := s.putTimes[reqID]
	if t != nil && t.S5 == 0 {
		t.S5 = time.Now().UnixNano()
		out = timingServerPut{
			Side:    "server",
			Op:      "rdma_put",
			Req:     reqID,
			S0:      t.S0,
			S1:      t.S1,
			S2:      t.S2,
			S3:      t.S3,
			S4:      t.S4,
			S5:      t.S5,
			TotalNS: t.S5 - t.S0,
		}
		delete(s.putTimes, reqID)
	}
	s.timeMu.Unlock()
	if out.Req != 0 {
		s.emitJSON(out)
	}
}

func (s *serverSession) markGetReq(reqID uint32) {
	if !s.timing {
		return
	}
	s.timeMu.Lock()
	defer s.timeMu.Unlock()
	t := s.getTimes[reqID]
	if t == nil {
		t = &getTiming{}
		s.getTimes[reqID] = t
	}
	if t.S0 == 0 {
		t.S0 = time.Now().UnixNano()
	}
}

func (s *serverSession) markGetMinioStart(reqID uint32) {
	if !s.timing {
		return
	}
	s.timeMu.Lock()
	defer s.timeMu.Unlock()
	t := s.getTimes[reqID]
	if t == nil {
		t = &getTiming{}
		s.getTimes[reqID] = t
	}
	if t.S1 == 0 {
		t.S1 = time.Now().UnixNano()
	}
}

func (s *serverSession) markGetMinioDone(reqID uint32) {
	if !s.timing {
		return
	}
	s.timeMu.Lock()
	defer s.timeMu.Unlock()
	t := s.getTimes[reqID]
	if t == nil {
		t = &getTiming{}
		s.getTimes[reqID] = t
	}
	if t.S2 == 0 {
		t.S2 = time.Now().UnixNano()
	}
}

func (s *serverSession) markGetReady(reqID uint32) {
	if !s.timing {
		return
	}
	s.timeMu.Lock()
	defer s.timeMu.Unlock()
	t := s.getTimes[reqID]
	if t == nil {
		t = &getTiming{}
		s.getTimes[reqID] = t
	}
	if t.S3 == 0 {
		t.S3 = time.Now().UnixNano()
	}
}

func (s *serverSession) markGetDone(reqID uint32) {
	if !s.timing {
		return
	}
	var out timingServerGet
	s.timeMu.Lock()
	t := s.getTimes[reqID]
	if t != nil && t.S4 == 0 {
		t.S4 = time.Now().UnixNano()
		out = timingServerGet{
			Side:    "server",
			Op:      "rdma_get",
			Req:     reqID,
			S0:      t.S0,
			S1:      t.S1,
			S2:      t.S2,
			S3:      t.S3,
			S4:      t.S4,
			TotalNS: t.S4 - t.S0,
		}
		delete(s.getTimes, reqID)
	}
	s.timeMu.Unlock()
	if out.Req != 0 {
		s.emitJSON(out)
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
