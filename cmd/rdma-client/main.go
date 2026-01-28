package main

import (
	"context"
	"crypto/sha256"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"rdma_gateway_go/internal/proto"
	"rdma_gateway_go/internal/rdma"
)

const (
	ctrlBufSize = 4096
	recvDepth   = 64
)

type clientSession struct {
	conn *rdma.Conn

	recvCh chan []byte
	sendCh chan []byte
	errCh  chan error

	pendingMu sync.Mutex
	pending   map[uint32]chan []byte

	sendMu   sync.Mutex
	sendBufs map[uintptr][]byte

	rdmaMu   sync.Mutex
	rdmaWait map[uintptr]chan struct{}
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	serverIP := "127.0.0.1"
	port := 7471

	subcmd := os.Args[1]
	switch subcmd {
	case "put":
		fs := flag.NewFlagSet("put", flag.ContinueOnError)
		fs.SetOutput(os.Stdout)
		fs.StringVar(&serverIP, "server-ip", serverIP, "Server IP address")
		fs.IntVar(&port, "port", port, "Server port")
		bucket := fs.String("bucket", "", "Bucket name")
		key := fs.String("key", "", "Object key")
		file := fs.String("file", "", "Local file path")
		concurrency := fs.Int("concurrency", 1, "Number of concurrent requests")
		count := fs.Int("count", 0, "Total number of requests (default: concurrency)")
		measure := fs.Bool("timing", false, "Print end-to-end timing per request")

		fs.Usage = func() {
			fmt.Fprintf(fs.Output(), "Usage: %s put [flags]\n\nFlags:\n", os.Args[0])
			fs.PrintDefaults()
		}

		if err := fs.Parse(os.Args[2:]); err != nil {
			if err == flag.ErrHelp {
				return
			}
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}

		conn, err := rdma.Dial(serverIP, port)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		defer conn.Close()

		if err := hello(conn); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}

		s := newClientSession(conn)
		s.run()

		countVal, concVal := normalizeCounts(*count, *concurrency)
		if err := runPuts(s, *bucket, *key, *file, concVal, countVal, *measure); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case "get":
		fs := flag.NewFlagSet("get", flag.ContinueOnError)
		fs.SetOutput(os.Stdout)
		fs.StringVar(&serverIP, "server-ip", serverIP, "Server IP address")
		fs.IntVar(&port, "port", port, "Server port")
		bucket := fs.String("bucket", "", "Bucket name")
		key := fs.String("key", "", "Object key")
		out := fs.String("out", "", "Output file path")
		concurrency := fs.Int("concurrency", 1, "Number of concurrent requests")
		count := fs.Int("count", 0, "Total number of requests (default: concurrency)")
		measure := fs.Bool("timing", false, "Print end-to-end timing per request")

		fs.Usage = func() {
			fmt.Fprintf(fs.Output(), "Usage: %s get [flags]\n\nFlags:\n", os.Args[0])
			fs.PrintDefaults()
		}

		if err := fs.Parse(os.Args[2:]); err != nil {
			if err == flag.ErrHelp {
				return
			}
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}

		conn, err := rdma.Dial(serverIP, port)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		defer conn.Close()

		if err := hello(conn); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}

		s := newClientSession(conn)
		s.run()

		countVal, concVal := normalizeCounts(*count, *concurrency)
		if err := runGets(s, *bucket, *key, *out, concVal, countVal, *measure); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case "minio-put":
		fs := flag.NewFlagSet("minio-put", flag.ContinueOnError)
		fs.SetOutput(os.Stdout)
		endpoint := fs.String("minio-endpoint", "http://127.0.0.1:9000", "MinIO endpoint URL")
		accessKey := fs.String("access-key", "", "MinIO access key")
		secretKey := fs.String("secret-key", "", "MinIO secret key")
		bucket := fs.String("bucket", "", "Bucket name")
		key := fs.String("key", "", "Object key")
		file := fs.String("file", "", "Local file path")
		concurrency := fs.Int("concurrency", 1, "Number of concurrent requests")
		count := fs.Int("count", 0, "Total number of requests (default: concurrency)")
		measure := fs.Bool("timing", false, "Print end-to-end timing per request")

		fs.Usage = func() {
			fmt.Fprintf(fs.Output(), "Usage: %s minio-put [flags]\n\nFlags:\n", os.Args[0])
			fs.PrintDefaults()
		}
		if err := fs.Parse(os.Args[2:]); err != nil {
			if err == flag.ErrHelp {
				return
			}
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}

		countVal, concVal := normalizeCounts(*count, *concurrency)
		if err := runMinioPuts(*endpoint, *accessKey, *secretKey, *bucket, *key, *file, concVal, countVal, *measure); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case "minio-get":
		fs := flag.NewFlagSet("minio-get", flag.ContinueOnError)
		fs.SetOutput(os.Stdout)
		endpoint := fs.String("minio-endpoint", "http://127.0.0.1:9000", "MinIO endpoint URL")
		accessKey := fs.String("access-key", "", "MinIO access key")
		secretKey := fs.String("secret-key", "", "MinIO secret key")
		bucket := fs.String("bucket", "", "Bucket name")
		key := fs.String("key", "", "Object key")
		out := fs.String("out", "", "Output file path")
		concurrency := fs.Int("concurrency", 1, "Number of concurrent requests")
		count := fs.Int("count", 0, "Total number of requests (default: concurrency)")
		measure := fs.Bool("timing", false, "Print end-to-end timing per request")

		fs.Usage = func() {
			fmt.Fprintf(fs.Output(), "Usage: %s minio-get [flags]\n\nFlags:\n", os.Args[0])
			fs.PrintDefaults()
		}
		if err := fs.Parse(os.Args[2:]); err != nil {
			if err == flag.ErrHelp {
				return
			}
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}

		countVal, concVal := normalizeCounts(*count, *concurrency)
		if err := runMinioGets(*endpoint, *accessKey, *secretKey, *bucket, *key, *out, concVal, countVal, *measure); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintf(os.Stdout, "Usage: %s <put|get|minio-put|minio-get> [flags]\n\n", os.Args[0])
	fmt.Fprintln(os.Stdout, "Subcommands:")
	fmt.Fprintln(os.Stdout, "  put   Upload an object")
	fmt.Fprintln(os.Stdout, "  get   Download an object")
	fmt.Fprintln(os.Stdout, "  minio-put   Upload directly to MinIO")
	fmt.Fprintln(os.Stdout, "  minio-get   Download directly from MinIO")
	fmt.Fprintln(os.Stdout)
	fmt.Fprintln(os.Stdout, "Run with -h after a subcommand to see flags.")
}

func normalizeCounts(count, concurrency int) (int, int) {
	if concurrency < 1 {
		concurrency = 1
	}
	if count <= 0 {
		count = concurrency
	}
	if count < concurrency {
		concurrency = count
	}
	return count, concurrency
}

func newClientSession(conn *rdma.Conn) *clientSession {
	return &clientSession{
		conn:     conn,
		recvCh:   make(chan []byte, 256),
		sendCh:   make(chan []byte, 256),
		errCh:    make(chan error, 1),
		pending:  make(map[uint32]chan []byte),
		sendBufs: make(map[uintptr][]byte),
		rdmaWait: make(map[uintptr]chan struct{}),
	}
}

func (s *clientSession) run() {
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
	go s.sendLoop()
	go s.pollLoop()
	go s.dispatchLoop()
}

func (s *clientSession) sendLoop() {
	for buf := range s.sendCh {
		if err := s.conn.Send(buf); err != nil {
			s.freeSend(buf)
			s.errCh <- err
			return
		}
	}
}

func (s *clientSession) pollLoop() {
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
		case rdma.OpRdmaWrite, rdma.OpRdmaRead:
			s.signalRDMA(buf)
		case rdma.OpUnknown:
			continue
		}
	}
}

func (s *clientSession) dispatchLoop() {
	for msg := range s.recvCh {
		hdr, err := proto.ReadHeader(msg)
		if err != nil {
			continue
		}
		s.pendingMu.Lock()
		ch := s.pending[hdr.ReqID]
		s.pendingMu.Unlock()
		if ch != nil {
			ch <- msg
		}
	}
}

func (s *clientSession) registerReq(reqID uint32) chan []byte {
	ch := make(chan []byte, 4)
	s.pendingMu.Lock()
	s.pending[reqID] = ch
	s.pendingMu.Unlock()
	return ch
}

func (s *clientSession) unregisterReq(reqID uint32) {
	s.pendingMu.Lock()
	delete(s.pending, reqID)
	s.pendingMu.Unlock()
}

func (s *clientSession) waitMsg(ch <-chan []byte, expect proto.MsgType, timeout time.Duration) ([]byte, error) {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		select {
		case msg := <-ch:
			hdr, err := proto.ReadHeader(msg)
			if err != nil {
				return nil, err
			}
			if hdr.Type == expect {
				return msg, nil
			}
		case err := <-s.errCh:
			return nil, err
		case <-deadline.C:
			return nil, fmt.Errorf("timeout waiting for %v", expect)
		}
	}
}

func (s *clientSession) send(buf []byte) {
	ptr := uintptr(unsafe.Pointer(&buf[0]))
	s.sendMu.Lock()
	s.sendBufs[ptr] = buf
	s.sendMu.Unlock()
	s.sendCh <- buf
}

func (s *clientSession) freeSend(buf []byte) {
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

func (s *clientSession) registerRDMA(buf []byte) chan struct{} {
	ch := make(chan struct{})
	ptr := uintptr(unsafe.Pointer(&buf[0]))
	s.rdmaMu.Lock()
	s.rdmaWait[ptr] = ch
	s.rdmaMu.Unlock()
	return ch
}

func (s *clientSession) signalRDMA(buf []byte) {
	ptr := uintptr(unsafe.Pointer(&buf[0]))
	s.rdmaMu.Lock()
	ch := s.rdmaWait[ptr]
	delete(s.rdmaWait, ptr)
	s.rdmaMu.Unlock()
	if ch != nil {
		close(ch)
	}
}

func (s *clientSession) waitRDMA(ch <-chan struct{}, timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ch:
		return nil
	case err := <-s.errCh:
		return err
	case <-timer.C:
		return fmt.Errorf("rdma op timeout")
	}
}

func runPuts(s *clientSession, bucket, key, path string, concurrency, count int, measure bool) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var reqID uint32
	tasks := make(chan int, count)
	for i := 0; i < count; i++ {
		tasks <- i
	}
	close(tasks)

	errCh := make(chan error, 1)
	durCh := make(chan time.Duration, count)
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range tasks {
				id := atomic.AddUint32(&reqID, 1)
				dur, err := doPut(s, id, bucket, key, data)
				if err != nil {
					errCh <- err
					return
				}
				durCh <- dur
			}
		}()
	}

	wg.Wait()
	close(durCh)
	select {
	case err := <-errCh:
		return err
	default:
		reportTiming("RDMA PUT", start, count, durCh, measure)
		return nil
	}
}

func runGets(s *clientSession, bucket, key, out string, concurrency, count int, measure bool) error {
	var reqID uint32
	tasks := make(chan int, count)
	for i := 0; i < count; i++ {
		tasks <- i
	}
	close(tasks)

	errCh := make(chan error, 1)
	durCh := make(chan time.Duration, count)
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range tasks {
				id := atomic.AddUint32(&reqID, 1)
				outPath := outputPath(out, id, count)
				dur, err := doGet(s, id, bucket, key, outPath)
				if err != nil {
					errCh <- err
					return
				}
				durCh <- dur
			}
		}()
	}

	wg.Wait()
	close(durCh)
	select {
	case err := <-errCh:
		return err
	default:
		reportTiming("RDMA GET", start, count, durCh, measure)
		return nil
	}
}

func outputPath(base string, reqID uint32, count int) string {
	if count <= 1 {
		return base
	}
	return fmt.Sprintf("%s.%d", base, reqID)
}

func reportTiming(label string, start time.Time, count int, durCh <-chan time.Duration, perReq bool) {
	total := time.Since(start)
	var sum time.Duration
	var n int
	for d := range durCh {
		if perReq {
			fmt.Printf("%s req %d end-to-end=%s\n", label, n+1, d)
		}
		sum += d
		n++
	}
	if n == 0 {
		fmt.Printf("%s total=%s count=%d\n", label, total, count)
		return
	}
	avg := sum / time.Duration(n)
	fmt.Printf("%s total=%s count=%d avg=%s\n", label, total, n, avg)
}

func newMinioClient(endpoint, accessKey, secretKey string) (*minio.Client, error) {
	if endpoint == "" {
		return nil, fmt.Errorf("minio endpoint is empty")
	}
	ep, secure, err := normalizeEndpoint(endpoint)
	if err != nil {
		return nil, err
	}
	return minio.New(ep, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: secure,
	})
}

func normalizeEndpoint(endpoint string) (string, bool, error) {
	if strings.Contains(endpoint, "://") {
		u, err := url.Parse(endpoint)
		if err != nil {
			return "", false, err
		}
		secure := u.Scheme == "https"
		if u.Host == "" {
			return "", false, fmt.Errorf("invalid endpoint: %s", endpoint)
		}
		return u.Host, secure, nil
	}
	return endpoint, false, nil
}

func runMinioPuts(endpoint, accessKey, secretKey, bucket, key, path string, concurrency, count int, measure bool) error {
	client, err := newMinioClient(endpoint, accessKey, secretKey)
	if err != nil {
		return err
	}
	var reqID uint32
	tasks := make(chan int, count)
	for i := 0; i < count; i++ {
		tasks <- i
	}
	close(tasks)

	errCh := make(chan error, 1)
	durCh := make(chan time.Duration, count)
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range tasks {
				id := atomic.AddUint32(&reqID, 1)
				dur, err := doMinioPut(client, bucket, key, path, id)
				if err != nil {
					errCh <- err
					return
				}
				durCh <- dur
			}
		}()
	}
	wg.Wait()
	close(durCh)
	select {
	case err := <-errCh:
		return err
	default:
		reportTiming("MINIO PUT", start, count, durCh, measure)
		return nil
	}
}

func runMinioGets(endpoint, accessKey, secretKey, bucket, key, out string, concurrency, count int, measure bool) error {
	client, err := newMinioClient(endpoint, accessKey, secretKey)
	if err != nil {
		return err
	}
	var reqID uint32
	tasks := make(chan int, count)
	for i := 0; i < count; i++ {
		tasks <- i
	}
	close(tasks)

	errCh := make(chan error, 1)
	durCh := make(chan time.Duration, count)
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range tasks {
				id := atomic.AddUint32(&reqID, 1)
				outPath := outputPath(out, id, count)
				dur, err := doMinioGet(client, bucket, key, outPath)
				if err != nil {
					errCh <- err
					return
				}
				durCh <- dur
			}
		}()
	}
	wg.Wait()
	close(durCh)
	select {
	case err := <-errCh:
		return err
	default:
		reportTiming("MINIO GET", start, count, durCh, measure)
		return nil
	}
}

func doMinioPut(client *minio.Client, bucket, key, path string, reqID uint32) (time.Duration, error) {
	start := time.Now()
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return 0, err
	}
	_, err = client.PutObject(context.Background(), bucket, key, f, st.Size(), minio.PutObjectOptions{})
	if err != nil {
		return 0, err
	}
	fmt.Printf("MINIO PUT OK req=%d size=%d\n", reqID, st.Size())
	return time.Since(start), nil
}

func doMinioGet(client *minio.Client, bucket, key, out string) (time.Duration, error) {
	start := time.Now()
	obj, err := client.GetObject(context.Background(), bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return 0, err
	}
	defer obj.Close()
	if out == "" {
		return 0, fmt.Errorf("missing --out")
	}
	f, err := os.Create(out)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	if _, err := io.Copy(f, obj); err != nil {
		return 0, err
	}
	return time.Since(start), nil
}

func doPut(s *clientSession, reqID uint32, bucket, key string, data []byte) (time.Duration, error) {
	start := time.Now()
	ch := s.registerReq(reqID)
	defer s.unregisterReq(reqID)

	sendBuf, err := rdma.AllocBuffer(ctrlBufSize)
	if err != nil {
		return 0, err
	}
	req := proto.PutReq{ReqID: reqID, Bucket: bucket, Key: key, ObjLen: uint32(len(data))}
	n, err := proto.EncodePutReq(sendBuf, req)
	if err != nil {
		rdma.FreeBuffer(sendBuf)
		return 0, err
	}
	fmt.Printf("PUT req=%d send len=%d\n", reqID, len(data))
	s.send(sendBuf[:n])

	msg, err := s.waitMsg(ch, proto.MsgPutLease, 10*time.Second)
	if err != nil {
		return 0, err
	}
	lease, err := proto.DecodePutLease(msg)
	if err != nil {
		return 0, err
	}
	if lease.MaxLen < uint32(len(data)) {
		return 0, fmt.Errorf("object too large for lease")
	}
	fmt.Printf("PUT lease req=%d addr=0x%x rkey=0x%x max=%d len=%d\n", reqID, lease.Addr, lease.RKey, lease.MaxLen, len(data))

	local, err := rdma.AllocBuffer(len(data))
	if err != nil {
		return 0, err
	}
	copy(local, data)
	waitCh := s.registerRDMA(local)
	if err := s.conn.PostRdmaWrite(lease.Addr, lease.RKey, local); err != nil {
		_ = s.conn.DeregisterBuffer(local)
		rdma.FreeBuffer(local)
		return 0, err
	}
	if err := s.waitRDMA(waitCh, 10*time.Second); err != nil {
		_ = s.conn.DeregisterBuffer(local)
		rdma.FreeBuffer(local)
		return 0, err
	}
	_ = s.conn.DeregisterBuffer(local)
	rdma.FreeBuffer(local)

	commit := proto.PutCommit{ReqID: reqID, Token: lease.Token, DataLen: uint32(len(data)), Bucket: bucket, Key: key}
	sendBuf2, err := rdma.AllocBuffer(ctrlBufSize)
	if err != nil {
		return 0, err
	}
	n, err = proto.EncodePutCommit(sendBuf2, commit)
	if err != nil {
		rdma.FreeBuffer(sendBuf2)
		return 0, err
	}
	s.send(sendBuf2[:n])

	msg, err = s.waitMsg(ch, proto.MsgPutDone, 10*time.Second)
	if err != nil {
		return 0, err
	}
	done, err := proto.DecodePutDone(msg)
	if err != nil {
		return 0, err
	}
	if done.Status != 0 {
		return 0, fmt.Errorf("put failed status=%d", done.Status)
	}

	sum := sha256.Sum256(data)
	fmt.Printf("PUT OK req=%d sha256=%x\n", reqID, sum)
	return time.Since(start), nil
}

func doGet(s *clientSession, reqID uint32, bucket, key, out string) (time.Duration, error) {
	start := time.Now()
	if out == "" {
		return 0, fmt.Errorf("missing --out")
	}
	ch := s.registerReq(reqID)
	defer s.unregisterReq(reqID)

	sendBuf, err := rdma.AllocBuffer(ctrlBufSize)
	if err != nil {
		return 0, err
	}
	req := proto.GetReq{ReqID: reqID, Bucket: bucket, Key: key}
	n, err := proto.EncodeGetReq(sendBuf, req)
	if err != nil {
		rdma.FreeBuffer(sendBuf)
		return 0, err
	}
	s.send(sendBuf[:n])

	msg, err := s.waitMsg(ch, proto.MsgGetReady, 10*time.Second)
	if err != nil {
		return 0, err
	}
	ready, err := proto.DecodeGetReady(msg)
	if err != nil {
		return 0, err
	}
	if ready.DataLen == 0 {
		return 0, fmt.Errorf("empty object")
	}

	local, err := rdma.AllocBuffer(int(ready.DataLen))
	if err != nil {
		return 0, err
	}
	waitCh := s.registerRDMA(local)
	if err := s.conn.PostRdmaRead(ready.Addr, ready.RKey, local); err != nil {
		_ = s.conn.DeregisterBuffer(local)
		rdma.FreeBuffer(local)
		return 0, err
	}
	if err := s.waitRDMA(waitCh, 10*time.Second); err != nil {
		_ = s.conn.DeregisterBuffer(local)
		rdma.FreeBuffer(local)
		return 0, err
	}
	if err := os.WriteFile(out, local, 0o644); err != nil {
		_ = s.conn.DeregisterBuffer(local)
		rdma.FreeBuffer(local)
		return 0, err
	}
	sum := sha256.Sum256(local)
	fmt.Printf("GET OK req=%d sha256=%x\n", reqID, sum)
	_ = s.conn.DeregisterBuffer(local)
	rdma.FreeBuffer(local)

	done := proto.GetDone{ReqID: reqID, Token: ready.Token}
	sendBuf2, err := rdma.AllocBuffer(ctrlBufSize)
	if err != nil {
		return 0, err
	}
	n, err = proto.EncodeGetDone(sendBuf2, done)
	if err != nil {
		rdma.FreeBuffer(sendBuf2)
		return 0, err
	}
	s.send(sendBuf2[:n])
	return time.Since(start), nil
}

func hello(conn *rdma.Conn) error {
	recvBuf, err := rdma.AllocBuffer(ctrlBufSize)
	if err != nil {
		return err
	}

	if err := conn.PostRecv(recvBuf); err != nil {
		_ = conn.DeregisterBuffer(recvBuf)
		return err
	}

	sendBuf, err := rdma.AllocBuffer(ctrlBufSize)
	if err != nil {
		_ = conn.DeregisterBuffer(recvBuf)
		rdma.FreeBuffer(recvBuf)
		return err
	}

	hdr := proto.MsgHdr{
		Magic:   proto.Magic,
		Version: proto.Version,
		Type:    proto.MsgHello,
		ReqID:   1,
		BodyLen: 0,
	}
	if err := proto.WriteHeader(sendBuf[:proto.HeaderLen], hdr); err != nil {
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

	recvDone := false
	sendDone := false
	deadline := time.Now().Add(5 * time.Second)
	for !recvDone || !sendDone {
		op, buf, n, err := conn.Poll(200)
		if err != nil {
			return err
		}
		switch op {
		case rdma.OpRecv:
			if n < proto.HeaderLen {
				return fmt.Errorf("unexpected completion")
			}
			ack, err := proto.ReadHeader(buf[:proto.HeaderLen])
			if err != nil {
				return err
			}
			if ack.Magic != proto.Magic || ack.Version != proto.Version || ack.Type != proto.MsgHelloAck {
				return fmt.Errorf("invalid hello ack")
			}
			recvDone = true
		case rdma.OpSend:
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
