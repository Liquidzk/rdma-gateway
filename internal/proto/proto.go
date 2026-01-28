package proto

import (
	"encoding/binary"
	"fmt"
)

const (
	Magic   uint32 = 0x52444d41 // "RDMA"
	Version uint16 = 1
)

const HeaderLen = 16

type MsgType uint16

const (
	MsgHello     MsgType = 1
	MsgHelloAck  MsgType = 2
	MsgPutReq    MsgType = 10
	MsgPutLease  MsgType = 11
	MsgPutCommit MsgType = 12
	MsgPutDone   MsgType = 13
	MsgGetReq    MsgType = 20
	MsgGetReady  MsgType = 21
	MsgGetDone   MsgType = 22
)

type MsgHdr struct {
	Magic   uint32
	Version uint16
	Type    MsgType
	ReqID   uint32
	BodyLen uint32
}

func WriteHeader(buf []byte, hdr MsgHdr) error {
	if len(buf) < HeaderLen {
		return fmt.Errorf("buffer too small for header")
	}
	binary.LittleEndian.PutUint32(buf[0:4], hdr.Magic)
	binary.LittleEndian.PutUint16(buf[4:6], hdr.Version)
	binary.LittleEndian.PutUint16(buf[6:8], uint16(hdr.Type))
	binary.LittleEndian.PutUint32(buf[8:12], hdr.ReqID)
	binary.LittleEndian.PutUint32(buf[12:16], hdr.BodyLen)
	return nil
}

func ReadHeader(buf []byte) (MsgHdr, error) {
	if len(buf) < HeaderLen {
		return MsgHdr{}, fmt.Errorf("buffer too small for header")
	}
	h := MsgHdr{
		Magic:   binary.LittleEndian.Uint32(buf[0:4]),
		Version: binary.LittleEndian.Uint16(buf[4:6]),
		Type:    MsgType(binary.LittleEndian.Uint16(buf[6:8])),
		ReqID:   binary.LittleEndian.Uint32(buf[8:12]),
		BodyLen: binary.LittleEndian.Uint32(buf[12:16]),
	}
	return h, nil
}

type PutReq struct {
	ReqID  uint32
	Bucket string
	Key    string
	ObjLen uint32
}

type PutLease struct {
	ReqID  uint32
	Token  uint32
	Addr   uint64
	RKey   uint32
	MaxLen uint32
}

type PutCommit struct {
	ReqID   uint32
	Token   uint32
	DataLen uint32
	Bucket  string
	Key     string
}

type PutDone struct {
	ReqID  uint32
	Status uint16
}

func EncodePutReq(buf []byte, req PutReq) (int, error) {
	bucketLen := len(req.Bucket)
	keyLen := len(req.Key)
	bodyLen := 4 + 2 + 2 + bucketLen + keyLen
	if len(buf) < HeaderLen+bodyLen {
		return 0, fmt.Errorf("buffer too small")
	}
	h := MsgHdr{
		Magic:   Magic,
		Version: Version,
		Type:    MsgPutReq,
		ReqID:   req.ReqID,
		BodyLen: uint32(bodyLen),
	}
	if err := WriteHeader(buf, h); err != nil {
		return 0, err
	}
	off := HeaderLen
	binary.LittleEndian.PutUint32(buf[off:off+4], req.ObjLen)
	off += 4
	binary.LittleEndian.PutUint16(buf[off:off+2], uint16(bucketLen))
	off += 2
	binary.LittleEndian.PutUint16(buf[off:off+2], uint16(keyLen))
	off += 2
	copy(buf[off:off+bucketLen], req.Bucket)
	off += bucketLen
	copy(buf[off:off+keyLen], req.Key)
	off += keyLen
	return off, nil
}

func DecodePutReq(buf []byte) (PutReq, error) {
	h, err := ReadHeader(buf)
	if err != nil {
		return PutReq{}, err
	}
	if h.Type != MsgPutReq {
		return PutReq{}, fmt.Errorf("unexpected msg type")
	}
	if len(buf) < HeaderLen+int(h.BodyLen) {
		return PutReq{}, fmt.Errorf("buffer too small for body")
	}
	off := HeaderLen
	objLen := binary.LittleEndian.Uint32(buf[off : off+4])
	off += 4
	bucketLen := int(binary.LittleEndian.Uint16(buf[off : off+2]))
	off += 2
	keyLen := int(binary.LittleEndian.Uint16(buf[off : off+2]))
	off += 2
	if bucketLen+keyLen > int(h.BodyLen)-8 {
		return PutReq{}, fmt.Errorf("invalid lengths")
	}
	bucket := string(buf[off : off+bucketLen])
	off += bucketLen
	key := string(buf[off : off+keyLen])
	return PutReq{ReqID: h.ReqID, Bucket: bucket, Key: key, ObjLen: objLen}, nil
}

func EncodePutLease(buf []byte, lease PutLease) (int, error) {
	bodyLen := 4 + 8 + 4 + 4
	if len(buf) < HeaderLen+bodyLen {
		return 0, fmt.Errorf("buffer too small")
	}
	h := MsgHdr{
		Magic:   Magic,
		Version: Version,
		Type:    MsgPutLease,
		ReqID:   lease.ReqID,
		BodyLen: uint32(bodyLen),
	}
	if err := WriteHeader(buf, h); err != nil {
		return 0, err
	}
	off := HeaderLen
	binary.LittleEndian.PutUint32(buf[off:off+4], lease.Token)
	off += 4
	binary.LittleEndian.PutUint64(buf[off:off+8], lease.Addr)
	off += 8
	binary.LittleEndian.PutUint32(buf[off:off+4], lease.RKey)
	off += 4
	binary.LittleEndian.PutUint32(buf[off:off+4], lease.MaxLen)
	off += 4
	return off, nil
}

func DecodePutLease(buf []byte) (PutLease, error) {
	h, err := ReadHeader(buf)
	if err != nil {
		return PutLease{}, err
	}
	if h.Type != MsgPutLease {
		return PutLease{}, fmt.Errorf("unexpected msg type")
	}
	if len(buf) < HeaderLen+int(h.BodyLen) {
		return PutLease{}, fmt.Errorf("buffer too small for body")
	}
	off := HeaderLen
	token := binary.LittleEndian.Uint32(buf[off : off+4])
	off += 4
	addr := binary.LittleEndian.Uint64(buf[off : off+8])
	off += 8
	rkey := binary.LittleEndian.Uint32(buf[off : off+4])
	off += 4
	maxLen := binary.LittleEndian.Uint32(buf[off : off+4])
	return PutLease{ReqID: h.ReqID, Token: token, Addr: addr, RKey: rkey, MaxLen: maxLen}, nil
}

func EncodePutCommit(buf []byte, commit PutCommit) (int, error) {
	bucketLen := len(commit.Bucket)
	keyLen := len(commit.Key)
	bodyLen := 4 + 4 + 2 + 2 + bucketLen + keyLen
	if len(buf) < HeaderLen+bodyLen {
		return 0, fmt.Errorf("buffer too small")
	}
	h := MsgHdr{
		Magic:   Magic,
		Version: Version,
		Type:    MsgPutCommit,
		ReqID:   commit.ReqID,
		BodyLen: uint32(bodyLen),
	}
	if err := WriteHeader(buf, h); err != nil {
		return 0, err
	}
	off := HeaderLen
	binary.LittleEndian.PutUint32(buf[off:off+4], commit.Token)
	off += 4
	binary.LittleEndian.PutUint32(buf[off:off+4], commit.DataLen)
	off += 4
	binary.LittleEndian.PutUint16(buf[off:off+2], uint16(bucketLen))
	off += 2
	binary.LittleEndian.PutUint16(buf[off:off+2], uint16(keyLen))
	off += 2
	copy(buf[off:off+bucketLen], commit.Bucket)
	off += bucketLen
	copy(buf[off:off+keyLen], commit.Key)
	off += keyLen
	return off, nil
}

func DecodePutCommit(buf []byte) (PutCommit, error) {
	h, err := ReadHeader(buf)
	if err != nil {
		return PutCommit{}, err
	}
	if h.Type != MsgPutCommit {
		return PutCommit{}, fmt.Errorf("unexpected msg type")
	}
	if len(buf) < HeaderLen+int(h.BodyLen) {
		return PutCommit{}, fmt.Errorf("buffer too small for body")
	}
	off := HeaderLen
	token := binary.LittleEndian.Uint32(buf[off : off+4])
	off += 4
	dataLen := binary.LittleEndian.Uint32(buf[off : off+4])
	off += 4
	bucketLen := int(binary.LittleEndian.Uint16(buf[off : off+2]))
	off += 2
	keyLen := int(binary.LittleEndian.Uint16(buf[off : off+2]))
	off += 2
	if bucketLen+keyLen > int(h.BodyLen)-12 {
		return PutCommit{}, fmt.Errorf("invalid lengths")
	}
	bucket := string(buf[off : off+bucketLen])
	off += bucketLen
	key := string(buf[off : off+keyLen])
	return PutCommit{ReqID: h.ReqID, Token: token, DataLen: dataLen, Bucket: bucket, Key: key}, nil
}

func EncodePutDone(buf []byte, done PutDone) (int, error) {
	bodyLen := 2
	if len(buf) < HeaderLen+bodyLen {
		return 0, fmt.Errorf("buffer too small")
	}
	h := MsgHdr{
		Magic:   Magic,
		Version: Version,
		Type:    MsgPutDone,
		ReqID:   done.ReqID,
		BodyLen: uint32(bodyLen),
	}
	if err := WriteHeader(buf, h); err != nil {
		return 0, err
	}
	off := HeaderLen
	binary.LittleEndian.PutUint16(buf[off:off+2], done.Status)
	off += 2
	return off, nil
}

func DecodePutDone(buf []byte) (PutDone, error) {
	h, err := ReadHeader(buf)
	if err != nil {
		return PutDone{}, err
	}
	if h.Type != MsgPutDone {
		return PutDone{}, fmt.Errorf("unexpected msg type")
	}
	if len(buf) < HeaderLen+int(h.BodyLen) {
		return PutDone{}, fmt.Errorf("buffer too small for body")
	}
	status := binary.LittleEndian.Uint16(buf[HeaderLen : HeaderLen+2])
	return PutDone{ReqID: h.ReqID, Status: status}, nil
}

type GetReq struct {
	ReqID  uint32
	Bucket string
	Key    string
}

type GetReady struct {
	ReqID   uint32
	Token   uint32
	Addr    uint64
	RKey    uint32
	DataLen uint32
}

type GetDone struct {
	ReqID uint32
	Token uint32
}

func EncodeGetReq(buf []byte, req GetReq) (int, error) {
	bucketLen := len(req.Bucket)
	keyLen := len(req.Key)
	bodyLen := 2 + 2 + bucketLen + keyLen
	if len(buf) < HeaderLen+bodyLen {
		return 0, fmt.Errorf("buffer too small")
	}
	h := MsgHdr{
		Magic:   Magic,
		Version: Version,
		Type:    MsgGetReq,
		ReqID:   req.ReqID,
		BodyLen: uint32(bodyLen),
	}
	if err := WriteHeader(buf, h); err != nil {
		return 0, err
	}
	off := HeaderLen
	binary.LittleEndian.PutUint16(buf[off:off+2], uint16(bucketLen))
	off += 2
	binary.LittleEndian.PutUint16(buf[off:off+2], uint16(keyLen))
	off += 2
	copy(buf[off:off+bucketLen], req.Bucket)
	off += bucketLen
	copy(buf[off:off+keyLen], req.Key)
	off += keyLen
	return off, nil
}

func DecodeGetReq(buf []byte) (GetReq, error) {
	h, err := ReadHeader(buf)
	if err != nil {
		return GetReq{}, err
	}
	if h.Type != MsgGetReq {
		return GetReq{}, fmt.Errorf("unexpected msg type")
	}
	if len(buf) < HeaderLen+int(h.BodyLen) {
		return GetReq{}, fmt.Errorf("buffer too small for body")
	}
	off := HeaderLen
	bucketLen := int(binary.LittleEndian.Uint16(buf[off : off+2]))
	off += 2
	keyLen := int(binary.LittleEndian.Uint16(buf[off : off+2]))
	off += 2
	if bucketLen+keyLen > int(h.BodyLen)-4 {
		return GetReq{}, fmt.Errorf("invalid lengths")
	}
	bucket := string(buf[off : off+bucketLen])
	off += bucketLen
	key := string(buf[off : off+keyLen])
	return GetReq{ReqID: h.ReqID, Bucket: bucket, Key: key}, nil
}

func EncodeGetReady(buf []byte, ready GetReady) (int, error) {
	bodyLen := 4 + 8 + 4 + 4
	if len(buf) < HeaderLen+bodyLen {
		return 0, fmt.Errorf("buffer too small")
	}
	h := MsgHdr{
		Magic:   Magic,
		Version: Version,
		Type:    MsgGetReady,
		ReqID:   ready.ReqID,
		BodyLen: uint32(bodyLen),
	}
	if err := WriteHeader(buf, h); err != nil {
		return 0, err
	}
	off := HeaderLen
	binary.LittleEndian.PutUint32(buf[off:off+4], ready.Token)
	off += 4
	binary.LittleEndian.PutUint64(buf[off:off+8], ready.Addr)
	off += 8
	binary.LittleEndian.PutUint32(buf[off:off+4], ready.RKey)
	off += 4
	binary.LittleEndian.PutUint32(buf[off:off+4], ready.DataLen)
	off += 4
	return off, nil
}

func DecodeGetReady(buf []byte) (GetReady, error) {
	h, err := ReadHeader(buf)
	if err != nil {
		return GetReady{}, err
	}
	if h.Type != MsgGetReady {
		return GetReady{}, fmt.Errorf("unexpected msg type")
	}
	if len(buf) < HeaderLen+int(h.BodyLen) {
		return GetReady{}, fmt.Errorf("buffer too small for body")
	}
	off := HeaderLen
	token := binary.LittleEndian.Uint32(buf[off : off+4])
	off += 4
	addr := binary.LittleEndian.Uint64(buf[off : off+8])
	off += 8
	rkey := binary.LittleEndian.Uint32(buf[off : off+4])
	off += 4
	dataLen := binary.LittleEndian.Uint32(buf[off : off+4])
	return GetReady{ReqID: h.ReqID, Token: token, Addr: addr, RKey: rkey, DataLen: dataLen}, nil
}

func EncodeGetDone(buf []byte, done GetDone) (int, error) {
	bodyLen := 4
	if len(buf) < HeaderLen+bodyLen {
		return 0, fmt.Errorf("buffer too small")
	}
	h := MsgHdr{
		Magic:   Magic,
		Version: Version,
		Type:    MsgGetDone,
		ReqID:   done.ReqID,
		BodyLen: uint32(bodyLen),
	}
	if err := WriteHeader(buf, h); err != nil {
		return 0, err
	}
	binary.LittleEndian.PutUint32(buf[HeaderLen:HeaderLen+4], done.Token)
	return HeaderLen + bodyLen, nil
}

func DecodeGetDone(buf []byte) (GetDone, error) {
	h, err := ReadHeader(buf)
	if err != nil {
		return GetDone{}, err
	}
	if h.Type != MsgGetDone {
		return GetDone{}, fmt.Errorf("unexpected msg type")
	}
	if len(buf) < HeaderLen+int(h.BodyLen) {
		return GetDone{}, fmt.Errorf("buffer too small for body")
	}
	token := binary.LittleEndian.Uint32(buf[HeaderLen : HeaderLen+4])
	return GetDone{ReqID: h.ReqID, Token: token}, nil
}
