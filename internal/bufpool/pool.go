package bufpool

import (
	"fmt"
	"sync"

	"rdma_gateway_go/internal/rdma"
)

type Lease struct {
	Token  uint32
	Addr   uint64
	RKey   uint32
	MaxLen uint64
}

type Pool struct {
	mu        sync.Mutex
	mr        *rdma.MR
	blockSize uint64
	free      []bool
}

func New(mr *rdma.MR, blockSize uint64) (*Pool, error) {
	if mr == nil || mr.Size == 0 {
		return nil, fmt.Errorf("invalid MR")
	}
	if blockSize == 0 || mr.Size < blockSize {
		return nil, fmt.Errorf("invalid block size")
	}
	blocks := int(mr.Size / blockSize)
	free := make([]bool, blocks)
	for i := range free {
		free[i] = true
	}
	return &Pool{mr: mr, blockSize: blockSize, free: free}, nil
}

func (p *Pool) Acquire() (Lease, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i, ok := range p.free {
		if ok {
			p.free[i] = false
			addr := p.mr.Base + uint64(i)*p.blockSize
			return Lease{
				Token:  uint32(i),
				Addr:   addr,
				RKey:   p.mr.RKey,
				MaxLen: p.blockSize,
			}, true
		}
	}
	return Lease{}, false
}

func (p *Pool) Release(token uint32) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	idx := int(token)
	if idx < 0 || idx >= len(p.free) {
		return fmt.Errorf("invalid token")
	}
	if p.free[idx] {
		return fmt.Errorf("double release")
	}
	p.free[idx] = true
	return nil
}
