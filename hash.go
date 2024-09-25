package consistentHash

import (
	"sync"
)

const (
	offset32 = 2166136261
	prime32  = 16777619
)

type (
	HashFunc func(data []byte) uint32
	fnv1a    uint32
)

func newFnv1a() *fnv1a {
	var h fnv1a = offset32
	return &h
}
func (s *fnv1a) Write(data []byte) (int, error) {
	h := *s
	for _, c := range data {
		h ^= fnv1a(c)
		h *= prime32
	}
	*s = h
	return len(data), nil
}
func (s *fnv1a) Sum(in []byte) []byte {
	v := uint32(*s)
	return append(in,
		byte(v>>24),
		byte(v>>16),
		byte(v>>8),
		byte(v),
	)
}
func (s *fnv1a) Sum32() uint32 {
	return uint32(*s)
}
func (s *fnv1a) Reset()         { *s = offset32 }
func (s *fnv1a) Size() int      { return 4 }
func (s *fnv1a) BlockSize() int { return 1 }

var (
	hPool = sync.Pool{
		New: func() any {
			return newFnv1a()
		},
	}
)

func Fnv1a(data []byte) uint32 {
	h := hPool.Get().(*fnv1a)
	_, _ = h.Write(data)
	v := h.Sum32()
	h.Reset()
	hPool.Put(h)
	return v
}
