package jig

import "sync"

type StringSeq struct {
	mu sync.Mutex
	m  map[string]uint64
}

func MakeStringSeq() *StringSeq {
	return &StringSeq{m: make(map[string]uint64)}
}

func (s *StringSeq) ID(str string) uint64 {
	s.mu.Lock()
	if n, ok := s.m[str]; ok {
		s.mu.Unlock()
		return n
	}
	n := uint64(len(s.m) + 1)
	s.m[str] = n
	s.mu.Unlock()
	return n
}

func (s *StringSeq) Max() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return uint64(len(s.m) + 1)
}
