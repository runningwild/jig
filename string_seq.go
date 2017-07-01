package jig

import (
	"bytes"
	"hash/crc64"
	"sync"
)

type StringSeq struct {
	mu       sync.Mutex
	m        [][]*entry // map from masked crc to slice of entries whose crcs go into this bucket
	pow      uint
	count    uint64
	maxSlice int

	tab *crc64.Table
}

type entry struct {
	crc uint64
	id  uint64
	b   []byte
}

func MakeStringSeq() *StringSeq {
	var pow uint = 10
	return &StringSeq{
		m:   make([][]*entry, 1<<pow),
		pow: pow,
		tab: crc64.MakeTable(crc64.ECMA),
	}
}

// s.mu must be held
func (s *StringSeq) double() {
	s.pow += 2
	mask := uint64(1<<s.pow - 1)
	nm := make([][]*entry, 1<<s.pow)
	for _, entries := range s.m {
		for _, e := range entries {
			index := e.crc & mask
			nm[index] = append(nm[index], e)
		}
	}
	s.m = nm

	s.maxSlice = 0
	for i := range s.m {
		if len(s.m[i]) > s.maxSlice {
			s.maxSlice = len(s.m[i])
		}
	}
}

func find(entries []*entry, index int, crc uint64, b []byte) (uint64, bool) {
	for _, e := range entries {
		if e.crc != crc {
			continue
		}
		if bytes.Equal(b, e.b) {
			return e.id, true
		}
	}
	return 0, false
}

func (s *StringSeq) ID(b []byte) uint64 {
	crc := crc64.Checksum(b, s.tab)
	s.mu.Lock()
	pow := s.pow
	index := int(crc & uint64(1<<pow-1))
	entries := s.m[index]

	// No need to hold the lock while we're looking for this entry.
	s.mu.Unlock()
	if n, ok := find(entries, index, crc, b); ok {
		return n
	}

	// We'll need to add this entry, so copy the data since we don't want the caller to have to
	// worry about that.
	cb := make([]byte, len(b))
	copy(cb, b)

	// If things changed while we weren't holding the lock we'll need to search for this entry again
	// to make sure we don't accidentally write two entries with the same data.
	s.mu.Lock()
	if s.pow != pow || len(s.m[index]) != len(entries) {
		index = int(crc & uint64(1<<s.pow-1))
		if s.pow == pow {
			// If we didn't resize we can just resume searching from where we left off.
			entries = s.m[index][len(entries):]
		} else {
			entries = s.m[index]
		}
		if n, ok := find(entries, index, crc, b); ok {
			s.mu.Unlock()
			return n
		}
	}
	s.count++
	n := s.count
	s.m[index] = append(s.m[index], &entry{crc: crc, b: cb, id: n})
	if len(s.m[index]) > s.maxSlice {
		s.maxSlice = len(s.m[index])
	}
	// Double our table when we have to go through more than 50 at a time.  This number was reached
	// experimentally, the optimal threshold probably varies wildly across platforms.
	if s.maxSlice > 30 {
		s.double()
	}
	s.mu.Unlock()
	return n
}

func (s *StringSeq) Max() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count + 1
	// return uint64(len(s.m) + 1)
}
