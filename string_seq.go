package jig

import (
	"bytes"
	crand "crypto/rand"
	"hash/crc32"
	"math/rand"
	"sync"
	"time"
)

type StringSeq struct {
	mu       sync.Mutex
	m        [][]*entry // map from masked crc to slice of entries whose crcs go into this bucket
	pow      uint
	count    uint64
	maxSlice int
	salt     []byte

	tab *crc32.Table
}

type entry struct {
	crc uint32
	id  uint64
	b   []byte
}

func MakeStringSeq() *StringSeq {
	var pow uint = 10
	var b [4]byte
	n, err := crand.Read(b[:])
	if n != 4 || err != nil {
		rng := rand.New(rand.NewSource(int64(time.Now().UnixNano())))
		b[0] = byte(rng.Intn(256))
		b[1] = byte(rng.Intn(256))
		b[2] = byte(rng.Intn(256))
		b[3] = byte(rng.Intn(256))
	}
	return &StringSeq{
		m:    make([][]*entry, 1<<pow),
		pow:  pow,
		tab:  crc32.MakeTable(crc32.Castagnoli),
		salt: b[:],
	}
}

// s.mu must be held
func (s *StringSeq) double() {
	s.pow += 2
	mask := uint32(1<<s.pow - 1)
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

func find(entries []*entry, index int, crc uint32, b []byte) (uint64, bool) {
	for _, e := range entries {
		if e.crc == crc && bytes.Equal(b, e.b) {
			return e.id, true
		}
	}
	return 0, false
}

func (s *StringSeq) ID(b []byte) uint64 {
	crc := crc32.Checksum(b, s.tab)
	crc = crc32.Update(crc, s.tab, s.salt)

	s.mu.Lock()
	pow := s.pow
	index := int(crc & uint32(1<<pow-1))
	entries := s.m[index]
	s.mu.Unlock()

	// No need to hold the lock while we're looking for this entry.
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
		index = int(crc & uint32(1<<s.pow-1))
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
	// Double our table when we have to go through more than 30 at a time.  This number was reached
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
}
