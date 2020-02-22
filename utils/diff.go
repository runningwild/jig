package utils

import (
	"bytes"
	"fmt"
	"sort"
)

func Diff(a, b [][]byte) []DiffBlock {
	css := GetCommonSubstrings(a, b)

	// Sort by A index so we can find deletions
	sort.Slice(css, func(i, j int) bool {
		return css[i].Ai < css[j].Ai
	})
	fmt.Printf("%d common substrings\n", len(css))
	for _, cs := range css {
		fmt.Printf("%v\n", cs)
	}
	var delA [][2]int
	if css[0].Ai > 0 {
		delA = append(delA, [2]int{0, css[0].Length})
	}
	for i := 1; i < len(css); i++ {
		prev := css[i-1].Ai + css[i-1].Length
		if css[i].Ai > prev {
			delA = append(delA, [2]int{prev, css[i].Ai - prev})
		}
	}
	if end := css[len(css)-1].Ai + css[len(css)-1].Length; end < len(a) {
		delA = append(delA, [2]int{end, len(a) - end})
	}
	delMap := make(map[int]int)
	for i := range delA {
		delMap[delA[i][0]] = delA[i][1]
	}

	moves := MinWeightedMoves(css)
	fmt.Printf("A:\n%s\n", bytes.Join(a, []byte{'\n'}))
	fmt.Printf("%v\n", moves)
	sort.Slice(css, func(i, j int) bool {
		return css[i].Bi < css[j].Bi
	})
	rblock := make(map[int]int)
	for i := range moves {
		rblock[moves[i][1]] = i
	}
	moveIndex := 0
	bi := 0
	var dbs []DiffBlock
	for i := range css {
		for moveIndex < len(moves) && i > moves[moveIndex][0] {
			fmt.Printf("<< Block %d moved from here\n", moveIndex)
			dbs = append(dbs, ExportBlock{})
			moveIndex++
		}
		if bi < css[i].Bi {
			dbs = append(dbs, InsertionBlock{bi, css[i].Bi - bi})
		}
		for bi < css[i].Bi {
			fmt.Printf("+ %s\n", b[bi])
			bi++
		}
		if block, ok := rblock[i]; ok {
			fmt.Printf(">>> Block %d\n", block)
			dbs = append(dbs, ImportBlock{block, css[i].Bi, css[i].Length})
		} else {
			dbs = append(dbs, CommonBlock{css[i].Bi, css[i].Length})
		}
		for _, line := range b[css[i].Bi : css[i].Bi+css[i].Length] {
			fmt.Printf("  %s\n", line)
		}
		if _, ok := rblock[i]; ok {
			fmt.Printf(">>>\n")
		}
		aEnd := css[i].Ai + css[i].Length
		if delLen := delMap[aEnd]; delLen > 0 {
			dbs = append(dbs, DeletionBlock{aEnd, delLen})
			for i := aEnd; i < aEnd+delLen; i++ {
				fmt.Printf("- %s\n", a[i])
			}
		}
		bi += css[i].Length
	}
	return dbs
}

func GetCommonSubstrings(chunks0, chunks1 [][]byte) []CommonSubstring {
	// Converts lists of chunks to lists of uint64 so that we can run them through LCS2.
	var vs [][]uint64
	m := make(map[string]uint64)
	for _, chunks := range [][][]byte{chunks0, chunks1} {
		var v []uint64
		for _, chunk := range chunks {
			s := string(chunk)
			n, ok := m[s]
			if !ok {
				n = uint64(len(m) + 1)
				m[s] = n
			}
			v = append(v, n)
		}
		vs = append(vs, v)
	}
	fmt.Printf("A: %v\n", vs[0])
	fmt.Printf("B: %v\n", vs[1])
	return LCS2(vs[0], vs[1])
}

type DiffBlock interface {
}

type CommonBlock struct {
	Bi, Length int
}

type InsertionBlock struct {
	Bi, Length int
}

type DeletionBlock struct {
	Ai, Length int
}

type ExportBlock struct {
}

type ImportBlock struct {
	BlockID, Bi, Length int
}
