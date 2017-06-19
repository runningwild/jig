package jig

import (
	"flag"
	"fmt"
	"io/ioutil"
	"sort"
	"strings"
)

var (
	filea = flag.String("a", "", "file a")
	fileb = flag.String("b", "", "file b")
)

func main() {
	flag.Parse()
	dataA, err := ioutil.ReadFile(*filea)
	if err != nil {
		panic(err)
	}
	dataB, err := ioutil.ReadFile(*fileb)
	if err != nil {
		panic(err)
	}
	linesA := strings.Split(string(dataA), "\n")
	linesB := strings.Split(string(dataB), "\n")
	v, max := stringsToUint64s([][]string{linesA, linesB})
	mapping := make(map[uint64]uint64)
	substrs := make(map[uint64]*substr)
	for {
		fmt.Printf("---------------------------------------------\n")
		fmt.Printf("lengths: %d %d\n", len(v[0]), len(v[1]))
		ai, bi, length := LCS(v[0], v[1])
		if length <= 0 {
			break
		}
		fmt.Printf("Pos(%d %d), Length %d\n", ai, bi, length)
		v[0][ai] = max
		v[1][bi] = max + 1
		substrs[max] = &substr{start: ai, length: length}
		substrs[max+1] = &substr{start: bi, length: length}
		mapping[max] = max + 1
		max += 2

		for i := ai + 1; i+length-1 < len(v[0]); i++ {
			v[0][i] = v[0][i+length-1]
		}
		v[0] = v[0][0 : len(v[0])-length+1]
		for i := range v[0] {
			fmt.Printf("%d: %d\n", i, v[0][i])
		}

		for i := bi + 1; i+length-1 < len(v[1]); i++ {
			v[1][i] = v[1][i+length-1]
		}
		v[1] = v[1][0 : len(v[1])-length+1]
	}
	// liness := [][]string{linesA, linesB}
	for i := range v {
		add := 0
		for j := range v[i] {
			if s, ok := substrs[v[i][j]]; ok {
				s.start += add
				add += s.length
				fmt.Printf("Lines %d %d: ", i, s.start)
				// fmt.Printf("%s\n", liness[i][s.start])
			} else {
				add++
			}
		}
	}
	fmt.Printf("%v\n%v\n%v\n", v[0], v[1], mapping)
	fmt.Printf("A:\n")
	for _, v := range v[0] {
		fmt.Printf("%d -> %v\n", v, substrs[v])
	}
	fmt.Printf("B:\n")
	for _, v := range v[1] {
		fmt.Printf("%d -> %v\n", v, substrs[v])
	}
}

type substr struct {
	start, length int
}

func stringsToUint64s(v [][]string) ([][]uint64, uint64) {
	var ret [][]uint64
	var count uint64
	unique := make(map[string]uint64)
	for i := range v {
		var cur []uint64
		for _, s := range v[i] {
			if _, ok := unique[s]; !ok {
				unique[s] = count
				count++
			}
			cur = append(cur, unique[s])
		}
		ret = append(ret, cur)
	}
	return ret, count
}

// LCS finds the Longest Common Substring between two 'strings' of uint64.  ai and bi are the start
// of the substring in each of the input arrays, and length is the length of the common substring.
// If there is no common substring, ai and bi will be -1 and length will be 0.
func LCS(a, b []uint64) (ai, bi, length int) {
	// Construct a single list containing all of the suffixes for each string.
	var sufs []suf
	for i := range a {
		sufs = append(sufs, suf{suffix: a[i:], start: i, source: 0})
	}
	for i := range b {
		sufs = append(sufs, suf{suffix: b[i:], start: i, source: 1})
	}

	// Sort the suffixes lexicographically
	sort.Slice(sufs, func(i, j int) bool {
		a := sufs[i].suffix
		b := sufs[j].suffix
		for k := 0; k < len(a) && k < len(b); k++ {
			if a[k] == b[k] {
				continue
			}
			return a[k] < b[k]
		}
		return false
	})

	// Find adjacent pairs in the array such that the two elements don't both come from the same
	// string.  Such pairs indicate a common substring, and all common substrings will be
	// represented in this way, so the longest one is the LCS.
	length = 0
	index := -1
	for i := 0; i < len(sufs)-1; i++ {
		if sufs[i].source == sufs[i+1].source {
			continue
		}
		var prefix int
		a := sufs[i].suffix
		b := sufs[i+1].suffix
		for prefix = 0; prefix < len(a) && prefix < len(b); prefix++ {
			if a[prefix] != b[prefix] {
				break
			}
		}
		if prefix > length {
			length = prefix
			index = i
		}
	}
	if index == -1 {
		return -1, -1, 0
	}
	ai = sufs[index].start
	bi = sufs[index+1].start
	if sufs[index].source == 1 {
		ai, bi = bi, ai
	}
	return
}

type suf struct {
	suffix []uint64
	start  int
	source int
}
