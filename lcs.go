package jig

import (
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"time"
)

var (
	filea = flag.String("a", "", "file a")
	fileb = flag.String("b", "", "file b")
)

func stringToUint64s(s string) []uint64 {
	var v []uint64
	for _, r := range s {
		v = append(v, uint64(r))
	}
	return v
}

func main2() {
	in := stringToUint64s("abcdefghij")
	skew := InducedSuffixArray(in)
	dumb := DumbSuffixArray(in)
	fmt.Printf("Skew: %v\n", skew)
	fmt.Printf("Dumb: %v\n", dumb)
}

func InducedSuffixArray(s []uint64) []int {
	m := make(map[uint64]uint64)
	for _, v := range s {
		m[v]++
	}
	var keys []uint64
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	m = make(map[uint64]uint64)
	for i, key := range keys {
		m[key] = uint64(i + 1)
	}
	var T []uint64
	for _, v := range s {
		T = append(T, m[v])
	}
	return inducedSuffixArrayHelper(T)
}

// inducedSuffixArrayHelper requires that no element in T is 0. (I thikn?)
// elements in s should be in the range [1, len(s)].
func inducedSuffixArrayHelper(T []uint64) []int {
	if len(T) == 0 {
		return []int{}
	}
	t := make([]plusOrMinusType, len(T)) // true indicates - type
	t[len(t)-1] = minusType
	σ := T[len(T)-1]
	for i := len(T) - 2; i >= 0; i-- {
		if T[i] > σ {
			σ = T[i]
		}
		if T[i] != T[i+1] {
			if T[i] < T[i+1] {
				t[i] = plusType
			} else {
				t[i] = minusType
			}
		} else {
			t[i] = t[i+1]
		}
	}
	var P1 []int
	prev := t[0]
	for i := 1; i < len(t); i++ {
		if t[i] == plusType && prev == minusType {
			P1 = append(P1, i)
		}
		prev = t[i]
	}

	// TODO: this sort needs to be done faster
	sort.Slice(P1, func(i, j int) bool {
		i, j = P1[i], P1[j]
		for k := 0; k+i < len(T) && k+j < len(T); k++ {
			if T[k+i] != T[k+j] {
				return T[k+i] < T[k+j]
			}
		}
		return i > j
	})
	Cstar := make([]Dequeue, len(T)+1)
	for _, v := range P1 {
		Cstar[int(T[v])].PushBack(v)
	}

	Cminus := InduceMinusSuffixes(T, Cstar, int(σ)+1)
	Cplus := InducePlusSuffixes(T, Cminus, int(σ)+1)
	SA := make([]int, 0, len(T))
	for i := range Cminus {
		for j := 0; j < Cminus[i].Len(); j++ {
			SA = append(SA, Cminus[i].At(j))
		}
		for j := 0; j < Cplus[i].Len(); j++ {
			SA = append(SA, Cplus[i].At(j))
		}
	}
	return SA
}

type plusOrMinusType bool

const plusType plusOrMinusType = true
const minusType plusOrMinusType = false

func InduceMinusSuffixes(T []uint64, Cstar []Dequeue, σ int) []Dequeue {
	Cminus := make([]Dequeue, σ)
	Cminus[int(T[len(T)-1])].PushBack(len(T) - 1)
	for a := 1; a < σ; a++ {
		var c Dequeue
		for Cminus[a].Len() > 0 {
			i := Cminus[a].PopFront()
			c.PushBack(i)
			if i > 0 && T[i-1] >= uint64(a) {
				Cminus[int(T[i-1])].PushBack(i - 1)
			}
		}
		Cminus[a] = c
		for j := 0; j < Cstar[a].Len(); j++ {
			i := Cstar[a].At(j)
			Cminus[int(T[i-1])].PushBack(i - 1)
		}
	}

	return Cminus
}

func InducePlusSuffixes(T []uint64, Cminus []Dequeue, σ int) []Dequeue {
	Cplus := make([]Dequeue, σ)
	for a := σ - 1; a >= 1; a-- {
		var c Dequeue
		for Cplus[a].Len() > 0 {
			i := Cplus[a].PopBack()
			c.PushFront(i)
			if i > 0 && T[i-1] <= uint64(a) {
				Cplus[int(T[i-1])].PushFront(i - 1)
			}
		}
		Cplus[a] = c
		for j := Cminus[a].Len() - 1; j >= 0; j-- {
			i := Cminus[a].At(j)
			if i > 0 && T[i-1] < uint64(a) {
				Cplus[int(T[i-1])].PushFront(i - 1)
			}
		}
	}
	return Cplus
}

func boolsAsInts(b []plusOrMinusType) string {
	var v string
	for _, t := range b {
		if t == plusType {
			v += "+"
		} else {
			v += "-"
		}
	}
	return v
}

var readersToLinesBufferSize int = 100000

func readersToLines(fa, fb io.Reader) ([][]uint64, uint64, error) {
	var mu sync.RWMutex
	m := make(map[string]uint64)
	var count uint64 = 1
	var finalErr error

	// update finds the value that corresponds to s and appends that to v.  If v is not a value that
	// has been seen before, a new value will be added into the mapping, m, and that value will be
	// appended to v.
	update := func(s string, v *[]uint64) {
		mu.Lock()
		n, ok := m[s]
		if !ok {
			m[s] = count
			n = count
			count++
		}
		mu.Unlock()
		*v = append(*v, n)
	}

	var wg sync.WaitGroup
	readers := []io.Reader{fa, fb}
	vs := make([][]uint64, 2)
	for i := range readers {
		wg.Add(1)
		// Launch one go-routine per reader.  The go-routine will read lines from the reader,
		// convert those into uint64s, then append those values onto *v.
		go func(f io.Reader, v *[]uint64) {
			defer wg.Done()
			buf := make([]byte, readersToLinesBufferSize)
			var cur []byte
			for {
				amt, err := f.Read(buf)
				if err != nil {
					if err == io.EOF {
						if len(cur) > 0 {
							update(string(cur), v)
						}
					} else {
						mu.Lock()
						if finalErr != nil {
							finalErr = err
						}
						mu.Unlock()
					}
					return
				}
				tmp := buf[0:amt]
				prev := 0
				for i := range tmp {
					if tmp[i] != '\n' {
						continue
					}
					if cur == nil {
						cur = tmp[prev:i]
					} else {
						cur = append(cur, tmp[prev:i]...)
					}
					update(string(cur), v)
					cur = nil
					prev = i + 1
				}
				cur = append(cur, tmp[prev:]...)
			}
		}(readers[i], &vs[i])
	}
	wg.Wait()

	if finalErr != nil {
		return nil, 0, finalErr
	}

	return vs, count, nil
}

func main() {
	flag.Parse()
	fa, err := os.Open(*filea)
	if err != nil {
		panic(err)
	}
	fb, err := os.Open(*fileb)
	if err != nil {
		panic(err)
	}
	start := time.Now()
	v, max, err := readersToLines(fa, fb)
	fa.Close()
	fb.Close()
	if err != nil {
		panic(err)
	}
	fmt.Printf("Time to read, split, and parse: %v\n", time.Since(start))
	for _, lcsfunc := range []func(a, b []uint64) []commonSubstring{LCS2} {
		fmt.Printf("---------------------------------------------\n")
		start := time.Now()
		// fmt.Printf("Time to convert: %v\n", time.Since(start))
		mapping := make(map[uint64]uint64)
		substrs := make(map[uint64]*substr)
		for {
			// fmt.Printf("lengths: %d %d\n", len(v[0]), len(v[1]))
			lscStart := time.Now()
			css := lcsfunc(v[0], v[1])
			fmt.Printf("LCS Time: %v\n", time.Since(lscStart))
			if len(css) <= 0 {
				break
			}
			// ai, bi, length :=
			for _, cs := range css {
				fmt.Printf("Pos(%d %d), Length %d\n", cs.ai, cs.bi, cs.length)
				v[0][cs.ai] = max
				v[1][cs.bi] = max + 1
				substrs[max] = &substr{start: cs.ai, length: cs.length}
				substrs[max+1] = &substr{start: cs.bi, length: cs.length}
				mapping[max] = max + 1
				max += 2

				for i := cs.ai + 1; i+cs.length-1 < len(v[0]); i++ {
					v[0][i] = v[0][i+cs.length-1]
				}
				v[0] = v[0][0 : len(v[0])-cs.length+1]
				// for i := range v[0] {
				// 	fmt.Printf("%d: %d\n", i, v[0][i])
				// }

				for i := cs.bi + 1; i+cs.length-1 < len(v[1]); i++ {
					v[1][i] = v[1][i+cs.length-1]
				}
				v[1] = v[1][0 : len(v[1])-cs.length+1]
			}
		}
		fmt.Printf("Finished after %v\n", time.Since(start))
		// liness := [][]string{linesA, linesB}
		for i := range v {
			add := 0
			for j := range v[i] {
				if s, ok := substrs[v[i][j]]; ok {
					s.start += add
					add += s.length
					// fmt.Printf("Lines %d %d: ", i, s.start)
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
		fmt.Printf("Finished more after %v\n", time.Since(start))
	}
}

type substr struct {
	start, length int
}

type runes []rune

func (r runes) String() string {
	var s string
	for _, v := range r {
		s += string(v) + " "
	}
	return "[" + s + "]"
}
func uint64sAsRunes(i []uint64) runes {
	var s []rune
	for _, v := range i {
		s = append(s, rune(v))
	}
	return s
}

func DumbSuffixArray(v []uint64) []int {
	r := make([]int, len(v))
	for i := range r {
		r[i] = i
	}
	sort.Slice(r, func(i, j int) bool {
		i, j = r[i], r[j]
		for k := 0; k+i < len(v) && k+j < len(v); k++ {
			if v[i+k] != v[j+k] {
				return v[i+k] < v[j+k]
			}
		}
		return i > j // this just means the prefix starting at i is shorter
	})
	return r
}

// LCS finds the Longest Common Substring between two 'strings' of uint64.  ai and bi are the start
// of the substring in each of the input arrays, and length is the length of the common substring.
// If there is no common substring, ai and bi will be -1 and length will be 0.
type commonSubstring struct {
	ai, bi, length int
}

func LCS2(a, b []uint64) []commonSubstring {
	var input []uint64
	var max uint64
	for _, v := range a {
		input = append(input, v)
		if v > max {
			max = v
		}
	}
	middle := len(input)
	input = append(input, 0)
	for _, v := range b {
		input = append(input, v)
		if v > max {
			max = v
		}
	}
	input[middle] = max + 1
	start := time.Now()
	sa := InducedSuffixArray(input)
	fmt.Printf("SA time: %v\n", time.Since(start))

	var pairs [][3]int
	for i := 0; i < len(sa)-1; i++ {
		if (sa[i] < middle) == (sa[i+1] < middle) {
			continue
		}
		if sa[i] == middle || sa[i+1] == middle {
			continue
		}
		var aoff, boff int
		if sa[i] < middle {
			aoff = sa[i]
			boff = sa[i+1] - middle - 1
		}
		if sa[i+1] < middle {
			aoff = sa[i+1]
			boff = sa[i] - middle - 1
		}
		if a[aoff] != b[boff] {
			continue
		}
		pairs = append(pairs, [3]int{aoff, boff, 0})
	}
	if len(pairs) == 0 {
		return nil
	}

	// New cool stuff here
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i][0] != pairs[j][0] {
			return pairs[i][0] < pairs[j][0]
		}
		return pairs[i][1] < pairs[j][1]
	})
	// for _, p := range pairs {
	// 	fmt.Printf("%v\n", p)
	// }
	var css []commonSubstring
	var cs commonSubstring
	for i := range pairs {
		if pairs[i][2] > 0 {
			continue
		}
		prev := i
		for {
			// We know the value we are looking for is after prev, so restrict our search to that.
			tp := pairs[prev:]
			dex := sort.Search(len(tp), func(idx int) bool {
				return tp[idx][0] > pairs[prev][0]+1 ||
					(tp[idx][0] == pairs[prev][0]+1 && tp[idx][1] >= pairs[prev][1]+1)
			})
			dex += prev
			if dex >= len(pairs) {
				break
			}
			if pairs[dex][0] != pairs[prev][0]+1 || pairs[dex][1] != pairs[prev][1]+1 {
				break
			}
			pairs[dex][2] = 1
			prev = dex
		}
		if pairs[prev][0]-pairs[i][0]+1 > cs.length {
			cs = commonSubstring{
				length: pairs[prev][0] - pairs[i][0] + 1,
				ai:     pairs[i][0],
				bi:     pairs[i][1],
			}
			// fmt.Printf("Setting LCS: %v %v %v\n", ai, bi, length)
		}
	}
	css = append(css, cs)

	return css
}

func LCS(a, b []uint64) (ai, bi, length int) {
	// Construct a single list containing all of the suffixes for each string.
	var sufs []suf
	for i := range a {
		sufs = append(sufs, suf{suffix: a[i:], start: i, source: 0})
	}
	for i := range b {
		sufs = append(sufs, suf{suffix: b[i:], start: i, source: 1})
	}

	start := time.Now()
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
	fmt.Printf("SA Time: %v\n", time.Since(start))

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
