package utils

import (
	"io"
	"sort"
	"sync"
)

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
	t := make([]plusOrMinusType, len(T))
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

const ReadersToLinesBufferSize int = 10000

func ReadersToLines(fa, fb io.Reader) ([][]uint64, uint64, error) {
	var mu sync.RWMutex
	var finalErr error

	ss := MakeStringSeq()
	var wg sync.WaitGroup
	readers := []io.Reader{fa, fb}
	vs := make([][]uint64, 2)
	for i := range readers {
		wg.Add(1)
		// Launch one go-routine per reader.  The go-routine will read lines from the reader,
		// convert those into uint64s, then append those values onto *v.
		go func(f io.Reader, v *[]uint64) {
			defer wg.Done()
			buf := make([]byte, ReadersToLinesBufferSize)
			var cur []byte
			for {
				amt, err := f.Read(buf)
				if err != nil {
					if err == io.EOF {
						if len(cur) > 0 {
							*v = append(*v, ss.ID(cur))
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
					*v = append(*v, ss.ID(cur))
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

	return vs, ss.Max(), nil
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

// CommonSubstring indicates a common substring between two strings.  Ai and Bi are the indices into
// the first and second string, and Length is the length of the common substring starting at those indices.
type CommonSubstring struct {
	Ai, Bi, Length int
}

// LCS2 finds the maximal length common substrings between two 'strings' of uint64.
func LCS2(a, b []uint64) []CommonSubstring {
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
	input = append(input, max+2)
	input[middle] = max + 1
	sa := InducedSuffixArray(input)
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
	// 	fmt.Printf("pair: %v\n", p)
	// }
	var css []CommonSubstring
	for {
		var cs CommonSubstring
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
			if pairs[prev][0]-pairs[i][0]+1 > cs.Length {
				cs = CommonSubstring{
					Ai:     pairs[i][0],
					Bi:     pairs[i][1],
					Length: pairs[prev][0] - pairs[i][0] + 1,
				}
			}
		}
		// fmt.Printf("Got Common Substring: %v\n", cs)
		if cs.Length == 0 {
			break
		}
		count := 0
		for i := 0; i < len(pairs); i++ {
			pairs[count] = pairs[i]
			pairs[count][2] = 0
			if (pairs[i][0] >= cs.Ai && pairs[i][0] < cs.Ai+cs.Length) ||
				(pairs[i][1] >= cs.Bi && pairs[i][1] < cs.Bi+cs.Length) {
			} else {
				count++
			}
		}
		// fmt.Printf("%d down to %d/%d\n", len(pairs), count, cs.Length)
		// if len(pairs) == 1 {
		// 	fmt.Printf("%v\n", pairs)
		// }
		pairs = pairs[0:count]
		css = append(css, cs)
	}
	// fmt.Printf("Got %d runs with %d pairs remaining\n", len(css), len(pairs))
	return css
}
