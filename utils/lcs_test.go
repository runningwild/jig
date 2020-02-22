package utils_test

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"strings"
	"testing"

	"github.com/runningwild/jig/utils"
	. "github.com/smartystreets/goconvey/convey"
)

func TestReadersToLines(t *testing.T) {
	inputs := [][2][]byte{
		[2][]byte{[]byte("a\n\n\nb\n\n\n"), []byte("a\n\n\nb\n\n\nc")},
		[2][]byte{makeLinearInput(100), makeLinearInput(150)},
		[2][]byte{makeRandomInput(100, 0), makeRandomEdits(makeRandomInput(100, 0), 1, 0)},
		[2][]byte{makeRandomInput(100, 1), makeRandomEdits(makeRandomInput(100, 1), 5, 0)},
		[2][]byte{makeRandomInput(100, 2), makeRandomEdits(makeRandomInput(100, 2), 10, 0)},
		[2][]byte{makeRandomInput(1000, 0), makeRandomEdits(makeRandomInput(1000, 0), 1, 0)},
		[2][]byte{makeRandomInput(1000, 0), makeRandomEdits(makeRandomInput(1000, 0), 5, 0)},
		[2][]byte{makeRandomInput(1000, 0), makeRandomEdits(makeRandomInput(1000, 0), 10, 0)},
		[2][]byte{makeRandomInput(10000, 0), makeRandomEdits(makeRandomInput(10000, 0), 10, 0)},
		// The first parameter is actually number of lines, but this will guarantee that the input
		// arrays are at least as long as the reader buffer size.
		[2][]byte{makeRandomInput(utils.ReadersToLinesBufferSize, 0), makeRandomEdits(makeRandomInput(utils.ReadersToLinesBufferSize, 0), 10, 0)},
	}

	Convey("ReadersToLines", t, func() {
		Convey("can properly split several newlines in a row", func() {
			input := []byte("a\n\n\nb\n\n\n")
			lines, max, err := utils.ReadersToLines(bytes.NewBuffer(input), bytes.NewBuffer(input))
			So(err, ShouldBeNil)
			So(max, ShouldEqual, 4)
			So(lines, shouldMatchLines, [][]uint64{[]uint64{0, 1, 1, 2, 1, 1}, []uint64{0, 1, 1, 2, 1, 1}})
		})
		Convey("matches the simple reader", func() {
			Convey("with a different buffer sizes", func() {
				for _, input := range inputs {
					lines, max, err := utils.ReadersToLines(bytes.NewBuffer(input[0]), bytes.NewBuffer(input[1]))
					So(err, ShouldBeNil)
					wantLines, wantMax, _ := simpleReadersToLines(bytes.NewBuffer(input[0]), bytes.NewBuffer(input[1]))
					So(max, ShouldEqual, wantMax)
					So(lines, shouldMatchLines, wantLines)
				}
			})

		})
	})
}

func BenchmarkReadersToLines(b *testing.B) {
	type testcase struct {
		lines, edits int
	}
	for _, tc := range []testcase{
		{1000, 0},
		{1000, 3},
		{1000, 50},
		{10000, 0},
		{10000, 3},
		{10000, 50},
		{100000, 0},
		{100000, 3},
		{100000, 50},
		{100000, 500},
		{100000, 5000},
	} {
		b.Run(fmt.Sprintf("benchmark-readerstolines-%d_%d", tc.lines, tc.edits), func(b *testing.B) {
			b.StopTimer()
			inputA := makeRandomInput(tc.lines, 123)
			inputB := makeRandomEdits(inputA, tc.edits, 123)
			b.StartTimer()
			for i := 0; i < b.N; i++ {
				utils.ReadersToLines(bytes.NewBuffer(inputA), bytes.NewBuffer(inputB))
			}
		})
	}
}

func shouldMatchLines(_a interface{}, _bs ...interface{}) string {
	a := _a.([][]uint64)
	b := _bs[0].([][]uint64)

	if len(a) != 2 || len(b) != 2 {
		return "slices should have length 2"
	}
	if len(a[0]) != len(b[0]) {
		return fmt.Sprintf("lengths of the first slices don't match (%d != %d)", len(a[0]), len(b[0]))
	}
	if len(a[1]) != len(b[1]) {
		return fmt.Sprintf("lengths of the second slices don't match (%d != %d)", len(a[1]), len(b[1]))
	}

	// Canonicalize by rewriting them in the order they appear.
	for _, v := range [][][]uint64{a, b} {
		m := make(map[uint64]uint64)
		for i := range v {
			for j := range v[i] {
				if _, ok := m[v[i][j]]; !ok {
					m[v[i][j]] = uint64(len(m) + 1)
				}
			}
		}
		for i := range v {
			for j := range v[i] {
				v[i][j] = m[v[i][j]]
			}
		}
	}

	for i := range a {
		for j := range a[i] {
			if a[i][j] != b[i][j] {
				return "slices don't have the same form"
			}
		}
	}

	return ""
}

func makeLinearInput(lines int) []byte {
	var s string
	for i := 0; i < lines; i++ {
		s += fmt.Sprintf("%d\n", i)
	}
	return []byte(s)
}

func makeRandomInput(lines, seed int) []byte {
	rng := rand.New(rand.NewSource(int64(seed)))
	var s string
	for i := 0; i < lines; i++ {
		var line string
		lineLength := rng.Intn(100)
		for len(line) < lineLength {
			line += fmt.Sprintf("%v ", rng.Intn(100000))
		}
		s += line + "\n"
	}
	return []byte(s)
}

func makeRandomEdits(input []byte, edits, seed int) []byte {
	rng := rand.New(rand.NewSource(int64(seed)))
	lines := strings.Split(string(input), "\n")
	for i := 0; i < edits; i++ {
		switch rng.Intn(3) {
		case 0: // delete
			if len(lines) == 0 {
				break
			}
			start := rng.Intn(len(lines))
			amt := 1 + rng.Intn(1+int(math.Sqrt(float64(len(lines)-start))))
			lines, _ = removeLines(lines, start, amt)

		case 1: // insert
			insert := make([]string, 1+rng.Intn(1+int(math.Sqrt(float64(len(lines))))))
			for i := range insert {
				insert[i] = fmt.Sprintf("%d", rng.Intn(1000000))
			}
			lines = insertLines(lines, insert, rng.Intn(1+len(lines)))

		case 2: // move
			if len(lines) == 0 {
				break
			}
			start := rng.Intn(len(lines))
			amt := 1 + rng.Intn(1+int(math.Sqrt(float64(len(lines)-start))))
			var removed []string
			lines, removed = removeLines(lines, start, amt)
			lines = insertLines(lines, removed, rng.Intn(1+len(lines)))
		}
	}
	return []byte(strings.Join(lines, "\n"))
}

func removeLines(lines []string, start, amt int) (edited, removed []string) {
	if start+amt > len(lines) {
		amt = len(lines) - start
	}
	removed = make([]string, amt)
	copy(removed, lines[start:start+amt])
	for i := start; i < len(lines)-amt; i++ {
		lines[i] = lines[i+amt]
	}
	lines = lines[0 : len(lines)-amt]
	return lines, removed
}

func insertLines(lines, insert []string, start int) (edited []string) {
	for i := len(lines) - 1; i >= start+len(insert); i-- {
		lines[i] = lines[i-len(insert)]
	}
	copy(lines[start:], insert)
	return lines
}

func simpleReadersToLines(ra, rb io.Reader) ([][]uint64, uint64, error) {
	a, err := ioutil.ReadAll(ra)
	if err != nil {
		return nil, 0, err
	}
	b, err := ioutil.ReadAll(rb)
	if err != nil {
		return nil, 0, err
	}
	linesA := strings.Split(string(a), "\n")
	linesB := strings.Split(string(b), "\n")
	// Strip the last line if it's empty
	if len(linesA) > 0 && linesA[len(linesA)-1] == "" {
		linesA = linesA[0 : len(linesA)-1]
	}
	if len(linesB) > 0 && linesB[len(linesB)-1] == "" {
		linesB = linesB[0 : len(linesB)-1]
	}
	var vs [][]uint64
	m := make(map[string]uint64)
	for _, lines := range [][]string{linesA, linesB} {
		var v []uint64
		for _, line := range lines {

			if _, ok := m[line]; !ok {
				m[line] = uint64(len(m) + 1)
			}
			v = append(v, m[line])
		}
		vs = append(vs, v)
	}
	return vs, uint64(len(m) + 1), nil
}

func TestSuffixArray(t *testing.T) {
	Convey("Induced algorithm works", t, func() {
		Convey("on some canned inputs", func() {
			tests := [][]uint64{
				{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				{1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0},
				{1, 2, 0, 1, 2, 0, 0, 1, 2, 0, 1, 1, 2, 2, 0, 1, 1, 2, 1, 1, 2, 2, 0},
				{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
				{10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0},
			}
			for _, test := range tests {
				got := utils.InducedSuffixArray(test)
				want := utils.DumbSuffixArray(test)
				So(got, ShouldResemble, want)
			}
		})
		Convey("on medium length inputs with small alphabets", func() {
			rng := rand.New(rand.NewSource(123))
			for i := 0; i < 1000; i++ {
				length := rng.Intn(1000) + 100
				alphabet := rng.Intn(5) + 2
				input := make([]uint64, length)
				for i := range input {
					input[i] = uint64(rng.Intn(alphabet))
				}
				got := utils.InducedSuffixArray(input)
				want := utils.DumbSuffixArray(input)
				So(got, ShouldResemble, want)
			}
		})
		Convey("on some random long inputs", func() {
			rng := rand.New(rand.NewSource(123))
			for i := 0; i < 100; i++ {
				length := rng.Intn(1000) + 100
				alphabet := rng.Intn(500) + 2
				input := make([]uint64, length)
				for i := range input {
					input[i] = uint64(rng.Intn(alphabet))
				}
				got := utils.InducedSuffixArray(input)
				want := utils.DumbSuffixArray(input)
				So(got, ShouldResemble, want)
			}
		})
	})
}

func BenchmarkInducedSuffixArray(b *testing.B) {
	type testcase struct {
		length, alphabet int
	}
	for _, tc := range []testcase{
		{10, 10},
		{100, 10},
		{1000, 10},
		{10000, 10},
		{100000, 10},
		{1000000, 10},
		{1000, 1000},
		{10000, 1000},
		{100000, 1000},
		{1000000, 1000},
	} {
		b.Run(fmt.Sprintf("benchmark-isa-%d_%d", tc.length, tc.alphabet), func(b *testing.B) {
			b.StopTimer()
			v := make([]uint64, tc.length)
			rng := rand.New(rand.NewSource(123))
			for i := range v {
				v[i] = uint64(rng.Intn(tc.alphabet))
			}
			b.StartTimer()
			for i := 0; i < b.N; i++ {
				utils.InducedSuffixArray(v)
			}
		})
	}
}

func TestLCS(t *testing.T) {
	Convey("LCS finds the longest common substring", t, func() {
		Convey("on short strings", func() {
			css := utils.LCS2(
				toUint64s("abc"),
				toUint64s("abc"))
			So(css, ShouldHaveLength, 1)
			So(css[0].Ai, ShouldEqual, 0)
			So(css[0].Bi, ShouldEqual, 0)
			So(css[0].Length, ShouldEqual, 3)
		})
		Convey("on very short self-similar strings", func() {
			css := utils.LCS2(
				toUint64s("aa"),
				toUint64s("aa"))
			So(css, ShouldHaveLength, 1)
			So(css[0].Ai, ShouldEqual, 0)
			So(css[0].Bi, ShouldEqual, 0)
			So(css[0].Length, ShouldEqual, 2)
		})
		Convey("on medium self-similar strings", func() {
			css := utils.LCS2(
				toUint64s("abba"),
				toUint64s("abba"))
			So(css, ShouldHaveLength, 1)
			So(css[0].Ai, ShouldEqual, 0)
			So(css[0].Bi, ShouldEqual, 0)
			So(css[0].Length, ShouldEqual, 4)
		})
		Convey("on short self-similar strings", func() {
			css := utils.LCS2(
				toUint64s("aba"),
				toUint64s("aba"))
			So(css, ShouldHaveLength, 1)
			So(css[0].Ai, ShouldEqual, 0)
			So(css[0].Bi, ShouldEqual, 0)
			So(css[0].Length, ShouldEqual, 3)
		})
		Convey("at the start of one string and end of the other", func() {
			css := utils.LCS2(
				toUint64s("abcdefghijklmnopqrstuvwxyz"),
				toUint64s("012345678abcdefghijklmnopqrs"))
			So(css, ShouldHaveLength, 1)
			So(css[0].Ai, ShouldEqual, 0)
			So(css[0].Bi, ShouldEqual, 9)
			So(css[0].Length, ShouldEqual, 19)
		})
		Convey("at the end of one string and start of the other", func() {
			css := utils.LCS2(
				toUint64s("012345678abcdefghijklmnopqrs"),
				toUint64s("abcdefghijklmnopqrstuvwxyz"))
			So(css, ShouldHaveLength, 1)
			So(css[0].Ai, ShouldEqual, 9)
			So(css[0].Bi, ShouldEqual, 0)
			So(css[0].Length, ShouldEqual, 19)
		})
		Convey("when the strings are equal", func() {
			css := utils.LCS2(
				toUint64s("abcdefghijklmnopqrstuvwxyz"),
				toUint64s("abcdefghijklmnopqrstuvwxyz"))
			So(css, ShouldHaveLength, 1)
			So(css[0].Ai, ShouldEqual, 0)
			So(css[0].Bi, ShouldEqual, 0)
			So(css[0].Length, ShouldEqual, 26)
		})
		Convey("when the strings have no common values", func() {
			css := utils.LCS2(
				toUint64s("abcdefghijklmnopqrstuvwxyz"),
				toUint64s("0123456789"))
			So(css, ShouldHaveLength, 0)
		})
	})
}

func toUint64s(s string) []uint64 {
	var v []uint64
	for _, r := range s {
		v = append(v, uint64(r))
	}
	return v
}

func BenchmarkLCS(b *testing.B) {
	type testcase struct {
		lena, lenb, overlap int
	}
	for _, tc := range []testcase{
		{100, 100, 50},
		{1000, 1000, 500},
		{10000, 10000, 5000},
		{10000, 10000, 10},
		{10000, 10000, 10000 - 10},
		{100, 10000, 10},
		{100, 10000, 100},
	} {
		b.Run(fmt.Sprintf("benchmark-lcs-%d_%d_%d", tc.lena, tc.lenb, tc.overlap), func(b *testing.B) {
			b.StopTimer()
			ab := make([]uint64, tc.lena+tc.lenb-tc.overlap)
			for i := range ab {
				ab[i] = uint64(i)
			}
			b.StartTimer()
			for i := 0; i < b.N; i++ {
				utils.LCS2(ab[0:tc.lena], ab[tc.overlap:])
			}
		})
	}
}
