package utils_test

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/runningwild/jig/utils"

	. "github.com/smartystreets/goconvey/convey"
)

func TestMinWeightedMoves(t *testing.T) {
	Convey("MinWeightedMoves", t, func() {
		type testCase struct {
			input  []utils.CommonSubstring
			weight int
		}
		for _, tc := range []testCase{
			{
				input: []utils.CommonSubstring{
					{2, 0, 1},
					{1, 1, 2},
					{0, 2, 4},
					{3, 3, 8},
				},
				weight: 3,
			},
			{
				input: []utils.CommonSubstring{
					{6, 0, 1},
					{1, 1, 2},
					{0, 2, 4},
					{3, 3, 8},
					{2, 4, 16},
					{5, 5, 32},
					{4, 6, 64},
				},
				weight: 43,
			},
		} {
			v := make([]int, len(tc.input))
			for i := range v {
				v[i] = i
			}
			sort.Slice(v, func(i, j int) bool { return tc.input[v[i]].Ai < tc.input[v[j]].Ai })
			fmt.Printf("Starting order: %v\n", v)
			moves := utils.MinWeightedMovesSlow(tc.input)
			weight := 0
			for _, move := range moves {
				fmt.Printf("Domove %v\n", move)
				fmt.Printf("--  %v\n", v)
				weight += tc.input[v[move[0]]].Length
				doMove(v, move[0], move[1])
			}
			want := make([]int, len(tc.input))
			for i := range want {
				want[i] = i
			}
			So(v, ShouldResemble, want)
			So(weight, ShouldResemble, tc.weight)
		}
	})
}

func doMove(v []int, from, to int) {
	val := v[from]
	copy(v[from:], v[from+1:])
	if to == len(v) {
		v[len(v)-1] = val
		return
	}
	copy(v[to+1:], v[to:])
	v[to] = val
}
func BenchmarkMinWeightedMoves(b *testing.B) {
	for _, N := range []int{2, 4, 8, 16} {
		b.Run(fmt.Sprintf("%d", N), func(b *testing.B) {
			rand.Seed(int64(N))
			A := make([]int, N)
			B := make([]int, N)
			for i := range A {
				A[i] = i
				B[i] = i
			}
			rand.Shuffle(N, func(i, j int) { A[i], A[j] = A[j], A[i] })
			rand.Shuffle(N, func(i, j int) { B[i], B[j] = B[j], B[i] })
			var css []utils.CommonSubstring
			for i := range A {
				css = append(css, utils.CommonSubstring{Ai: A[i], Bi: B[i], Length: rand.Intn(100)})
			}
			for i := 0; i < b.N; i++ {
				utils.MinWeightedMovesSlow(css)
			}
		})
	}
}
