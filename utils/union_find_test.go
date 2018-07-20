package utils_test

import (
	"math"
	"math/rand"
	"testing"

	"github.com/runningwild/jig/utils"

	. "github.com/smartystreets/goconvey/convey"
)

func TestUnionFind(t *testing.T) {
	Convey("UnionFind", t, func() {
		for round := 0; round < 100; round++ {
			N := 10000
			numGroups := rand.Intn(int(math.Sqrt(float64(N)))) + 1
			v := make([][]int, numGroups)
			for i := 0; i < N; i++ {
				r := rand.Intn(numGroups)
				v[r] = append(v[r], i)
			}
			// Do a bunch of random unions then verify that nothing is inconsistent, repeat until converged.
			uf := utils.NewUnionFind(N)
			maxIters := 100
			fullyConnected := false
			iters := 0
			for !fullyConnected || iters > maxIters {
				iters++
				for i := 0; i < N; i++ {
					g := v[rand.Intn(numGroups)]
					uf.Union(g[rand.Intn(len(g))], g[rand.Intn(len(g))])
				}
				m := make(map[int]int) // map from UF group number to our local group number
				for _, g := range v {
					for i := range g {
						n := uf.Find(g[i])
						if k, ok := m[n]; ok && k != n {
							t.Errorf("FAIL")
						}
					}
				}
				fullyConnected = true
			checkConnectedness:
				for i := range v {
					for j := 1; j < len(v[i]); j++ {
						if uf.Find(v[i][j]) != uf.Find(v[i][0]) {
							fullyConnected = false
							break checkConnectedness
						}
					}
				}
			}
			So(iters, ShouldBeLessThan, maxIters)
		}
	})
}

func BenchmarkUnionFind1M(b *testing.B) {
	size := 1000 * 1000
	b.StopTimer()
	joins := make([]int, size)
	for i := range joins {
		joins[i] = i
	}
	for i := range joins {
		swap := rand.Intn(len(joins)-i) + i
		joins[i], joins[swap] = joins[swap], joins[i]
	}
	ufs := make([]*utils.UnionFind, b.N)
	for i := range ufs {
		ufs[i] = utils.NewUnionFind(size)
	}
	b.StartTimer()
	for _, uf := range ufs {
		for j := 0; j < len(joins)-1; j++ {
			uf.Union(joins[j], joins[j+1])
		}
	}
}
