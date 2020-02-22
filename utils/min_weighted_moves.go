package utils

import (
	"fmt"
	"sort"

	"github.com/petar/GoLLRB/llrb"
)

func MinWeightedMoves(css []CommonSubstring) [][2]int {
	N := len(css)
	orderA := make([]int, N)
	orderB := make([]int, N)
	for i := range css {
		orderA[i] = i
		orderB[i] = i
	}
	sort.Slice(orderA, func(i, j int) bool { return css[orderA[i]].Ai < css[orderA[j]].Ai })
	sort.Slice(orderB, func(i, j int) bool { return css[orderB[i]].Bi < css[orderB[j]].Bi })

	// Calculate the maximum weighted increasing subsequence on orderB, this will tell us which
	// blocks need to remain fixed in place, all other blocks will move around those.  The weight
	// of each block is its length, but I think calling this maximum length increasing subsequence
	// might get confusing.
	maxWeightAtIndex := make([]int, len(orderB))
	maxmax := 0
	for i := len(maxWeightAtIndex) - 1; i >= 0; i-- {
		max := 0
		for j := i + 1; j < len(maxWeightAtIndex); j++ {
			if css[orderA[i]].Bi < css[orderA[j]].Bi && // This ensures an increasing subsequence.
				maxWeightAtIndex[j] > max {
				max = maxWeightAtIndex[j]
			}
		}
		max += css[orderA[i]].Length
		maxWeightAtIndex[i] = max
		if max > maxmax {
			maxmax = max
		}
	}

	// Now pull out a subsequence that matches this, we'll always take the first possible choice so
	// the result will be deterministic.
	fixedSet := make([]bool, len(orderB))
	ssLen := 0
	for i, mw := range maxWeightAtIndex {
		if mw == maxmax {
			fixedSet[i] = true
			maxmax -= css[orderA[i]].Length
			ssLen++
		}
	}
	if maxmax != 0 {
		panic("bug in MinWeightedMoves")
	}

	biLookup := make(map[int]int)
	for i := range orderB {
		biLookup[css[orderB[i]].Bi] = i
	}
	var ret [][2]int
	src := -1
	fmt.Printf("FixedSet: %v\n", fixedSet)
	for i := range fixedSet {
		if fixedSet[i] {
			src = i
			continue
		}
		fmt.Printf("Appending %v\n", [2]int{src, biLookup[css[orderA[i]].Bi]})
		ret = append(ret, [2]int{src, biLookup[css[orderA[i]].Bi]})
	}

	return ret
}

func MinWeightedMovesSlow(css []CommonSubstring) [][2]int {
	N := len(css)
	orderA := make([]int, N)
	orderB := make([]int, N)
	for i := range css {
		orderA[i] = i
		orderB[i] = i
	}
	sort.Slice(orderA, func(i, j int) bool { return css[orderA[i]].Ai < css[orderA[j]].Ai })
	sort.Slice(orderB, func(i, j int) bool { return css[orderB[i]].Bi < css[orderB[j]].Bi })

	// fmt.Printf("CSS:\n")
	// for i := range css {
	// 	fmt.Printf("%d: %v\n", i, css[i])
	// }

	used := make(map[string]bool)
	nodes := llrb.New()
	nodes.InsertNoReplace(moveNode{weight: 0, order: orderA, swaps: nil})
	reverse := make([]int, N)
	for nodes.Len() > 0 {
		n := nodes.DeleteMin().(moveNode)
		str := fmt.Sprintf("%v", n.order)
		if used[str] {
			continue
		}
		used[str] = true
		// fmt.Printf("Processing %q with swaps %v and weight %d\n", str, n.swaps, n.weight)
		if intSlicesEqual(n.order, orderB) {
			// fmt.Printf("To order %v:\n", orderA)
			for _, swap := range n.swaps {
				// fmt.Printf("  %v\n", swap)
				doMove(orderA, swap[0], swap[1])
				// fmt.Printf("%v\n", orderA)
			}
			return n.swaps
		}

		for i := range n.order {
			reverse[n.order[i]] = i
		}

		for i := range n.order {
			beforeCorrect := (n.order[i] == 0 && i == 0) || (i > 0 && n.order[i] == n.order[i-1]+1)
			afterCorrect := (n.order[i] == N-1 && i == N-1) || (i < N-1 && n.order[i] == n.order[i+1]-1)
			if beforeCorrect && afterCorrect {
				continue
			}
			if !beforeCorrect {
				// Try moving this value after the one that should preceed it.
				from := i
				to := 0
				if n.order[i] > 0 {
					to = reverse[n.order[i]-1] + 1
				}
				if from < to {
					to--
				}
				next := make([]int, N)
				copy(next, n.order)
				doMove(next, from, to)
				v := moveNode{
					weight: n.weight + css[n.order[i]].Length,
					order:  next,
					swaps:  append(n.swaps, [2]int{from, to}),
				}
				nodes.InsertNoReplace(v)
			}
			if !afterCorrect {
				// Try moving this value before the one that should succeed it.
				from := i
				to := N
				if n.order[i] < N-1 {
					to = reverse[n.order[i]+1]
				}
				if from < to {
					to--
				}
				next := make([]int, N)
				copy(next, n.order)
				doMove(next, from, to)
				v := moveNode{
					weight: n.weight + css[n.order[i]].Length,
					order:  next,
					swaps:  append(n.swaps, [2]int{from, to}),
				}
				nodes.InsertNoReplace(v)
			}
		}
	}
	return nil
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

type moveNode struct {
	weight int
	order  []int
	swaps  [][2]int
}

func (mn moveNode) Less(_item llrb.Item) bool {
	item := _item.(moveNode)
	if mn.weight != item.weight {
		return mn.weight < item.weight
	}
	for i := range mn.order {
		if mn.order[i] != item.order[i] {
			return mn.order[i] < item.order[i]
		}
	}
	return false
}

func intSlicesEqual(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
