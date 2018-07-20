package utils

// func main() {
// 	N := 100000
// 	u := NewUnionFind(N)
// 	var pairs [][2]int
// 	for i := 0; i < N-2; i++ {
// 		pairs = append(pairs, [2]int{i, i + 2})
// 	}
// 	for i := range pairs {
// 		swap := rand.Intn(len(pairs)-i) + i
// 		pairs[i], pairs[swap] = pairs[swap], pairs[i]
// 	}
// 	for _, p := range pairs {
// 		if rand.Intn(2) == 0 {
// 			u.Union(p[0], p[1])
// 		} else {
// 			u.Union(p[1], p[0])
// 		}
// 	}
// 	u.Union(1, 2)
// 	for i := 0; i < len(u.groups); i++ {
// 		for j := 0; j < len(u.groups); j++ {
// 			if i == j {
// 				continue
// 			}
// 			fmt.Printf("%d,%d: %v\n", i, j, u.AreConnected(i, j))
// 		}
// 	}
// }

type UnionFind struct {
	sizes, groups []int
}

func NewUnionFind(n int) *UnionFind {
	u := &UnionFind{
		sizes:  make([]int, n),
		groups: make([]int, n),
	}
	for i := range u.groups {
		u.groups[i] = -1
	}
	return u
}

func (u *UnionFind) Union(a, b int) {
	if u.groups[a] == -1 {
		u.sizes[a] = 1
		u.groups[a] = a
	}
	if u.groups[b] == -1 {
		u.sizes[b] = 1
		u.groups[b] = b
	}
	rootA := u.findRoot(a)
	rootB := u.findRoot(b)
	// Both are part of groups already
	if u.sizes[rootA] < u.sizes[rootB] {
		a, b = b, a
		rootA, rootB = rootB, rootA
	}
	u.sizes[rootA] += u.sizes[rootB]
	u.compress(a, rootA)
	u.compress(b, rootA)
}
func (u *UnionFind) findRoot(a int) int {
	if u.groups[a] == a {
		return a
	}
	return u.findRoot(u.groups[a])
}
func (u *UnionFind) compress(a int, root int) {
	if a != u.groups[a] {
		u.compress(u.groups[a], root)
	}
	u.groups[a] = root
}

func (u *UnionFind) Find(a int) int {
	if u.groups[a] == -1 {
		return a
	}
	root := u.findRoot(a)
	u.compress(a, root)
	return root
}

// func (u *UnionFind) AreConnected(a, b int) bool {
// 	if u.groups[a] == -1 || u.groups[b] == -1 {
// 		return false
// 	}
// 	rootA := u.findRoot(a)
// 	rootB := u.findRoot(b)
// 	if rootA == rootB && u.groups[a] != u.groups[b] {
// 		u.compress(a, rootA)
// 		u.compress(b, rootA)

// 	}
// 	return rootA == rootB
// }
