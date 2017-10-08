package graph

import (
	"bytes"
	"fmt"
	"sort"
)

type Verge struct {
	r Repo
	f Frontier

	// Maps from commit hash to node hash for each edge on the verge.
	forward, backward map[string]string

	deps, rdeps *simpleGraph
}

func MakeVerge(r Repo, f Frontier, path string) *Verge {
	v := &Verge{
		r:        r,
		f:        f,
		forward:  make(map[string]string),
		backward: make(map[string]string),
		deps:     makeSimpleGraph(),
		rdeps:    makeSimpleGraph(),
	}
	n := r.GetNode("src:" + path)
	for _, e := range n.Out {
		v.forward[e.Commit] = e.Node
		c := r.GetCommit(e.Commit)
		for _, dep := range c.Deps {
			v.rdeps.addNode(e.Commit)
			v.rdeps.addEdge(dep, e.Commit)
		}
	}
	return v
}

func (v *Verge) Clone() *Verge {
	v2 := &Verge{
		r:        v.r,
		f:        v.f,
		forward:  make(map[string]string),
		backward: make(map[string]string),
		deps:     v.deps.Clone(),
		rdeps:    v.rdeps.Clone(),
	}
	for commit, node := range v.forward {
		v2.forward[commit] = node
	}
	for commit, node := range v.backward {
		v2.backward[commit] = node
	}
	return v2
}

// Advance advances the verge past node.  It panics if node is not a valid verge to advance past.
func (v *Verge) Advance(node string) {
	v.move(node, v.forwardMover())
}

// Retract retracts the verge past node.  It panics if node is not a valid verge to retract past.
func (v *Verge) Retract(node string) {
	v.move(node, v.backwardMover())
}

// move allows us to do Advance and Retract with the same code, all that's required is that we can
// swap v.forward and v.backward, and that we can swap the in and out edges on all nodes.
func (v *Verge) move(node string, mov mover) {
	n := mov.GetNode(node)
	// n := v.r.GetNode(node)
	for _, e := range mov.GetIn(n) {
		if !v.f.Observes(e.Commit) {
			continue
		}

		// Going through the reference is actually only necessary on a Retract.
		// check := mov.ForwardEdges()[e.Commit]
		// if r := v.r.GetRef(check); r != "" {
		// 	check = r
		// }
		// if check != node {
		// 	panic("invalid advance node")
		// }

		delete(mov.ForwardEdges(), e.Commit)
		delete(mov.BackwardEdges(), e.Commit)
		// fmt.Printf("RDEPS remove node %s\n", e.Commit)
		v.rdeps.removeNode(e.Commit)
	}
	for _, e := range mov.GetOut(n) {
		if !v.f.Observes(e.Commit) {
			continue
		}
		mov.ForwardEdges()[e.Commit] = mov.GetHead(mov.GetNode(e.Node))
		mov.BackwardEdges()[e.Commit] = mov.GetTail(mov.GetNode(node))
		for _, dep := range v.r.GetCommit(e.Commit).Deps {
			v.rdeps.addNode(e.Commit)
			v.rdeps.addEdge(dep, e.Commit)
			// fmt.Printf("RDEPS add edge %s -> %s\n", dep, e.Commit)
		}
	}
}

func nodeContent(r Repo, node string) string {
	if r := r.GetRef(node); r != "" {
		node = r
	}
	n := r.GetNode(node)
	s := string(bytes.Join(r.GetContent(n.Content), []byte(".")))
	return fmt.Sprintf("(%s) %s (%s)", n.Head, s, n.Tail)
}

func (v *Verge) state() {
	var keys []string
	for key := range v.forward {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	// fmt.Printf("Current forward state (%d edges):\n", len(keys))
	for _, key := range keys {
		// var content string
		ns := v.forward[key]
		if r := v.r.GetRef(ns); r != "" {
			ns = r
		}
		n := v.r.GetNode(ns)
		if n != nil {
			// content = string(bytes.Join(v.r.GetContent(n.Content), []byte(".")))
		}
		// fmt.Printf("Commit %s -> Node %s: %s\n", key, v.forward[key], content)
	}
}

func (v *Verge) Conflicts() []string {
	v.state()
	visible := make(map[string]bool)
	for c := range v.forward {
		if v.f.Observes(c) {
			visible[c] = true
		}
	}
	// fmt.Printf("Visible state: %v\n", visible)
	// Find all commits that are not dominated by at least one other commit.
	dominators := v.rdeps.dominators()
	if len(dominators) == 1 {
		return nil
	}
	return dominators
}

// TODO: A lot of this depends on the fact that a Verge can never cut two edges from the same commit
// at the same time (excepting the reverse-edge situation).  This needs to be part of verification.
// next returns a list of nodes that could be used as the next node the Verge can pass.
// TODO: handle reverse edges
func (v *Verge) Next() []string {
	return v.look(v.forwardMover())
}

func (v *Verge) Prev() []string {
	return v.look(v.backwardMover())
}

func (v *Verge) look(mov mover) []string {
	// Get a set of all nodes on the out end of all edges on the Verge.
	dsts := make(map[string]bool)
	for _, dst := range mov.ForwardEdges() {
		dsts[dst] = true
	}

	// var keys []string
	// for key := range mov.ForwardEdges() {
	// 	keys = append(keys, key)
	// }
	// sort.Strings(keys)

	// TODO: verify the bold claim made regarding moves in the paragraph below.
	// Acceptable nodes are ones whose inputs edges are all on the Verge.  The one exception is an
	// incoming edge that comes from a commit with a forward edge currently on the verge, this is
	// what happens in a move, and it is safe to advance in that case by just removing the forward
	// edge from the verge.
	var good []string
	for dst := range dsts {
		n := mov.GetNode(dst)
		count := 0
		observable := 0
		for _, e := range mov.GetIn(n) {
			if !v.f.Observes(e.Commit) {
				continue
			}
			observable++
			target := mov.ForwardEdges()[e.Commit]
			// if r := v.r.GetRef(target); r != "" {
			// 	target = r
			// }
			if target == dst || v.rdeps.nodes[e.Commit] {
				count++
			}
		}
		if count != observable {
			continue
		}
		good = append(good, dst)
	}
	return good
}

// mover let's us write a function that goes forward through the graph and reuse it to do the same
// thing backwards.  There are two implementations, forwardMover and backwardMover.
type mover interface {
	GetIn(n *Node) []Edge
	GetOut(n *Node) []Edge
	GetHead(n *Node) string
	GetTail(n *Node) string
	GetNode(n string) *Node
	Next() []string
	Prev() []string
	ForwardEdges() map[string]string
	BackwardEdges() map[string]string
}

func (v *Verge) forwardMover() mover {
	return (*forwardMover)(v)
}

type forwardMover Verge

func (f *forwardMover) GetIn(n *Node) []Edge {
	return n.In
}
func (f *forwardMover) GetOut(n *Node) []Edge {
	return n.Out
}
func (f *forwardMover) GetHead(n *Node) string {
	return n.Head
}
func (f *forwardMover) GetTail(n *Node) string {
	return n.Tail
}
func (f *forwardMover) GetNode(n string) *Node {
	return f.r.GetNode(n)
}
func (f *forwardMover) Next() []string {
	return ((*Verge)(f)).Next()
}
func (f *forwardMover) Prev() []string {
	return ((*Verge)(f)).Prev()
}
func (f *forwardMover) ForwardEdges() map[string]string {
	return f.forward
}
func (f *forwardMover) BackwardEdges() map[string]string {
	return f.backward
}

func (v *Verge) backwardMover() mover {
	return (*backwardMover)(v)
}

type backwardMover Verge

func (b *backwardMover) GetIn(n *Node) []Edge {
	return n.Out
}
func (b *backwardMover) GetOut(n *Node) []Edge {
	return n.In
}
func (b *backwardMover) GetHead(n *Node) string {
	return n.Tail
}
func (b *backwardMover) GetTail(n *Node) string {
	return n.Head
}
func (b *backwardMover) GetNode(n string) *Node {
	// fmt.Printf("%s -> %v\n", n, b.r.GetRef(n))
	// fmt.Printf("%s -> %s\n", n, bytes.Join(b.r.GetContent(b.r.GetNode(n).Content), []byte(".")))
	return b.r.GetNode(b.r.GetRef(n))
}
func (b *backwardMover) Next() []string {
	return ((*Verge)(b)).Prev()
}
func (b *backwardMover) Prev() []string {
	return ((*Verge)(b)).Next()
}
func (b *backwardMover) ForwardEdges() map[string]string {
	return b.backward
}
func (b *backwardMover) BackwardEdges() map[string]string {
	return b.forward
}

func (v *Verge) AdvanceUntilConverged() (string, map[string]bool) {
	return v.moveUntilConverged(v.forwardMover())
}
func (v *Verge) RetractUntilConverged() (string, map[string]bool) {
	return v.moveUntilConverged(v.backwardMover())
}

// TODO: Prove this works.
// Always advance the verge by choosing a node that doesn't collapse any of the commits we're
// tracking.  Every time the verge advances we start tracking any edges that belong to commits that
// conflict on the verge at its current position.  Once we find a node that collapses all of the
// commits we're currently tracking, we're done, even if there are more conflicting edges on the
// other side of that node.  Returns the hash of the node at which everything converges.  This is a
// more simplified version of the method I first devised for tracking edges.  Returns a set of all
// commits that were involved in a conflict.
func (v *Verge) moveUntilConverged(mov mover) (string, map[string]bool) {
	// track is the set of commits that have conflicted and are still on the verge.
	track := make(map[string]bool)
	conflicts := make(map[string]bool)
	for _, c := range v.Conflicts() {
		track[c] = true
		fmt.Printf("Tracking %v\n", c)
		for _, d := range v.Conflicts() {
			if c != d {
				conflicts[c] = true
				conflicts[d] = true
			}
		}
	}
	fmt.Printf("Verge: %v\n", v.forward)
	fmt.Printf("Initial Conflicts: %v\n", v.Conflicts())

	collapse := func(n *Node) map[string]bool {
		remove := make(map[string]bool)
		for _, e := range mov.GetIn(n) {
			if track[e.Commit] {
				remove[e.Commit] = true
			}
		}
		for _, e := range mov.GetOut(n) {
			delete(remove, e.Commit)
		}
		return remove
	}

	for {
		next := mov.Next()
		if len(next) == 0 {
			panic("ran out of ways to advance the verge before everything converged")
		}

		var n *Node
		// Try to find a node that doesn't collapse anything.
		for _, h := range next {
			m := mov.GetNode(h)
			if len(collapse(m)) == 0 {
				n = m
				fmt.Printf("Found node that didn't collapse anyhting: %q\n", nodeContent(v.r, n.Head))
				break
			}
		}
		// Otherwise any node is fine.
		if n == nil {
			n = mov.GetNode(next[0])
		}

		sat := 0
		for _, e := range mov.GetIn(n) {
			if track[e.Commit] {
				sat++
			}
		}
		if sat == len(track) {
			return mov.GetHead(n), conflicts
		}

		remove := collapse(n)
		for c := range remove {
			fmt.Printf("Collapsing %v\n", c)
			delete(track, c)
		}
		v.move(mov.GetHead(n), mov)
		// v.Advance(mov.GetHead(n))
		for _, c := range v.Conflicts() {
			track[c] = true
			fmt.Printf("Tracking %v\n", c)
			// for _, d := range v.Conflicts() {
			// if c != d {
			conflicts[c] = true
			// conflicts[d] = true
			// }
			// }
		}
	}
}

// TOOD: get rid of this if it ends up being unused.
func ToposortSubgraph(g map[string][]string) []string {
	heads := make(map[string]bool)
	for c := range g {
		heads[c] = false
	}
	for c := range g {
		for _, d := range g[c] {
			delete(heads, d)
		}
	}
	// Heads should now contain only those commits that are undominated.
	var res []string
	used := make(map[string]bool)
	for h := range heads {
		topohelper(g, h, used, &res)
	}
	for i := 0; i < len(res)/2; i++ {
		swap := len(res) - 1 - i
		res[i], res[swap] = res[swap], res[i]
	}
	return res
}

func topohelper(g map[string][]string, cur string, used map[string]bool, res *[]string) {
	if used[cur] {
		return
	}
	for _, c := range g[cur] {
		topohelper(g, c, used, res)
	}
	used[cur] = true
	*res = append(*res, cur)
}

type simpleGraph struct {
	edges map[string]map[string]bool
	nodes map[string]bool
}

func makeSimpleGraph() *simpleGraph {
	return &simpleGraph{
		edges: make(map[string]map[string]bool),
		nodes: make(map[string]bool),
	}
}

func (sg simpleGraph) Clone() *simpleGraph {
	sg2 := simpleGraph{
		edges: make(map[string]map[string]bool),
		nodes: make(map[string]bool),
	}
	for src, edges := range sg.edges {
		if sg2.edges[src] == nil {
			sg2.edges[src] = make(map[string]bool)
		}
		for dst := range edges {
			sg2.edges[src][dst] = true
		}
	}
	for node := range sg.nodes {
		sg2.nodes[node] = true
	}
	return &sg2
}

func (sg simpleGraph) String() string {
	var ret string
	var s []string
	for k := range sg.edges {
		s = append(s, k)
	}
	sort.Strings(s)
	for _, k := range s {
		ret += fmt.Sprintf("%s -> ", k)
		var t []string
		for j := range sg.edges[k] {
			t = append(t, j)
		}
		sort.Strings(t)
		ret += fmt.Sprintf("%v\n", t)
	}
	return ret
}

func (sg *simpleGraph) addNode(n string) {
	sg.nodes[n] = true
}

func (sg *simpleGraph) addEdge(src, dst string) {
	// fmt.Printf("AddEdge %s -> %s\n", src, dst)
	if _, ok := sg.edges[src]; !ok {
		sg.edges[src] = make(map[string]bool)
	}
	sg.edges[src][dst] = true
	if sg.edges[dst] == nil {
		sg.edges[dst] = make(map[string]bool)
	}
}
func (sg *simpleGraph) removeNode(n string) {
	// fmt.Printf("RemoveNode %s\n", n)
	delete(sg.nodes, n)
}

func (sg *simpleGraph) dominators() []string {
	var ds []string
	for node := range sg.nodes {
		if len(sg.edges[node]) == 0 {
			ds = append(ds, node)
		}
	}
	return ds
}
