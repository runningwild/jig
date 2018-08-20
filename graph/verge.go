package graph

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	jpb "github.com/runningwild/jig/proto"
)

type Verge struct {
	r Repo
	f Frontier

	// Maps from commit hash to node hash for each edge on the verge.
	forward, backward map[string]string

	// rdeps is a sloppy representation of a subgraph of the dependecy graph.  We permanently track
	// any edges that are added to rdeps, but we will add and remove nodes, which really means we
	// changing which commits are being 'tracked' by the verge.
	rdeps *simpleGraph
}

func MakeVerge(r Repo, f Frontier, path string) *Verge {
	v := &Verge{
		r:        r,
		f:        f,
		forward:  make(map[string]string),
		backward: make(map[string]string),
		rdeps:    makeSimpleGraph(),
	}
	n := r.GetNode("src:" + path)
	fmt.Printf("src node out edges: %v\n", n.Out)
	for _, e := range n.Out {
		v.forward[e.Commit] = e.Node
		c := r.GetCommit(e.Commit)
		fmt.Printf("Deps on src: %v\n", c.Deps)
		v.rdeps.addNode(e.Commit)
		for _, dep := range c.Deps {
			v.rdeps.addEdge(dep, e.Commit)
		}
	}
	fmt.Printf("RDEPS: %v\n", v.rdeps)
	return v
}

func (v *Verge) Clone() *Verge {
	v2 := &Verge{
		r:        v.r,
		f:        v.f,
		forward:  make(map[string]string),
		backward: make(map[string]string),
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
	fmt.Printf("Moving (%T) past %s\n", mov, n.Head)
	fmt.Printf("In:  %v\n", mov.GetIn(n))
	fmt.Printf("Out: %v\n", mov.GetOut(n))
	for _, e := range mov.GetIn(n) {
		// We can ignore this edge if the frontier doesn't observe it, or if it is a join edge,
		// since in that case that commit will continue through the entire node and the verge will
		// still be cutting it after it has passed it.
		if !v.f.Observes(e.Commit) || e.Join {
			continue
		}

		delete(mov.ForwardEdges(), e.Commit)
		delete(mov.BackwardEdges(), e.Commit)
		fmt.Printf("removing %s from rdeps\n", e.Commit)
		v.rdeps.removeNode(e.Commit)
	}
	for _, e := range mov.GetOut(n) {
		if !v.f.Observes(e.Commit) {
			continue
		}
		mov.ForwardEdges()[e.Commit] = mov.GetHead(mov.GetNode(e.Node))
		mov.BackwardEdges()[e.Commit] = mov.GetTail(mov.GetNode(node))
		v.rdeps.addNode(e.Commit)
		for _, dep := range v.r.GetCommit(e.Commit).Deps {
			fmt.Printf("adding %s -> %s to rdeps\n", e.Commit, dep)
			v.rdeps.addEdge(dep, e.Commit)
		}
	}
	if strings.HasPrefix(node, "snk:") && len(v.rdeps.nodes) == 0 {
		v.backward[""] = node
	}
	if strings.HasPrefix(node, "src:") && len(v.rdeps.nodes) == 0 {
		v.forward[""] = node
	}
	fmt.Printf("Dominators: %v\n", v.rdeps.dominators())
	fmt.Printf("Rdeps:\n%v\n", v.rdeps)
}

func nodeContent(r Repo, node string) string {
	if r := r.GetRef(node); r != "" {
		node = r
	}
	n := r.GetNode(node)
	s := string(bytes.Join(r.GetContent(n.GetContentHash()), []byte(".")))
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
	if len(dominators) == 0 && v.forward[""] == "" && v.backward[""] == "" {
		fmt.Printf("%v\n", v.rdeps.nodes)
		fmt.Printf("%v\n", v.rdeps.edges)
		panic("no dominators is impossible")
	}
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

	// fmt.Printf("Look Keys: %v\n", keys)

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
			// Why did I ever use rdeps to decide what an appropriate out edge was!?!?
			// if target == dst || v.rdeps.nodes[e.Commit] {
			if target == dst {
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
	GetIn(n *jpb.Node) []*jpb.Edge
	GetOut(n *jpb.Node) []*jpb.Edge
	GetHead(n *jpb.Node) string
	GetTail(n *jpb.Node) string
	GetNode(n string) *jpb.Node
	Next() []string
	Prev() []string
	ForwardEdges() map[string]string
	BackwardEdges() map[string]string
}

func (v *Verge) forwardMover() mover {
	return (*forwardMover)(v)
}

type forwardMover Verge

func (f *forwardMover) GetIn(n *jpb.Node) []*jpb.Edge {
	return n.In
}
func (f *forwardMover) GetOut(n *jpb.Node) []*jpb.Edge {
	return n.Out
}
func (f *forwardMover) GetHead(n *jpb.Node) string {
	return n.Head
}
func (f *forwardMover) GetTail(n *jpb.Node) string {
	return n.Tail
}
func (f *forwardMover) GetNode(n string) *jpb.Node {
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

func (b *backwardMover) GetIn(n *jpb.Node) []*jpb.Edge {
	return n.Out
}
func (b *backwardMover) GetOut(n *jpb.Node) []*jpb.Edge {
	return n.In
}
func (b *backwardMover) GetHead(n *jpb.Node) string {
	return n.Tail
}
func (b *backwardMover) GetTail(n *jpb.Node) string {
	return n.Head
}
func (b *backwardMover) GetNode(n string) *jpb.Node {
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

func (v *Verge) AdvanceUntilConflicted() (conflited bool) {
	for len(v.Conflicts()) == 0 {
		next := v.Next()
		if len(next) == 0 {
			return false
		}
		fmt.Printf("RDeps: %v\n", v.rdeps)
		fmt.Printf("%d options for advancing:\n", len(next))
		x := make([]string, len(next))
		copy(x, next)
		sort.Strings(x)
		for _, n := range x {
			fmt.Printf("  %s: %s\n", n, nodeContent(v.r, n))
		}
		node := next[0]
		fmt.Printf("Advancing past %s: %s\n", node, nodeContent(v.r, node))
		v.Advance(node)
	}
	return true
}
func (v *Verge) RetractUntilConflicted() (conflited bool) {
	for len(v.Conflicts()) == 0 {
		prev := v.Prev()
		if len(prev) == 0 {
			return false
		}
		node := prev[0]
		v.Retract(node)
	}
	return true
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

	// collapse returns the commits that would be collapsed by moving past n.
	collapse := func(n *jpb.Node) map[string]bool {
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
		var trackList []string
		for t := range track {
			trackList = append(trackList, t)
		}
		fmt.Printf("Tracking: %v\n", trackList)

		next := mov.Next()
		if len(next) == 0 {
			panic("ran out of ways to advance the verge before everything converged")
		}

		var n *jpb.Node
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

		// It is easier to collapse everything at once because we don't have to worry about a commit
		// that has an incoming and outgoing edge at a node.  If all tracked commits do that then we
		// can actually stop here, otherwise we'll have to keep tracking them all.  This is why sat
		// manually checks which commits it can satisfy rather than just checking len(collapse(n)).
		sat := 0
		for _, e := range mov.GetIn(n) {
			if track[e.Commit] {
				sat++
			}
		}
		if sat == len(track) {
			fmt.Printf("Completed because %v can complete %v : %v\n", n, collapse(n), track)
			return mov.GetHead(n), conflicts
		}

		remove := collapse(n)
		for c := range remove {
			fmt.Printf("Collapsing %v\n", c)
			delete(track, c)
		}
		fmt.Printf("Moving past %v\n", n)
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

// We have to advance until we find a conflict, then advance until converged, then retract until we
// find a conflict, then retract until converged.  *UntilConverged may or may not return with the
// verge in conflict because it's not always obvious at the start or end of a conflict.
// var bean = 0

func findConflicts(v *Verge) ([]Conflict, error) {
	if !v.AdvanceUntilConflicted() {
		return nil, nil
	}
	end, commits := v.AdvanceUntilConverged()
	v2 := v.Clone()
	if !v2.RetractUntilConflicted() {
		return nil, fmt.Errorf("verge is broken")
	}
	start, commits2 := v2.RetractUntilConverged()
	for k, v := range commits {
		if commits2[k] != v {
			return nil, fmt.Errorf("conflict detection failed")
		}
	}
	for k, v := range commits2 {
		if commits[k] != v {
			return nil, fmt.Errorf("conflict detection failed")
		}
	}
	v.Advance(end)
	conflicts, err := findConflicts(v)
	if err != nil {
		return nil, err
	}
	return append([]Conflict{Conflict{Start: start, End: end, Commits: commits}}, conflicts...), nil
}

func commitSetToListList(r Repo, set map[string]bool) [][]string {
	return nil // lists
}

func FindConflicts(r Repo, f Frontier, path string) ([]Conflict, error) {
	cs, err := findConflicts(MakeVerge(r, f, path))
	if err != nil {
		return nil, err
	}

	for i := range cs {
		if err := cs[i].computeGroups(r); err != nil {
			return nil, fmt.Errorf("error compute conflicts: %v", err)
		}
	}

	return cs, nil
}

func (c *Conflict) computeGroups(r Repo) error {
	var commitHashes []string
	for commitHash := range c.Commits {
		commitHashes = append(commitHashes, commitHash)
	}
	sort.Strings(commitHashes)
	reverse := make(map[string]int)
	for i, commitHash := range commitHashes {
		reverse[commitHash] = i
	}

	var relDeps [][]string
	for _, commitHash := range commitHashes {
		commit := r.GetCommit(commitHash)
		if commit == nil {
			return fmt.Errorf("failed to get commit %q", commitHash)
		}
		var relDep []string
		for _, dep := range commit.Deps {
			if _, ok := c.Commits[dep]; ok {
				relDep = append(relDep, dep)
			}
		}
		sort.Strings(relDep) // should already be sorted but whatever
		relDeps = append(relDeps, relDep)
	}
	relDepsInts := make([][]int, len(relDeps))
	for i := range relDeps {
		for j := range relDeps[i] {
			relDepsInts[i] = append(relDepsInts[i], reverse[relDeps[i][j]])
		}
	}

	// tcs[i] will represent the transitive closure of commits in c.Commits that commit[i] depends on.
	// tcs[i] will be nil if it hasn't been filled out yet, and a non-nil, empty slice if it has.
	tcs := make([][]int, len(commitHashes))
	necks := make([]bool, len(tcs))
	fmt.Printf("Compute TC for %v\n", relDepsInts)
	for i := range tcs {
		computeTC(i, relDepsInts, tcs)
		for j := range tcs[i] {
			necks[tcs[i][j]] = true
		}
	}
	fmt.Printf("Computed TCs: %v\n", tcs)
	fmt.Printf("Computed Necks: %v\n", necks)
	c.Groups = nil
	for i := range necks {
		if necks[i] {
			continue
		}
		group := make([]string, len(tcs[i]))
		for j := range tcs[i] {
			group[j] = commitHashes[tcs[i][j]]
		}
		group = append(group, commitHashes[i])
		sort.Strings(group)
		c.Groups = append(c.Groups, group)
	}
	fmt.Printf("Computed Groups: %v\n", c.Groups)
	return nil
}

func computeTC(index int, deps [][]int, tcs [][]int) {
	if tcs[index] != nil {
		return
	}
	cover := make([]bool, len(deps))
	for _, dep := range deps[index] {
		cover[dep] = true
	}
	for _, dep := range deps[index] {
		computeTC(dep, deps, tcs)
		for _, c := range tcs[dep] {
			cover[c] = true
		}
	}
	var tc []int
	for i, c := range cover {
		if c {
			tc = append(tc, i)
		}
	}
	tcs[index] = tc
}

// NEXT: The Commits field needs to be changed to handle the fact that A,B can conflict with C, but
// A and B don't conflict with eachother.  It should be sufficient to just group together chains of commits.
// Oddly enough I think Union Find is the best way to do this.
type Conflict struct {
	Start   string
	End     string
	Groups  [][]string // each element is a list of commits that agree on one version of the conflict
	Commits map[string]bool
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
	var nodes []string
	for n := range sg.nodes {
		nodes = append(nodes, n)
	}
	sort.Strings(nodes)
	var ret string
	ret += fmt.Sprintf("Nodes: %v\n", nodes)

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
		count := 0
		for dep := range sg.edges[node] {
			if sg.nodes[dep] {
				count++
				break
			}
		}
		if count == 0 {
			ds = append(ds, node)
		}
	}
	return ds
}
