package graph

import (
	"bytes"
	"fmt"
	"sort"
)

func FindConflicts(r Repo, f Frontier, path string) ([][]*Node, error) {
	n := r.GetNode(fmt.Sprintf("src:%s", path))
	if n == nil {
		return nil, fmt.Errorf("file not found")
	}
	// TODO: Might want to have a simple node validation function that we can call here.

	MakeVerge(r, f, "src:"+path)
	return nil, nil
}

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

// Advance advances the verge past node.  It panics if node is not a valid verge to advance past.
func (v *Verge) Advance(node string) {
	v.move(node, v.forward, v.backward, func(n *Node) []Edge { return n.Out }, func(n *Node) []Edge { return n.In })
}

// Retract retracts the verge past node.  It panics if node is not a valid verge to retract past.
func (v *Verge) Retract(node string) {
	v.move(node, v.backward, v.forward, func(n *Node) []Edge { return n.In }, func(n *Node) []Edge { return n.Out })
}

// move allows us to do Advance and Retract with the same code, all that's required is that we can
// swap v.forward and v.backward, and that we can swap the in and out edges on all nodes.
func (v *Verge) move(node string, forward, backward map[string]string, getForward, getBackward func(n *Node) []Edge) {
	n := v.r.GetNode(node)
	fmt.Printf("Trying to advance verge past %q (%s)\n", node, v.nodeContent(node))
	for _, e := range getBackward(n) {
		if !v.f.Observes(e.Commit) {
			continue
		}

		// Going through the reference is actually only necessary on a Retract.
		check := forward[e.Commit]
		if r := v.r.GetRef(check); r != "" {
			check = r
		}
		if check != node {
			panic("invalid advance node")
		}

		delete(forward, e.Commit)
		delete(backward, e.Commit)
		fmt.Printf("RDEPS remove node %s\n", e.Commit)
		v.rdeps.removeNode(e.Commit)
	}
	for _, e := range getForward(n) {
		if !v.f.Observes(e.Commit) {
			continue
		}
		forward[e.Commit] = e.Node
		backward[e.Commit] = node
		for _, dep := range v.r.GetCommit(e.Commit).Deps {
			v.rdeps.addNode(e.Commit)
			v.rdeps.addEdge(dep, e.Commit)
			fmt.Printf("RDEPS add edge %s -> %s\n", dep, e.Commit)
		}
	}
}

func (v *Verge) nodeContent(node string) string {
	if r := v.r.GetRef(node); r != "" {
		node = r
	}
	return string(bytes.Join(v.r.GetContent(v.r.GetNode(node).Content), []byte(".")))
}

func (v *Verge) state() {
	var keys []string
	for key := range v.forward {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	fmt.Printf("Current forward state (%d edges):\n", len(keys))
	for _, key := range keys {
		var content string
		ns := v.forward[key]
		if r := v.r.GetRef(ns); r != "" {
			ns = r
		}
		n := v.r.GetNode(ns)
		if n != nil {
			content = string(bytes.Join(v.r.GetContent(n.Content), []byte(".")))
		}
		fmt.Printf("Commit %s -> Node %s: %s\n", key, v.forward[key], content)
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
	fmt.Printf("Visible state: %v\n", visible)
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
	return v.look(v.forward, func(n *Node) []Edge { return n.In })
}

func (v *Verge) Prev() []string {
	return v.look(v.backward, func(n *Node) []Edge { return n.Out })
}

func (v *Verge) look(forward map[string]string, getIn func(*Node) []Edge) []string {
	// Get a set of all nodes on the out end of all edges on the Verge.
	dsts := make(map[string]bool)
	for _, dst := range forward {
		dsts[dst] = true
	}

	var keys []string
	for key := range forward {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	// Acceptable nodes are ones whose inputs edges are all on the Verge.
	var good []string
	for dst := range dsts {
		if v.r.GetRef(dst) != "" {
			dst = v.r.GetRef(dst)
		}
		n := v.r.GetNode(dst)
		count := 0
		observable := 0
		for _, e := range getIn(n) {
			if !v.f.Observes(e.Commit) {
				continue
			}
			observable++
			target := forward[e.Commit]
			if r := v.r.GetRef(target); r != "" {
				target = r
			}
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
	fmt.Printf("AddEdge %s -> %s\n", src, dst)
	if _, ok := sg.edges[src]; !ok {
		sg.edges[src] = make(map[string]bool)
	}
	sg.edges[src][dst] = true
	if sg.edges[dst] == nil {
		sg.edges[dst] = make(map[string]bool)
	}
}
func (sg *simpleGraph) removeNode(n string) {
	fmt.Printf("RemoveNode %s\n", n)
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
