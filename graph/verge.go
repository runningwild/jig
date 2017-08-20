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
			// v.deps.addEdge(e.Commit, dep)
			v.rdeps.addNode(e.Commit)
			v.rdeps.addEdge(dep, e.Commit)
		}
	}
	return v
}

func (v *Verge) Advance(node string) {
	n := v.r.GetNode(node)
	fmt.Printf("Trying to advance verge past %q (%s)\n", node, v.nodeContent(node))

	for _, e := range n.In {
		if !v.f.Observes(e.Commit) {
			fmt.Printf("Frontier doesn't observe edge from commit %s: %s (%s) -> %s (%s)\n", e.Commit, n.Head, v.nodeContent(n.Head), e.Node, v.nodeContent(e.Node))
			continue
		} else {
			fmt.Printf("Frontier observes edge from commit %s: %s (%s) -> %s (%s)\n", e.Commit, n.Head, v.nodeContent(n.Head), e.Node, v.nodeContent(e.Node))
		}
		if v.forward[e.Commit] != node {
			panic("invalid advance node")
		}
		delete(v.forward, e.Commit)
		delete(v.backward, e.Commit)
		fmt.Printf("RDEPS remove node %s\n", e.Commit)
		v.rdeps.removeNode(e.Commit)
	}
	for _, e := range n.Out {
		if !v.f.Observes(e.Commit) {
			fmt.Printf("Frontier doesn't observe edge from commit %s: %s (%s) -> %s (%s)\n", e.Commit, n.Head, v.nodeContent(n.Head), e.Node, v.nodeContent(e.Node))
			continue
		} else {
			fmt.Printf("Frontier observes edge from commit %s: %s (%s) -> %s (%s)\n", e.Commit, n.Head, v.nodeContent(n.Head), e.Node, v.nodeContent(e.Node))
		}
		v.forward[e.Commit] = e.Node
		v.backward[e.Commit] = node
		for _, dep := range v.r.GetCommit(e.Commit).Deps {
			v.rdeps.addNode(e.Commit)
			v.rdeps.addEdge(dep, e.Commit)
			fmt.Printf("RDEPS add edge %s -> %s\n", dep, e.Commit)
		}
	}
}

func (v *Verge) Retract(node string) {
	n := v.r.GetNode(node)
	for _, e := range n.Out {
		if !v.f.Observes(e.Commit) {
			fmt.Printf("Frontier doesn't observe edge from commit %s: %s (%s) -> %s (%s)\n", e.Commit, n.Head, v.nodeContent(n.Head), e.Node, v.nodeContent(e.Node))
			continue
		} else {
			fmt.Printf("Frontier observes edge from commit %s: %s (%s) -> %s (%s)\n", e.Commit, n.Head, v.nodeContent(n.Head), e.Node, v.nodeContent(e.Node))
		}
		check := v.backward[e.Commit]
		if r := v.r.GetRef(check); r != "" {
			check = r
		}
		if check != node {
			panic("invalid advance node")
		}
		delete(v.backward, e.Commit)
		delete(v.forward, e.Commit)
		v.rdeps.removeNode(e.Commit)
	}
	for _, e := range n.In {
		if !v.f.Observes(e.Commit) {
			fmt.Printf("Frontier doesn't observe edge from commit %s: %s (%s) -> %s (%s)\n", e.Commit, n.Head, v.nodeContent(n.Head), e.Node, v.nodeContent(e.Node))
			continue
		} else {
			fmt.Printf("Frontier observes edge from commit %s: %s (%s) -> %s (%s)\n", e.Commit, n.Head, v.nodeContent(n.Head), e.Node, v.nodeContent(e.Node))
		}
		v.backward[e.Commit] = e.Node
		v.forward[e.Commit] = node
		for _, dep := range v.r.GetCommit(e.Commit).Deps {
			v.rdeps.addNode(e.Commit)
			v.rdeps.addEdge(dep, e.Commit)
		}
	}
	// fmt.Printf("rdeps after:\n%v\n", v.rdeps)
	// fmt.Printf("Back: %v\n", v.backward)
}
func (v *Verge) nodeContent(node string) string {
	if r := v.r.GetRef(node); r != "" {
		node = r
	}
	// if v.r.GetNode(node) == nil {
	// 	return fmt.Sprintf("%s is invalid", node)
	// }
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
	// var dominators []string
	// fmt.Printf("rdeps: %v\n", v.rdeps)
	// for k, v := range v.rdeps.edges {
	// 	if len(v) == 0 {
	// 		dominators = append(dominators, k)
	// 	} else {
	// 		fmt.Printf("%s is dominated by %v\n", k, v)
	// 	}
	// }
	// fmt.Printf("dominators: %v\n", dominators)
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
	// fmt.Printf("Looking through:\n")
	// for _, key := range keys {
	// 	var content string
	// 	ns := forward[key]
	// 	if r := v.r.GetRef(ns); r != "" {
	// 		ns = r
	// 	}
	// 	n := v.r.GetNode(ns)
	// 	if n != nil {
	// 		content = string(bytes.Join(v.r.GetContent(n.Content), []byte(".")))
	// 	}
	// 	fmt.Printf("Commit %s -> Node %s: %s\n", key, forward[key], content)
	// }
	// fmt.Printf("done looking\n")

	// Acceptable nodes are ones whose inputs edges are all on the Verge.
	var good []string
	for dst := range dsts {
		// fmt.Printf("Ref %s -> %s\n", dst, v.r.GetRef(dst))
		if v.r.GetRef(dst) != "" {
			dst = v.r.GetRef(dst)
		}
		n := v.r.GetNode(dst)
		count := 0
		observable := 0
		for _, e := range getIn(n) {
			if !v.f.Observes(e.Commit) {
				fmt.Printf("Frontier doesn't observe edge from commit %s: %s (%s) -> %s (%s)\n", e.Commit, n.Head, v.nodeContent(n.Head), e.Node, v.nodeContent(e.Node))
				continue
			} else {
				fmt.Printf("Frontier observes edge from commit %s: %s (%s) -> %s (%s)\n", e.Commit, n.Head, v.nodeContent(n.Head), e.Node, v.nodeContent(e.Node))
			}
			observable++
			target := forward[e.Commit]
			if r := v.r.GetRef(target); r != "" {
				target = r
			}
			// fmt.Printf("Checking %v vs %v\n", target, dst)
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
