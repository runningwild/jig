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

	deps, rdeps simpleGraph
}

func MakeVerge(r Repo, f Frontier, path string) *Verge {
	v := &Verge{
		r:        r,
		f:        f,
		forward:  make(map[string]string),
		backward: make(map[string]string),
		deps:     make(simpleGraph),
		rdeps:    make(simpleGraph),
	}
	n := r.GetNode("src:" + path)
	for _, e := range n.Out {
		v.forward[e.Commit] = e.Node
		c := r.GetCommit(e.Commit)
		for _, dep := range c.Deps {
			// v.deps.addEdge(e.Commit, dep)
			v.rdeps.addEdge(dep, e.Commit)
		}
	}
	return v
}

func (v *Verge) move(node string, forward, backward map[string]string, getIn func(*Node) []Edge) {
	n := v.r.GetNode(node)
	for _, e := range getIn(n) {
		if v.forward[e.Commit] != node {
			panic("invalid advance node")
		}
		delete(forward, e.Commit)
		delete(backward, e.Commit)
		v.rdeps.removeNode(e.Commit)
	}
	for _, e := range n.Out {
		forward[e.Commit] = e.Node
		backward[e.Commit] = node
		for _, dep := range v.r.GetCommit(e.Commit).Deps {
			v.rdeps.addEdge(dep, e.Commit)
		}
	}
}

func (v *Verge) Advance(node string) {
	// v.move(node, v.forward, v.backward, func(n *Node) []Edge { return n.In })
	// return
	n := v.r.GetNode(node)
	fmt.Printf("Trying to advance verge past %q\n", node)
	fmt.Printf("%d inputs to this node:\n", len(n.In))
	for _, e := range n.In {
		nn := v.r.GetNode(e.Node)
		var content string
		if nn != nil {
			content = string(bytes.Join(v.r.GetContent(nn.Content), []byte(".")))
		}
		fmt.Printf("    Commit %q -> node %q: %s\n", e.Commit, e.Node, content)
	}
	fmt.Printf("%d outputs from this node:\n", len(n.Out))
	for _, e := range n.Out {
		nn := v.r.GetNode(e.Node)
		var content string
		if nn != nil {
			content = string(bytes.Join(v.r.GetContent(nn.Content), []byte(".")))
		}
		fmt.Printf("    Commit %q -> node %q: %s\n", e.Commit, e.Node, content)
	}
	fmt.Printf("Current forward set:\n")
	for k, v := range v.forward {
		fmt.Printf("    %s %s\n", k, v)
	}
	fmt.Printf("rdeps before:\n%v\n", v.rdeps)

	for _, e := range n.In {
		fmt.Printf("Checking incoming edge from commit %q from node %q\n", e.Commit, e.Node)
		fmt.Printf("  That is a ref for %q\n", v.r.GetRef(e.Node))
		if v.forward[e.Commit] != node {
			panic("invalid advance node")
		}
		delete(v.forward, e.Commit)
		delete(v.backward, e.Commit)
		// v.deps.removeNode(e.Commit)
		v.rdeps.removeNode(e.Commit)
	}
	for _, e := range n.Out {
		v.forward[e.Commit] = e.Node
		v.backward[e.Commit] = node
		for _, dep := range v.r.GetCommit(e.Commit).Deps {
			// v.deps.addEdge(e.Commit, dep)
			v.rdeps.addEdge(dep, e.Commit)
		}
	}
	fmt.Printf("rdeps after:\n%v\n", v.rdeps)
}

func (v *Verge) Retract(node string) {
	fmt.Printf("Back: %v\n", v.backward)
	// v.move(node, v.backward, v.forward, func(n *Node) []Edge { return n.Out })
	// return
	n := v.r.GetNode(node)
	fmt.Printf("Trying to retract verge past %q\n", node)
	fmt.Printf("%d inputs to this node:\n", len(n.In))
	for _, e := range n.In {
		fmt.Printf("    Commit %q -> node %q\n", e.Commit, e.Node)
	}
	fmt.Printf("%d outputs from this node:\n", len(n.Out))
	for _, e := range n.Out {
		fmt.Printf("    Commit %q -> node %q\n", e.Commit, e.Node)
	}
	fmt.Printf("Current backward set:\n")
	for k, v := range v.backward {
		fmt.Printf("    %s %s\n", k, v)
	}
	fmt.Printf("rdeps before:\n%v\n", v.rdeps)

	for _, e := range n.Out {
		fmt.Printf("Checking incoming edge from commit %q from node %q\n", e.Commit, e.Node)
		fmt.Printf("  That is a ref for %q\n", v.r.GetRef(e.Node))
		// if r := v.r.GetRef(node); r != "" {
		// node = r
		// }
		check := v.backward[e.Commit]
		if r := v.r.GetRef(check); r != "" {
			check = r
		}
		if check != node {
			fmt.Printf("Looking for %s\n", node)
			fmt.Printf("Checking it in %v, %v\n", v.backward, v.backward[e.Commit])
			panic("invalid advance node")
		}
		delete(v.backward, e.Commit)
		delete(v.forward, e.Commit)
		// v.deps.removeNode(e.Commit)
		v.rdeps.removeNode(e.Commit)
	}
	for _, e := range n.In {
		fmt.Printf("Adding a backward edge to %s", e.Node)
		fmt.Printf("%s\n", v.r.GetRef(e.Node))
		v.backward[e.Commit] = e.Node
		v.forward[e.Commit] = node
		for _, dep := range v.r.GetCommit(e.Commit).Deps {
			// v.deps.addEdge(e.Commit, dep)
			v.rdeps.addEdge(dep, e.Commit)
		}
	}
	fmt.Printf("rdeps after:\n%v\n", v.rdeps)
	fmt.Printf("Back: %v\n", v.backward)
}

func (v *Verge) Conflicts() []string {
	visible := make(map[string]bool)
	for c := range v.forward {
		if v.f.Observes(c) {
			visible[c] = true
		}
	}

	// Look for a single commit that dominates all of the commits in visible.
	var dominators []string
	for k, v := range v.rdeps {
		if len(v) == 0 {
			dominators = append(dominators, k)
		}
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
	fmt.Printf("Looking through:\n")
	for _, key := range keys {
		var content string
		ns := forward[key]
		if r := v.r.GetRef(ns); r != "" {
			ns = r
		}
		n := v.r.GetNode(ns)
		if n != nil {
			content = string(bytes.Join(v.r.GetContent(n.Content), []byte(".")))
		}
		fmt.Printf("Commit %s -> Node %s: %s\n", key, forward[key], content)
	}
	fmt.Printf("done looking\n")

	// Acceptable nodes are ones whose inputs edges are all on the Verge.
	var good []string
	for dst := range dsts {
		fmt.Printf("Ref %s -> %s\n", dst, v.r.GetRef(dst))
		if v.r.GetRef(dst) != "" {
			dst = v.r.GetRef(dst)
		}
		n := v.r.GetNode(dst)
		count := 0
		for _, e := range getIn(n) {
			target := forward[e.Commit]
			if r := v.r.GetRef(target); r != "" {
				target = r
			}
			fmt.Printf("Checking %v vs %v\n", target, dst)
			if target == dst {
				count++
			}
		}
		if count != len(getIn(n)) {
			continue
		}
		good = append(good, dst)
	}

	return good
}

type simpleGraph map[string]map[string]bool

func (sg simpleGraph) String() string {
	var ret string
	var s []string
	for k := range sg {
		s = append(s, k)
	}
	sort.Strings(s)
	for _, k := range s {
		ret += fmt.Sprintf("%s ->\n", k)
		var t []string
		for j := range sg[k] {
			t = append(t, j)
		}
		sort.Strings(t)
		for _, m := range t {
			ret += fmt.Sprintf("\t\t%s\n", m)
		}
		ret += "\n"
	}
	return ret
}

func (sg simpleGraph) addEdge(src, dst string) {
	fmt.Printf("AddEdge %s -> %s\n", src, dst)
	if _, ok := sg[src]; !ok {
		sg[src] = make(map[string]bool)
	}
	sg[src][dst] = true
	if sg[dst] == nil {
		sg[dst] = make(map[string]bool)
	}
}
func (sg simpleGraph) removeNode(n string) {
	fmt.Printf("RemoveNode %s\n", n)
	var remove []string
	for key := range sg {
		delete(sg[key], n)
		if len(sg[key]) == 0 {
			remove = append(remove, key)
		}
	}
	for _, r := range remove {
		delete(sg, r)
	}
	delete(sg, n)
}
