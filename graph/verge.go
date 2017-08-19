package graph

import (
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

func (v *Verge) Advance(node string) {
	n := v.r.GetNode(node)
	fmt.Printf("Trying to advance verge past %q\n", node)
	fmt.Printf("%d inputs to this node:\n", len(n.In))
	for _, e := range n.In {
		fmt.Printf("    Commit %q -> node %q\n", e.Commit, e.Node)
	}
	fmt.Printf("%d outputs from this node:\n", len(n.Out))
	for _, e := range n.Out {
		fmt.Printf("    Commit %q -> node %q\n", e.Commit, e.Node)
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
	n := v.r.GetNode(node)
	fmt.Printf("Trying to advance verge past %q\n", node)
	fmt.Printf("%d inputs to this node:\n", len(n.In))
	for _, e := range n.In {
		fmt.Printf("    Commit %q -> node %q\n", e.Commit, e.Node)
	}
	fmt.Printf("%d outputs from this node:\n", len(n.Out))
	for _, e := range n.Out {
		fmt.Printf("    Commit %q -> node %q\n", e.Commit, e.Node)
	}
	fmt.Printf("Current forward set:\n")
	for k, v := range v.forward {
		fmt.Printf("    %s %s\n", k, v)
	}
	fmt.Printf("rdeps before:\n%v\n", v.rdeps)

	for _, e := range n.Out {
		fmt.Printf("Checking incoming edge from commit %q from node %q\n", e.Commit, e.Node)
		fmt.Printf("  That is a ref for %q\n", v.r.GetRef(e.Node))
		if v.backward[e.Commit] != node {
			panic("invalid advance node")
		}
		delete(v.backward, e.Commit)
		delete(v.forward, e.Commit)
		// v.deps.removeNode(e.Commit)
		v.rdeps.removeNode(e.Commit)
	}
	for _, e := range n.Out {
		v.backward[e.Commit] = e.Node
		v.forward[e.Commit] = node
		for _, dep := range v.r.GetCommit(e.Commit).Deps {
			// v.deps.addEdge(e.Commit, dep)
			v.rdeps.addEdge(dep, e.Commit)
		}
	}
	fmt.Printf("rdeps after:\n%v\n", v.rdeps)
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

func transitiveClosureHelper(r Repo, used, tc map[string]bool, commit string) {
	if used[commit] {
		return
	}
	used[commit] = true
	tc[commit] = true
	for _, dep := range r.GetCommit(commit).Deps {
		transitiveClosureHelper(r, used, tc, dep)
	}

}

// TODO: A lot of this depends on the fact that a Verge can never cut two edges from the same commit
// at the same time (excepting the reverse-edge situation).  This needs to be part of verification.
// next returns a list of nodes that could be used as the next node the Verge can pass.
// TODO: handle reverse edges
func (v *Verge) Next() []string {
	// Get a set of all nodes on the out end of all edges on the Verge.
	dsts := make(map[string]bool)
	for _, dst := range v.forward {
		dsts[dst] = true
	}

	// Acceptable nodes are ones whose inputs edges are all on the Verge.
	var good []string
	for dst := range dsts {
		n := v.r.GetNode(dst)
		count := 0
		for _, e := range n.In {
			if v.forward[e.Commit] == dst {
				count++
			}
		}
		if count != len(n.In) {
			continue
		}
		good = append(good, dst)
	}

	return good
}

func (v *Verge) Prev() []string {
	// Get a set of all nodes on the out end of all edges on the Verge.
	dsts := make(map[string]bool)
	for _, dst := range v.backward {
		dsts[dst] = true
	}
	fmt.Printf("Nodes on the verge: %v\n", dsts)

	// Acceptable nodes are ones whose inputs edges are all on the Verge.
	var good []string
	for dst := range dsts {
		n := v.r.GetNode(dst)
		count := 0
		for _, e := range n.Out {
			if v.backward[e.Commit] == dst {
				count++
			}
		}
		if count != len(n.Out) {
			continue
		}
		good = append(good, dst)
	}
	fmt.Printf("Reachable: %v\n", good)
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
