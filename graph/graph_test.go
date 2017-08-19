package graph_test

import (
	"bytes"
	"os"
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"

	"fmt"

	"github.com/runningwild/jig/graph"
)

func stringsToContent(ss ...string) [][]byte {
	var lines [][]byte
	for _, s := range ss {
		lines = append(lines, []byte(s))
	}
	return lines
}

func contentToString(content [][]byte) string {
	return string(bytes.Join(content, []byte{'\n'}))
}

func TestNodeHashes(t *testing.T) {
	Convey("CalculateNodeHashes", t, func() {
		c0 := stringsToContent("foo", "bar", "wing")
		c1 := stringsToContent("ding", "monkey", "ball")
		head, tail := graph.CalculateNodeHashes("commit", "prev", graph.FormText, append(c0, c1...))
		So(head, ShouldNotEqual, tail)
		head2, middle := graph.CalculateNodeHashes("commit", "prev", graph.FormText, c0)
		So(head2, ShouldEqual, head)
		_, tail2 := graph.CalculateNodeHashes("commit", middle, graph.FormText, c1)
		So(tail2, ShouldEqual, tail)
	})
}

func TestSplitNode(t *testing.T) {
	Convey("SplitNode", t, func() {
		r := graph.MakeFakeRepo()
		sampleContent := stringsToContent("foo", "bar", "wing", "ding", "monkey", "ball", "")
		contentHash := r.PutContent(sampleContent)
		head, tail := graph.CalculateNodeHashes("commit-0", "path:sample.txt", graph.FormText, sampleContent)
		r.PutNode(&graph.Node{
			Head:    head,
			Tail:    tail,
			Form:    graph.FormText,
			Content: contentHash,
			Count:   7,
			In:      []graph.Edge{{Commit: "commit-0", Node: "path:sample.txt", Primary: true}},
			Out:     []graph.Edge{{Commit: "commit-0", Node: "1"}},
		})
		Convey("can split a node where dist < n.Count", func() {
			tail0, head1, err := graph.SplitNode(r, head, 3)
			So(err, ShouldBeNil)
			So(tail0, ShouldNotEqual, "")
			So(head1, ShouldNotEqual, "")
			node0 := r.GetNode(head)
			So(node0, ShouldNotBeNil)
			So(node0.Head, ShouldEqual, head)
			So(node0.Tail, ShouldEqual, tail0)
			So(node0.Out[0].Node, ShouldEqual, head1)
			node1 := r.GetNode(head1)
			So(node1, ShouldNotBeNil)
			So(node1.In[0].Node, ShouldEqual, tail0)
			So(node0.Count, ShouldEqual, 3)
			So(node1.Count, ShouldEqual, 4)
			So(contentToString(r.GetContent(node0.Content)), ShouldEqual, "foo\nbar\nwing")
			So(contentToString(r.GetContent(node1.Content)), ShouldEqual, "ding\nmonkey\nball\n")
			Convey("and and where dist > n.Count", func() {
				tail1, head2, err := graph.SplitNode(r, head, 5)
				So(err, ShouldBeNil)
				So(tail1, ShouldNotEqual, "")
				So(head2, ShouldNotEqual, "")
				node1 := r.GetNode(head1)
				So(node1, ShouldNotBeNil)
				So(node1.Out[0].Node, ShouldEqual, head2)
				node2 := r.GetNode(head2)
				So(node2, ShouldNotBeNil)
				So(r.GetRef(node2.In[0].Node), ShouldEqual, head1)
				So(node0.Count, ShouldEqual, 3)
				So(node1.Count, ShouldEqual, 2)
				So(node2.Count, ShouldEqual, 2)
				So(contentToString(r.GetContent(node0.Content)), ShouldEqual, "foo\nbar\nwing")
				So(contentToString(r.GetContent(node1.Content)), ShouldEqual, "ding\nmonkey")
				So(contentToString(r.GetContent(node2.Content)), ShouldEqual, "ball\n")
			})
		})
		Convey("doesn't split if the split point is at the end of an existing node", func() {
			before := len(r.Nodes)
			a, b, err := graph.SplitNode(r, head, 7)
			So(err, ShouldBeNil)
			So(a, ShouldEqual, tail)
			So(b, ShouldEqual, "1")
			So(len(r.Nodes), ShouldEqual, before)
		})
	})
}

func TestVerge(t *testing.T) {
	Convey("applied commits", t, func() {
		r := graph.MakeFakeRepo()
		c0 := &graph.Commit{
			Deps:     nil,
			EdgeRefs: []graph.EdgeRef{{Src: 0, Dst: -1}},
			Contents: []graph.NewContent{
				{
					Path:    "foo.txt",
					Form:    graph.FormText,
					Content: stringsToContent("alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india", ""),
				},
			},
		}
		graph.Apply(r, c0)

		// This commit capitalizes the lines with 'bravo' and 'charlie'
		c1 := &graph.Commit{
			Deps: []string{c0.Hash()},
			EdgeRefs: []graph.EdgeRef{
				{Src: 0, Dst: 2},
				{Src: 2, Dst: 1},
			},
			NodeRefs: []graph.NodeRef{
				{Node: "src:foo.txt", Depth: 2}, // 'alpha'
				{Node: "src:foo.txt", Depth: 5}, // 'delta'
			},
			Contents: []graph.NewContent{
				{Content: stringsToContent("BRAVO", "CHARLIE")},
			},
		}
		graph.Apply(r, c1)

		// This commit replaces 'charlie' and 'delta'
		c2 := &graph.Commit{
			Deps: []string{c0.Hash()},
			EdgeRefs: []graph.EdgeRef{
				{Src: 0, Dst: 2},
				{Src: 2, Dst: 1},
			},
			NodeRefs: []graph.NodeRef{
				{Node: "src:foo.txt", Depth: 3}, // 'bravo'
				{Node: "src:foo.txt", Depth: 6}, // 'echo'
			},
			Contents: []graph.NewContent{
				{Content: stringsToContent("Charles", "Deltoid")},
			},
		}
		if false {
			graph.Apply(r, c2)
		}

		// This commit replaces 'charlie' and 'delta'
		c3 := &graph.Commit{
			Deps: []string{c1.Hash(), c2.Hash()},
			EdgeRefs: []graph.EdgeRef{
				{Src: 0, Dst: 2},
				{Src: 2, Dst: 1},
			},
			NodeRefs: []graph.NodeRef{
				{Node: "src:foo.txt", Depth: 2}, // 'charlie'
				{Node: "src:foo.txt", Depth: 6}, // 'echo'
			},
			Contents: []graph.NewContent{
				{Content: stringsToContent("all", "your", "base", "are", "belong", "to", "us")},
			},
		}
		if false {
			graph.Apply(r, c3)
		}
		// t.Errorf("C0: %s\n", c0.Hash())
		// t.Errorf("C1: %s\n", c1.Hash())
		// t.Errorf("C2: %s\n", c2.Hash())
		// t.Errorf("C3: %s\n", c3.Hash())
		{
			for h, n := range r.Nodes {
				fmt.Printf("%s\n", h)
				fmt.Printf("%s\n", bytes.Join(r.GetContent(n.Content), []byte("\n")))
				fmt.Printf("\n")
			}
		}
		v := graph.MakeVerge(r, headFrontier{}, "foo.txt")
		// t.Errorf("Next: %v\n", v.Next())
		n := v.Next()[0]
		v.Advance(n)
		So(v.Prev(), ShouldContain, n)
		// t.Errorf("Next: %v\n", v.Next())
		// t.Errorf("CONFLICTS: %v\n", v.Conflicts())
		n = v.Next()[0]
		v.Advance(n)
		So(v.Prev(), ShouldContain, n)
		// t.Errorf("Next: %v\n", v.Next())
		// t.Errorf("CONFLICTS: %v\n", v.Conflicts())
		n = v.Next()[0]
		v.Advance(n)
		So(v.Prev(), ShouldContain, n)
		// t.Errorf("Next: %v\n", v.Next())
		// t.Errorf("CONFLICTS: %v\n", v.Conflicts())
		n = v.Next()[0]
		v.Advance(n)
		So(v.Prev(), ShouldContain, n)
		n = v.Next()[0]
		v.Advance(n)
		So(v.Prev(), ShouldContain, n)
		n = v.Next()[0]
		v.Advance(n)
		So(v.Prev(), ShouldContain, n)
		n = v.Next()[0]
		v.Advance(n)
		So(v.Prev(), ShouldContain, n)
	})
}

type headFrontier struct{}

func (headFrontier) Observes(string) bool { return true }

// TODO: Need to test the following kinds of invalid commits at the very least:
// - Edges that create cycles.
// - An edge from nodes in one file to nodes in another file, without corresponding nodes from the other
//   file back to the first one.  This would cause future modifications to that shared potion to be
//   reflected in both files.
func TestApplyCommits(t *testing.T) {
	Convey("applied commits", t, func() {
		r := graph.MakeFakeRepo()
		content0 := stringsToContent("alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india", "")
		c0 := &graph.Commit{
			Deps:     nil,
			EdgeRefs: []graph.EdgeRef{{Src: 0, Dst: -1}},
			Contents: []graph.NewContent{
				{
					Path:    "foo.txt",
					Form:    graph.FormText,
					Content: content0,
				},
			},
		}
		So(graph.Apply(r, c0), ShouldBeNil)
		So(graph.Apply(r, c0), ShouldNotBeNil) // Can't apply twice

		buf := bytes.NewBuffer(nil)
		So(graph.PrintFile(r, "foo.txt", nil, buf), ShouldBeNil)
		So(string(buf.Bytes()), ShouldResemble, strings.Join([]string{"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india", ""}, "\n"))
		So(graph.PrintFile(r, "bar.txt", nil, buf), ShouldNotBeNil)

		content1 := stringsToContent("BRAVO", "CHARLIE")
		// This commit capitalizes the lines with 'bravo' and 'charlie'
		c1 := &graph.Commit{
			Deps: []string{c0.Hash()},
			EdgeRefs: []graph.EdgeRef{
				{Src: 0, Dst: 2},
				{Src: 2, Dst: 1},
			},
			NodeRefs: []graph.NodeRef{
				{Node: "src:foo.txt", Depth: 2}, // 'alpha'
				{Node: "src:foo.txt", Depth: 5}, // 'delta'
			},
			Contents: []graph.NewContent{
				{Content: content1},
			},
		}

		So(graph.Apply(r, c1), ShouldBeNil)
		buf.Truncate(0)
		So(graph.PrintFile(r, "foo.txt", nil, buf), ShouldBeNil)
		So(string(buf.Bytes()), ShouldResemble, strings.Join([]string{"alpha", "BRAVO", "CHARLIE", "delta", "echo", "foxtrot", "golf", "hotel", "india", ""}, "\n"))

		return
		fmt.Printf("Printing 'foo.txt'----------\n")
		graph.PrintFile(r, "foo.txt", nil, os.Stdout)
		fmt.Printf("----------------------------\n")

		fmt.Printf("Starting with %q\n", "src:foo.txt")
		fmt.Printf("Now to %q\n", r.Nodes["src:foo.txt"].Out[0].Node)
		fmt.Printf("Now to %q\n", r.Nodes[r.Nodes["src:foo.txt"].Out[0].Node].Out[1].Node)
		fmt.Printf(" on commit %q\n", r.Nodes[r.Nodes["src:foo.txt"].Out[0].Node].Out[1].Commit)

		// This should be the node with the capitalized text
		// r.Nodes["src:foo.txt"].Out[1].Node
		c2 := &graph.Commit{
			Deps: []string{c0.Hash(), c1.Hash()},
			EdgeRefs: []graph.EdgeRef{
				{Src: 0, Dst: 1},
			},
			NodeRefs: []graph.NodeRef{
				{
					Node:  r.Nodes[r.Nodes["src:foo.txt"].Out[0].Node].Out[1].Node,
					Depth: 2,
				},
				{
					Node:  "src:foo.txt",
					Depth: 6,
				},
			},
			Contents: nil,
		}

		for _, n := range r.Nodes {
			fmt.Printf("Node:\nHead: %s\nTail: %s\n", n.Head, n.Tail)
			fmt.Printf("Out: %v\n", n.Out)
			fmt.Printf("In: %v\n", n.In)
			fmt.Printf("Content: \n`%s`\n", r.GetContent(n.Content))
			fmt.Printf("\n\n\n\n\n")
		}

		fmt.Printf("Applying a commit...\n")
		if err := graph.Apply(r, c2); err != nil {
			fmt.Printf("Failed to apply commit: %v", err)
			return
		}
		fmt.Printf("Printing 'foo.txt'----------\n")
		graph.PrintFile(r, "foo.txt", nil, os.Stdout)
		fmt.Printf("----------------------------\n")
	})
}

// type testRepo struct {
// 	nodes        map[string]*graph.Node
// 	content      map[string][][]byte
// 	refs         map[string]string
// 	commits      map[string]*graph.Commit
// 	transactions int
// }

// func makeFakeRepo() *testRepo {
// 	return &testRepo{
// 		nodes:   make(map[string]*graph.Node),
// 		content: make(map[string][][]byte),
// 		refs:    make(map[string]string),
// 		commits: make(map[string]*graph.Commit),
// 	}
// }

// func (r *testRepo) GetRef(ptr string) string {
// 	return r.refs[ptr]
// }
// func (r *testRepo) GetNode(nodeHash string) *graph.Node {
// 	return r.Nodes[nodeHash]
// }
// func (r *testRepo) GetCommit(commitHash string) *graph.Commit {
// 	return r.commits[commitHash]
// }
// func (r *testRepo) GetContent(contentHash string) [][]byte {
// 	return r.content[contentHash]
// }
// func (r *testRepo) StartTransaction() {
// 	if r.transactions != 0 {
// 		panic("omg")
// 	}
// 	r.transactions = 1
// }
// func (r *testRepo) EndTransaction() error {
// 	if r.transactions != 1 {
// 		panic("zomg")
// 	}
// 	r.transactions = 0
// 	return nil
// }
// func (r *testRepo) PutRef(ptr, val string) {
// 	r.refs[ptr] = val
// }
// func (r *testRepo) PutNode(n *graph.Node) {
// 	r.Nodes[n.Head] = n
// }
// func (r *testRepo) PutCommit(c *graph.Commit) {
// 	r.commits[c.Hash()] = c
// }
// func (r *testRepo) DeleteNode(nodeHash string) {
// 	delete(r.Nodes, nodeHash)
// }
// func (r *testRepo) PutContent(content [][]byte) string {
// 	hash := hashContent(content)
// 	r.content[hash] = content
// 	return hash
// }
// func hashContent(content [][]byte) string {
// 	h := skein512.NewHash512(128)
// 	for _, line := range content {
// 		length := uint32(len(line))
// 		h.Write([]byte{byte(length), byte(length >> 8), byte(length >> 16), byte(length >> 24)})
// 		h.Write(line)
// 	}
// 	return fmt.Sprintf("%x", h.Sum(nil))
// }
// func (r *testRepo) DeleteContent(contentHash string) {
// 	delete(r.content, contentHash)
// }
