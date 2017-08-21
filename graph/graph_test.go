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
			EdgeRefs: []graph.EdgeRef{{0, 1}, {1, 2}},
			Contents: []graph.NewContent{
				{
					Path: "foo.txt",
					Form: graph.FormFileSrc,
				},
				{
					Form:    graph.FormText,
					Content: stringsToContent("alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india", ""),
				},
				{
					Path: "foo.txt",
					Form: graph.FormFileSnk,
				},
			},
		}
		graph.Apply(r, c0)

		c1 := &graph.Commit{
			Deps: []string{c0.Hash()},
			EdgeRefs: []graph.EdgeRef{
				{Src: 0, Dst: 2},
				{Src: 2, Dst: 1},
			},
			NodeRefs: []graph.NodeRef{
				{Node: "src:foo.txt", Depth: 3}, // 'bravo'
				{Node: "src:foo.txt", Depth: 9}, // 'hotel'
			},
			Contents: []graph.NewContent{
				{Content: stringsToContent("CHARLIE", "DELTA", "ECHO", "FOXTROT", "GOLF")},
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
				{Node: "src:foo.txt", Depth: 5}, // 'delta'
				{Node: "src:foo.txt", Depth: 6}, // 'echo'
			},
			Contents: []graph.NewContent{
				{Content: stringsToContent("Buttons", "the", "buttonsball")},
			},
		}
		graph.Apply(r, c2)

		// This commit replaces resolves the conflict between c1 and c2.
		c3 := &graph.Commit{
			Deps: []string{c0.Hash(), c1.Hash(), c2.Hash()},
			EdgeRefs: []graph.EdgeRef{
				{Src: 0, Dst: 2},
				{Src: 2, Dst: 1},
			},
			NodeRefs: []graph.NodeRef{
				{Node: "src:foo.txt", Depth: 3},  // 'bravo'
				{Node: "src:foo.txt", Depth: 10}, // 'india'
			},
			Contents: []graph.NewContent{
				{Content: stringsToContent("all", "your", "base", "are", "belong", "to", "us")},
			},
		}
		graph.Apply(r, c3)

		Convey("if the frontier doesn't see conflicts then the verge shouldn't see conflicts", func() {
			v := graph.MakeVerge(r, explicitFrontier(c0, c1), "foo.txt")
			// Should be able to advance until we get to the snk node.
			for n := v.Next()[0]; n != "snk:foo.txt"; n = v.Next()[0] {
				v.Advance(n)
				fmt.Printf("prev: %v\n", v.Prev())
				So(v.Prev(), ShouldContain, n)
				So(len(v.Next()), ShouldBeGreaterThan, 0)
				So(len(v.Conflicts()), ShouldBeZeroValue)
			}

			// Should be able to retract until we get to the snk node.
			for n := v.Prev()[0]; n != "src:foo.txt"; n = v.Prev()[0] {
				v.Retract(n)
				So(v.Next(), ShouldContain, n)
				So(len(v.Prev()), ShouldBeGreaterThan, 0)
				So(len(v.Conflicts()), ShouldBeZeroValue)
			}
		})

		Convey("if the frontier can see conflicts then the verge should see conflicts", func() {
			v := graph.MakeVerge(r, explicitFrontier(c0, c1, c2), "foo.txt")
			foundConflict := false
			// Should be able to advance until we get to the snk node.
			for n := v.Next()[0]; n != "snk:foo.txt"; n = v.Next()[0] {
				v.Advance(n)
				fmt.Printf("prev: %v\n", v.Prev())
				So(v.Prev(), ShouldContain, n)
				So(len(v.Next()), ShouldBeGreaterThan, 0)
				if len(v.Conflicts()) > 0 {
					foundConflict = true
				}
			}
			So(foundConflict, ShouldBeTrue)

			foundConflict = false
			// Should be able to retract until we get to the snk node.
			for n := v.Prev()[0]; n != "src:foo.txt"; n = v.Prev()[0] {
				v.Retract(n)
				So(v.Next(), ShouldContain, n)
				So(len(v.Prev()), ShouldBeGreaterThan, 0)
				if len(v.Conflicts()) > 0 {
					foundConflict = true
					So(len(v.Conflicts()), ShouldEqual, 2)
					So(v.Conflicts(), ShouldContain, c1.Hash())
					So(v.Conflicts(), ShouldContain, c2.Hash())
				}
			}
			So(foundConflict, ShouldBeTrue)
		})

		Convey("if the frontier can see a commit that resolves a conflict then it shouldn't see the conflict", func() {
			v := graph.MakeVerge(r, explicitFrontier(c0, c1, c2, c3), "foo.txt")
			// Should be able to advance until we get to the snk node.
			for n := v.Next()[0]; n != "snk:foo.txt"; n = v.Next()[0] {
				v.Advance(n)
				fmt.Printf("prev: %v\n", v.Prev())
				So(v.Prev(), ShouldContain, n)
				So(len(v.Next()), ShouldBeGreaterThan, 0)
				So(len(v.Conflicts()), ShouldBeZeroValue)
			}

			// Should be able to retract until we get to the snk node.
			for n := v.Prev()[0]; n != "src:foo.txt"; n = v.Prev()[0] {
				v.Retract(n)
				So(v.Next(), ShouldContain, n)
				So(len(v.Prev()), ShouldBeGreaterThan, 0)
				So(len(v.Conflicts()), ShouldBeZeroValue)
			}
		})
	})
}

type simpleFrontier map[string]bool

func (s simpleFrontier) Observes(c string) bool { return s[c] }

func explicitFrontier(commits ...*graph.Commit) simpleFrontier {
	s := make(simpleFrontier)
	for _, c := range commits {
		s[c.Hash()] = true
	}
	return s
}

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
			EdgeRefs: []graph.EdgeRef{{0, 1}, {1, 2}},
			Contents: []graph.NewContent{
				{
					Path: "foo.txt",
					Form: graph.FormFileSrc,
				},
				{
					Form:    graph.FormText,
					Content: content0,
				},
				{
					Path: "foo.txt",
					Form: graph.FormFileSnk,
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
			Deps:     []string{c0.Hash()},
			EdgeRefs: []graph.EdgeRef{{0, 2}, {2, 1}},
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
			Deps:     []string{c0.Hash(), c1.Hash()},
			EdgeRefs: []graph.EdgeRef{{0, 1}},
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
