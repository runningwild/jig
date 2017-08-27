package graph_test

import (
	"bytes"
	"os"
	"sort"
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
				{Node: "src:foo.txt", Depth: 8}, // 'golf'
			},
			Contents: []graph.NewContent{
				{Content: stringsToContent("CHARLIE", "DELTA", "ECHO", "FOXTROT")},
			},
		}
		graph.Apply(r, c1)

		//  This inserts some text between delta and echo.
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
				{Content: stringsToContent("buttons", "the", "buttonsball")},
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

		Convey("monkeys", func() {
			// This inserts some text between delta and echo, just like c2, but in all caps.
			c2x := &graph.Commit{
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
					{Content: stringsToContent("BUTTONS", "THE", "BUTTONSBALL")},
				},
			}
			graph.Apply(r, c2x)

			// Deletes 'hotel'
			c4 := &graph.Commit{
				Deps: []string{c0.Hash()},
				EdgeRefs: []graph.EdgeRef{
					{Src: 0, Dst: 1},
				},
				NodeRefs: []graph.NodeRef{
					{Node: "src:foo.txt", Depth: 8},  // 'golf'
					{Node: "src:foo.txt", Depth: 10}, // '' (trailing new-line)
				},
			}
			graph.Apply(r, c4)

			v := graph.MakeVerge(r, explicitFrontier(c0, c1, c2, c2x, c4), "foo.txt")
			var c colorGraph
			allConflicts := make(map[string]bool)
			for n := v.Next()[0]; n != "snk:foo.txt"; n = v.Next()[0] {
				v.Advance(n)
				for _, c0 := range v.Conflicts() {
					for _, c1 := range v.Conflicts() {
						if c0 == c1 {
							continue
						}
						fmt.Printf("Conflict: %v %v\n", c0, c1)
						c.addEdge(c0, c1)
						allConflicts[c0] = true
						allConflicts[c1] = true
					}
				}
			}
			fmt.Printf("%v\n", c)
			colors := c.getColors()

			// Verify that c1, c2 and c2x are all mutually conflicted, and that c0 and c4 are not.
			So(len(colors), ShouldEqual, 3)
			So(len(colors[0]), ShouldEqual, 1)
			So(allConflicts[colors[0][0]], ShouldBeTrue)
			So(len(colors[1]), ShouldEqual, 1)
			So(allConflicts[colors[1][0]], ShouldBeTrue)
			So(len(colors[2]), ShouldEqual, 1)
			So(allConflicts[colors[2][0]], ShouldBeTrue)
			So(len(allConflicts), ShouldEqual, 3)

			buf := bytes.NewBuffer(nil)
			So(graph.PrintPath(r, explicitFrontier(c0, c1, c4), "src:foo.txt", "snk:foo.txt", buf, "."), ShouldBeNil)
			So(buf.String(), ShouldEqual, "alpha.bravo.CHARLIE.DELTA.ECHO.FOXTROT.golf.india.")
			buf.Truncate(0)
			So(graph.PrintPath(r, explicitFrontier(c0, c2, c4), "src:foo.txt", "snk:foo.txt", buf, "."), ShouldBeNil)
			So(buf.String(), ShouldEqual, "alpha.bravo.charlie.delta.buttons.the.buttonsball.echo.foxtrot.golf.india.")

			// NEXT: several steps to covering the full conflict:
			// 1. Retract the verge until a single node is found that begins the conflict.
			//    This requires tracking which commits are in conflict as specified in the doc.
			// 2. Scan forward until the single node at which the conflict ends is found.
			// 3. For every set of conflicting commits, track each pair that conflicts, then find a
			//    coloring so we know how many different sets of commits need to be shown to the user.
			//    This is NP-hard so don't knock your brains out.
			// 4. For each color in the coloring, create a frontier that sees those commits but not the
			//    conflicting ones, then traverse the conflict with that frontier.  Each color's traversal
			//    should be displayed to the user.
		})
	})
}

type colorGraph struct {
	edges map[string]map[string]bool
}

func (c *colorGraph) addEdge(a, b string) {
	if c.edges == nil {
		c.edges = make(map[string]map[string]bool)
	}
	for _, v := range [][2]string{{a, b}, {b, a}} {
		if c.edges[v[0]] == nil {
			c.edges[v[0]] = make(map[string]bool)
		}
		c.edges[v[0]][v[1]] = true
	}
}

func (c *colorGraph) getColors() [][]string {
	if len(c.edges) == 0 {
		return nil
	}

	// Two-coloring is easy, so try that first.
	if colors := c.twoColor(); colors != nil {
		return colors
	}

	var nodes []string
	for node := range c.edges {
		nodes = append(nodes, node)
	}
	sort.Strings(nodes)

	used := make(map[string]bool)
	var colors [][]string
	for len(used) < len(c.edges) {
		var color []string
		adj := make(map[string]bool)
		for _, node := range nodes {
			if used[node] || adj[node] {
				continue
			}
			color = append(color, node)
			used[node] = true
			for m := range c.edges[node] {
				adj[m] = true
			}
		}
		colors = append(colors, color)
	}

	return colors
}

func (c *colorGraph) twoColor() [][]string {
	black := make(map[string]bool)
	for n := range c.edges {
		if _, ok := black[n]; !ok {
			if !c.recursiveTwoColor(n, true, black) {
				return nil
			}
		}
	}
	colors := make([][]string, 2)
	for n, b := range black {
		if b {
			colors[0] = append(colors[0], n)
		} else {
			colors[1] = append(colors[1], n)
		}
	}
	return colors
}

func (c *colorGraph) recursiveTwoColor(n string, color bool, black map[string]bool) bool {
	if b, ok := black[n]; ok {
		return b == color
	}
	black[n] = color
	for m := range c.edges[n] {
		if !c.recursiveTwoColor(m, !color, black) {
			return false
		}
	}
	return true
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
