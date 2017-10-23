package graph_test

import (
	"bytes"
	"fmt"
	"sort"
	"testing"

	. "github.com/smartystreets/goconvey/convey"

	"github.com/runningwild/jig"
	"github.com/runningwild/jig/graph"
	"github.com/runningwild/jig/testutils"
)

func stringsToContent(ss ...string) [][]byte {
	var lines [][]byte
	for _, s := range ss {
		lines = append(lines, []byte(s))
	}
	return lines
}

func contentToString(content [][]byte) string {
	return string(bytes.Join(content, []byte{'.'}))
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
		r := testutils.MakeFakeRepo()
		sampleContent := stringsToContent("alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf")
		contentHash := r.PutContent(sampleContent)
		head, tail := graph.CalculateNodeHashes("commit-0", "src:sample.txt", graph.FormText, sampleContent)
		r.PutNode(&graph.Node{
			Head:    head,
			Tail:    tail,
			Form:    graph.FormText,
			Content: contentHash,
			Count:   7,
			In:      []graph.Edge{{Commit: "commit-0", Node: "src:sample.txt", Primary: true}},
			Out:     []graph.Edge{{Commit: "commit-0", Node: "snk:sample.txt", Primary: true}},
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
			So(contentToString(r.GetContent(node0.Content)), ShouldEqual, "alpha.bravo.charlie")
			So(contentToString(r.GetContent(node1.Content)), ShouldEqual, "delta.echo.foxtrot.golf")
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
				So(contentToString(r.GetContent(node0.Content)), ShouldEqual, "alpha.bravo.charlie")
				So(contentToString(r.GetContent(node1.Content)), ShouldEqual, "delta.echo")
				So(contentToString(r.GetContent(node2.Content)), ShouldEqual, "foxtrot.golf")
			})
		})
		Convey("doesn't split if the split point is at the end of an existing node", func() {
			before := len(r.Nodes)
			a, b, err := graph.SplitNode(r, head, 7)
			So(err, ShouldBeNil)
			So(a, ShouldEqual, tail)
			So(b, ShouldEqual, "snk:sample.txt")
			So(len(r.Nodes), ShouldEqual, before)
		})
	})
}

func TestCommits(t *testing.T) {
	Convey("Commits", t, func() {
		r := testutils.MakeFakeRepo()
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
					Content: stringsToContent("alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf"),
				},
				{
					Path: "foo.txt",
					Form: graph.FormFileSnk,
				},
			},
		}
		So(graph.Apply(r, c0), ShouldBeNil)

		Convey("can delete the first line of a file", func() {
			c1 := &graph.Commit{
				Deps: []string{c0.Hash()},
				EdgeRefs: []graph.EdgeRef{
					{Src: 0, Dst: 1},
				},
				NodeRefs: []graph.NodeRef{
					{Node: "src:foo.txt", Depth: 0},
					{Node: "src:foo.txt", Depth: 3}, // 'bravo'
				},
				Contents: nil,
			}
			So(graph.Apply(r, c1), ShouldBeNil)
			data, err := graph.ReadVersion(r, allFrontier{}, "src:foo.txt", "snk:foo.txt", &graph.ReadMetadata{})
			So(err, ShouldBeNil)
			So(string(bytes.Join(data, []byte("."))), ShouldEqual, "bravo.charlie.delta.echo.foxtrot.golf")
		})

		Convey("can modify the first line of a file", func() {
			c1 := &graph.Commit{
				Deps: []string{c0.Hash()},
				EdgeRefs: []graph.EdgeRef{
					{Src: 0, Dst: 2},
					{Src: 2, Dst: 1},
				},
				NodeRefs: []graph.NodeRef{
					{Node: "src:foo.txt", Depth: 0},
					{Node: "src:foo.txt", Depth: 3}, // 'bravo'
				},
				Contents: []graph.NewContent{
					{Content: stringsToContent("ALPHA")},
				},
			}
			So(graph.Apply(r, c1), ShouldBeNil)
			data, err := graph.ReadVersion(r, allFrontier{}, "src:foo.txt", "snk:foo.txt", &graph.ReadMetadata{})
			So(err, ShouldBeNil)
			So(string(bytes.Join(data, []byte("."))), ShouldEqual, "ALPHA.bravo.charlie.delta.echo.foxtrot.golf")
		})

		Convey("can delete the last line of a file", func() {
			c1 := &graph.Commit{
				Deps: []string{c0.Hash()},
				EdgeRefs: []graph.EdgeRef{
					{Src: 0, Dst: 1},
				},
				NodeRefs: []graph.NodeRef{
					{Node: "src:foo.txt", Depth: 7}, // 'foxtrot'
					{Node: "snk:foo.txt", Depth: 0},
				},
				Contents: nil,
			}
			So(graph.Apply(r, c1), ShouldBeNil)
			data, err := graph.ReadVersion(r, allFrontier{}, "src:foo.txt", "snk:foo.txt", &graph.ReadMetadata{})
			So(err, ShouldBeNil)
			So(string(bytes.Join(data, []byte("."))), ShouldEqual, "alpha.bravo.charlie.delta.echo.foxtrot")
		})

		Convey("can modify the last line of a file", func() {
			c1 := &graph.Commit{
				Deps: []string{c0.Hash()},
				EdgeRefs: []graph.EdgeRef{
					{Src: 0, Dst: 2},
					{Src: 2, Dst: 1},
				},
				NodeRefs: []graph.NodeRef{
					{Node: "src:foo.txt", Depth: 7}, // 'foxtrot'
					{Node: "snk:foo.txt", Depth: 0},
				},
				Contents: []graph.NewContent{
					{Content: stringsToContent("GOLF")},
				},
			}
			So(graph.Apply(r, c1), ShouldBeNil)
			data, err := graph.ReadVersion(r, allFrontier{}, "src:foo.txt", "snk:foo.txt", &graph.ReadMetadata{})
			So(err, ShouldBeNil)
			So(string(bytes.Join(data, []byte("."))), ShouldEqual, "alpha.bravo.charlie.delta.echo.foxtrot.GOLF")
		})

		Convey("can move the first line of a file into the middle of the file", func() {
			c1 := &graph.Commit{
				Deps: []string{c0.Hash()},
				EdgeRefs: []graph.EdgeRef{
					{Src: 0, Dst: 2},
					{Src: 3, Dst: 1},
					{Src: 1, Dst: 4},
				},
				NodeRefs: []graph.NodeRef{
					{Node: "src:foo.txt", Depth: 0},
					{Node: "src:foo.txt", Depth: 2}, // 'alpha'
					{Node: "src:foo.txt", Depth: 3}, // 'bravo'
					{Node: "src:foo.txt", Depth: 5}, // 'delta'
					{Node: "src:foo.txt", Depth: 6}, // 'echo'
				},
				Contents: []graph.NewContent{},
			}
			So(graph.Apply(r, c1), ShouldBeNil)
			data, err := graph.ReadVersion(r, allFrontier{}, "src:foo.txt", "snk:foo.txt", &graph.ReadMetadata{})
			So(err, ShouldBeNil)
			So(string(bytes.Join(data, []byte("."))), ShouldEqual, "bravo.charlie.delta.alpha.echo.foxtrot.golf")
		})

		Convey("can move the last line of a file into the middle of the file", func() {
			c1 := &graph.Commit{
				Deps: []string{c0.Hash()},
				EdgeRefs: []graph.EdgeRef{
					{Src: 0, Dst: 3},
					{Src: 3, Dst: 1},
					{Src: 2, Dst: 4},
				},
				NodeRefs: []graph.NodeRef{
					{Node: "src:foo.txt", Depth: 4}, // 'charlie'
					{Node: "src:foo.txt", Depth: 5}, // 'delta'
					{Node: "src:foo.txt", Depth: 7}, // 'foxtrot'
					{Node: "src:foo.txt", Depth: 8}, // 'golf'
					{Node: "snk:foo.txt", Depth: 0},
				},
				Contents: []graph.NewContent{},
			}
			So(graph.Apply(r, c1), ShouldBeNil)
			data, err := graph.ReadVersion(r, allFrontier{}, "src:foo.txt", "snk:foo.txt", &graph.ReadMetadata{})
			So(err, ShouldBeNil)
			So(string(bytes.Join(data, []byte("."))), ShouldEqual, "alpha.bravo.charlie.golf.delta.echo.foxtrot")
		})

		Convey("can move the first two lines of a file into the middle of the file", func() {
			c1 := &graph.Commit{
				Deps: []string{c0.Hash()},
				EdgeRefs: []graph.EdgeRef{
					{Src: 0, Dst: 3},
					{Src: 4, Dst: 1},
					{Src: 2, Dst: 5},
				},
				NodeRefs: []graph.NodeRef{
					{Node: "src:foo.txt", Depth: 0},
					{Node: "src:foo.txt", Depth: 2}, // 'alpha'
					{Node: "src:foo.txt", Depth: 3}, // 'bravo'
					{Node: "src:foo.txt", Depth: 4}, // 'charlie'
					{Node: "src:foo.txt", Depth: 5}, // 'delta'
					{Node: "src:foo.txt", Depth: 6}, // 'echo'
				},
				Contents: []graph.NewContent{},
			}
			So(graph.Apply(r, c1), ShouldBeNil)
			data, err := graph.ReadVersion(r, allFrontier{}, "src:foo.txt", "snk:foo.txt", &graph.ReadMetadata{})
			So(err, ShouldBeNil)
			So(string(bytes.Join(data, []byte("."))), ShouldEqual, "charlie.delta.alpha.bravo.echo.foxtrot.golf")
		})

		Convey("can move the last two lines of a file into the middle of the file", func() {
			c1 := &graph.Commit{
				Deps: []string{c0.Hash()},
				EdgeRefs: []graph.EdgeRef{
					{Src: 0, Dst: 3},
					{Src: 4, Dst: 1},
					{Src: 2, Dst: 5},
				},
				NodeRefs: []graph.NodeRef{
					{Node: "src:foo.txt", Depth: 4}, // 'charlie'
					{Node: "src:foo.txt", Depth: 5}, // 'delta'
					{Node: "src:foo.txt", Depth: 6}, // 'echo'
					{Node: "src:foo.txt", Depth: 7}, // 'foxtrot'
					{Node: "src:foo.txt", Depth: 8}, // 'golf'
					{Node: "snk:foo.txt", Depth: 0},
				},
				Contents: []graph.NewContent{},
			}
			So(graph.Apply(r, c1), ShouldBeNil)
			data, err := graph.ReadVersion(r, allFrontier{}, "src:foo.txt", "snk:foo.txt", &graph.ReadMetadata{})
			So(err, ShouldBeNil)
			So(string(bytes.Join(data, []byte("."))), ShouldEqual, "alpha.bravo.charlie.foxtrot.golf.delta.echo")
		})
	})
}

func TestVerge(t *testing.T) {
	Convey("applied commits", t, func() {
		r := testutils.MakeFakeRepo()
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
				So(v.Prev(), ShouldContain, r.GetNode(n).Tail)
				So(len(v.Next()), ShouldBeGreaterThan, 0)
				So(len(v.Conflicts()), ShouldBeZeroValue)
			}

			// Should be able to retract until we get to the snk node.
			for n := v.Prev()[0]; n != "src:foo.txt"; n = v.Prev()[0] {
				v.Retract(n)
				So(v.Next(), ShouldContain, r.GetRef(n))
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
				So(v.Prev(), ShouldContain, r.GetNode(n).Tail)
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
				So(v.Next(), ShouldContain, r.GetRef(n))
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
				So(v.Prev(), ShouldContain, r.GetNode(n).Tail)
				So(len(v.Next()), ShouldBeGreaterThan, 0)
				So(len(v.Conflicts()), ShouldBeZeroValue)
			}

			// Should be able to retract until we get to the snk node.
			for n := v.Prev()[0]; n != "src:foo.txt"; n = v.Prev()[0] {
				v.Retract(n)
				So(v.Next(), ShouldContain, r.GetRef(n))
				So(len(v.Prev()), ShouldBeGreaterThan, 0)
				So(len(v.Conflicts()), ShouldBeZeroValue)
			}
		})

		Convey("advancement functions can find conflicts", func() {
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
			allConflicts := make(map[string]bool)
			for n := v.Next()[0]; n != "snk:foo.txt"; n = v.Next()[0] {
				v.Advance(n)
				for _, c0 := range v.Conflicts() {
					for _, c1 := range v.Conflicts() {
						if c0 == c1 {
							continue
						}
						allConflicts[c0] = true
						allConflicts[c1] = true
					}
				}
			}

			// capitalizes 'bravo' and 'charlie'
			c5a := &graph.Commit{
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
			graph.Apply(r, c5a)

			// capitalizes 'echo' and 'foxtrot'
			c5b := &graph.Commit{
				Deps: []string{c0.Hash()},
				EdgeRefs: []graph.EdgeRef{
					{Src: 0, Dst: 2},
					{Src: 2, Dst: 1},
				},
				NodeRefs: []graph.NodeRef{
					{Node: "src:foo.txt", Depth: 5}, // 'delta'
					{Node: "src:foo.txt", Depth: 8}, // 'golf'
				},
				Contents: []graph.NewContent{
					{Content: stringsToContent("ECHO", "FOXTROT")},
				},
			}
			graph.Apply(r, c5b)

			// munges 'bravo', 'charlie', and 'delta'
			c6a := &graph.Commit{
				Deps: []string{c0.Hash()},
				EdgeRefs: []graph.EdgeRef{
					{Src: 0, Dst: 2},
					{Src: 2, Dst: 1},
				},
				NodeRefs: []graph.NodeRef{
					{Node: "src:foo.txt", Depth: 2}, // 'alpha'
					{Node: "src:foo.txt", Depth: 6}, // 'echo'
				},
				Contents: []graph.NewContent{
					{Content: stringsToContent("brAvO", "chArlIE", "dEltA")},
				},
			}
			graph.Apply(r, c6a)

			// munges 'echo' and 'foxtrot'
			c6b := &graph.Commit{
				Deps: []string{c0.Hash()},
				EdgeRefs: []graph.EdgeRef{
					{Src: 0, Dst: 2},
					{Src: 2, Dst: 1},
				},
				NodeRefs: []graph.NodeRef{
					{Node: "src:foo.txt", Depth: 6}, // 'echo'
					{Node: "src:foo.txt", Depth: 8}, // 'golf'
				},
				Contents: []graph.NewContent{
					{Content: stringsToContent("fOxtrOt")},
				},
			}
			graph.Apply(r, c6b)

			fmt.Printf("c0(%s): %s\n", c0.Hash(), "all the stuff")
			fmt.Printf("c5a(%s): %s\n", c5a.Hash(), "BRAVO.CHARLIE")
			fmt.Printf("c5b(%s): %s\n", c5b.Hash(), "ECHO.FOXTROT")
			fmt.Printf("c6a(%s): %s\n", c6a.Hash(), "brAvO.chArlIE.dEltA")
			fmt.Printf("c6b(%s): %s\n", c6b.Hash(), "fOxtrOt")
			fmt.Printf("Advancing Verge\n")

			f := explicitFrontier(c0, c5a, c5b, c6a, c6b)
			// We'll set these values with one of the two following Convey stanzas.  Either we
			// will advance the verge all the way forward, then backward, or we will advance it
			// all the way backward, then forward.  Either way we should get the same start and
			// end to the conflict, and the same set of commits involved in the conflict.
			var start, end string
			var conflictsList []string
			var conflicts map[string]bool
			Convey("we can find conflicts by going forward and then backward", func() {
				v := graph.MakeVerge(r, f, "foo.txt")
				for n := v.Next()[0]; len(v.Conflicts()) == 0; n = v.Next()[0] {
					v.Advance(n)
					fmt.Printf("%v\n", v)
				}
				end, _ = v.AdvanceUntilConverged()
				cont := r.GetContent(r.GetNode(end).Content)
				So(string(cont[0]), ShouldEqual, "golf")
				start, conflicts = v.RetractUntilConverged()
				cont = r.GetContent(r.GetNode(r.GetRef(start)).Content)
				So(string(cont[len(cont)-1]), ShouldEqual, "alpha")
				for c := range conflicts {
					conflictsList = append(conflictsList, c)
				}
			})
			Convey("we can find conflicts by going backward and then forward", func() {
				v := graph.MakeVerge(r, f, "foo.txt")
				for n := v.Next()[0]; len(v.Conflicts()) == 0; n = v.Next()[0] {
					v.Advance(n)
					fmt.Printf("%v\n", v)
				}
				start, _ = v.RetractUntilConverged()
				startRef := r.GetRef(start)
				cont := r.GetContent(r.GetNode(startRef).Content)
				So(string(cont[0]), ShouldEqual, "alpha")
				end, conflicts = v.AdvanceUntilConverged()
				cont = r.GetContent(r.GetNode(end).Content)
				So(string(cont[len(cont)-1]), ShouldEqual, "golf")
				for c := range conflicts {
					conflictsList = append(conflictsList, c)
				}
			})
			So(conflictsList, ShouldNotContain, c0.Hash())
			So(conflictsList, ShouldNotContain, c1.Hash())
			So(conflictsList, ShouldNotContain, c2.Hash())
			So(conflictsList, ShouldNotContain, c2x.Hash())
			So(conflictsList, ShouldNotContain, c4.Hash())
			So(conflictsList, ShouldContain, c5a.Hash())
			So(conflictsList, ShouldContain, c5b.Hash())
			So(conflictsList, ShouldContain, c6a.Hash())
			So(conflictsList, ShouldContain, c6b.Hash())
			versions, err := graph.ReadVersions(r, f, explicitFrontier(c0, c5a, c5b), r.GetRef(start), end, conflicts, []byte("."))
			So(err, ShouldBeNil)
			So(versions, ShouldNotBeNil)
			So(len(versions), ShouldEqual, 3)
			unhit := map[string]bool{
				"alpha.BRAVO.CHARLIE.delta.ECHO.FOXTROT.golf": true,
				"alpha.brAvO.chArlIE.dEltA.echo.foxtrot.golf": true,
				"alpha.bravo.charlie.delta.echo.fOxtrOt.golf": true,
			}
			for i := range versions {
				s := string(versions[i].Data)
				delete(unhit, s)
				if s == "alpha.BRAVO.CHARLIE.delta.ECHO.FOXTROT.golf" {
					So(len(versions[i].Commits), ShouldEqual, 2)
					So(versions[i].Commits[c5a.Hash()], ShouldBeTrue)
					So(versions[i].Commits[c5b.Hash()], ShouldBeTrue)
				} else if s == "alpha.brAvO.chArlIE.dEltA.echo.foxtrot.golf" {
					So(len(versions[i].Commits), ShouldEqual, 1)
					So(versions[i].Commits[c6a.Hash()], ShouldBeTrue)
				} else if s == "alpha.bravo.charlie.delta.echo.fOxtrOt.golf" {
					So(len(versions[i].Commits), ShouldEqual, 1)
					So(versions[i].Commits[c6b.Hash()], ShouldBeTrue)
				} else {
					t.Errorf("unexpected version %q", s)
				}
			}
			So(unhit, ShouldBeEmpty)
		})
	})
}

func TestProgrammaticCommits(t *testing.T) {
	Convey("beans are beans", t, func() {
		r := testutils.MakeFakeRepo()
		c0 := &graph.Commit{
			Deps:     nil,
			EdgeRefs: []graph.EdgeRef{{0, 1}, {1, 2}},
			Contents: []graph.NewContent{
				{
					Path: "sample.txt",
					Form: graph.FormFileSrc,
				},
				{
					Form:    graph.FormText,
					Content: stringsToContent("alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india", ""),
				},
				{
					Path: "sample.txt",
					Form: graph.FormFileSnk,
				},
			},
		}
		So(graph.Apply(r, c0), ShouldBeNil)
		data, err := graph.ReadVersion(r, allFrontier{}, "src:sample.txt", "snk:sample.txt", &graph.ReadMetadata{})
		So(err, ShouldBeNil)
		So(string(bytes.Join(data, []byte("."))), ShouldEqual, "alpha.bravo.charlie.delta.echo.foxtrot.golf.hotel.india.")
		diffmachine(r, allFrontier{}, "sample.txt", [][]byte{[]byte("alpha"), []byte("bravo"), []byte("foxtrot"), []byte("golf"), []byte("hotel"), []byte("indeia"), []byte("")})
		// Let's remove 'delta'.  This will require two splits and a new edge.
		// NEXT: we need a function that takes a list of depths and returns the appropriate node/depth pair to use as a node-ref.
		//       this shouldn't depend on which commits are involved, but when we make the edges we'll need to determine those.
		// head0, _, err := graph.SplitNode(r, "src:sample.txt", 3)
		// So(err, ShouldBeNil)
		// _, tail1, err := graph.SplitNode(r, "snk:sample.txt", 3)
		// So(err, ShouldBeNil)
		// c1 := &graph.Commit{
		// 	Deps:     []string{c0.Hash()},
		// 	EdgeRefs: []graph.EdgeRef{},
		// 	Contents: nil,
		// 	NodeRefs: []graph.NodeRef{},
		// }
	})
}

func diffmachine(r graph.Repo, f graph.Frontier, path string, lines1 [][]byte) {
	var ranges []graph.ReadRange
	lines0, err := graph.ReadVersion(r, f, fmt.Sprintf("src:%s", path), fmt.Sprintf("snk:%s", path), &graph.ReadMetadata{Ranges: &ranges})
	So(err, ShouldBeNil)
	So(len(lines0), ShouldBeGreaterThan, 0)
	So(len(ranges), ShouldBeGreaterThan, 0)

	var vs [][]uint64
	m := make(map[string]uint64)
	for _, lines := range [][][]byte{lines0, lines1} {
		var v []uint64
		for _, line := range lines {
			s := string(line)
			n, ok := m[s]
			if !ok {
				n = uint64(len(m) + 1)
				m[s] = n
			}
			v = append(v, n)
		}
		vs = append(vs, v)
	}
	css := jig.LCS2(vs[0], vs[1])
	So(len(css), ShouldBeGreaterThan, 0)
	// panic(fmt.Sprintf("%v", css))
	sort.Slice(css, func(i, j int) bool {
		return css[i].Bi < css[j].Bi
	})

}

func TestToposort(t *testing.T) {
	Convey("Toposort works", t, func() {
		Convey("on a very simple graph", func() {
			g := map[string][]string{
				"a": []string{"b", "c", "d"},
				"b": []string{"d"},
				"c": []string{"d"},
				"d": []string{"e"},
			}
			t := graph.ToposortSubgraph(g)
			So(len(t), ShouldEqual, 5)
			r := make(map[string]int)
			for i, v := range t {
				r[v] = i
			}
			So(r["a"], ShouldBeLessThan, r["b"])
			So(r["a"], ShouldBeLessThan, r["c"])
			So(r["a"], ShouldBeLessThan, r["d"])
			So(r["a"], ShouldBeLessThan, r["e"])
			So(r["b"], ShouldBeLessThan, r["d"])
			So(r["b"], ShouldBeLessThan, r["e"])
			So(r["c"], ShouldBeLessThan, r["d"])
			So(r["c"], ShouldBeLessThan, r["e"])
			So(r["d"], ShouldBeLessThan, r["e"])
		})
		Convey("on a disjoint graph", func() {
			g := map[string][]string{
				"a": []string{"b", "c", "d"},
				"b": []string{"d"},
				"c": []string{"d"},
				"d": []string{"e"},
				"A": []string{"B", "C", "D"},
				"B": []string{"D"},
				"C": []string{"D"},
				"D": []string{"E"},
			}
			t := graph.ToposortSubgraph(g)
			So(len(t), ShouldEqual, 10)
			r := make(map[string]int)
			for i, v := range t {
				r[v] = i
			}
			So(r["a"], ShouldBeLessThan, r["b"])
			So(r["a"], ShouldBeLessThan, r["c"])
			So(r["a"], ShouldBeLessThan, r["d"])
			So(r["a"], ShouldBeLessThan, r["e"])
			So(r["b"], ShouldBeLessThan, r["d"])
			So(r["b"], ShouldBeLessThan, r["e"])
			So(r["c"], ShouldBeLessThan, r["d"])
			So(r["c"], ShouldBeLessThan, r["e"])
			So(r["d"], ShouldBeLessThan, r["e"])
			So(r["A"], ShouldBeLessThan, r["B"])
			So(r["A"], ShouldBeLessThan, r["C"])
			So(r["A"], ShouldBeLessThan, r["D"])
			So(r["A"], ShouldBeLessThan, r["E"])
			So(r["B"], ShouldBeLessThan, r["D"])
			So(r["B"], ShouldBeLessThan, r["E"])
			So(r["C"], ShouldBeLessThan, r["D"])
			So(r["C"], ShouldBeLessThan, r["E"])
			So(r["D"], ShouldBeLessThan, r["E"])
		})
	})
}

type allFrontier struct{}

func (allFrontier) Observes(string) bool { return true }

func explicitFrontier(commits ...*graph.Commit) simpleFrontier {
	s := make(simpleFrontier)
	for _, c := range commits {
		s[c.Hash()] = true
	}
	return s
}

func explicitFrontierStrings(commits ...string) simpleFrontier {
	s := make(simpleFrontier)
	for _, c := range commits {
		s[c] = true
	}
	return s
}

type simpleFrontier map[string]bool

func (s simpleFrontier) Observes(c string) bool { return s[c] }

type commitOnlyRepo struct {
	graph.Repo
	commits map[string][]string
}

func (r *commitOnlyRepo) GetCommit(c string) *graph.Commit {
	deps := r.commits[c]
	if deps == nil {
		return nil
	}
	return &graph.Commit{
		Deps: deps,
	}
}

func (r *commitOnlyRepo) ListCommits(start string, commits []string) (n int) {
	var allKeys []string
	for key := range r.commits {
		allKeys = append(allKeys, key)
	}
	sort.Strings(allKeys)
	for len(allKeys) > 0 && allKeys[0] <= start {
		allKeys = allKeys[1:]
	}
	copy(commits, allKeys)
	if len(commits) > len(allKeys) {
		return len(allKeys)
	}
	return len(commits)
}

func TestMoves(t *testing.T) {
	Convey("commits with moves work like any other", t, func() {
		r := testutils.MakeFakeRepo()
		c0 := &graph.Commit{
			Deps:     nil,
			EdgeRefs: []graph.EdgeRef{{0, 1}, {1, 2}},
			Contents: []graph.NewContent{
				{Path: "foo.txt", Form: graph.FormFileSrc},
				{
					Form:    graph.FormText,
					Content: stringsToContent("alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india", ""),
				},
				{Path: "foo.txt", Form: graph.FormFileSnk},
			},
		}
		graph.Apply(r, c0)

		// This moves bravo.charlie.delta to between golf and hotel
		cmove := &graph.Commit{
			Deps: []string{c0.Hash()},
			EdgeRefs: []graph.EdgeRef{
				{Src: 0, Dst: 1},
				{Src: 2, Dst: 3},
				{Src: 4, Dst: 5},
			},
			NodeRefs: []graph.NodeRef{
				{Node: "src:foo.txt", Depth: 2}, // 'alpha'
				{Node: "src:foo.txt", Depth: 6}, // 'echo'
				{Node: "src:foo.txt", Depth: 8}, // 'golf'
				{Node: "src:foo.txt", Depth: 3}, // 'bravo'
				{Node: "src:foo.txt", Depth: 5}, // 'delta'
				{Node: "src:foo.txt", Depth: 9}, // 'hotel'
			},
			Contents: []graph.NewContent{},
		}
		graph.Apply(r, cmove)

		// This capitalizes 'charlie'
		cedit := &graph.Commit{
			Deps: []string{c0.Hash()},
			EdgeRefs: []graph.EdgeRef{
				{Src: 0, Dst: 2},
				{Src: 2, Dst: 1},
			},
			NodeRefs: []graph.NodeRef{
				{Node: "src:foo.txt", Depth: 3}, // 'bravo'
				{Node: "src:foo.txt", Depth: 5}, // 'delta'
			},
			Contents: []graph.NewContent{
				{Content: stringsToContent("CHARLIE")},
			},
		}
		graph.Apply(r, cedit)

		// This munges 'charlie'
		cmunge := &graph.Commit{
			Deps: []string{c0.Hash()},
			EdgeRefs: []graph.EdgeRef{
				{Src: 0, Dst: 2},
				{Src: 2, Dst: 1},
			},
			NodeRefs: []graph.NodeRef{
				{Node: "src:foo.txt", Depth: 3}, // 'bravo'
				{Node: "src:foo.txt", Depth: 5}, // 'delta'
			},
			Contents: []graph.NewContent{
				{Content: stringsToContent("chArlIE")},
			},
		}
		graph.Apply(r, cmunge)

		atFrontierShouldRead := validator(r, ".")
		So("foo.txt", atFrontierShouldRead(explicitFrontier(c0)), "alpha.bravo.charlie.delta.echo.foxtrot.golf.hotel.india.")
		So("foo.txt", atFrontierShouldRead(explicitFrontier(c0, cmove)), "alpha.echo.foxtrot.golf.bravo.charlie.delta.hotel.india.")
		So("foo.txt", atFrontierShouldRead(explicitFrontier(c0, cedit)), "alpha.bravo.CHARLIE.delta.echo.foxtrot.golf.hotel.india.")
		So("foo.txt", atFrontierShouldRead(explicitFrontier(c0, cmove, cedit)), "alpha.echo.foxtrot.golf.bravo.CHARLIE.delta.hotel.india.")
		So("foo.txt", atFrontierShouldRead(explicitFrontier(c0, cmove, cmunge)), "alpha.echo.foxtrot.golf.bravo.chArlIE.delta.hotel.india.")

		// TODO: Need to verify that graph.ReadVersion works with conflicts
		// conflicts := make(map[string]bool)
		// data, err := graph.ReadVersion(r, allFrontier{}, "src:foo.txt", "snk:foo.txt", []byte("."), &graph.ReadMetadata{})
		// So(conflicts, ShouldBeEmpty)
		// So(string(data), ShouldNotEqual, "")
		// So(err, ShouldNotBeNil)

		// The code below currently fails with moves because there are cycles in the file graph.
		// The verge needs to be able to pass a node with an incoming edge if that incoming edge
		// comes from a commit currently on the verge, and then maybe that's it?  Maybe it's very simple.
		var start, end string
		var conflictsList []string
		var conflicts map[string]bool
		Convey("we can find conflicts in insane files by going forward and then backward", func() {
			v := graph.MakeVerge(r, allFrontier{}, "foo.txt")
			for n := v.Next()[0]; len(v.Conflicts()) == 0; n = v.Next()[0] {
				fmt.Printf("Advancing past %v %s\n", n, r.GetContent(r.GetNode(n).Content))
				v.Advance(n)
				fmt.Printf("%v\n", v)
			}
			end, _ = v.AdvanceUntilConverged()
			cont := r.GetContent(r.GetNode(end).Content)
			So(string(cont[0]), ShouldEqual, "delta")
			start, conflicts = v.RetractUntilConverged()
			cont = r.GetContent(r.GetNode(r.GetRef(start)).Content)
			So(string(cont[len(cont)-1]), ShouldEqual, "bravo")
			for c := range conflicts {
				conflictsList = append(conflictsList, c)
			}
		})
		vs, err := graph.ReadVersions(r, allFrontier{}, nil, start, end, conflicts, []byte("."))
		So(err, ShouldBeNil)
		So(len(vs), ShouldEqual, 2)
		unhit := map[string]bool{
			"bravo.CHARLIE.delta": true,
			"bravo.chArlIE.delta": true,
		}
		for i := range vs {
			s := string(vs[i].Data)
			delete(unhit, s)
			if s == "bravo.CHARLIE.delta" {
				So(len(vs[i].Commits), ShouldEqual, 1)
				So(vs[i].Commits[cedit.Hash()], ShouldBeTrue)
			} else if s == "bravo.chArlIE.delta" {
				So(len(vs[i].Commits), ShouldEqual, 1)
				So(vs[i].Commits[cmunge.Hash()], ShouldBeTrue)
			} else {
				t.Errorf("unexpected version %q", s)
			}
		}
		So(unhit, ShouldBeEmpty)
	})
}

func validator(r graph.Repo, sep string) func(f graph.Frontier) func(file interface{}, expected ...interface{}) string {
	return func(f graph.Frontier) func(file interface{}, expected ...interface{}) string {
		return func(file interface{}, expected ...interface{}) string {
			lines, err := graph.ReadVersion(r, f, fmt.Sprintf("src:%s", file), fmt.Sprintf("snk:%s", file), &graph.ReadMetadata{})
			if err != nil {
				return fmt.Sprintf("failed to read %s: %v", file, err)
			}
			line := string(bytes.Join(lines, []byte(sep)))
			if line == expected[0].(string) {
				return ""
			} else {
				return fmt.Sprintf("File %q had the wrong contents:\nActual:   %q\nExpected: %q", file, string(line), expected[0].(string))
			}
		}
	}
}

// func FileAtFrontier(r graph.Repo, f graph.Frontier, filename string, sep string)

func TestErrorConditions(t *testing.T) {
	Convey("graph.ReadVersion doesn't panic, just errors", t, func() {
		Convey("on incompletely defined nodes", func() {
			r := testutils.MakeFakeRepo()
			r.Nodes["badnode-0"] = &graph.Node{Out: []graph.Edge{{Node: "badnode-1"}}}
			r.Nodes["badnode-1"] = &graph.Node{Out: []graph.Edge{{Node: "badnode-2"}}}
			r.Nodes["badnode-2"] = &graph.Node{}
			_, err := graph.ReadVersion(r, allFrontier{}, "noexist-0", "badnode-1", &graph.ReadMetadata{})
			So(err, ShouldNotBeNil)
			_, err = graph.ReadVersion(r, allFrontier{}, "badnode-1", "badnode-2", &graph.ReadMetadata{})
			So(err, ShouldNotBeNil)
			_, err = graph.ReadVersion(r, allFrontier{}, "badnode-2", "badnode-3", &graph.ReadMetadata{})
			So(err, ShouldNotBeNil)
			_, err = graph.ReadVersion(r, allFrontier{}, "badnode-3", "noexist-1", &graph.ReadMetadata{})
			So(err, ShouldNotBeNil)
		})
		Convey("on good data with bad parameters", func() {
			r := testutils.MakeFakeRepo()
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
						Content: stringsToContent("alpha", "bravo", "charlie"),
					},
					{
						Path: "foo.txt",
						Form: graph.FormFileSnk,
					},
				},
			}
			graph.Apply(r, c0)
			c1 := &graph.Commit{
				Deps:     []string{c0.Hash()},
				EdgeRefs: []graph.EdgeRef{{0, 2}, {2, 1}},
				NodeRefs: []graph.NodeRef{
					{Node: "src:foo.txt", Depth: 2}, // 'alpha'
					{Node: "src:foo.txt", Depth: 4}, // 'charlie'
				},
				Contents: []graph.NewContent{
					{
						Form:    graph.FormText,
						Content: stringsToContent("BRAVO"),
					},
				},
			}
			graph.Apply(r, c1)
			data, err := graph.ReadVersion(r, allFrontier{}, "src:foo.txt", "snk:foo.txt", &graph.ReadMetadata{})
			So(err, ShouldBeNil)
			So(string(bytes.Join(data, []byte("."))), ShouldEqual, "alpha.BRAVO.charlie")

			_, err = graph.ReadVersion(r, allFrontier{}, "src:foo.txt", "src:foo.txt", &graph.ReadMetadata{})
			So(err, ShouldNotBeNil)
			_, err = graph.ReadVersion(r, allFrontier{}, "snk:foo.txt", "snk:foo.txt", &graph.ReadMetadata{})
			So(err, ShouldNotBeNil)

			n0 := r.GetNode("src:foo.txt")
			n1 := r.GetNode(n0.Out[len(n0.Out)-1].Node)
			m0 := r.GetNode("snk:foo.txt")
			m1 := r.GetNode(r.GetRef(m0.In[len(m0.In)-1].Node))
			data, err = graph.ReadVersion(r, allFrontier{}, n1.Head, m1.Head, &graph.ReadMetadata{})
			So(err, ShouldBeNil)
			So(string(bytes.Join(data, []byte("."))), ShouldEqual, "alpha.BRAVO.charlie")

			// Can't start and end at the same internal node.
			_, err = graph.ReadVersion(r, allFrontier{}, n1.Head, n1.Head, &graph.ReadMetadata{})
			So(err, ShouldNotBeNil)
			_, err = graph.ReadVersion(r, allFrontier{}, m1.Head, m1.Head, &graph.ReadMetadata{})
			So(err, ShouldNotBeNil)

			// Can't go backward.
			_, err = graph.ReadVersion(r, allFrontier{}, m1.Head, n1.Head, &graph.ReadMetadata{})
			So(err, ShouldNotBeNil)

			// Now corrupt the repo and try queries that previously would have been successful.
			delete(r.Nodes, n1.Head)
			_, err = graph.ReadVersion(r, allFrontier{}, "src:foo.txt", "snk:foo.txt", &graph.ReadMetadata{})
			So(err, ShouldNotBeNil)
		})
	})
}

// TODO: Need to test the following kinds of invalid commits at the very least:
// - Edges that create cycles.
// - An edge from nodes in one file to nodes in another file, without corresponding nodes from the other
//   file back to the first one.  This would cause future modifications to that shared potion to be
//   reflected in both files.
func TestApplyCommits(t *testing.T) {
	Convey("applied commits", t, func() {
		r := testutils.MakeFakeRepo()
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

		data, err := graph.ReadVersion(r, allFrontier{}, "src:foo.txt", "snk:foo.txt", &graph.ReadMetadata{})
		So(err, ShouldBeNil)
		So(string(bytes.Join(data, []byte("."))), ShouldEqual, "alpha.bravo.charlie.delta.echo.foxtrot.golf.hotel.india.")

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
		data, err = graph.ReadVersion(r, allFrontier{}, "src:foo.txt", "snk:foo.txt", &graph.ReadMetadata{})
		So(err, ShouldBeNil)
		So(string(bytes.Join(data, []byte("."))), ShouldEqual, "alpha.BRAVO.CHARLIE.delta.echo.foxtrot.golf.hotel.india.")
	})
}
