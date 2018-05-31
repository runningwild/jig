package graph_test

import (
	"bytes"
	"fmt"
	"sort"
	"testing"

	"github.com/runningwild/jig/graph"
	"github.com/runningwild/jig/testutils"

	. "github.com/smartystreets/goconvey/convey"
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

func nodeContent(r graph.Repo, node string) string {
	return contentToString(r.GetContent(r.GetNode(node).Content))
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
			In:      []graph.Edge{{Commit: "commit-0", Node: "src:sample.txt", Join: true}},
			Out:     []graph.Edge{{Commit: "commit-0", Node: "snk:sample.txt", Join: true}},
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
			So(nodeContent(r, node0.Head), ShouldEqual, "alpha.bravo.charlie")
			So(nodeContent(r, node1.Head), ShouldEqual, "delta.echo.foxtrot.golf")
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
				So(nodeContent(r, node0.Head), ShouldEqual, "alpha.bravo.charlie")
				So(nodeContent(r, node1.Head), ShouldEqual, "delta.echo")
				So(nodeContent(r, node2.Head), ShouldEqual, "foxtrot.golf")
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
			Deps: nil,
			EdgeRefs: []graph.EdgeRef{
				{
					Src: graph.NodeRef{
						Node:  "src:foo.txt",
						Depth: 1,
					},
					Content: &graph.NewContent{
						Form:    graph.FormText,
						Content: stringsToContent("alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf"),
					},
					Dst: graph.NodeRef{
						Node: "snk:foo.txt",
					},
				},
			},
		}
		So(graph.Apply(r, c0), ShouldBeNil)
		var ranges []graph.ReadRange
		data, err := graph.ReadVersion(r, allFrontier{}, "src:foo.txt", "snk:foo.txt", &graph.ReadMetadata{Ranges: &ranges})
		So(err, ShouldBeNil)
		So(contentToString(data), ShouldEqual, "alpha.bravo.charlie.delta.echo.foxtrot.golf")
		So(ranges, ShouldHaveLength, 1)
		So(ranges[0].Commit, ShouldEqual, c0.Hash())
		head := ranges[0].Node

		Convey("can delete the first line of a file", func() {
			c1 := &graph.Commit{
				Deps: []string{c0.Hash()},
				EdgeRefs: []graph.EdgeRef{
					{
						Src: graph.NodeRef{
							Node:  "src:foo.txt",
							Depth: 1,
						},
						Content: nil,
						Dst: graph.NodeRef{
							Node:  head,
							Depth: 1,
						},
					},
				},
			}
			So(graph.Apply(r, c1), ShouldBeNil)
			data, err := graph.ReadVersion(r, allFrontier{}, "src:foo.txt", "snk:foo.txt", &graph.ReadMetadata{})
			So(err, ShouldBeNil)
			So(contentToString(data), ShouldEqual, "bravo.charlie.delta.echo.foxtrot.golf")
		})

		Convey("can modify the first line of a file", func() {
			c1 := &graph.Commit{
				Deps: []string{c0.Hash()},
				EdgeRefs: []graph.EdgeRef{
					{
						Src: graph.NodeRef{
							Node:  "src:foo.txt",
							Depth: 1,
						},
						Content: &graph.NewContent{
							Form:    graph.FormText,
							Content: stringsToContent("ALPHA"),
						},
						Dst: graph.NodeRef{
							Node:  head,
							Depth: 1, // bravo
						},
					},
				},
			}
			So(graph.Apply(r, c1), ShouldBeNil)
			data, err := graph.ReadVersion(r, allFrontier{}, "src:foo.txt", "snk:foo.txt", &graph.ReadMetadata{})
			So(err, ShouldBeNil)
			So(contentToString(data), ShouldEqual, "ALPHA.bravo.charlie.delta.echo.foxtrot.golf")
		})

		Convey("can delete the last line of a file", func() {
			c1 := &graph.Commit{
				Deps: []string{c0.Hash()},
				EdgeRefs: []graph.EdgeRef{
					{
						Src: graph.NodeRef{
							Node:  head,
							Depth: 6, // foxtrot
						},
						Content: nil,
						Dst: graph.NodeRef{
							Node:  "snk:foo.txt",
							Depth: 0,
						},
					},
				},
			}
			So(graph.Apply(r, c1), ShouldBeNil)
			data, err := graph.ReadVersion(r, allFrontier{}, "src:foo.txt", "snk:foo.txt", &graph.ReadMetadata{})
			So(err, ShouldBeNil)
			So(contentToString(data), ShouldEqual, "alpha.bravo.charlie.delta.echo.foxtrot")
		})

		Convey("can modify the last line of a file", func() {
			c1 := &graph.Commit{
				Deps: []string{c0.Hash()},
				EdgeRefs: []graph.EdgeRef{
					{
						Src: graph.NodeRef{
							Node:  head,
							Depth: 6, // foxtrot
						},
						Content: &graph.NewContent{
							Form:    graph.FormText,
							Content: stringsToContent("GOLF"),
						},
						Dst: graph.NodeRef{
							Node:  "snk:foo.txt",
							Depth: 0,
						},
					},
				},
			}
			So(graph.Apply(r, c1), ShouldBeNil)
			data, err := graph.ReadVersion(r, allFrontier{}, "src:foo.txt", "snk:foo.txt", &graph.ReadMetadata{})
			So(err, ShouldBeNil)
			So(contentToString(data), ShouldEqual, "alpha.bravo.charlie.delta.echo.foxtrot.GOLF")
		})

		Convey("can move the first line of a file into the middle of the file", func() {
			c1 := &graph.Commit{
				Deps: []string{c0.Hash()},
				EdgeRefs: []graph.EdgeRef{
					{
						Src: graph.NodeRef{
							Node:  "src:foo.txt",
							Depth: 1,
						},
						Content: nil,
						Dst: graph.NodeRef{
							Node:  head,
							Depth: 1, // bravo
						},
					},
					{
						Src: graph.NodeRef{
							Node:  head,
							Depth: 4, // delta
						},
						Content: nil,
						Dst: graph.NodeRef{
							Node:  head,
							Depth: 0, // alpha
						},
					},
					{
						Src: graph.NodeRef{
							Node:  head,
							Depth: 1, // alpha
						},
						Content: nil,
						Dst: graph.NodeRef{
							Node:  head,
							Depth: 4, // echo
						},
					},
				},
			}
			So(graph.Apply(r, c1), ShouldBeNil)
			data, err := graph.ReadVersion(r, allFrontier{}, "src:foo.txt", "snk:foo.txt", &graph.ReadMetadata{})
			So(err, ShouldBeNil)
			So(contentToString(data), ShouldEqual, "bravo.charlie.delta.alpha.echo.foxtrot.golf")
		})

		Convey("can move the last line of a file into the middle of the file", func() {
			c1 := &graph.Commit{
				Deps: []string{c0.Hash()},
				EdgeRefs: []graph.EdgeRef{
					{
						Src: graph.NodeRef{
							Node:  head,
							Depth: 3, // charlie
						},
						Content: nil,
						Dst: graph.NodeRef{
							Node:  head,
							Depth: 6, // golf
						},
					},
					{
						Src: graph.NodeRef{
							Node:  head,
							Depth: 7, // golf
						},
						Content: nil,
						Dst: graph.NodeRef{
							Node:  head,
							Depth: 3, // delta
						},
					},
					{
						Src: graph.NodeRef{
							Node:  head,
							Depth: 6, // foxtrot
						},
						Content: nil,
						Dst: graph.NodeRef{
							Node:  "snk:foo.txt",
							Depth: 0,
						},
					},
				},
			}
			So(graph.Apply(r, c1), ShouldBeNil)
			data, err := graph.ReadVersion(r, allFrontier{}, "src:foo.txt", "snk:foo.txt", &graph.ReadMetadata{})
			So(err, ShouldBeNil)
			So(contentToString(data), ShouldEqual, "alpha.bravo.charlie.golf.delta.echo.foxtrot")
		})

		Convey("can move the first two lines of a file into the middle of the file", func() {
			c1 := &graph.Commit{
				Deps: []string{c0.Hash()},
				EdgeRefs: []graph.EdgeRef{
					{
						Src: graph.NodeRef{
							Node:  "src:foo.txt",
							Depth: 1,
						},
						Content: nil,
						Dst: graph.NodeRef{
							Node:  head,
							Depth: 2, // charlie
						},
					},
					{
						Src: graph.NodeRef{
							Node:  head,
							Depth: 4, // delta
						},
						Content: nil,
						Dst: graph.NodeRef{
							Node:  head,
							Depth: 0, // alpha
						},
					},
					{
						Src: graph.NodeRef{
							Node:  head,
							Depth: 2, // bravo
						},
						Content: nil,
						Dst: graph.NodeRef{
							Node:  head,
							Depth: 4, // echo
						},
					},
				},
			}
			So(graph.Apply(r, c1), ShouldBeNil)
			data, err := graph.ReadVersion(r, allFrontier{}, "src:foo.txt", "snk:foo.txt", &graph.ReadMetadata{})
			So(err, ShouldBeNil)
			So(contentToString(data), ShouldEqual, "charlie.delta.alpha.bravo.echo.foxtrot.golf")
		})

		Convey("can move the last two lines of a file into the middle of the file", func() {
			c1 := &graph.Commit{
				Deps: []string{c0.Hash()},
				EdgeRefs: []graph.EdgeRef{
					{
						Src: graph.NodeRef{
							Node:  head,
							Depth: 3, // charlie
						},
						Content: nil,
						Dst: graph.NodeRef{
							Node:  head,
							Depth: 5, // foxtrot
						},
					},
					{
						Src: graph.NodeRef{
							Node:  head,
							Depth: 7, // golf
						},
						Content: nil,
						Dst: graph.NodeRef{
							Node:  head,
							Depth: 3, // delta
						},
					},
					{
						Src: graph.NodeRef{
							Node:  head,
							Depth: 5, // echo
						},
						Content: nil,
						Dst: graph.NodeRef{
							Node:  "snk:foo.txt",
							Depth: 0,
						},
					},
				},
			}
			So(graph.Apply(r, c1), ShouldBeNil)
			data, err := graph.ReadVersion(r, allFrontier{}, "src:foo.txt", "snk:foo.txt", &graph.ReadMetadata{})
			So(err, ShouldBeNil)
			So(contentToString(data), ShouldEqual, "alpha.bravo.charlie.foxtrot.golf.delta.echo")
		})

		Convey("can insert two lines in the middle of the file", func() {
			c1 := &graph.Commit{
				Deps: []string{c0.Hash()},
				EdgeRefs: []graph.EdgeRef{
					{
						Src: graph.NodeRef{
							Node:  head,
							Depth: 3, // charlie
						},
						Content: &graph.NewContent{
							Form:    graph.FormText,
							Content: stringsToContent("thunder", "buttons"),
						},
						Dst: graph.NodeRef{
							Node:  head,
							Depth: 3, // delta
						},
					},
				},
			}
			So(graph.Apply(r, c1), ShouldBeNil)
			data, err := graph.ReadVersion(r, allFrontier{}, "src:foo.txt", "snk:foo.txt", &graph.ReadMetadata{})
			So(err, ShouldBeNil)
			So(contentToString(data), ShouldEqual, "alpha.bravo.charlie.thunder.buttons.delta.echo.foxtrot.golf")

			Convey("and can refer to nodes created by other commits", func() {
				// This commit is going to add an edge that points directly at an inner node created
				// by a previous commit.  In order to do this we need to find the relevant node.
				var ranges []graph.ReadRange
				_, err := graph.ReadVersion(r, allFrontier{}, "src:foo.txt", "snk:foo.txt", &graph.ReadMetadata{Ranges: &ranges})
				So(err, ShouldBeNil)
				var relevant []graph.ReadRange
				for _, r := range ranges {
					if r.Commit == c1.Hash() {
						relevant = append(relevant, r)
					}
				}
				So(relevant, ShouldHaveLength, 1)
				So(nodeContent(r, relevant[0].Node), ShouldEqual, "thunder.buttons")

				// Now we're going to delete the line before the thunder.buttons. This will require
				// creating an edge that points directly at that node.
				c2 := &graph.Commit{
					Deps: []string{c0.Hash()},
					EdgeRefs: []graph.EdgeRef{
						{
							Src: graph.NodeRef{
								Node:  head,
								Depth: 2, // bravo
							},
							Content: nil,
							Dst: graph.NodeRef{
								Node:  relevant[0].Node,
								Depth: 0,
							},
						},
					},
				}
				So(graph.Apply(r, c2), ShouldBeNil)
				data, err := graph.ReadVersion(r, allFrontier{}, "src:foo.txt", "snk:foo.txt", &graph.ReadMetadata{})
				So(err, ShouldBeNil)
				So(contentToString(data), ShouldEqual, "alpha.bravo.thunder.buttons.delta.echo.foxtrot.golf")
			})
		})
	})
}

func TestVerge(t *testing.T) {
	Convey("applied commits", t, func() {
		r := testutils.MakeFakeRepo()
		c0 := &graph.Commit{
			Deps: nil,
			EdgeRefs: []graph.EdgeRef{
				{
					Src: graph.NodeRef{
						Node:  "src:foo.txt",
						Depth: 1,
					},
					Content: &graph.NewContent{
						Form:    graph.FormText,
						Content: stringsToContent("alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india", ""),
					},
					Dst: graph.NodeRef{
						Node: "snk:foo.txt",
					},
				},
			},
		}
		So(graph.Apply(r, c0), ShouldBeNil)
		var ranges []graph.ReadRange
		data, err := graph.ReadVersion(r, allFrontier{}, "src:foo.txt", "snk:foo.txt", &graph.ReadMetadata{Ranges: &ranges})
		So(err, ShouldBeNil)
		So(contentToString(data), ShouldEqual, "alpha.bravo.charlie.delta.echo.foxtrot.golf.hotel.india.")
		So(ranges, ShouldHaveLength, 1)
		head := ranges[0].Node
		c1 := &graph.Commit{
			Deps: []string{c0.Hash()},
			EdgeRefs: []graph.EdgeRef{
				{
					Src: graph.NodeRef{
						Node:  head,
						Depth: 2,
					},
					Content: &graph.NewContent{
						Form:    graph.FormText,
						Content: stringsToContent("CHARLIE", "DELTA", "ECHO", "FOXTROT"),
					},
					Dst: graph.NodeRef{
						Node:  head,
						Depth: 6,
					},
				},
			},
		}
		So(graph.Apply(r, c1), ShouldBeNil)

		//  This inserts some text between delta and echo.
		c2 := &graph.Commit{
			Deps: []string{c0.Hash()},
			EdgeRefs: []graph.EdgeRef{
				{
					Src: graph.NodeRef{
						Node:  head,
						Depth: 4,
					},
					Content: &graph.NewContent{
						Form:    graph.FormText,
						Content: stringsToContent("buttons", "the", "buttonsball"),
					},
					Dst: graph.NodeRef{
						Node:  head,
						Depth: 4,
					},
				},
			},
		}
		So(graph.Apply(r, c2), ShouldBeNil)

		// This commit replaces resolves the conflict between c1 and c2.
		c3 := &graph.Commit{
			Deps: []string{c0.Hash(), c1.Hash(), c2.Hash()},
			EdgeRefs: []graph.EdgeRef{
				{
					Src: graph.NodeRef{
						Node:  head,
						Depth: 1,
					},
					Content: &graph.NewContent{
						Form:    graph.FormText,
						Content: stringsToContent("all", "your", "base", "are", "belong", "to", "us"),
					},
					Dst: graph.NodeRef{
						Node:  head,
						Depth: 9,
					},
				},
			},
		}
		So(graph.Apply(r, c3), ShouldBeNil)

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
					{
						Src: graph.NodeRef{
							Node:  head,
							Depth: 4,
						},
						Content: &graph.NewContent{
							Form:    graph.FormText,
							Content: stringsToContent("BUTTONS", "THE", "BUTTONSBALL"),
						},
						Dst: graph.NodeRef{
							Node:  head,
							Depth: 4,
						},
					},
				},
			}
			So(graph.Apply(r, c2x), ShouldBeNil)

			// Deletes 'hotel'
			c4 := &graph.Commit{
				Deps: []string{c0.Hash()},
				EdgeRefs: []graph.EdgeRef{
					{
						Src: graph.NodeRef{
							Node:  head,
							Depth: 7,
						},
						Dst: graph.NodeRef{
							Node:  head,
							Depth: 8,
						},
					},
				},
			}
			So(graph.Apply(r, c4), ShouldBeNil)

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
					{
						Src: graph.NodeRef{
							Node:  head,
							Depth: 1,
						},
						Content: &graph.NewContent{
							Form:    graph.FormText,
							Content: stringsToContent("BRAVO", "CHARLIE"),
						},
						Dst: graph.NodeRef{
							Node:  head,
							Depth: 3,
						},
					},
				},
			}
			So(graph.Apply(r, c5a), ShouldBeNil)

			// capitalizes 'echo' and 'foxtrot'
			c5b := &graph.Commit{
				Deps: []string{c0.Hash()},
				EdgeRefs: []graph.EdgeRef{
					{
						Src: graph.NodeRef{
							Node:  head,
							Depth: 4,
						},
						Content: &graph.NewContent{
							Form:    graph.FormText,
							Content: stringsToContent("ECHO", "FOXTROT"),
						},
						Dst: graph.NodeRef{
							Node:  head,
							Depth: 6,
						},
					},
				},
			}
			So(graph.Apply(r, c5b), ShouldBeNil)

			// munges 'bravo', 'charlie', and 'delta'
			c6a := &graph.Commit{
				Deps: []string{c0.Hash()},
				EdgeRefs: []graph.EdgeRef{
					{
						Src: graph.NodeRef{
							Node:  head,
							Depth: 1,
						},
						Content: &graph.NewContent{
							Form:    graph.FormText,
							Content: stringsToContent("brAvO", "chArlIE", "dEltA"),
						},
						Dst: graph.NodeRef{
							Node:  head,
							Depth: 4,
						},
					},
				},
			}
			So(graph.Apply(r, c6a), ShouldBeNil)

			// munges 'echo' and 'foxtrot'
			c6b := &graph.Commit{
				Deps: []string{c0.Hash()},
				EdgeRefs: []graph.EdgeRef{
					{
						Src: graph.NodeRef{
							Node:  head,
							Depth: 5,
						},
						Content: &graph.NewContent{
							Form:    graph.FormText,
							Content: stringsToContent("fOxtrOt"),
						},
						Dst: graph.NodeRef{
							Node:  head,
							Depth: 6,
						},
					},
				},
			}
			So(graph.Apply(r, c6b), ShouldBeNil)

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

func TestSplitNodeProperties(t *testing.T) {
	Convey("SplitNode propagates edges properly", t, func() {
		r := testutils.MakeFakeRepo()

		c0 := &graph.Commit{
			Deps: nil,
			EdgeRefs: []graph.EdgeRef{
				{
					Src: graph.NodeRef{
						Node:  "src:foo.txt",
						Depth: 1,
					},
					Content: &graph.NewContent{
						Form:    graph.FormText,
						Content: stringsToContent("alpha", "bravo", "charlie", "delta", "echo"),
					},
					Dst: graph.NodeRef{
						Node: "snk:foo.txt",
					},
				},
			},
		}
		So(graph.Apply(r, c0), ShouldBeNil)
		So(snippet{r, explicitFrontier(c0), "src:foo.txt", "snk:foo.txt"}, shouldRead, "alpha.bravo.charlie.delta.echo")

		c1 := &graph.Commit{
			Deps: []string{c0.Hash()},
			EdgeRefs: []graph.EdgeRef{
				{
					Src: graph.NodeRef{
						Node:  "src:foo.txt",
						Depth: 2,
					},
					Content: &graph.NewContent{
						Form:    graph.FormText,
						Content: stringsToContent("brAvO", "chArlIE", "dEltA"),
					},
					Dst: graph.NodeRef{
						Node:  "src:foo.txt",
						Depth: 5,
					},
				},
			},
		}
		So(graph.Apply(r, c1), ShouldBeNil)
		So(snippet{r, explicitFrontier(c0, c1), "src:foo.txt", "snk:foo.txt"}, shouldRead, "alpha.brAvO.chArlIE.dEltA.echo")

		c2 := &graph.Commit{
			Deps: []string{c0.Hash()},
			EdgeRefs: []graph.EdgeRef{
				{
					Src: graph.NodeRef{
						Node:  "src:foo.txt",
						Depth: 2,
					},
					Content: &graph.NewContent{
						Form:    graph.FormText,
						Content: stringsToContent("BRaVo", "CHaRLie", "DeLTa"),
					},
					Dst: graph.NodeRef{
						Node:  "src:foo.txt",
						Depth: 5,
					},
				},
			},
		}
		So(graph.Apply(r, c2), ShouldBeNil)
		So(snippet{r, explicitFrontier(c0, c2), "src:foo.txt", "snk:foo.txt"}, shouldRead, "alpha.BRaVo.CHaRLie.DeLTa.echo")

		v := graph.MakeVerge(r, explicitFrontier(c0, c1, c2), "foo.txt")
		conflicts := findConflicts(v)
		So(conflicts, ShouldHaveLength, 1)

		c3 := &graph.Commit{
			Deps: []string{c0.Hash()},
			EdgeRefs: []graph.EdgeRef{
				{
					Src: graph.NodeRef{
						Node:  "src:foo.txt",
						Depth: 2,
					},
					Content: &graph.NewContent{
						Form:    graph.FormText,
						Content: stringsToContent("BRAVO", "CHARLIE", "DELTA"),
					},
					Dst: graph.NodeRef{
						Node:  "src:foo.txt",
						Depth: 5,
					},
				},
			},
		}
		So(graph.Apply(r, c3), ShouldBeNil)
		So(snippet{r, explicitFrontier(c0, c3), "src:foo.txt", "snk:foo.txt"}, shouldRead, "alpha.BRAVO.CHARLIE.DELTA.echo")
		So(findConflicts(graph.MakeVerge(r, explicitFrontier(c0, c1, c2, c3), "foo.txt")), ShouldHaveLength, 1)

		metadata := &graph.ReadMetadata{Ranges: new([]graph.ReadRange)}
		_, err := graph.ReadVersion(r, explicitFrontier(c0, c1, c2, c3), "src:foo.txt", "snk:foo.txt", metadata)
		So(err, ShouldBeNil)
		var n string
		for _, r := range *metadata.Ranges {
			if r.Commit == c3.Hash() {
				n = r.Node
			}
		}
		So(n, ShouldNotEqual, "")
		c4 := &graph.Commit{
			Deps: []string{c0.Hash(), c1.Hash(), c2.Hash(), c3.Hash()},
			EdgeRefs: []graph.EdgeRef{
				{
					Src: graph.NodeRef{
						Node:  "src:foo.txt",
						Depth: 2,
					},
					Dst: graph.NodeRef{
						Node:  n,
						Depth: 0,
						Join:  true,
					},
				}, {
					Src: graph.NodeRef{
						Node:  n,
						Depth: 3,
						Join:  true,
					},
					Dst: graph.NodeRef{
						Node:  "src:foo.txt",
						Depth: 5,
					},
				},
			},
		}
		So(graph.Apply(r, c4), ShouldBeNil)
		So(snippet{r, explicitFrontier(c0, c1, c2, c3, c4), "src:foo.txt", "snk:foo.txt"}, shouldRead, "alpha.BRAVO.CHARLIE.DELTA.echo")
		So(findConflicts(graph.MakeVerge(r, explicitFrontier(c0, c1, c2, c3, c4), "foo.txt")), ShouldHaveLength, 0)

		// c5 is irrelevant, but it will split a node that c3 must keep its edges on
		c5 := &graph.Commit{
			Deps: []string{c3.Hash()},
			EdgeRefs: []graph.EdgeRef{
				{
					Src: graph.NodeRef{
						Node:  n,
						Depth: 2,
					},
					Content: &graph.NewContent{
						Form:    graph.FormText,
						Content: stringsToContent("i do not belong here"),
					},
					Dst: graph.NodeRef{
						Node:  n,
						Depth: 2,
					},
				},
			},
		}
		So(graph.Apply(r, c5), ShouldBeNil)
		So(snippet{r, explicitFrontier(c0, c1, c2, c3, c5), "src:foo.txt", "snk:foo.txt"}, shouldRead, "alpha.BRAVO.CHARLIE.i do not belong here.DELTA.echo")

		fmt.Printf("c3(%s)\nc4(%s)\n", c3.Hash(), c4.Hash())
		// This line is failing because SplitNode doesn't properly propagate edges, we probably need
		// a new way to indicate that an edge should propagate completely through a node.
		So(findConflicts(graph.MakeVerge(r, explicitFrontier(c0, c1, c2, c3, c4), "foo.txt")), ShouldHaveLength, 0)
	})
}
func TestReadVersions(t *testing.T) {
	Convey("ReadVersions", t, func() {
		r := testutils.MakeFakeRepo()
		c0 := &graph.Commit{
			Deps: nil,
			EdgeRefs: []graph.EdgeRef{
				{
					Src: graph.NodeRef{
						Node:  "src:foo.txt",
						Depth: 1,
					},
					Content: &graph.NewContent{
						Form:    graph.FormText,
						Content: stringsToContent("alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india", "juliet"),
					},
					Dst: graph.NodeRef{
						Node: "snk:foo.txt",
					},
				},
			},
		}
		So(graph.Apply(r, c0), ShouldBeNil)
		var ranges []graph.ReadRange
		data, err := graph.ReadVersion(r, allFrontier{}, "src:foo.txt", "snk:foo.txt", &graph.ReadMetadata{Ranges: &ranges})
		So(err, ShouldBeNil)
		So(contentToString(data), ShouldEqual, "alpha.bravo.charlie.delta.echo.foxtrot.golf.hotel.india.juliet")
		So(ranges, ShouldHaveLength, 1)
		head := ranges[0].Node
		fmt.Printf("c0: %v\n", c0.Hash())

		// capitalize bravo through delta
		c1 := &graph.Commit{
			Deps: []string{c0.Hash()},
			EdgeRefs: []graph.EdgeRef{
				{
					Src: graph.NodeRef{
						Node:  head,
						Depth: 1,
					},
					Content: &graph.NewContent{
						Form:    graph.FormText,
						Content: stringsToContent("BRAVO", "CHARLIE", "DELTA"),
					},
					Dst: graph.NodeRef{
						Node:  head,
						Depth: 4,
					},
				},
			},
		}
		So(graph.Apply(r, c1), ShouldBeNil)
		fmt.Printf("c1: %v\n", c1.Hash())
		So(snippet{r, explicitFrontier(c0, c1), "src:foo.txt", "snk:foo.txt"}, shouldRead, "alpha.BRAVO.CHARLIE.DELTA.echo.foxtrot.golf.hotel.india.juliet")

		// capitalize charlie through echo
		c2 := &graph.Commit{
			Deps: []string{c0.Hash()},
			EdgeRefs: []graph.EdgeRef{
				{
					Src: graph.NodeRef{
						Node:  head,
						Depth: 2,
					},
					Content: &graph.NewContent{
						Form:    graph.FormText,
						Content: stringsToContent("CHARLIE", "DELTA", "ECHO"),
					},
					Dst: graph.NodeRef{
						Node:  head,
						Depth: 5,
					},
				},
			},
		}
		So(graph.Apply(r, c2), ShouldBeNil)
		fmt.Printf("c2: %v\n", c2.Hash())
		So(snippet{r, explicitFrontier(c0, c2), "src:foo.txt", "snk:foo.txt"}, shouldRead, "alpha.bravo.CHARLIE.DELTA.ECHO.foxtrot.golf.hotel.india.juliet")

		// capitalize juliet
		c3 := &graph.Commit{
			Deps: []string{c0.Hash()},
			EdgeRefs: []graph.EdgeRef{
				{
					Src: graph.NodeRef{
						Node:  head,
						Depth: 9,
					},
					Content: &graph.NewContent{
						Form:    graph.FormText,
						Content: stringsToContent("JULIET"),
					},
					Dst: graph.NodeRef{
						Node: "snk:foo.txt",
					},
				},
			},
		}
		So(graph.Apply(r, c3), ShouldBeNil)
		So(snippet{r, explicitFrontier(c0, c3), "src:foo.txt", "snk:foo.txt"}, shouldRead, "alpha.bravo.charlie.delta.echo.foxtrot.golf.hotel.india.JULIET")
		fmt.Printf("c3: %v\n", c3.Hash())

		// capitalize india and delete juliet
		c4 := &graph.Commit{
			Deps: []string{c0.Hash()},
			EdgeRefs: []graph.EdgeRef{
				{
					Src: graph.NodeRef{
						Node:  head,
						Depth: 8,
					},
					Content: &graph.NewContent{
						Form:    graph.FormText,
						Content: stringsToContent("INDIA"),
					},
					Dst: graph.NodeRef{
						Node: "snk:foo.txt",
					},
				},
			},
		}
		So(graph.Apply(r, c4), ShouldBeNil)
		So(snippet{r, explicitFrontier(c0, c4), "src:foo.txt", "snk:foo.txt"}, shouldRead, "alpha.bravo.charlie.delta.echo.foxtrot.golf.hotel.INDIA")

		// The repo should be able to have multiple conflicts, but if the frontier doesn't include
		// them then we shouldn't see them.
		if false {
			v := graph.MakeVerge(r, explicitFrontier(c0, c1, c2), "foo.txt")
			conflicts := findConflicts(v)
			So(conflicts, ShouldHaveLength, 1)
			So(conflicts[0].commits, ShouldHaveLength, 2)
			So(conflicts[0].commits, ShouldContainKey, c1.Hash())
			So(conflicts[0].commits, ShouldContainKey, c2.Hash())
			So(snippet{r, explicitFrontier(c0, c1), conflicts[0].start, conflicts[0].end}, shouldRead, "alpha.BRAVO.CHARLIE.DELTA.echo.foxtrot")
			So(snippet{r, explicitFrontier(c0, c2), conflicts[0].start, conflicts[0].end}, shouldRead, "alpha.bravo.CHARLIE.DELTA.ECHO.foxtrot")
			versions, err := graph.ReadVersions(r, allFrontier{}, nil, conflicts[0].start, conflicts[0].end, conflicts[0].commits, []byte{'.'})
			So(err, ShouldBeNil)
			So(versions, ShouldHaveLength, 2)
		}
		{
			v := graph.MakeVerge(r, explicitFrontier(c0, c3, c4), "foo.txt")
			fmt.Printf("***************************************************************************\n")
			fmt.Printf("c0: %v\n", c0.Hash())
			fmt.Printf("c1: %v\n", c1.Hash())
			fmt.Printf("c2: %v\n", c2.Hash())
			fmt.Printf("c3: %v\n", c3.Hash())
			fmt.Printf("c4: %v\n", c4.Hash())
			conflicts := findConflicts(v)
			So(conflicts, ShouldHaveLength, 1)
			So(conflicts[0].commits, ShouldHaveLength, 2)
			So(conflicts[0].commits, ShouldContainKey, c3.Hash())
			So(conflicts[0].commits, ShouldContainKey, c4.Hash())
			So(snippet{r, explicitFrontier(c0, c3), conflicts[0].start, conflicts[0].end}, shouldRead, "hotel.india.JULIET")
			So(snippet{r, explicitFrontier(c0, c4), conflicts[0].start, conflicts[0].end}, shouldRead, "hotel.INDIA")
			versions, err := graph.ReadVersions(r, allFrontier{}, nil, conflicts[0].start, conflicts[0].end, conflicts[0].commits, []byte{'.'})
			So(err, ShouldBeNil)
			So(versions, ShouldHaveLength, 2)
		}

		// Now let's try different ways of resolving c1 and c2.

		// Resolve by taking CHARLIE.DELTA from c1.
		metadata := &graph.ReadMetadata{Ranges: new([]graph.ReadRange)}
		_, err = graph.ReadVersion(r, explicitFrontier(c0, c1), "src:foo.txt", "snk:foo.txt", metadata)
		So(err, ShouldBeNil)
		var n1 string
		for _, r := range *metadata.Ranges {
			if r.Commit == c1.Hash() {
				n1 = r.Node
			}
		}
		So(n1, ShouldNotEqual, "")
		fmt.Printf("Relevant node is %q\n", n1)
		// TODO: Commits need to be verified for correctness.  One thing we should check right here is that
		// if a commit is trying to resolve conflicting commits that it actually does so.  I'm not sure how
		// to define it, but it's easy to screw things up here.  In this case we have two commits that conflict
		// and aren't aligned, so the resolution needs to include both the path in the file covered by both
		// commits, but also the parts covered by only one where the verge can detect the conflict beginning
		// or ending.
		cR12a := &graph.Commit{
			Deps: []string{c0.Hash(), c1.Hash(), c2.Hash()},
			EdgeRefs: []graph.EdgeRef{
				{
					// This edge is required to make sure that the verge doesn't detect a conflict
					// here.  This should be verified.
					// NEXT: verify the above, and also verify the same thing at the end of this
					// commit where the second commit is present without the first.
					Src: graph.NodeRef{
						Node:  head,
						Depth: 1,
					},
					Dst: graph.NodeRef{
						Node:  head,
						Depth: 1, // NEXT: There is somethign wrong here, it's not diving into this node like it should
					},
				},
				{
					Src: graph.NodeRef{
						Node:  head,
						Depth: 2,
					},
					Dst: graph.NodeRef{
						Node:  n1,
						Depth: 1, // NEXT: There is somethign wrong here, it's not diving into this node like it should
					},
				},
				{
					Src: graph.NodeRef{
						Node:  n1,
						Depth: 3,
					},
					Dst: graph.NodeRef{
						Node:  head,
						Depth: 4,
					},
				},
			},
		}
		So(graph.Apply(r, cR12a), ShouldBeNil)
		fmt.Printf("Relevant commits:\n")
		for _, c := range []string{c0.Hash(), c1.Hash(), c2.Hash(), cR12a.Hash()} {
			fmt.Printf("  %s\n", c)
		}
		So(snippet{r, explicitFrontier(c0, c1, c2, cR12a), "src:foo.txt", "snk:foo.txt"}, shouldRead, "alpha.bravo.CHARLIE.DELTA.echo.foxtrot.golf.hotel.india.juliet")
	})
}

type snippet struct {
	r     graph.Repo
	f     graph.Frontier
	start string
	end   string
}

func shouldRead(_a interface{}, _bs ...interface{}) string {
	s, ok := _a.(snippet)
	if !ok {
		return fmt.Sprintf("shouldRead got first parameter %T, wanted snippet", _a)
	}
	if len(_bs) != 1 {
		return fmt.Sprintf("shouldRead got %d parameters, wanted 2", len(_bs))
	}
	wantMsg, ok := _bs[0].(string)
	if !ok {
		return fmt.Sprintf("shouldRead got second parameter %T, wanted string", _bs[0])
	}
	lines, err := graph.ReadVersion(s.r, s.f, s.start, s.end, &graph.ReadMetadata{})
	if err != nil {
		return fmt.Sprintf("error on ReadVersion: %v", err)
	}
	gotMsg := string(bytes.Join(lines, []byte{'.'}))
	if wantMsg != gotMsg {
		return fmt.Sprintf("Expected: '%s'\nActual:   '%s'\n", wantMsg, gotMsg)
	}
	return ""
}

// We have to advance until we find a conflict, then advance until converged, then retract until we
// find a conflict, then retract until converged.  *UntilConverged may or may not return with the
// verge in conflict because it's not always obvious at the start or end of a conflict.
// var bean = 0

func findConflicts(v *graph.Verge) []conflict {
	fmt.Printf("Conflicts: ")
	fmt.Printf("%v\n", v.Conflicts())
	fmt.Printf("Advancing from %v to conflict\n", v)
	if !v.AdvanceUntilConflicted() {
		return nil
	}
	fmt.Printf("Conflicts: %v\n", v.Conflicts())
	fmt.Printf("Advancing from %v to convergance\n", v)
	end, commits := v.AdvanceUntilConverged()
	v2 := v.Clone()
	fmt.Printf("Conflicts: %v\n", v.Conflicts())
	fmt.Printf("Retracting from %v to conflict\n", v)
	if !v2.RetractUntilConflicted() {
		panic("what the balls")
		return nil
	}
	fmt.Printf("Conflicts: %v\n", v.Conflicts())
	fmt.Printf("Retracting from %v to convergance\n", v)
	start, commits2 := v2.RetractUntilConverged()
	for k, v := range commits {
		if commits2[k] != v {
			panic("commits don't match")
		}
	}
	for k, v := range commits2 {
		if commits[k] != v {
			panic("commits don't match")
		}
	}
	// bean++
	// if bean > 2 {
	// 	panic("e")
	// }
	fmt.Printf("Final Advancement\n")
	v.Advance(end)
	return append([]conflict{conflict{start, end, commits}}, findConflicts(v)...)
}

type conflict struct {
	start   string
	end     string
	commits map[string]bool
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
