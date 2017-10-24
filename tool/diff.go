package main

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/runningwild/jig"
	"github.com/runningwild/jig/graph"
	"github.com/runningwild/jig/testutils"
)

func main() {
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
	if err := graph.Apply(r, c0); err != nil {
		panic(err)
	}
	data, err := graph.ReadVersion(r, allFrontier{}, "src:sample.txt", "snk:sample.txt", &graph.ReadMetadata{})
	if err != nil {
		panic(err)
	}
	if str := string(bytes.Join(data, []byte("."))); str != "alpha.bravo.charlie.delta.echo.foxtrot.golf.hotel.india." {
		panic(fmt.Sprintf("%q vs %q", str, "alpha.bravo.charlie.delta.echo.foxtrot.golf.hotel.india."))
	}
	c1 := diffmachine(r, allFrontier{}, "sample.txt", stringsToContent("alpha", "bravo", "charlie", "DELTA", "ECHO", "foxtrot", "golf", "hotel", "india", ""))
	c1.Deps = append(c1.Deps, c0.Hash())
	fmt.Printf("Applying commit: %v\n", c1)
	if err := graph.Apply(r, c1); err != nil {
		panic(fmt.Sprintf("failed to apply commit: %v", err))
	}

	data, err = graph.ReadVersion(r, allFrontier{}, "src:sample.txt", "snk:sample.txt", &graph.ReadMetadata{})
	if err != nil {
		panic(err)
	}
	if str := string(bytes.Join(data, []byte("."))); str != "alpha.bravo.charlie.DELTA.ECHO.foxtrot.golf.hotel.india." {
		panic(fmt.Sprintf("%q vs %q", str, "alpha.bravo.charlie.DELTA.ECHO.foxtrot.golf.hotel.india."))
	}

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
}

func diffmachine(r graph.Repo, f graph.Frontier, path string, lines1 [][]byte) *graph.Commit {
	var ranges []graph.ReadRange
	lines0, err := graph.ReadVersion(r, f, fmt.Sprintf("src:%s", path), fmt.Sprintf("snk:%s", path), &graph.ReadMetadata{Ranges: &ranges})
	if err != nil {
		panic(err)
	}
	fmt.Printf("lines0: %s\n", lines0)
	fmt.Printf("Ranges: %v\n", ranges)

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
	sort.Slice(css, func(i, j int) bool {
		return css[i].Bi < css[j].Bi
	})
	fmt.Printf("css: %v\n", css)
	total := 0

	var monkeyNodes [][2]interface{}
	var prevMonkey interface{}
	type refspec struct {
		src     bool
		snk     bool
		node    bool
		content bool
		index   int
	}
	var edges [][2]refspec
	var c graph.Commit

	prevMonkey = graph.NodeRef{Node: fmt.Sprintf("src:%s", path), Depth: 1}
	prev := fmt.Sprintf("src:%s", path)
	prevSpec := refspec{src: true, index: 0}
	c.NodeRefs = append(c.NodeRefs, graph.NodeRef{Node: fmt.Sprintf("src:%s", path), Depth: 1})

	// Loop over the common substrings, this will cover the whole file, though we might need to fill
	// in gaps with new content.
	for _, cs := range css {
		// If Bi has skipped ahead of the total then it can only be because there was new content
		// inserted here that we need to account for.
		if cs.Bi > total {
			next := fmt.Sprintf("NEW(%q)", lines1[total:cs.Bi])
			nextSpec := refspec{content: true, index: len(c.Contents)}
			edges = append(edges, [2]refspec{prevSpec, nextSpec})
			nextMonkey := graph.NewContent{Form: graph.FormText, Content: lines1[total:cs.Bi]}
			monkeyNodes = append(monkeyNodes, [2]interface{}{prevMonkey, nextMonkey})
			prevMonkey = nextMonkey
			fmt.Printf("0 Adding edge spec: %v\n", edges[len(edges)-1])
			fmt.Printf("EDGE: %s -> NEW(%s)\n", prev, next)
			prev = next
			prevSpec = nextSpec
			c.Contents = append(c.Contents, graph.NewContent{Form: graph.FormText, Content: lines1[total:cs.Bi]})
		}

		// The next one or more lines are copied from the source file.

		// This searches for the ReadRange that spans Bi, the start of the text copied from the
		// source file.
		n := sort.Search(len(ranges), func(index int) bool {
			return ranges[index].Depth+ranges[index].Length >= cs.Bi
		})
		next := fmt.Sprintf("REF(%s@%d)", ranges[n].Commit, ranges[n].Depth+cs.Bi-ranges[n].ReadDepth)
		nextSpec := refspec{node: true, index: len(c.NodeRefs)}
		c.NodeRefs = append(c.NodeRefs, graph.NodeRef{Node: ranges[n].Node, Depth: ranges[n].Depth + cs.Bi - ranges[n].ReadDepth})
		edges = append(edges, [2]refspec{prevSpec, nextSpec})
		monkeyNodes = append(monkeyNodes, [2]interface{}{prevMonkey, graph.NodeRef{Node: ranges[n].Node, Depth: 1 + ranges[n].Depth + cs.Bi - ranges[n].ReadDepth}})
		fmt.Printf("1 Adding edge spec: %v\n", edges[len(edges)-1])
		fmt.Printf("EDGE: %s -> %s\n", prev, next)
		fmt.Printf("%d existing lines: %s\n", cs.Length, lines1[cs.Bi:cs.Bi+cs.Length])
		i := cs.Bi
		prev = fmt.Sprintf("beans!!")
		prevMonkey = nil
		for i < cs.Bi+cs.Length {
			used := cs.Length
			fmt.Printf("%d < %d\n", i, cs.Bi+cs.Length)
			if used > ranges[n].Length {
				used = ranges[n].Length
			}
			fmt.Printf("%d lines from %s\n", used, ranges[n].Commit)
			fmt.Printf("i: %d\n", i)
			i += ranges[n].Length
			{
				// This miiiight be right, but we need to test when the common substrings go over multiple
				// nodes, that might screw this up.
				prev = fmt.Sprintf("REFx(%s@%d)", ranges[n].Commit, i-ranges[n].Length+used)
				prevSpec = refspec{node: true, index: len(c.NodeRefs)}
				prevMonkey = graph.NodeRef{Node: ranges[n].Node, Depth: i - ranges[n].Length + used}
			}
			fmt.Printf("i: %d\n", i)
			n++
		}
		total = cs.Bi + cs.Length
	}
	fmt.Printf("EDGE: %s -> %s\n", prev, fmt.Sprintf("snk:%s", path))
	edges = append(edges, [2]refspec{prevSpec, refspec{snk: true, index: len(c.NodeRefs)}})
	monkeyNodes = append(monkeyNodes, [2]interface{}{prevMonkey, graph.NodeRef{Node: fmt.Sprintf("snk:%s", path)}})
	fmt.Printf("Adding edge spec: %v\n", edges[len(edges)-1])
	c.NodeRefs = append(c.NodeRefs, graph.NodeRef{Node: fmt.Sprintf("snk:%s", path)})
	for _, edge := range edges {
		if edge[0].content {
			edge[0].index += len(c.NodeRefs)
		}
		if edge[1].content {
			edge[1].index += len(c.NodeRefs)
		}
		c.EdgeRefs = append(c.EdgeRefs, graph.EdgeRef{Src: edge[0].index, Dst: edge[1].index})
	}
	fmt.Printf("%d monkey nodes\n", len(monkeyNodes))
	for _, n := range monkeyNodes {
		fmt.Printf("(%T - %v) -> (%T - %v)\n", n[0], n[0], n[1], n[1])
	}
	c.NodeRefs = nil
	c.EdgeRefs = nil
	numNodeRefs := 0
	for _, mn := range monkeyNodes {
		if _, ok := mn[0].(graph.NodeRef); ok {
			numNodeRefs++
		}
		if _, ok := mn[1].(graph.NodeRef); ok && mn[1] != mn[0] {
			numNodeRefs++
		}
	}
	for _, mn := range monkeyNodes {
		src, dst := mn[0], mn[1]
		var srcIndex, dstIndex int
		switch n := src.(type) {
		case graph.NodeRef:
			srcIndex = len(c.NodeRefs)
			c.NodeRefs = append(c.NodeRefs, n)
		case graph.NewContent:
			srcIndex = len(c.Contents) + numNodeRefs
			c.Contents = append(c.Contents, n)
		default:
			panic("idiot")
		}
		switch n := dst.(type) {
		case graph.NodeRef:
			dstIndex = len(c.NodeRefs)
			c.NodeRefs = append(c.NodeRefs, n)
		case graph.NewContent:
			dstIndex = len(c.Contents) + numNodeRefs
			// c.Contents = append(c.Contents, n)
		default:
			panic("idiot")
		}
		c.EdgeRefs = append(c.EdgeRefs, graph.EdgeRef{Src: srcIndex, Dst: dstIndex})
	}
	// return &graph.Commit{
	// 	Deps:     nil,
	// 	EdgeRefs: []*graph.EdgeRef{},
	// 	NodeRefs: []*graph.NodeRef{},
	// 	Contents: []*graph.NewContent{},
	// }

	return &c
}

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
