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
	diffmachine(r, allFrontier{}, "sample.txt", stringsToContent("alpha", "bravo", "charlie", "DELTA", "ECHO", "foxtrot", "golf", "hotel", "india", ""))

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

func diffmachine(r graph.Repo, f graph.Frontier, path string, lines1 [][]byte) {
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

	prev := fmt.Sprintf("src:%s", path)
	for _, cs := range css {
		if cs.Bi > total {
			next := fmt.Sprintf("NEW(%q)", lines1[total:cs.Bi])
			fmt.Printf("EDGE: %s -> NEW(%s)\n", prev, next)
			prev = next
		}
		n := sort.Search(len(ranges), func(index int) bool {
			return ranges[index].Depth+ranges[index].Length >= cs.Bi
		})
		next := fmt.Sprintf("REF(%s@%d)", ranges[n].Commit, ranges[n].Depth+cs.Bi-ranges[n].ReadDepth)
		fmt.Printf("EDGE: %s -> %s\n", prev, next)
		fmt.Printf("%d existing lines: %s\n", cs.Length, lines1[cs.Bi:cs.Bi+cs.Length])
		i := cs.Bi
		prev = fmt.Sprintf("beans!!")
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
			}
			fmt.Printf("i: %d\n", i)
			n++
		}
		total = cs.Bi + cs.Length
	}
	fmt.Printf("EDGE: %s -> %s\n", prev, fmt.Sprintf("snk:%s", path))
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
