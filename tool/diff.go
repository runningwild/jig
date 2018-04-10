package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/runningwild/jig"
	"github.com/runningwild/jig/graph"
	"github.com/runningwild/jig/testutils"
)

func main() {
	r := testutils.MakeFakeRepo()
	var commits []*graph.Commit
	c0 := &graph.Commit{
		Deps: nil,
		EdgeRefs: []graph.EdgeRef{
			{
				Src:     graph.NodeRef{Node: "src:sample.txt", Depth: 1},
				Content: &graph.NewContent{Form: graph.FormText, Content: stringsToContent("alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india", "")},
				Dst:     graph.NodeRef{Node: "snk:sample.txt", Depth: 0},
			},
		},
	}
	commits = append(commits, c0)
	fmt.Printf("Appling initial commit\n")
	if err := graph.Apply(r, c0); err != nil {
		panic(err)
	}
	fmt.Printf("Done appling initial commit\n")
	data, err := graph.ReadVersion(r, allFrontier{}, "src:sample.txt", "snk:sample.txt", &graph.ReadMetadata{})
	if err != nil {
		panic(err)
	}
	if str := string(bytes.Join(data, []byte("."))); str != "alpha.bravo.charlie.delta.echo.foxtrot.golf.hotel.india." {
		panic(fmt.Sprintf("%q vs %q", str, "alpha.bravo.charlie.delta.echo.foxtrot.golf.hotel.india."))
	}
	fmt.Printf("*********************************************************************************************************\n")

	allDeps := []string{c0.Hash()}
	prevLines, err := graph.ReadVersion(r, allFrontier{}, "src:sample.txt", "snk:sample.txt", &graph.ReadMetadata{})
	if err != nil {
		panic(err)
	}
	prev := strings.Split(contentToString(prevLines), ",")
	for _, expected := range []string{
		"alpha.bravo.charlie.delta.echo.foxtrot.golf.HOTEL.INDIA",
		"hotel.india.alpha.bravo.charlie.delta.echo.foxtrot.GOLF",
		"alpha.bravo.charlie.DELTA.ECHO.foxtrot.golf.hotel.india",
		"alpha.bravo.golf.hotel.charlie.DELTA.ECHO.foxtrot.india",
		"alpha.hotel.charlie.DELTA.ECHO.foxtrot.india",
		"alpha.ECHO.foxtrot.india.hotel.charlie.DELTA",
		"alpha.ECHO.foxtrot.india.hotel.charlie.DELTA.BEANS.buttons.machines",
		"hotel.charlie.DELTA.BEANS.buttons.foxtrot.india.machines.alpha.ECHO",
	} {
		fmt.Printf("*********************************************************************************************************\n")
		fmt.Printf("\n     %s\n\n", expected)
		lines := strings.Split(expected, ".")
		c1 := diffmachine(r, allFrontier{}, "sample.txt", stringsToContent(lines...))
		commits = append(commits, c1)
		// c1.Deps = append(c1.Deps, allDeps...)
		allDeps = append(allDeps, c1.Hash())

		if prev[0] == lines[0] {
			// Verify that we didn't get an edge from the src node when we didn't need one.
			for _, e := range c1.EdgeRefs {
				if e.Src.Node == "src:sample.txt" {
					panic("got an extraneous edge from a src node")
				}
			}
		}
		if prev[len(prev)-1] == lines[len(lines)-1] {
			// Verify that we didn't get an edge from the src node when we didn't need one.
			for _, e := range c1.EdgeRefs {
				if e.Dst.Node == "snk:sample.txt" {
					panic("got an extraneous edge to a snk node")
				}
			}
		}
		prev = lines

		for i, e := range c1.EdgeRefs {
			fmt.Printf("Edge %d/%d\n", i, len(c1.EdgeRefs))
			fmt.Printf("  Src: %v\n", e.Src)
			fmt.Printf("  Con: %v\n", e.Content)
			fmt.Printf("  Dst: %v\n", e.Dst)
		}

		fmt.Printf("Applying commit: %v\n", c1)
		if err := graph.Apply(r, c1); err != nil {
			panic(fmt.Sprintf("failed to apply commit: %v", err))
		}
		fmt.Printf("Done applying commit: %v\n", c1)
		data, err = graph.ReadVersion(r, allFrontier{}, "src:sample.txt", "snk:sample.txt", &graph.ReadMetadata{})
		if err != nil {
			panic(err)
		}
		expected := strings.Join(lines, ".")
		if str := string(bytes.Join(data, []byte("."))); str != expected {
			panic(fmt.Sprintf("%q vs %q", str, expected))
		}
	}
	fmt.Printf("All Commits:\n")
	for _, c := range commits {
		fmt.Printf("%s: %v\n", c.Hash(), c.Deps)
	}
}

func diffmachine(r graph.Repo, f graph.Frontier, path string, lines1 [][]byte) *graph.Commit {
	var ranges []graph.ReadRange
	lines0, err := graph.ReadVersion(r, f, fmt.Sprintf("src:%s", path), fmt.Sprintf("snk:%s", path), &graph.ReadMetadata{Ranges: &ranges})
	if err != nil {
		panic(err)
	}

	fmt.Printf("lines0: %s\n", lines0)
	for i := range ranges {
		d, _ := json.MarshalIndent(ranges[i], "", "  ")
		fmt.Printf("%d: %s\n", i, d)
	}

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
	fmt.Printf("Common substrings:\n")
	for _, cs := range css {
		a := string(bytes.Join(lines0[cs.Ai:cs.Ai+cs.Length], []byte{'.'}))
		b := string(bytes.Join(lines1[cs.Bi:cs.Bi+cs.Length], []byte{'.'}))
		fmt.Printf("%s vs %s\n", a, b)
		if a != b {
			panic("balls")
		}
	}
	total := 0

	var c graph.Commit

	curEdge := &graph.EdgeRef{
		Src: graph.NodeRef{Node: fmt.Sprintf("src:%s", path), Depth: 1},
	}
	// prevMonkey = graph.NodeRef{Node: fmt.Sprintf("src:%s", path), Depth: 1}
	// prev := fmt.Sprintf("src:%s", path)
	// prevSpec := refspec{src: true, index: 0}
	// c.NodeRefs = append(c.NodeRefs, graph.NodeRef{Node: fmt.Sprintf("src:%s", path), Depth: 1})

	// Loop over the common substrings, this will cover the whole file, though we might need to fill
	// in gaps with new content.
	for _, cs := range css {
		fmt.Printf("Common Substring(%s): Ai: %d,  Bi: %d,  Length: %d\n", contentToString(lines1[cs.Bi:cs.Bi+cs.Length]), cs.Ai, cs.Bi, cs.Length)
		// If Bi has skipped ahead of the total then it can only be because there was new content
		// inserted here that we need to account for.
		if cs.Bi > total {
			fmt.Printf("New Content: %s\n", contentToString(lines1[total:cs.Bi]))
			curEdge.Content = &graph.NewContent{Form: graph.FormText, Content: lines1[total:cs.Bi]}
		}

		// The next one or more lines are copied from the source file.

		// This searches for the ReadRange that spans Ai, the start of the text copied from the
		// source file.
		n := sort.Search(len(ranges), func(index int) bool {
			return ranges[index].ReadDepth+ranges[index].Length > cs.Ai
		})
		fmt.Printf("Dst node: %s\n", ranges[n].Node)
		fmt.Printf("Dst Depth: %d\n", ranges[n].Depth)

		// unused is how much of the node isn't part of this common substring.  This can only be
		// positive for the first ReadRange in a common substring, and it affects the dst node of
		// the current edge, and potentially the src node of the next edge if we don't have to move
		// to the next ReadRange.
		unused := cs.Ai - ranges[n].ReadDepth
		fmt.Printf("Ai vs ReadDepth: %d vs %d\n", cs.Ai, ranges[n].ReadDepth)

		curEdge.Dst = graph.NodeRef{Node: ranges[n].Node, Depth: unused + ranges[n].Depth}
		fmt.Printf("Using src: %v\n", curEdge.Src)
		fmt.Printf("Setting Dst: %v\n", curEdge.Dst)
		fmt.Printf("Using range: %v\n", ranges[n])
		// Check that we aren't duplicating an existing edge from a src node.
		if strings.HasPrefix(curEdge.Src.Node, "src:") && css[0].Ai == 0 && css[0].Bi == 0 {
			fmt.Printf("skipping src:* edge because it already exists\n")
		} else {
			fmt.Printf("Inserting edge %d\n", len(c.EdgeRefs))
			c.EdgeRefs = append(c.EdgeRefs, *curEdge)
		}
		curEdge = &graph.EdgeRef{}

		fmt.Printf("--- Checking the next common substring: %q\n", lines1[cs.Bi:cs.Bi+cs.Length])
		i := cs.Bi
		covered := 0 // Number of nodes covered so far in the iterations of the following loop.
		for i < cs.Bi+cs.Length {
			// depth := ranges[n].Depth - unused
			used := cs.Length - covered
			if used > ranges[n].Length-unused {
				used = ranges[n].Length - unused
			}
			{
				theseLines := r.GetContent(r.GetNode(ranges[n].Node).Content)[ranges[n].Depth+unused : ranges[n].Depth+unused+used]
				fmt.Printf("Node %s @ %d:%d -> %q\n", ranges[n].Node, ranges[n].Depth+unused, ranges[n].Depth+unused+used, string(bytes.Join(theseLines, []byte{'.'})))
				d, _ := json.MarshalIndent(ranges[n], "  ", "  ")
				fmt.Printf("%s\n", d)
				curEdge.Src = graph.NodeRef{Node: ranges[n].Node, Depth: ranges[n].Depth + unused + used}
				fmt.Printf("  set src to %v, %d = %d + %d + %d\n", curEdge.Src, curEdge.Src.Depth, ranges[n].Depth, unused, used)
			}
			covered += used
			i += ranges[n].Length - unused
			n++
			fmt.Printf("   i: %d\n", i)
			fmt.Printf("   cs.Bi: %d\n", cs.Bi)
			fmt.Printf("   cs.Length: %d\n", cs.Length)
			unused = 0
		}
		// if n < len(ranges) {
		// 	theseLines := r.GetContent(r.GetNode(ranges[n].Node).Content)[ranges[n].Depth : ranges[n].Depth+ranges[n].Length]
		// 	fmt.Printf("Ended at %s: %s\n", ranges[n].Node, theseLines)
		// }
		total = cs.Bi + cs.Length
		fmt.Printf("Ended loop with %d >= %d+%d\n", i, cs.Bi, cs.Length)
	}
	if total < len(lines1) {
		fmt.Printf("New Content: %s\n", contentToString(lines1[total:]))
		curEdge.Content = &graph.NewContent{Form: graph.FormText, Content: lines1[total:]}
	}
	if cs := css[len(css)-1]; cs.Ai+cs.Length == len(lines0) && cs.Bi+cs.Length == len(lines1) {
		fmt.Printf("skipping snk:* edge because it already exists\n")
	} else {
		curEdge.Dst = graph.NodeRef{Node: fmt.Sprintf("snk:%s", path), Depth: 0}
		c.EdgeRefs = append(c.EdgeRefs, *curEdge)
	}

	depSet := make(map[string]bool)
	for _, e := range c.EdgeRefs {
		if e.Src.Node == e.Dst.Node {
			depSet[r.GetNode(e.Src.Node).In[0].Commit] = true
			fmt.Printf("Added %q to deps\n", r.GetNode(e.Src.Node).In[0].Commit)
			continue
		}
		fmt.Printf("edge ref: %s -> %s\n", e.Src.Node, e.Dst.Node)

		// If we are making a reverse edge we need to read the edges in reverse.  This will detect
		// that and swap src and dst if necessary.
		src, dst := e.Src.Node, e.Dst.Node
		if !strings.HasPrefix(e.Src.Node, "src:") && !strings.HasPrefix(e.Dst.Node, "snk:") {
			for _, r := range ranges {
				if r.Node == e.Src.Node {
					fmt.Printf("found src first\n")
					break
				}
				if r.Node == e.Dst.Node {
					fmt.Printf("found dst first\n")
					src, dst = dst, src
					break
				}
			}
		}
		if _, err := graph.ReadVersion(r, f, src, dst, &graph.ReadMetadata{Commits: depSet}); err != nil {
			panic(err)
		}
	}
	for dep := range depSet {
		c.Deps = append(c.Deps, dep)
	}
	sort.Strings(c.Deps)

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
	return string(bytes.Join(content, []byte{'.'}))
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
