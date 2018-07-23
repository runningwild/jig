package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/runningwild/jig"
	"github.com/runningwild/jig/graph"
	jpb "github.com/runningwild/jig/proto"
	"github.com/runningwild/jig/testutils"
)

var (
	dir = flag.String("dir", "", "directory to use for repo storage, empty to use an in-memory repo")
)

func main() {
	flag.Parse()
	var r graph.Repo
	if *dir == "" {
		r = testutils.MakeFakeRepo()
	} else {
		os.RemoveAll(*dir)
		r = testutils.MakeFileRepo(*dir)
	}
	var commits []*jpb.Commit
	c0 := &jpb.Commit{
		Deps: nil,
		EdgeRefs: []*jpb.EdgeRef{
			{
				Src:    &jpb.NodeRef{Node: "src:sample.txt", Depth: 1},
				Chunks: stringsToContent("a", "b", "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "y", "z", ""),
				Dst:    &jpb.NodeRef{Node: "snk:sample.txt", Depth: 0},
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
	if str := string(bytes.Join(data, []byte("."))); str != "a.b.alpha.bravo.charlie.delta.echo.foxtrot.y.z." {
		panic(fmt.Sprintf("%q vs %q", str, "a.b.alpha.bravo.charlie.delta.echo.foxtrot.y.z."))
	}
	fmt.Printf("*********************************************************************************************************\n")

	c1 := diffmachine(r, explicitFrontier(c0), "sample.txt", stringsToContent(strings.Split("a.b.alpha.BRAVO.CHARLIE.DELTA.echo.foxtrot.y.z.", ".")...))
	c2 := diffmachine(r, explicitFrontier(c0), "sample.txt", stringsToContent(strings.Split("a.b.alpha.bravo.CHARLIE.DELTA.echo.foxtrot.y.z.", ".")...))
	for _, c := range []*jpb.Commit{c1, c2} {
		if err := graph.Apply(r, c); err != nil {
			panic(fmt.Errorf("error applying %s: %v", graph.HashCommit(c), err))
		}
	}
	c3 := diffmachine(r, explicitFrontier(c0, c2), "sample.txt", stringsToContent(strings.Split("a.b.alpha.bravo.CHARLIE.DELTA.ECHO.foxtrot.y.z.", ".")...))
	for _, c := range []*jpb.Commit{c3} {
		if err := graph.Apply(r, c); err != nil {
			panic(fmt.Errorf("error applying %s: %v", graph.HashCommit(c), err))
		}
	}

	lines, err := graph.ReadVersion(r, explicitFrontier(c0, c1), "src:sample.txt", "snk:sample.txt", &graph.ReadMetadata{})
	if err != nil {
		panic(fmt.Errorf("failed to read: %v", err))
	}
	fmt.Printf("XXX: %s\n", bytes.Join(lines, []byte{'.'}))
	lines, err = graph.ReadVersion(r, explicitFrontier(c0, c2, c3), "src:sample.txt", "snk:sample.txt", &graph.ReadMetadata{})
	if err != nil {
		panic(fmt.Errorf("failed to read: %v", err))
	}
	fmt.Printf("XXX: %s\n", bytes.Join(lines, []byte{'.'}))

	conflictsList, err := graph.FindConflicts(r, explicitFrontier(c0, c1, c2, c3), "sample.txt")
	if err != nil {
		panic(err)
	}
	fmt.Printf("Conflict list: %v\n", conflictsList)
	conflictedCommits := make(map[string]bool)
	for _, conflict := range conflictsList {
		for commit := range conflict.Commits {
			conflictedCommits[commit] = true
		}
	}

	fmt.Printf("Conflicts: %v\n", conflictedCommits)
	vs, err := graph.ReadVersions(r, explicitFrontier(c0, c1, c2, c3), nil, "src:sample.txt", "snk:sample.txt", conflictedCommits, []byte("."))
	if err != nil {
		panic(err)
	}
	for _, v := range vs {
		fmt.Printf("%v: %s\n", v.Commits, v.Data)
	}
	output, err := graph.HumanReadable(r, explicitFrontier(c0, c1, c2, c3), nil, "sample.txt", conflictsList, []byte{'\n'})
	if err != nil {
		panic(err)
	}
	fmt.Printf("Output:\n%s\n", output)
}

func diffmachine(r graph.Repo, f graph.Frontier, path string, lines1 [][]byte) *jpb.Commit {
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

	var c jpb.Commit

	curEdge := &jpb.EdgeRef{
		Src: &jpb.NodeRef{Node: fmt.Sprintf("src:%s", path), Depth: 1},
	}
	// prevMonkey = jpb.NodeRef{Node: fmt.Sprintf("src:%s", path), Depth: 1}
	// prev := fmt.Sprintf("src:%s", path)
	// prevSpec := refspec{src: true, index: 0}
	// c.NodeRefs = append(c.NodeRefs, jpb.NodeRef{Node: fmt.Sprintf("src:%s", path), Depth: 1})

	// Loop over the common substrings, this will cover the whole file, though we might need to fill
	// in gaps with new content.
	for _, cs := range css {
		fmt.Printf("Common Substring(%s): Ai: %d,  Bi: %d,  Length: %d\n", contentToString(lines1[cs.Bi:cs.Bi+cs.Length]), cs.Ai, cs.Bi, cs.Length)
		// If Bi has skipped ahead of the total then it can only be because there was new content
		// inserted here that we need to account for.
		if cs.Bi > total {
			fmt.Printf("New Content: %s\n", contentToString(lines1[total:cs.Bi]))
			curEdge.Chunks = lines1[total:cs.Bi]
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

		curEdge.Dst = &jpb.NodeRef{Node: ranges[n].Node, Depth: int32(unused + ranges[n].Depth)}
		fmt.Printf("Using src: %v\n", curEdge.Src)
		fmt.Printf("Setting Dst: %v\n", curEdge.Dst)
		fmt.Printf("Using range: %v\n", ranges[n])
		// Check that we aren't duplicating an existing edge from a src node.
		if strings.HasPrefix(curEdge.Src.Node, "src:") && css[0].Ai == 0 && css[0].Bi == 0 {
			fmt.Printf("skipping src:* edge because it already exists\n")
		} else {
			fmt.Printf("Inserting edge %d\n", len(c.EdgeRefs))
			c.EdgeRefs = append(c.EdgeRefs, curEdge)
		}
		curEdge = &jpb.EdgeRef{}

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
				// NEXT: The fake repo was exposing more content than expected because it was slicing a larger
				// slice.  We made assumptions about that in there that need to be corrected.
				theseLines := r.GetContent(r.GetNode(ranges[n].Node).GetContentHash())[ranges[n].Depth+unused : ranges[n].Depth+unused+used]
				fmt.Printf("Node %s @ %d:%d -> %q\n", ranges[n].Node, ranges[n].Depth+unused, ranges[n].Depth+unused+used, string(bytes.Join(theseLines, []byte{'.'})))
				d, _ := json.MarshalIndent(ranges[n], "  ", "  ")
				fmt.Printf("%s\n", d)
				curEdge.Src = &jpb.NodeRef{Node: ranges[n].Node, Depth: int32(ranges[n].Depth + unused + used)}
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
		curEdge.Chunks = lines1[total:]
	}
	if cs := css[len(css)-1]; cs.Ai+cs.Length == len(lines0) && cs.Bi+cs.Length == len(lines1) {
		fmt.Printf("skipping snk:* edge because it already exists\n")
	} else {
		curEdge.Dst = &jpb.NodeRef{Node: fmt.Sprintf("snk:%s", path), Depth: 0}
		c.EdgeRefs = append(c.EdgeRefs, curEdge)
	}

	// NEXT: need this to work with conflicts.  There are two major issues that need to be addressed:
	// 1. LCS doesn't work with conflicted files, so we'll need a different or maybe hybrid approach.
	// 2. ReadVersion assumes the file is not in conflict, and we'll need to include both versions in
	//    the commit's deps to ensure that it actually resolves the conflict.
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

func explicitFrontier(commits ...*jpb.Commit) simpleFrontier {
	s := make(simpleFrontier)
	for _, c := range commits {
		s[graph.HashCommit(c)] = true
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
