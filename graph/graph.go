package graph

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash"
	"strings"

	skein512 "github.com/runningwild/skein/hash/512"
)

type Repo interface {
	GetRef(ptr string) string
	GetNode(nodeHash string) *Node
	GetContent(contentHash string) [][]byte
	GetCommit(commitHash string) *Commit

	// List methods all fill out the given slice with as many hashes as possible of the specified,
	// it returns the number of elements filled.
	ListRefs(start string, refs []string) (n int)
	ListNodes(start string, nodes []string) (n int)
	ListContents(start string, contents []string) (n int)
	ListCommits(start string, commits []string) (n int)

	StartTransaction()
	EndTransaction() error
	PutRef(ptr, val string)
	PutNode(n *Node)
	PutCommit(c *Commit)
	DeleteNode(nodeHash string)

	// TODO: Need to decide how to handle multiple references to a single content.  GC or reference counting?
	PutContent(content [][]byte) string
	DeleteContent(contentHash string)
}

// SplitNode takes a node and a depth and replaces that node with two nodes, split at the specified
// depth.  The first of those nodes will have the same Head hash, and the second will have the same
// Tail hash.  This function will return the Tail hash of the first node and the Head hash of the second.
func SplitNode(r Repo, node string, depth int) (tail, head string, err error) {
	fmt.Printf("Splitting %s @ %d\n", node, depth)
	if depth <= 0 {
		// panic("NOOB")
		return "", "", fmt.Errorf("cannot split a node at depth <= 0")
	}
	var n *Node
	for n = r.GetNode(node); n != nil && depth > n.Count && len(n.Out) > 0; n = r.GetNode(n.Out[0].Node) {
		depth -= n.Count
	}
	if n == nil || depth > n.Count {
		return "", "", fmt.Errorf("depth is beyond original node's length")
	}

	// For simplicity we'll just special case splitting at the very beginning of a file...
	if n.Form == FormFileSrc && depth == 1 {
		return n.Tail, "", nil
	}
	// ... and the very end.
	if n.Form == FormFileSnk && depth == 0 {
		return "", n.Head, nil
	}

	commitHash := n.In[0].Commit

	// Split the content
	// TODO: This needs to work for other formats
	if n.Form != FormText {
		panic("unsupported format")
	}
	content := r.GetContent(n.Content)
	if content == nil {
		return "", "", fmt.Errorf("node content not found")
	}
	if len(content) != n.Count {
		return "", "", fmt.Errorf("%q is malformed", n.Head)
	}
	if depth == len(content) {
		// No splitting necessary
		return n.Tail, n.Out[0].Node, nil
	}

	head0, tail0 := CalculateNodeHashes(commitHash, n.In[0].Node, n.Form, content[0:depth])
	head1, tail1 := CalculateNodeHashes(commitHash, tail0, n.Form, content[depth:])
	if head0 != n.Head {
		return "", "", fmt.Errorf("failed to calculate node head hashes properly")
	}
	if tail1 != n.Tail {
		return "", "", fmt.Errorf("failed to calculate node tail hashes properly")
	}

	r.StartTransaction()
	defer func() {
		tErr := r.EndTransaction()
		if err == nil {
			err = tErr
		}
	}()

	contentA := r.PutContent(content[0:depth])
	contentB := r.PutContent(content[depth:])

	a := &Node{
		Head:    head0,
		Tail:    tail0,
		Form:    n.Form,
		Content: contentA,
		Count:   depth,
		In:      n.In,
		Out:     []Edge{{Commit: commitHash, Node: head1}},
		OutDeps: nil,
	}
	b := &Node{
		Head:    head1,
		Tail:    tail1,
		Form:    n.Form,
		Content: contentB,
		Count:   n.Count - depth,
		In:      []Edge{{Commit: commitHash, Node: tail0}},
		Out:     n.Out,
		OutDeps: n.OutDeps,
	}

	r.PutNode(a) // This overwrite the previous node
	r.PutNode(b)

	// Update refs.  This will also update anything that pointed to the tail of the original node to
	// end up on the second node rather than the first.
	r.PutRef(tail1, head1)
	r.PutRef(tail0, head0)

	return tail0, head1, nil
}

// ReadVersions returns a []Version with one Version for each conflicting view of the file.  If prev
// is not nil, then one of the Versions will correspond to the conflicting commits, and all other
// Versions will contain a single conflicting commit.
// TODO: The restriction below should be verified.
// If prev is set it must be a frontier at which there is no conflict between start and end.
func ReadVersions(r Repo, f, prev Frontier, start, end string, conflicts map[string]bool, join []byte) ([]Version, error) {
	var versions []Version
	used := make(map[string]bool)
	if prev != nil {
		allCommits := make(map[string]bool)
		lines, err := ReadVersion(r, prev, start, end, &ReadMetadata{Commits: allCommits})
		if err != nil {
			return nil, err
		}
		data := bytes.Join(lines, join)
		commits := make(map[string]bool)
		for c := range allCommits {
			if _, ok := conflicts[c]; ok {
				commits[c] = true
			}
		}
		versions = append(versions, Version{Commits: commits, Data: data})
		used = commits
	}

	for c := range conflicts {
		if used[c] {
			continue
		}
		next := &addToFrontier{f: &removeFromFrontier{f: f, remove: conflicts}, add: map[string]bool{c: true}}
		lines, err := ReadVersion(r, next, start, end, &ReadMetadata{})
		if err != nil {
			return nil, err
		}
		data := bytes.Join(lines, join)
		versions = append(versions, Version{Commits: map[string]bool{c: true}, Data: data})
	}
	return versions, nil
}

type addToFrontier struct {
	f   Frontier
	add map[string]bool
}

func (f *addToFrontier) Observes(commit string) bool {
	return f.f.Observes(commit) || f.add[commit]
}

type removeFromFrontier struct {
	f      Frontier
	remove map[string]bool
}

func (f *removeFromFrontier) Observes(commit string) bool {
	return f.f.Observes(commit) && !f.remove[commit]
}

// commits is optional, if set it will get filled with all commits touched between start and end.
// Reads the data between start and end, including the last chunk of start, if any, and the first
// chunk of end, if any.
// TODO: Need to be able to distinguish between an empty file, a non-existent file, and a conflict.
func ReadVersion(r Repo, f Frontier, start, end string, metadata *ReadMetadata) ([][]byte, error) {
	var buf [][]byte
	n := r.GetNode(start)
	if n == nil {
		return nil, fmt.Errorf("failed to find start node %s", start)
	}
	if len(n.In) == 0 && len(n.Out) == 0 {
		return nil, fmt.Errorf("start node was invalid, it had no input or output edges")
	}
	if start == end {
		return nil, fmt.Errorf("start and end were the same node")
	}
	prev := n
	// fmt.Printf("End is %s\n", nodeContent(r, end))
	fmt.Printf("Pathing from %s to %s\n", start, end)

	// Only take the last chunk from the starting node.
	if content := r.GetContent(n.Content); len(content) > 0 {
		buf = append(buf, content[len(content)-1])
	}

	readDepth := 0
	for {
		fmt.Printf("On node %q: %s\n", n.Head, nodeContent(r, n.Head))
		for i := len(n.Out) - 1; i >= 0; i-- {
			e := n.Out[i]
			if !f.Observes(e.Commit) {
				continue
			}
			if metadata.Commits != nil {
				metadata.Commits[n.Out[0].Commit] = true
			}
			n = r.GetNode(e.Node)
			if n == nil {
				return nil, fmt.Errorf("failed to find node %s in the repo", e.Node)
			}
			break
		}
		// TODO: check for cycles as we traverse
		if n == prev {
			return nil, fmt.Errorf("failed to find an outgoing edge from %s", prev.Head)
		}
		if n.Head == end {
			break
		}
		// fmt.Printf("%s\n", nodeContent(r, n.Head))
		content := r.GetContent(n.Content)
		buf = append(buf, content...)
		if metadata.Ranges != nil {
			ref, depth := nodeRef(r, n)
			fmt.Printf("Node Reffing %v\n", n)
			fmt.Printf("nodeRef(%s) -> %s %d\n", n.Head, ref, depth)
			*metadata.Ranges = append(*metadata.Ranges, ReadRange{Commit: nodeCommit(n), Node: ref, Depth: depth, ReadDepth: readDepth, Length: len(content)})
			readDepth += len(content)
		}
		prev = n
	}

	// Only take the first chunk from the end node.
	if content := r.GetContent(n.Content); len(content) > 0 {
		buf = append(buf, content[0])
	}

	return buf, nil
}

func nodeCommit(n *Node) string {
	if len(n.In) != 0 {
		return n.In[0].Commit
	}
	if len(n.Out) != 0 {
		return n.Out[0].Commit
	}
	panic("how did you let this happen?")
}

func nodeRef(r Repo, n *Node) (string, int) {
	fmt.Printf("Getting ref for %s: %s\n", n.Head, nodeContent(r, n.Head))
	prev := r.GetNode(r.GetRef(n.In[0].Node))
	fmt.Printf("prev: %s\n", n.In[0].Node)
	if nodeCommit(prev) != nodeCommit(n) || prev.Form == FormFileSrc {
		return n.Head, 0
	}
	ref, d := nodeRef(r, prev)
	return ref, d + prev.Count
}

type ReadMetadata struct {
	// If non-nil this will be filled with all commits touched between start and end.  Reads the
	// data between start and end, including the last chunk of start, if any, and the first chunk
	// of end, if any.
	Commits map[string]bool

	// If non-nil this will be filled with the list of ReadRanges that covers everything read.
	Ranges *[]ReadRange
}

// ReadRange indicates what commit was responsible for content in a file, and how many contiguous
// nodes it was responsible for.
type ReadRange struct {
	Commit    string
	Node      string
	Depth     int // Depth into the node
	ReadDepth int // Depth into the read
	Length    int
}

type Version struct {
	Data    []byte
	Commits map[string]bool
}

func jigStandardHasher() hash.Hash {
	return skein512.NewHash512(24)
}

func HashContent(content [][]byte) string {
	h := jigStandardHasher()
	for _, line := range content {
		length := uint32(len(line))
		h.Write([]byte{byte(length), byte(length >> 8), byte(length >> 16), byte(length >> 24)})
		h.Write(line)
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

func Apply(r Repo, c *Commit) error {
	// Validate that we haven't already applied this commit.
	commitHash := c.Hash()
	if c2 := r.GetCommit(commitHash); c2 != nil {
		return fmt.Errorf("this commit has already been applied")
	}

	// Validate that we have all of the commits this commit depends on.
	for _, dep := range c.Deps {
		if d := r.GetCommit(dep); d == nil {
			return fmt.Errorf("this commit depends on %q which we don't have", dep)
		}
	}

	// TODO: Validate at LEAST all of the following:
	//   all new content has an outgoing edge (which may be to an EOF marker).
	//   no cycles
	//   no creation of two edges from or to the same node
	//   really there are so many more...
	//   new content nodes do not connect to other new content nodes
	//   new content nodes have an input and output edge

	for _, e := range c.EdgeRefs {
		if e.Src.Depth < 1 && !strings.HasPrefix(e.Src.Node, "src:") {
			return fmt.Errorf("cannot specify an EdgeRef with a content src node at depth < 1")
		}
		if e.Src.Depth < 0 && !strings.HasPrefix(e.Src.Node, "snk:") {
			return fmt.Errorf("cannot specify an EdgeRef with a content dst node at depth < 0")
		}
	}

	for _, e := range c.EdgeRefs {
		var tail, head string

		// Create src and snk nodes if they don't already exist.
		if strings.HasPrefix(e.Src.Node, "src:") {
			r.PutNode(&Node{Head: e.Src.Node, Tail: e.Src.Node, Form: FormFileSrc, Count: 1})
			r.PutRef(e.Src.Node, e.Src.Node)
		}
		if strings.HasPrefix(e.Dst.Node, "snk:") {
			r.PutNode(&Node{Head: e.Dst.Node, Tail: e.Dst.Node, Form: FormFileSnk, Count: 1})
			r.PutRef(e.Dst.Node, e.Dst.Node)
		}

		var err error
		fmt.Printf("Splitting src node %q at %d\n", e.Src.Node, e.Src.Depth)
		tail, _, err = SplitNode(r, e.Src.Node, e.Src.Depth)
		if err != nil {
			return fmt.Errorf("error splitting src node: %v", err)
		}

		if e.Dst.Depth == 0 {
			// This is fine, this just means an edge will connect directly to e.Dst and we don't
			// need to split anything to make that happen.
			head = e.Dst.Node
		} else {
			var err error
			_, head, err = SplitNode(r, e.Dst.Node, e.Dst.Depth)
			if err != nil {
				return fmt.Errorf("error splitting dst node: %v", err)
			}
		}

		srcRef := r.GetRef(tail)
		if srcRef == "" {
			return fmt.Errorf("failed to get ref for %s", tail)
		}
		src := r.GetNode(srcRef)
		if src == nil {
			return fmt.Errorf("failed to get node for ref %s from tail %s", srcRef, tail)
		}
		dst := r.GetNode(head)
		if dst == nil {
			return fmt.Errorf("failed to get dst node %s", head)
		}
		if e.Content == nil {
			src.Out = append(src.Out, Edge{Commit: commitHash, Node: dst.Head, Primary: true})
			dst.In = append(dst.In, Edge{Commit: commitHash, Node: src.Tail, Primary: true})
			r.PutNode(src)
			r.PutNode(dst)
			continue
		}

		content := r.PutContent(e.Content.Content)
		newHead, newTail := CalculateNodeHashes(commitHash, tail, e.Content.Form, e.Content.Content)
		middle := &Node{
			Head:    newHead,
			Tail:    newTail,
			Form:    e.Content.Form,
			Content: content,
			Count:   len(e.Content.Content),
			In: []Edge{{
				Commit:  commitHash,
				Node:    tail,
				Primary: true,
			}},
			Out: []Edge{{
				Commit:  commitHash,
				Node:    head,
				Primary: true,
			}},
			OutDeps: [][]int{}, // WHY AM I STILL DOING THIS!?!?!??!?
		}
		r.PutNode(middle)

		src.Out = append(src.Out, Edge{Commit: commitHash, Node: middle.Head, Primary: true})
		dst.In = append(dst.In, Edge{Commit: commitHash, Node: middle.Tail, Primary: true})
		r.PutNode(src)
		r.PutNode(dst)
	}

	r.PutCommit(c)
	return nil
}

type EdgeRef struct {
	Src, Dst NodeRef

	// Content inserted between Src and Dst.  This can be nil, in which case the edge created by
	// this EdgeRef connects Src directly to Dst.
	Content *NewContent
}

type NodeRef struct {
	// Typical hash of the node that this NodeRef refers to.  This may also refer to nodes that are
	// created by this commit.
	Node string

	// Depth indicates how many nodes should be used before inserting the edge in question.  For
	// example, if there are nodes connected like A -> B -> C and a src node specifies node A with
	// depth 2, that refers to an outgoing edge B, but a dst node that specifies node A with depth 2
	// is referring to an incoming edge into C.  Because of this a src node must specify Depth >= 1,
	// a dst node must specify Depth >= 0.
	Depth int
}

type NewContent struct {
	Form    Form
	Content [][]byte
}

type Commit struct {
	Deps     []string // Commit hashes
	EdgeRefs []EdgeRef
}

func (c *Commit) Hash() string {
	h := jigStandardHasher()

	binary.Write(h, binary.LittleEndian, uint32(len(c.Deps)))
	for _, d := range c.Deps {
		binary.Write(h, binary.LittleEndian, uint32(len(d)))
		h.Write([]byte(d))
	}

	binary.Write(h, binary.LittleEndian, uint32(len(c.EdgeRefs)))
	for _, e := range c.EdgeRefs {
		binary.Write(h, binary.LittleEndian, []byte(e.Src.Node))
		binary.Write(h, binary.LittleEndian, uint32(e.Src.Depth))

		if e.Content == nil {
			binary.Write(h, binary.LittleEndian, uint32(0))
		} else {
			binary.Write(h, binary.LittleEndian, uint32(e.Content.Form))
			binary.Write(h, binary.LittleEndian, uint32(len(e.Content.Content)))
			for _, line := range e.Content.Content {
				binary.Write(h, binary.LittleEndian, uint32(len(line)))
				binary.Write(h, binary.LittleEndian, []byte(line))
			}
		}

		binary.Write(h, binary.LittleEndian, []byte(e.Dst.Node))
		binary.Write(h, binary.LittleEndian, uint32(e.Dst.Depth))
	}

	return fmt.Sprintf("%x", h.Sum(nil))
}

// To apply a Commit we need to do the following:
// 1 - Verify that all commits in Deps have already been applied, if not we must bail.
// 2 - For each value in Contents we need to create a new Node with that Content, and must count the
//     internal nodes.
// 3 - For each edge in EdgeRefs we need to find the corresponding src and dst nodes and insert edges.

// Node contains POD.  All of the edge data is embedded so that they can be split when necessary.
// When a Node splits, we get two nodes whose Form, Content, and Count are determined in the obvious
// way.  The first in the pair gets the In edges and an Out edge to the second in the pair, the last
// gets the Out edges and an In edge to the first in the pair.
type Node struct {
	// Head is the hash of the first true node in this Node.  Whenever this Node is split, the head
	// hash will never change.  Tail is the hash of the last true node in this Node.  If Count == 1
	// it will always equal Head, otherwise any time this Node is split, the second Node created
	// will implicitly inherit the value of Tail prior to the split.
	Head, Tail string

	Form    Form
	Content string // Content hash
	Count   int    // Total number of nodes in this Node, will be at least 1.

	// Incoming edges.  If the first edge in this list is a primary edge then it was the one
	// originally responsible for the content in this Node (and possible others after it).
	In []Edge

	// Out edges come from different commits.  When traversing we need to select the newest one that
	// is in the transitive closure of our frontier.  Because edges can only be added once the
	// commits the depend on have been added, the commits corresponding to the edges in these list
	// are necessarily a topologicaly ordered subset of all commits, as such if the frontier
	// contains all commits and this file is not in conflict, then the correct edge to follow is
	// always the last edge in the list.
	Out     []Edge
	OutDeps [][]int
}

func CalculateNodeHashes(commit, prev string, form Form, content [][]byte) (head, tail string) {
	if form != FormText {
		panic("unknown form")
	}
	for _, line := range content {
		h := jigStandardHasher()
		binary.Write(h, binary.LittleEndian, uint32(len(commit)))
		h.Write([]byte(commit))
		binary.Write(h, binary.LittleEndian, uint32(len(prev)))
		h.Write([]byte(prev))
		h.Write(line)
		prev = fmt.Sprintf("%x", h.Sum(nil))
		if head == "" {
			head = prev
		}
	}
	tail = prev
	return
}

// A Frontier indicates a view of the repo.  It is used when traversing a file to decide which
// commits' edges should be used.
// TODO: Implement this thing, probably want to store which commits the Frontier *doesn't* observe,
// 		 since that set will typically be much smaller than those it *does* observe.
type Frontier interface {
	Observes(commit string) bool
}

type FileNode struct {
	// Path will always be implicit
	Out []Edge
}

type Edge struct {
	Commit string

	// There are two special values for output node hashes.
	// If this edge is coming from a file node:
	// "0": The file has been deleted
	// If this edge is coming from an internal node:
	// "1": This is the end of the file.
	Node string

	// Primary edges are added as part of a commit.  Secondary (i.e. !Primary) are implicit until a Node is split and
	// then they are made explicit.
	Primary bool
}

func (e *Edge) Hash() string {
	h := jigStandardHasher()

	binary.Write(h, binary.LittleEndian, uint32(len(e.Commit)))
	h.Write([]byte(e.Commit))

	binary.Write(h, binary.LittleEndian, uint32(len(e.Node)))
	h.Write([]byte(e.Node))

	if e.Primary {
		h.Write([]byte{1})
	} else {
		h.Write([]byte{0})
	}

	return fmt.Sprintf("%x", h.Sum(nil))
}

type Form int

const (
	// FormText is a text file.  Boundaries are '\n' or EOF.  A file that ends with a newline will
	// have a zero-length line at the end.
	FormText = iota

	// Used for the first and last nodes in a file only.
	FormFileSrc
	FormFileSnk

	// FormBinary is a binary file.  TODO: Need to define the chunking algorithm
//	FormBinary
)
