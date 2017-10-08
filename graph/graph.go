package graph

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash"

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
	if depth <= 0 {
		return "", "", fmt.Errorf("cannot split a node at depth <= 0")
	}
	var n *Node
	for n = r.GetNode(node); n != nil && depth > n.Count && len(n.Out) > 0; n = r.GetNode(n.Out[0].Node) {
		depth -= n.Count
	}
	if n == nil || depth > n.Count {
		return "", "", fmt.Errorf("depth is beyond original node's length")
	}

	// For simplicity we'll just special case splitting at the very beginning of a file.
	if n.Form == FormFileSrc && depth == 1 {
		return n.Tail, n.Out[0].Node, nil
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

// TODO: this currently can returns version that, while self-consistent, put A and B in separate version
// when A depends on B, which is a bit odd.  To fix this we need to be able to go from a set of commits
// to a subgraph that shows their transitive relationships.
// If prev is set it must be a frontier at which there is no conflict between start and end.
func ReadVersions(r Repo, f, prev Frontier, start, end string, conflicts map[string]bool, join []byte) ([]Version, error) {
	var versions []Version
	used := make(map[string]bool)
	if prev != nil {
		allCommits := make(map[string]bool)
		data, err := ReadVersion(r, prev, start, end, join, allCommits)
		if err != nil {
			return nil, err
		}
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
		data, err := ReadVersion(r, next, start, end, join, nil)
		if err != nil {
			return nil, err
		}
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
func ReadVersion(r Repo, f Frontier, start, end string, join []byte, commits map[string]bool) ([]byte, error) {
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
	fmt.Printf("End is %s\n", nodeContent(r, end))
	fmt.Printf("Pathing from %s to %s\n", start, end)

	// Only take the last chunk from the starting node.
	if content := r.GetContent(n.Content); len(content) > 0 {
		buf = append(buf, content[len(content)-1])
	}

	for {
		fmt.Printf("On node %q\n", n.Head)
		for i := len(n.Out) - 1; i >= 0; i-- {
			e := n.Out[i]
			if !f.Observes(e.Commit) {
				continue
			}
			if commits != nil {
				commits[n.Out[0].Commit] = true
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
		buf = append(buf, r.GetContent(n.Content)...)
		prev = n
	}

	// Only take the first chunk from the end node.
	if content := r.GetContent(n.Content); len(content) > 0 {
		buf = append(buf, content[0])
	}

	return bytes.Join(buf, join), nil
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

	// TODO: Validate that all new content has an outgoing edge (which may be to an EOF marker).

	// Split all nodes at the appropriate positions.
	for _, e := range c.EdgeRefs {
		if e.Src >= 0 && e.Src < len(c.NodeRefs) {
			if _, _, err := SplitNode(r, c.NodeRefs[e.Src].Node, c.NodeRefs[e.Src].Depth); err != nil {
				return fmt.Errorf("error splitting src node: %v", err)
			}
		}
		if e.Dst >= 0 && e.Dst < len(c.NodeRefs) {
			if _, _, err := SplitNode(r, c.NodeRefs[e.Dst].Node, c.NodeRefs[e.Dst].Depth-1); err != nil {
				return fmt.Errorf("error splitting dst node: %v", err)
			}
		}
	}

	// Now go back and get the hashes.  We have to do all of the splitting first because we might
	// split some nodes multiple times before we're done.

	// Create all of the new nodes added by this commit.
	var newNodes []string
	for i, nc := range c.Contents {
		if nc.Form == FormFileSrc || nc.Form == FormFileSnk {
			mode := "src"
			if nc.Form == FormFileSnk {
				mode = "snk"
			}
			n := &Node{
				Head:  fmt.Sprintf("%s:%s", mode, nc.Path),
				Form:  nc.Form,
				Count: 1,
			}
			n.Tail = n.Head
			r.PutNode(n)
			r.PutRef(n.Tail, n.Head)
			newNodes = append(newNodes, n.Head)
			continue
		}
		n := &Node{
			Form:    nc.Form,
			Content: HashContent(nc.Content), // Content hash
			Count:   len(nc.Content),
		}

		var prev string
		// We need to know the previous node's hash to calculate this node's hash.  If this is a
		// new file then it's just the path, otherwise we need to look through the edges to find
		// which one is pointing to this node, the other end of that edge will be the previous node.
		for _, e := range c.EdgeRefs {
			if e.Dst != len(c.NodeRefs)+i {
				continue
			}
			var prevNode *Node
			if e.Src < len(c.NodeRefs) {
				prevNode = r.GetNode(c.NodeRefs[e.Src].Node)
				prev = prevNode.Tail
			} else {
				// The only reason to have an edge to a NodeRef is if the NodeRef is a src or
				// snk node, otherwise the contents should have just been combined into a single
				// node.  Since we're looking at incoming edges, the only valid form is FormFileSrc.
				content := c.Contents[e.Src-len(c.NodeRefs)]
				if content.Form != FormFileSrc {
					return fmt.Errorf("malformed commit: edge connected something other than FormFile or FormFileSrc to another FormFile")
				}
				prev = fmt.Sprintf("src:%s", content.Path)
			}
		}
		n.Head, n.Tail = CalculateNodeHashes(commitHash, prev, n.Form, nc.Content)
		r.PutNode(n)
		r.PutRef(n.Tail, n.Head)
		newNodes = append(newNodes, n.Head)
	}

	// TODO: We might want transactions to be recursive, so that we can split nodes here and, if the
	// commit fails to apply, unsplit them for free.

	// r.StartTransaction()
	// defer r.EndTransaction()

	for _, nc := range c.Contents {
		r.PutContent(nc.Content)
	}

	depsMap := make(map[string]bool)
	for _, dep := range c.Deps {
		depsMap[dep] = true
	}

	for _, e := range c.EdgeRefs {
		var srcNode, dstNode *Node
		var dstHead string
		if e.Src < len(c.NodeRefs) {
			tail, _, err := SplitNode(r, c.NodeRefs[e.Src].Node, c.NodeRefs[e.Src].Depth)
			if err != nil {
				return fmt.Errorf("lame split error: %v", err)
			}
			srcNode = r.GetNode(r.GetRef(tail))
			// fmt.Printf("%q -> %q -> %v\n", tail, r.GetRef(tail), srcNode)
		} else {
			srcNode = r.GetNode(newNodes[e.Src-len(c.NodeRefs)])
		}

		switch {
		case e.Dst == -2:
			panic("not implemented yet")
		case e.Dst >= 0 && e.Dst < len(c.NodeRefs):
			var err error
			_, dstHead, err = SplitNode(r, c.NodeRefs[e.Dst].Node, c.NodeRefs[e.Dst].Depth-1)
			if err != nil {
				return fmt.Errorf("lame split error: %v", err)
			}
			dstNode = r.GetNode(dstHead)
		case e.Dst >= len(c.NodeRefs):
			dstNode = r.GetNode(newNodes[e.Dst-len(c.NodeRefs)])
			dstHead = dstNode.Head
		default:
			panic(fmt.Sprintf("what the beans?  got dst %d", e.Dst))
		}
		// fmt.Printf("Out: %q -> %q\n", srcNode.Tail, dstHead)
		srcNode.Out = append(srcNode.Out, Edge{
			Commit:  commitHash,
			Node:    dstHead,
			Primary: true,
		})
		var outDeps []int
		for i := range srcNode.Out {
			if depsMap[srcNode.Out[i].Commit] {
				outDeps = append(outDeps, i)
			}
		}
		srcNode.OutDeps = append(srcNode.OutDeps, outDeps)
		r.PutNode(srcNode)

		if dstNode != nil {
			// fmt.Printf("In: %q <- %q\n", srcNode.Tail, dstNode.Head)
			dstNode.In = append(dstNode.In, Edge{
				Commit:  commitHash,
				Node:    srcNode.Tail,
				Primary: true,
			})
			r.PutNode(dstNode)
		}
	}

	r.PutCommit(c)
	return nil
}

type EdgeRef struct {
	Src, Dst int // indexes into NodeRefs and Contents
	// Now we specify the snk node explicitly  // Dst == -1: "1", this is an EOF
	// Dst == -2: "2", this file was deleted (must come from a ref to a file)
}

type NodeRef struct {
	Node  string
	Depth int
}

type NewContent struct {
	// Path can be empty, if so it is additional data added into an existing file.
	Path string

	Form    Form
	Content [][]byte
}

type Commit struct {
	Deps     []string // Commit hashes
	EdgeRefs []EdgeRef
	NodeRefs []NodeRef
	Contents []NewContent
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
		binary.Write(h, binary.LittleEndian, uint32(e.Src))
		binary.Write(h, binary.LittleEndian, uint32(e.Dst))
	}

	binary.Write(h, binary.LittleEndian, uint32(len(c.NodeRefs)))
	for _, n := range c.NodeRefs {
		binary.Write(h, binary.LittleEndian, uint32(len(n.Node)))
		h.Write([]byte(n.Node))
		binary.Write(h, binary.LittleEndian, uint32(n.Depth))
	}

	binary.Write(h, binary.LittleEndian, uint32(len(c.Contents)))
	for _, c := range c.Contents {
		hash := HashContent(c.Content)
		binary.Write(h, binary.LittleEndian, uint32(len(hash)))
		h.Write([]byte(hash))
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
	// is in the transitive closure of our frontier.  If the file is not in conflict, then the edges
	// out will all belong to a single path in the commit dag by definition.
	// Out[i] depends on Out[OutDeps[i][0]] .. Out[OutDeps[i][n-1]]
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
