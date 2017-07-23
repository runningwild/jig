package graph

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	skein512 "github.com/runningwild/skein/hash/512"
)

type SimpleRepo struct {
	// Map from fmt.Sprintf("path:%s", path) to FileNode
	Files map[string]*FileNode

	// Map from Node hash to Node
	Nodes map[string]*Node

	// Map from Content hash to Content
	Content map[string][]byte

	// Map from Commit hash to list of Commits hashes that Commit depends on
	Deps map[string][]string
}

type Repo interface {
	GetNode(nodeHash string) *Node
	GetContent(contentHash string) []byte

	StartTransaction()
	EndTransaction() error
	PutNode(n *Node)
	DeleteNode(nodeHash string)

	// TODO: Need to decide how to handle multiple references to a single content.  GC or reference counting?
	PutContent(content []byte) string
	DeleteContent(contentHash string)
}

// NEXT: splitNode needs to split a node but maintain the hash for the front of the node.
//       this may be doable by finding a consistent way to hash nodes.
//       A new edge will either point to a new node, the head of an existing node, or the body of an
//       existing node.
//       - New Node: Easy, it's right there.
//       - Existing node head: If we can find a consistent hash, easy, otherwise maybe hard.
//       - Existing node body: As easy as the above, with additional information about how far into the node.
//
//       Maybe an edge can provide the previous node (must be part of a declared dependency), and then say
//       go to where that edge points and then go forward N more nodes.  This could work as long as there
//       is a simple (i.e. O(1)) way to traverse nodes that have been split in a way that is consistent with
//       the final node this is referring to.
//		 An edge is defined by the commit that added it, the source node, and the destination node.
//       Define a node by the first edge that pointed to it.
//       src -> A -> B -> C -> D -> snk
//       this will have a single node object storing ABCD, the edges are implicit, but the first one, src->A, is defined
//       by the filename (foo.txt, say) and the commit (C:0).
//       C:1 adds an edge A -> XY -> B  (insert 'X' between A and B)
//       We have to split ABCD into A and BCD.  A's incoming edges don't change, BCD's outgoing edges don't change.
//       A must get an outgoing edge to BCD that came from commit C:0, it was there before, it was just implicit.
//       Now if C:2 adds an edge B -> D (delete 'C') we have to split BCD into B, C, and D.
//       C:2 only depends on C:0, not C:1, so we can't use nodes created by C:1 when defining C:2.
//       how do we choose the appropriate edges to use?  From the edge we want to add, B->D, we can go backward using
//       only edges observable by the frontier of the deps for this commit.  When we would go backward across an edge
//       visible this way we know that edge would be part ... wait!  we'd go backward across A->B, which is part of C:0,
//       but we can't use that one, because someone only has it if they broke ABCD into A and BCD.  This is a mess!!
//       Wait again!  It's ok, we know which edges were added as part of any given commit, we can call those primary
//       edges, and implicit ones that are added later can be secondary edges.  If we go backward until we cross a
//       primary edge we should be ok.  So the new commit can say 'find edge X, follow it and go N more nodes'.  We
//       don't need to know the frontier for this because any secondary edge.  This still requires the person creating
//       the commit to be able to compute the appropriate transitive frontier, or does it?  From any edge we want to add
//       if we trace backward to the first primary initial edge, *that* edge defined the content we're currently in, so
//       that's when we depend on.  WOOO!!!  Primary Initial: Primary means it was added as part of a commit, Initial
//       means that it is listed first in the incoming edges of a node.  Trace backward to the PI edge, measure the
//       distance along the way.  When applying that commit you can go to the edge and walk forward, only using that
//       edge's commit's edges.
//
func SplitNode(r Repo, node string, dist int) (aHash, bHash string, err error) {
	if dist <= 0 {
		return "", "", fmt.Errorf("cannot split a node at dist < 0")
	}
	n := r.GetNode(node)
	if n == nil {
		return "", "", fmt.Errorf("node not found")
	}
	commitHash := n.In[0].Commit
	for dist > n.Count {
		dist -= n.Count
		n = r.GetNode(n.Out[0].Node)

		if n == nil {
			return "", "", fmt.Errorf("node not found")
		}
		if n.Out[0].Commit != commitHash {
			return "", "", fmt.Errorf("dist is beyond original node's length")
		}
		// TODO: Guard against bad stuff
	}

	// Split the content
	// TODO: This needs to work for other formats
	if n.Form != FormText {
		panic("unsupported format")
	}
	content := r.GetContent(n.Content)
	if content == nil {
		return "", "", fmt.Errorf("node content not found")
	}
	var offset, count int
	for offset = 0; offset < len(content); offset++ {
		fmt.Printf("Checking cahracter %q\n", content[offset])
		if content[offset] == '\n' {
			count++
			if count == dist {
				break
			}
		}
	}
	if offset == len(content) {
		count++
	}
	if count != dist {
		fmt.Printf("%d %d %d %d\n", offset, len(content), count, n.Count)
		return "", "", fmt.Errorf("content was malformed")
	}
	if offset == len(content) {
		// No splitting necessary
		return node, "", nil
	}

	r.StartTransaction()
	defer func() {
		tErr := r.EndTransaction()
		if err == nil {
			err = tErr
		}
	}()

	contentA := r.PutContent(content[0:offset])
	contentB := r.PutContent(content[offset:])

	a := &Node{
		Form:    n.Form,
		Content: contentA,
		Count:   dist,
		In:      n.In,
		//Out:     []Edge{{Commit: commitHash, Node: nodeHash(b)}},
		//OutDeps: nil,
	}
	b := &Node{
		Form:    n.Form,
		Content: contentB,
		Count:   n.Count - dist,
		//In:      []Edge{{Commit: commitHash, Node: nodeHash(a)}},
		Out:     n.Out,
		OutDeps: n.OutDeps,
	}
	// Must write b first, because only a's hash is set in stone at this point.
	b.In = []Edge{{Commit: commitHash, Node: ""}}
	a.Out = []Edge{{Commit: commitHash, Node: ""}}
	a.OutDeps = nil

	r.PutNode(a)
	r.PutNode(b)
	r.DeleteNode("")

	return ",", ",", nil
}

func nodeHash(n *Node) string {
	return "node + " + n.Content
}

func (s *SimpleRepo) Apply(c *Commit) error {
	commitHash := c.Hash()
	if _, ok := s.Deps[commitHash]; ok {
		return fmt.Errorf("we already have this commit")
	}
	for _, d := range c.Deps {
		if _, ok := s.Deps[d]; !ok {
			return fmt.Errorf("commit included a dependency on unknown commit %q", d)
		}
	}

	// TODO: Do the rest of the validation.

	// At this point all validation is done and we can apply the commit without issues.
	s.Deps[commitHash] = c.Deps

	var contRefs []string
	for i, cont := range c.Contents {
		contentHash := fmt.Sprintf("%x", skein512.Hash512(128, cont.Content))
		s.Content[contentHash] = cont.Content
		n := &Node{
			Form:    FormText,
			Content: contentHash,
			Count:   1,
		}
		for _, b := range cont.Content {
			if b == '\n' {
				n.Count++
			}
		}
		nodeID := fmt.Sprintf("%s:%d", commitHash, i)
		s.Nodes[nodeID] = n
		contRefs = append(contRefs, nodeID)
		if cont.Path != "" {
			fileID := fmt.Sprintf("file:%s", cont.Path)
			if _, ok := s.Files[fileID]; !ok {
				s.Files[fileID] = &FileNode{}
			}
			s.Files[fileID].Out = append(s.Files[fileID].Out, Edge{Commit: commitHash, Node: nodeID})
		}
	}

	for _, e := range c.EdgeRefs {
		src := e.Src
		if src < len(c.NodeRefs) {
			// TODO: Implement this - need to split nodes!
		} else {
			src -= len(c.NodeRefs)
			n := s.Nodes[contRefs[src]]
			if e.Dst == -1 {
				n.Out = append(n.Out, Edge{Commit: commitHash, Node: "1"})
			} else {
				// TODO: Implement this
			}
		}
	}

	return nil
}

func (s *SimpleRepo) PrintFile(path string, frontier map[string]bool, w io.Writer) error {
	fn, ok := s.Files[path]
	if !ok {
		return fmt.Errorf("file not found")
	}
	// Just taking the first one, in practice we need to take what's in the frontier, and order them
	// topologically.
	e := fn.Out[0]
	if e.Node == "0" {
		return fmt.Errorf("file was deleted")
	}
	for e.Node != "1" {
		n := s.Nodes[e.Node]
		if _, err := w.Write(s.Content[n.Content]); err != nil {
			return err
		}
		e = n.Out[0]
	}
	return nil
}

func main() {
	s := SimpleRepo{
		Files:   make(map[string]*FileNode),
		Nodes:   make(map[string]*Node),
		Content: make(map[string][]byte),
		Deps:    make(map[string][]string),
	}
	c0 := &Commit{
		Deps: nil,
		EdgeRefs: []EdgeRef{
			{Src: 0, Dst: -1},
			{Src: 1, Dst: -1},
		},
		NodeRefs: nil,
		Contents: []NewContent{
			{
				Path:    "foo.txt",
				Content: []byte("foo\nbar\nwing\nding\n"),
			},
			{
				Path:    "beans.txt",
				Content: []byte("beans\nbeans\nbeans\nbeans\nbeans\n"),
			},
		},
	}
	s.Apply(c0)
	c1 := &Commit{
		Deps: []string{c0.Hash()},
		EdgeRefs: []EdgeRef{
			{Src: 0, Dst: 1},
		},
		NodeRefs: []NodeRef{
			{Edge: "foo.txt", Depth: 1},
			{Edge: "foo.txt", Depth: 3},
		},
		Contents: nil,
	}
	s.Apply(c1)
	for path := range s.Files {
		buf := bytes.NewBuffer(nil)
		s.PrintFile(path, nil, buf)
		fmt.Printf("%s:\n%s--------------------\n", path, buf.Bytes())
	}
}

type EdgeRef struct {
	Src, Dst int // indexes into NodeRefs and Contents
}

type NodeRef struct {
	Edge  string
	Depth int
}

type NewContent struct {
	// Path can be empty, if so it is additional data added into an existing file.
	Path string

	Content []byte
}

type Commit struct {
	Deps     []string // Commit hashes
	EdgeRefs []EdgeRef
	NodeRefs []NodeRef
	Contents []NewContent
}

func (c *Commit) Hash() string {
	h := skein512.NewHash512(128)

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
		binary.Write(h, binary.LittleEndian, uint32(len(n.Edge)))
		h.Write([]byte(n.Edge))
		binary.Write(h, binary.LittleEndian, uint32(n.Depth))
	}

	binary.Write(h, binary.LittleEndian, uint32(len(c.Contents)))
	for _, c := range c.Contents {
		binary.Write(h, binary.LittleEndian, uint32(len(c.Content)))
		h.Write(c.Content)
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

func CalculateNodeHashes(commit, prev string, form Form, content []byte) (head, tail string) {
	if form != FormText {
		panic("unknown form")
	}
	divs := []int{0}
	for i := 1; i < len(content); i++ {
		if content[i] == '\n' {
			divs = append(divs, i)
		}
	}
	divs = append(divs, len(content))
	for i := 0; i < len(divs)-1; i++ {
		h := skein512.NewHash512(128)
		h.Write([]byte(commit))
		h.Write([]byte(prev))
		h.Write(content[divs[i]:divs[i+1]]) // This isn't including the newlines, but that's ok.
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
	h := skein512.NewHash512(128)

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

	// FormBinary is a binary file.  TODO: Need to define the chunking algorithm
//	FormBinary
)
