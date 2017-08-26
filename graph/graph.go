package graph

import (
	"encoding/binary"
	"fmt"
	"hash"
	"io"

	skein512 "github.com/runningwild/skein/hash/512"
)

type Repo interface {
	GetRef(ptr string) string
	GetNode(nodeHash string) *Node
	GetContent(contentHash string) [][]byte
	GetCommit(commitHash string) *Commit

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

func PrintFile(r Repo, path string, f Frontier, w io.Writer) error {
	n := r.GetNode("src:" + path)
	if n == nil {
		return fmt.Errorf("file not found")
	}

	if n.Out[len(n.Out)-1].Node == "0" {
		return fmt.Errorf("file was deleted")
	}

	first := true
	snk := fmt.Sprintf("snk:%s", path)
	for n.Out[len(n.Out)-1].Node != snk {
		prev := n.Out[len(n.Out)-1].Node
		n = r.GetNode(n.Out[len(n.Out)-1].Node)
		if n == nil {
			return fmt.Errorf("Failed to find node %q\n", prev)
		}
		content := r.GetContent(n.Content)
		for _, line := range content {
			if !first {
				if _, err := w.Write([]byte{'\n'}); err != nil {
					return err
				}
			}
			first = false
			if _, err := w.Write(line); err != nil {
				return err
			}
		}
	}
	return nil
}

// func Main() {
// 	r := MakeFakeRepo()

// 	content := bytes.Split([]byte(strings.Join([]string{"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india", ""}, "\n")), []byte("\n"))
// 	c0 := &Commit{
// 		Deps:     nil,
// 		EdgeRefs: []EdgeRef{{Src: 0, Dst: -1}},
// 		Contents: []NewContent{
// 			{
// 				Path:    "foo.txt",
// 				Form:    FormText,
// 				Content: content,
// 			},
// 		},
// 	}
// 	if err := Apply(r, c0); err != nil {
// 		fmt.Printf("failed to apply first commit: %v", err)
// 		return
// 	}

// 	content2 := bytes.Split([]byte(strings.Join([]string{"BRAVO", "CHARLIE"}, "\n")), []byte("\n"))
// 	// for _, n := range r.nodes {
// 	// 	fmt.Printf("Node:\nHead: %s\nTail: %s\n", n.Head, n.Tail)
// 	// 	fmt.Printf("Out: %v\n", n.Out)
// 	// 	fmt.Printf("In: %v\n", n.In)
// 	// 	fmt.Printf("Content: \n`%s`\n", r.GetContent(n.Content))
// 	// }
// 	fmt.Printf("Printing 'foo.txt'----------\n")
// 	PrintFile(r, "foo.txt", nil, os.Stdout)
// 	fmt.Printf("----------------------------\n\n\n")

// 	// This commit capitalizes the lines with 'bravo' and 'charlie'
// 	c1 := &Commit{
// 		Deps: []string{c0.Hash()},
// 		EdgeRefs: []EdgeRef{
// 			{Src: 0, Dst: 2},
// 			{Src: 2, Dst: 1},
// 		},
// 		NodeRefs: []NodeRef{
// 			{Node: "src:foo.txt", Depth: 2}, // 'alpha'
// 			{Node: "src:foo.txt", Depth: 5}, // 'delta'
// 		},
// 		Contents: []NewContent{
// 			{Content: content2},
// 		},
// 	}
// 	fmt.Printf("Applying commit %q...\n", c1.Hash())
// 	if err := Apply(r, c1); err != nil {
// 		fmt.Printf("Failed to apply commit: %v", err)
// 		return
// 	}
// 	fmt.Printf("Printing 'foo.txt'----------\n")
// 	PrintFile(r, "foo.txt", nil, os.Stdout)
// 	fmt.Printf("----------------------------\n")

// 	fmt.Printf("Starting with %q\n", "src:foo.txt")
// 	fmt.Printf("Now to %q\n", r.nodes["src:foo.txt"].Out[0].Node)
// 	fmt.Printf("Now to %q\n", r.nodes[r.nodes["src:foo.txt"].Out[0].Node].Out[1].Node)
// 	fmt.Printf(" on commit %q\n", r.nodes[r.nodes["src:foo.txt"].Out[0].Node].Out[1].Commit)

// 	// This should be the node with the capitalized text
// 	// r.nodes["src:foo.txt"].Out[1].Node
// 	c2 := &Commit{
// 		Deps: []string{c0.Hash(), c1.Hash()},
// 		EdgeRefs: []EdgeRef{
// 			{Src: 0, Dst: 1},
// 		},
// 		NodeRefs: []NodeRef{
// 			{
// 				Node:  r.nodes[r.nodes["src:foo.txt"].Out[0].Node].Out[1].Node,
// 				Depth: 2,
// 			},
// 			{
// 				Node:  "src:foo.txt",
// 				Depth: 6,
// 			},
// 		},
// 		Contents: nil,
// 	}

// 	for _, n := range r.nodes {
// 		fmt.Printf("Node:\nHead: %s\nTail: %s\n", n.Head, n.Tail)
// 		fmt.Printf("Out: %v\n", n.Out)
// 		fmt.Printf("In: %v\n", n.In)
// 		fmt.Printf("Content: \n`%s`\n", r.GetContent(n.Content))
// 		fmt.Printf("\n\n\n\n\n")
// 	}

// 	fmt.Printf("Applying a commit...\n")
// 	if err := Apply(r, c2); err != nil {
// 		fmt.Printf("Failed to apply commit: %v", err)
// 		return
// 	}
// 	fmt.Printf("Printing 'foo.txt'----------\n")
// 	PrintFile(r, "foo.txt", nil, os.Stdout)
// 	fmt.Printf("----------------------------\n")
// }

func jigStandardHasher() hash.Hash {
	return skein512.NewHash512(24)
}

func hashContent(content [][]byte) string {
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
		fmt.Printf("WOrking on %d %v\n", i, nc)
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
			newNodes = append(newNodes, n.Head)
			fmt.Printf("added %v\n", n)
			continue
		}
		n := &Node{
			Form:    nc.Form,
			Content: hashContent(nc.Content), // Content hash
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
		hash := hashContent(c.Content)
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

////// omgggggg

type testRepo struct {
	Nodes        map[string]*Node
	Commits      map[string]*Commit
	Content      map[string][][]byte
	Refs         map[string]string
	Transactions int
}

func MakeFakeRepo() *testRepo {
	return &testRepo{
		Nodes:   make(map[string]*Node),
		Commits: make(map[string]*Commit),
		Content: make(map[string][][]byte),
		Refs:    make(map[string]string),
	}
}

func (r *testRepo) GetRef(ptr string) string {
	return r.Refs[ptr]
}
func (r *testRepo) GetNode(nodeHash string) *Node {
	return r.Nodes[nodeHash]
}
func (r *testRepo) GetCommit(commitHash string) *Commit {
	return r.Commits[commitHash]
}
func (r *testRepo) GetContent(contentHash string) [][]byte {
	return r.Content[contentHash]
}
func (r *testRepo) StartTransaction() {
	if r.Transactions != 0 {
		panic("omg")
	}
	r.Transactions = 1
}
func (r *testRepo) EndTransaction() error {
	if r.Transactions != 1 {
		panic("zomg")
	}
	r.Transactions = 0
	return nil
}
func (r *testRepo) PutRef(ptr, val string) {
	r.Refs[ptr] = val
}
func (r *testRepo) PutNode(n *Node) {
	r.Nodes[n.Head] = n
}
func (r *testRepo) PutCommit(c *Commit) {
	r.Commits[c.Hash()] = c
}
func (r *testRepo) DeleteNode(nodeHash string) {
	delete(r.Nodes, nodeHash)
}
func (r *testRepo) PutContent(content [][]byte) string {
	hash := hashContent(content)
	r.Content[hash] = content
	return hash
}
func (r *testRepo) DeleteContent(contentHash string) {
	delete(r.Content, contentHash)
}
