package graph

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"

	jpb "github.com/runningwild/jig/proto"
	"golang.org/x/crypto/sha3"
)

type Repo interface {
	GetRef(ptr string) string
	GetNode(nodeHash string) *jpb.Node
	GetContent(contentHash string) [][]byte
	GetCommit(commitHash string) *jpb.Commit
	GetReverseDeps(commitHash string) []string

	// List methods all fill out the given slice with as many hashes as possible of the specified,
	// it returns the number of elements filled.
	ListRefs(start string, refs []string) (n int)
	ListNodes(start string, nodes []string) (n int)
	ListContents(start string, contents []string) (n int)
	ListCommits(start string, commits []string) (n int)

	StartTransaction()
	EndTransaction() error

	PutRef(ptr, val string)

	PutNode(n *jpb.Node)
	DeleteNode(nodeHash string)

	// TODO: Need to decide how to handle multiple references to a single content.  GC or reference counting?
	PutContent(content [][]byte) string
	DeleteContent(contentHash string)

	PutCommit(c *jpb.Commit)

	PutReverseDep(newCommit, oldCommit string)
	DeleteReverseDep(newCommit, oldCommit string)

	// TODO: Need some kind of first-class handling of frontiers.  Perhaps something like this:
	// CreateFrontier(commits []string) string  // creates a frontier out of commits and returns the ID
	// DeltaSet(from, to string) string // given two frontiers, returns an ID for a set representing all
	//									// commits in to that are not in from.
}

// SplitNode takes a node and a depth and replaces that node with two nodes, split at the specified
// depth.  The first of those nodes will have the same Head hash, and the second will have the same
// Tail hash.  This function will return the Tail hash of the first node and the Head hash of the second.
func SplitNode(r Repo, node string, depth int32) (tail, head string, err error) {
	if depth <= 0 {
		return "", "", fmt.Errorf("cannot split a node at depth <= 0")
	}
	var n *jpb.Node
	for n = r.GetNode(node); n != nil && depth > n.Count && len(n.Out) > 0; n = r.GetNode(n.Out[0].Node) {
		depth -= n.Count
	}
	if n == nil || depth > n.Count {
		return "", "", fmt.Errorf("depth is beyond original node's length")
	}

	// For simplicity we'll just special case splitting at the very beginning of a file...
	if n.GetSrc() != nil && depth == 1 {
		return n.Tail, "", nil
	}
	// ... and the very end.
	if n.GetSnk() != nil && depth == 0 {
		return "", n.Head, nil
	}
	commitHash := n.In[0].Commit

	// Split the content
	// TODO: This needs to work for other formats
	if n.GetContentHash() == "" {
		panic("unsupported format")
	}
	content := r.GetContent(n.GetContentHash())
	if content == nil {
		return "", "", fmt.Errorf("node content not found")
	}
	if len(content) != int(n.Count) {
		return "", "", fmt.Errorf("%q is malformed", n.Head)
	}
	if int(depth) == len(content) {
		// No splitting necessary
		return n.Tail, n.Out[0].Node, nil
	}

	head0, tail0 := CalculateNodeHashes(commitHash, n.In[0].Node, content[0:depth])
	head1, tail1 := CalculateNodeHashes(commitHash, tail0, content[depth:])
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

	// TODO: Return an error if we're trying to split a src or snk node.

	contentA := r.PutContent(content[0:depth])
	contentB := r.PutContent(content[depth:])

	// This is just to verify that things make sense.  Join edges should be present on both In and Out.
	internalCommits := make(map[string]bool)
	for _, e := range n.In {
		if e.Join {
			internalCommits[e.Commit] = true
		}
	}
	for _, e := range n.Out {
		if e.Join {
			if _, ok := internalCommits[e.Commit]; !ok {
				panic("internal commit was present in Out but not In")
			}
			delete(internalCommits, e.Commit)
		}
	}
	if len(internalCommits) != 0 {
		panic("internal commit was present in In but not Out")
	}

	fmt.Sprintf("Splitting node %s with In %v and Out %v\n", n.Head, n.In, n.Out)

	a := &jpb.Node{
		Head: head0,
		Tail: tail0,
		Content: &jpb.Node_ContentHash{
			ContentHash: contentA,
		},
		Count: int32(depth),
		In:    n.In,
		// Out:     []Edge{{Commit: commitHash, Node: head1}},
	}
	b := &jpb.Node{
		Head: head1,
		Tail: tail1,
		Content: &jpb.Node_ContentHash{
			ContentHash: contentB,
		},
		Count: n.Count - int32(depth),
		// In:      []Edge{{Commit: commitHash, Node: tail0}},
		Out: n.Out,
	}
	fmt.Printf("For node %s\n", n.Head)
	for i, e := range n.In {
		if e.Join {
			a.Out = append(a.Out, &jpb.Edge{Commit: e.Commit, Node: head1, Join: true})
			b.In = append(b.In, &jpb.Edge{Commit: e.Commit, Node: tail0, Join: true})
			fmt.Printf("  Joining edge %d with %s\n", i, e.Commit)
		}
	}

	r.PutNode(a) // This will overwrite the previous node
	r.PutNode(b)

	// Update refs.  This will also update anything that pointed to the tail of the original node to
	// end up on the second node rather than the first.
	r.PutRef(tail1, head1)
	r.PutRef(tail0, head0)

	return tail0, head1, nil
}

func HumanReadable(r Repo, f, prev Frontier, path string, conflicts []Conflict, join []byte) ([]byte, error) {
	conflictLookup := make(map[string]int)
	for i, c := range conflicts {
		conflictLookup[c.Start] = i
	}
	fmt.Printf("Lookups: %v\n", conflictLookup)
	var lines [][]byte
	nextNode := "src:" + path
	for !strings.HasPrefix(nextNode, "snk:") {
		next := r.GetNode(nextNode)
		if next == nil {
			return nil, fmt.Errorf("failed to find node %s", nextNode)
		}
		lines = append(lines, r.GetContent(next.GetContentHash())...)
		fmt.Printf("Node: %v %v\n", nextNode, r.GetNode(nextNode).Tail)
		if ci, ok := conflictLookup[next.Tail]; ok {
			// We have to display a conflict here.
			lines = lines[0 : len(lines)-1]
			con := conflicts[ci]
			fmt.Printf("ReadVersions with conflicts: %v\n", con.Commits)
			vs, err := ReadVersions(r, f, prev, con.Start, con.End, conflicts[ci].Groups, join)
			if err != nil {
				return nil, err
			}

			lines = append(lines, []byte("<<<<<<<"))
			for _, v := range vs {
				lines = append(lines, []byte(fmt.Sprintf("======= %v", v.Commits)))
				lines = append(lines, v.Data)
			}
			lines = append(lines, []byte(">>>>>>>"))
			next = r.GetNode(con.End)
			lines = append(lines, r.GetContent(r.GetNode(con.End).GetContentHash())[1:]...)

		} else {
		}

		fmt.Printf("Content(%s): %s\n", next.Head, r.GetContent(next.GetContentHash()))
		var edge *jpb.Edge
		for i := len(next.Out) - 1; i >= 0; i-- {
			if f.Observes(next.Out[i].Commit) {
				edge = next.Out[i]
				break
			}
		}
		if edge == nil {
			return nil, fmt.Errorf("failed pathing through %q", path)
		}
		nextNode = edge.Node
	}
	return bytes.Join(lines, join), nil
}

// ReadVersions returns a []Version with one Version for each conflicting view of the file.  If prev
// is not nil, then one of the Versions will correspond to the conflicting commits, and all other
// Versions will contain a single conflicting commit.
// TODO: The restriction below should be verified.
// If prev is set it must be a frontier at which there is no conflict between start and end.
func ReadVersions(r Repo, f, prev Frontier, start, end string, groups [][]string, join []byte) ([]Version, error) {
	allCommits := make(map[string]bool)
	var groupMaps []map[string]bool
	for i := range groups {
		groupMap := make(map[string]bool)
		for j := range groups[i] {
			groupMap[groups[i][j]] = true
			allCommits[groups[i][j]] = true
		}
		groupMaps = append(groupMaps, groupMap)

	}
	var versions []Version
	// used := make(map[string]bool)

	// TODO: Figure out how this works with prev
	// if prev != nil {
	// 	allCommits := make(map[string]bool)
	// 	lines, err := ReadVersion(r, prev, start, end, &ReadMetadata{Commits: allCommits})
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	data := bytes.Join(lines, join)
	// 	commits := make(map[string]bool)
	// 	for c := range allCommits {
	// 		if _, ok := conflicts[c]; ok {
	// 			commits[c] = true
	// 		}
	// 	}
	// 	versions = append(versions, Version{Commits: commits, Data: data})
	// 	used = commits
	// }

	// TODO: The way we're constructing the frontier here is a bit strange and I think it's not correct.
	for _, groupMap := range groupMaps {
		// if used[c] {
		// 	continue
		// }
		next := &addToFrontier{f: &removeFromFrontier{f: f, remove: allCommits}, add: groupMap}
		lines, err := ReadVersion(r, next, start, end, &ReadMetadata{})
		if err != nil {
			return nil, err
		}
		data := bytes.Join(lines, join)
		versions = append(versions, Version{Commits: groupMap, Data: data})
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

// Any fields within metadata that are non-nil will be filled with the relevant data.
// TODO: Need to be able to distinguish between an empty file, a non-existent file, and a conflict.
func ReadVersion(r Repo, f Frontier, start, end string, metadata *ReadMetadata) ([][]byte, error) {
	var buf [][]byte
	n := r.GetNode(start)
	if n == nil {
		startHead := r.GetRef(start)
		if startHead == "" {
			return nil, fmt.Errorf("failed to find start node %s", start)
		}
		n = r.GetNode(startHead)
	}
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

	// Only take the last chunk from the starting node.
	if content := r.GetContent(n.GetContentHash()); len(content) > 0 {
		buf = append(buf, content[len(content)-1])
	}

	used := make(map[string]struct{})
	readDepth := 0

	if metadata.Commits != nil && len(n.In) > 0 {
		// If a node has input edges then the first one is the one that created it, and that
		// represents a commit that this path depends on.
		metadata.Commits[n.In[0].Commit] = true
	}

	for {
		for i := len(n.Out) - 1; i >= 0; i-- {
			e := n.Out[i]
			if !f.Observes(e.Commit) {
				fmt.Printf("Not taking edge from commit %s\n", e.Commit)
				continue
			}
			if metadata.Commits != nil {
				metadata.Commits[e.Commit] = true
			}
			n = r.GetNode(e.Node)
			fmt.Printf("Took node with content: %q\n", r.GetContent(n.GetContentHash()))
			if n == nil {
				return nil, fmt.Errorf("failed to find node %s in the repo", e.Node)
			}
			break
		}

		// Prevents traversing cycles more than once.
		if _, ok := used[n.Head]; ok {
			return nil, fmt.Errorf("file graph contains a cycle %v", n.Head)
		}
		used[n.Head] = struct{}{}

		if n == prev {
			return nil, fmt.Errorf("failed to find an outgoing edge from %s", prev.Head)
		}
		if n.Head == end {
			break
		}
		if strings.HasPrefix(n.Head, "snk:") {
			return nil, fmt.Errorf("reached end of file without reaching the dst node")
		}
		content := r.GetContent(n.GetContentHash())
		buf = append(buf, content...)
		if metadata.Ranges != nil {
			ref, depth := nodeRef(r, n)
			*metadata.Ranges = append(*metadata.Ranges, ReadRange{Commit: nodeCommit(n), Node: ref, Depth: depth, ReadDepth: readDepth, Length: len(content)})
			readDepth += len(content)
		}
		prev = n
	}

	// Only take the first chunk from the end node.
	if content := r.GetContent(n.GetContentHash()); len(content) > 0 {
		buf = append(buf, content[0])
	}

	return buf, nil
}

func nodeCommit(n *jpb.Node) string {
	if len(n.In) != 0 {
		return n.In[0].Commit
	}
	if len(n.Out) != 0 {
		return n.Out[0].Commit
	}
	panic("how did you let this happen?")
}

func nodeRef(r Repo, n *jpb.Node) (string, int) {
	prev := r.GetNode(r.GetRef(n.In[0].Node))
	if nodeCommit(prev) != nodeCommit(n) || prev.GetSrc() != nil {
		return n.Head, 0
	}
	ref, d := nodeRef(r, prev)
	return ref, d + int(prev.Count)
}

// GetContent reads the content from the specified between depths specified by start and end.  This
// will follow primary edges to do so, and so is the appropriate way to read content specified by ReadRanges.
func GetContent(r Repo, nodeHash string, start, end int) [][]byte {
	fmt.Printf("GetContent(node:%s, content:%s): %d,%d\n", nodeHash, r.GetNode(nodeHash).GetContentHash(), start, end)
	n := r.GetNode(nodeHash)
	count := int(n.Count)
	if count < start {
		return GetContent(r, n.Out[0].Node, start-count, end-count)
	}
	content := r.GetContent(n.GetContentHash())
	if end <= count {
		return content[start:end]
	}
	return append(content[start:], GetContent(r, n.Out[0].Node, 0, end-count)...)
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

func jigStandardHasher() *jigHasher {
	return &jigHasher{Buffer: bytes.NewBuffer(nil)}
}

type jigHasher struct {
	*bytes.Buffer
}

func (j *jigHasher) Sum() string {
	b := make([]byte, 4)
	sha3.ShakeSum128(b, j.Bytes())
	return fmt.Sprintf("%x", b)
}

func HashContent(content [][]byte) string {
	h := jigStandardHasher()
	for _, line := range content {
		length := uint32(len(line))
		h.Write([]byte{byte(length), byte(length >> 8), byte(length >> 16), byte(length >> 24)})
		h.Write(line)
	}
	return h.Sum()
}

func Apply(r Repo, c *jpb.Commit) error {
	// Validate that we haven't already applied this commit.
	commitHash := HashCommit(c)
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
	//   refs that specify joins must come in pairs

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

		// TODO: Do there need to be any restrictions on when a src or snk node can be used as a noderef?

		// Create src and snk nodes if they don't already exist.
		if strings.HasPrefix(e.Src.Node, "src:") && r.GetNode(e.Src.Node) == nil {
			r.PutNode(&jpb.Node{Head: e.Src.Node, Tail: e.Src.Node, Content: &jpb.Node_Src{Src: &jpb.Src{}}, Count: 1})
			r.PutRef(e.Src.Node, e.Src.Node)
		}
		if strings.HasPrefix(e.Dst.Node, "snk:") && r.GetNode(e.Dst.Node) == nil {
			r.PutNode(&jpb.Node{Head: e.Dst.Node, Tail: e.Dst.Node, Content: &jpb.Node_Snk{Snk: &jpb.Snk{}}, Count: 1})
			r.PutRef(e.Dst.Node, e.Dst.Node)
		}

		var err error
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
			fmt.Printf("Splitting %s at depth %d\n", e.Dst.Node, e.Dst.Depth)
			_, head, err = SplitNode(r, e.Dst.Node, e.Dst.Depth)
			fmt.Printf("Using head %s\n", head)
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
		fmt.Printf("using dst node %s\n", dst.Head)
		if len(e.Chunks) == 0 {
			fmt.Printf("Pinning %t %t\n", e.Src.Join, e.Dst.Join)
			src.Out = append(src.Out, &jpb.Edge{Commit: commitHash, Node: dst.Head, Join: e.Src.Join})
			dst.In = append(dst.In, &jpb.Edge{Commit: commitHash, Node: src.Tail, Join: e.Dst.Join})
			r.PutNode(src)
			r.PutNode(dst)
			continue
		}

		content := r.PutContent(e.Chunks)
		newHead, newTail := CalculateNodeHashes(commitHash, tail, e.Chunks)
		middle := &jpb.Node{
			Head:    newHead,
			Tail:    newTail,
			Content: &jpb.Node_ContentHash{ContentHash: content},
			Count:   int32(len(e.Chunks)),
			In: []*jpb.Edge{{
				Commit: commitHash,
				Node:   tail,
				Join:   true,
			}},
			Out: []*jpb.Edge{{
				Commit: commitHash,
				Node:   head,
				Join:   true,
			}},
		}
		r.PutNode(middle)
		r.PutRef(middle.Tail, middle.Head)

		src.Out = append(src.Out, &jpb.Edge{Commit: commitHash, Node: middle.Head})
		dst.In = append(dst.In, &jpb.Edge{Commit: commitHash, Node: middle.Tail})
		r.PutNode(src)
		r.PutNode(dst)
	}

	for _, dep := range c.Deps {
		r.PutReverseDep(commitHash, dep)
	}

	r.PutCommit(c)
	return nil
}

func HashCommit(c *jpb.Commit) string {
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

		if len(e.Chunks) == 0 {
			binary.Write(h, binary.LittleEndian, uint32(0))
		} else {
			binary.Write(h, binary.LittleEndian, uint32(len(e.Chunks)))
			for _, line := range e.Chunks {
				binary.Write(h, binary.LittleEndian, uint32(len(line)))
				binary.Write(h, binary.LittleEndian, []byte(line))
			}
		}

		binary.Write(h, binary.LittleEndian, []byte(e.Dst.Node))
		binary.Write(h, binary.LittleEndian, uint32(e.Dst.Depth))
	}

	return h.Sum()
}

// To apply a Commit we need to do the following:
// 1 - Verify that all commits in Deps have already been applied, if not we must bail.
// 2 - For each value in Contents we need to create a new Node with that Content, and must count the
//     internal nodes.
// 3 - For each edge in EdgeRefs we need to find the corresponding src and dst nodes and insert edges.

func CalculateNodeHashes(commit, prev string, content [][]byte) (head, tail string) {
	for _, line := range content {
		h := jigStandardHasher()
		binary.Write(h, binary.LittleEndian, uint32(len(commit)))
		h.Write([]byte(commit))
		binary.Write(h, binary.LittleEndian, uint32(len(prev)))
		h.Write([]byte(prev))
		h.Write(line)
		prev = h.Sum()
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

func HashEdge(e *jpb.Edge) string {
	h := jigStandardHasher()

	binary.Write(h, binary.LittleEndian, uint32(len(e.Commit)))
	h.Write([]byte(e.Commit))

	binary.Write(h, binary.LittleEndian, uint32(len(e.Node)))
	h.Write([]byte(e.Node))

	binary.Write(h, binary.LittleEndian, e.Join)

	return h.Sum()
}
