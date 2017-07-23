package graph_test

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"

	"fmt"
	"github.com/runningwild/jig/graph"
	skein512 "github.com/runningwild/skein/hash/512"
)

func TestNodeHashes(t *testing.T) {
	Convey("CalculateNodeHashes", t, func() {
		c0 := []byte("foo\nbar\nwing")
		c1 := []byte("\nding\nmonkey\nball\n")
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
		r := makeFakeRepo()
		sampleContent := []byte("foo\nbar\nwing\nding\nmonkey\nball\n")
		contentHash := r.PutContent(sampleContent)
		head, tail := graph.CalculateNodeHashes("commit-0", "path:sample.txt", graph.FormText, sampleContent)
		r.PutNode(&graph.Node{
			Head:    head,
			Tail:    tail,
			Form:    graph.FormText,
			Content: contentHash,
			Count:   7,
			In:      []graph.Edge{{Commit: "commit-0", Node: "node-x", Primary: true}},
		})
		//Convey("can split a node where dist < n.Count", func() {
		//	a, b, err := graph.SplitNode(r, n, 3)
		//	So(err, ShouldBeNil)
		//	So(a, ShouldNotEqual, "")
		//	So(b, ShouldNotEqual, "")
		//	nodeA := r.GetNode(a)
		//	So(nodeA, ShouldNotBeNil)
		//	So(nodeA.Out[0].Node, ShouldEqual, b)
		//	nodeB := r.GetNode(b)
		//	So(nodeB, ShouldNotBeNil)
		//	So(nodeB.In[0].Node, ShouldEqual, a)
		//	So(r.GetNode(n), ShouldNotBeNil)
		//	// NEXT: Need to keep around the first node in a split, that can always be referred to later.
		//	So(r.GetContent(nodeA.Content), ShouldResemble, []byte("foo\nbar\nwing"))
		//	So(r.GetContent(nodeB.Content), ShouldResemble, []byte("\nding\nmonkey\nball\n"))
		//	Convey("and and where dist > n.Count", func() {
		//		c, d, err := graph.SplitNode(r, n, 5)
		//		So(err, ShouldBeNil)
		//		So(c, ShouldNotEqual, "")
		//		So(d, ShouldNotEqual, "")
		//		nodeC := r.GetNode(c)
		//		So(nodeC, ShouldNotBeNil)
		//		So(nodeC.Out[0].Node, ShouldEqual, d)
		//		nodeD := r.GetNode(d)
		//		So(nodeD, ShouldNotBeNil)
		//		So(nodeD.In[0].Node, ShouldEqual, c)
		//		So(r.GetNode(n), ShouldBeNil)
		//		So(r.GetContent(nodeA.Content), ShouldResemble, []byte("foo\nbar\nwing\nding\nmonkey"))
		//		So(r.GetContent(nodeB.Content), ShouldResemble, []byte("\nball\n"))
		//	})
		//})
		//Convey("doesn't split if the split point is at the end of an existing node", func() {
		//	a, b, err := graph.SplitNode(r, n, 7)
		//	So(err, ShouldBeNil)
		//	So(a, ShouldEqual, n)
		//	So(b, ShouldEqual, "")
		//})
	})
}

type testRepo struct {
	nodes        map[string]*graph.Node
	content      map[string][]byte
	transactions int
}

func makeFakeRepo() *testRepo {
	return &testRepo{
		nodes:   make(map[string]*graph.Node),
		content: make(map[string][]byte),
	}
}

func (r *testRepo) GetNode(nodeHash string) *graph.Node {
	return r.nodes[nodeHash]
}
func (r *testRepo) GetContent(contentHash string) []byte {
	return r.content[contentHash]
}
func (r *testRepo) StartTransaction() {
	if r.transactions != 0 {
		panic("omg")
	}
	r.transactions = 1
}
func (r *testRepo) EndTransaction() error {
	if r.transactions != 1 {
		panic("zomg")
	}
	r.transactions = 0
	return nil
}
func (r *testRepo) PutNode(n *graph.Node) {
	r.nodes[n.Head] = n
}
func (r *testRepo) DeleteNode(nodeHash string) {
	delete(r.nodes, nodeHash)
}
func (r *testRepo) PutContent(content []byte) string {
	hash := fmt.Sprintf("%x", skein512.Hash512(128, content))
	r.content[hash] = content
	return hash
}
func (r *testRepo) DeleteContent(contentHash string) {
	delete(r.content, contentHash)
}
