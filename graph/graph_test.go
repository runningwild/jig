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
			In:      []graph.Edge{{Commit: "commit-0", Node: "path:sample.txt", Primary: true}},
			Out:     []graph.Edge{{Commit: "commit-0", Node: "1"}},
		})
		Convey("can split a node where dist < n.Count", func() {
			tail0, head1, err := graph.SplitNode(r, head, 3)
			So(err, ShouldBeNil)
			So(tail0, ShouldNotEqual, "")
			So(head1, ShouldNotEqual, "")
			node0 := r.GetNode(head)
			So(node0, ShouldNotBeNil)
			So(node0.Head, ShouldEqual, head)
			So(node0.Tail, ShouldEqual, tail0)
			So(node0.Out[0].Node, ShouldEqual, head1)
			node1 := r.GetNode(head1)
			So(node1, ShouldNotBeNil)
			So(node1.In[0].Node, ShouldEqual, tail0)
			So(node0.Count, ShouldEqual, 3)
			So(node1.Count, ShouldEqual, 4)
			So(string(r.GetContent(node0.Content)), ShouldEqual, "foo\nbar\nwing")
			So(string(r.GetContent(node1.Content)), ShouldEqual, "\nding\nmonkey\nball\n")
			Convey("and and where dist > n.Count", func() {
				tail1, head2, err := graph.SplitNode(r, head, 5)
				So(err, ShouldBeNil)
				So(tail1, ShouldNotEqual, "")
				So(head2, ShouldNotEqual, "")
				node1 := r.GetNode(head1)
				So(node1, ShouldNotBeNil)
				So(node1.Out[0].Node, ShouldEqual, head2)
				node2 := r.GetNode(head2)
				So(node2, ShouldNotBeNil)
				So(r.GetRef(node2.In[0].Node), ShouldEqual, head1)
				So(node0.Count, ShouldEqual, 3)
				So(node1.Count, ShouldEqual, 2)
				So(node2.Count, ShouldEqual, 2)
				So(string(r.GetContent(node0.Content)), ShouldEqual, "foo\nbar\nwing")
				So(string(r.GetContent(node1.Content)), ShouldEqual, "\nding\nmonkey")
				So(string(r.GetContent(node2.Content)), ShouldEqual, "\nball\n")
			})
		})
		Convey("doesn't split if the split point is at the end of an existing node", func() {
			before := len(r.nodes)
			a, b, err := graph.SplitNode(r, head, 7)
			So(err, ShouldBeNil)
			So(a, ShouldEqual, tail)
			So(b, ShouldEqual, "1")
			So(len(r.nodes), ShouldEqual, before)
		})
	})
}

type testRepo struct {
	nodes        map[string]*graph.Node
	content      map[string][]byte
	refs         map[string]string
	transactions int
}

func makeFakeRepo() *testRepo {
	return &testRepo{
		nodes:   make(map[string]*graph.Node),
		content: make(map[string][]byte),
		refs:    make(map[string]string),
	}
}

func (r *testRepo) GetRef(ptr string) string {
	return r.refs[ptr]
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
func (r *testRepo) PutRef(ptr, val string) {
	r.refs[ptr] = val
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
