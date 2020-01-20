package testutils

import (
	"fmt"
	"github.com/runningwild/jig/graph"
	jpb "github.com/runningwild/jig/proto"
	"sort"
)

type fakeRepo struct {
	refs        map[string]string
	nodes       map[string]*jpb.Node
	commits     map[string]*jpb.Commit
	contents    map[string][][]byte
	reverseDeps map[string][]string
}

func MakeFakeRepo() graph.Repo {
	return &fakeRepo{
		refs:        make(map[string]string),
		nodes:       make(map[string]*jpb.Node),
		commits:     make(map[string]*jpb.Commit),
		contents:    make(map[string][][]byte),
		reverseDeps: make(map[string][]string),
	}
}

func (r *fakeRepo) GetRef(ptr string) string {
	return r.refs[ptr]
}
func (r *fakeRepo) GetNode(nodeHash string) *jpb.Node {
	return r.nodes[nodeHash]
}
func (r *fakeRepo) GetContent(contentHash string) [][]byte {
	return r.contents[contentHash]
}
func (r *fakeRepo) GetCommit(commitHash string) *jpb.Commit {
	return r.commits[commitHash]
}
func (r *fakeRepo) GetReverseDeps(commitHash string) []string {
	panic("not implemented")
}

func (r *fakeRepo) ListRefs(start string, refs []string) (n int) {
	var keys []string
	for key := range r.refs {
		keys = append(keys, key)
	}
	return r.fillWithKeys(keys, start, refs)
}
func (r *fakeRepo) ListNodes(start string, nodes []string) (n int) {
	var keys []string
	for key := range r.refs {
		keys = append(keys, key)
	}
	return r.fillWithKeys(keys, start, nodes)
}
func (r *fakeRepo) ListContents(start string, contents []string) (n int) {
	var keys []string
	for key := range r.refs {
		keys = append(keys, key)
	}
	return r.fillWithKeys(keys, start, contents)
}
func (r *fakeRepo) ListCommits(start string, commits []string) (n int) {
	var keys []string
	for key := range r.refs {
		keys = append(keys, key)
	}
	return r.fillWithKeys(keys, start, commits)
}

func (r *fakeRepo) fillWithKeys(keys []string, start string, dst []string) (n int) {
	sort.Strings(keys)
	startIndex := sort.SearchStrings(keys, start)
	var pos int
	for pos = startIndex; pos < len(keys) && pos-startIndex < len(dst); pos++ {
		dst[pos-startIndex] = keys[pos]
	}
	return pos - startIndex
}

func (r *fakeRepo) StartTransaction() {
	fmt.Printf("Transaction started...\n")
}
func (r *fakeRepo) EndTransaction() error {
	fmt.Printf("Transaction completed\n")
	return nil
}

func (r *fakeRepo) PutRef(ptr, val string) {
	r.refs[ptr] = val
}
func (r *fakeRepo) PutNode(n *jpb.Node) {
	r.nodes[n.Head] = n
}
func (r *fakeRepo) DeleteNode(nodeHash string) {
	panic("not implemented")
}
func (r *fakeRepo) PutContent(content [][]byte) string {
	contentCopy := make([][]byte, len(content))
	for i := range contentCopy {
		contentCopy[i] = make([]byte, len(content[i]))
		copy(contentCopy[i], content[i])
	}
	if graph.HashContent(contentCopy) != graph.HashContent(content) {
		panic("EXPLODE")
	}
	r.contents[graph.HashContent(contentCopy)] = contentCopy
	return graph.HashContent(contentCopy)
}
func (r *fakeRepo) DeleteContent(contentHash string) {
	panic("not implemented")
}
func (r *fakeRepo) PutCommit(c *jpb.Commit) {
	r.commits[graph.HashCommit(c)] = c
}
func (r *fakeRepo) PutReverseDep(newCommit, oldCommit string) {
	r.reverseDeps[oldCommit] = append(r.reverseDeps[oldCommit], newCommit)
}
func (r *fakeRepo) DeleteReverseDep(newCommit, oldCommit string) {
	panic("not implemented")
}
