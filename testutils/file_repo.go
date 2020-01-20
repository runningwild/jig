package testutils

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/golang/protobuf/proto"
	"log"
	"os"
	"path/filepath"

	"github.com/boltdb/bolt"
	"github.com/runningwild/jig/graph"
	jpb "github.com/runningwild/jig/proto"
)

type fileRepo struct {
	db *bolt.DB
}

func MakeFileRepo(dir string) graph.Repo {
	os.Mkdir(dir, 0777)
	db, err := bolt.Open(filepath.Join(dir, "db"), 0600, nil)
	if err != nil {
		panic(err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		for _, name := range []string{"ref", "node", "content", "commit", "rdep"} {
			if _, err := tx.CreateBucket([]byte(name)); err != nil {
				return fmt.Errorf("create bucket: %s", err)
			}
		}
		return nil
	}); err != nil {
		panic(err)
	}
	return &fileRepo{
		db: db,
	}
}

func (r *fileRepo) GetRef(ptr string) string {
	data := r.getRawData("ref", ptr)
	if data == nil {
		return ""
	}
	return string(data)
}
func (r *fileRepo) GetNode(nodeHash string) *jpb.Node {
	data := r.getRawData("node", nodeHash)
	if data == nil {
		return nil
	}
	node := &jpb.Node{}
	if err := proto.Unmarshal(data, node); err != nil {
		log.Printf("failed to unmarshal node %q: %v", nodeHash, err)
		return nil
	}
	return node
}
func (r *fileRepo) GetContent(contentHash string) [][]byte {
	data := r.getRawData("content", contentHash)
	if data == nil {
		return nil
	}
	return decodeSliceSliceBytes(data)
}
func (r *fileRepo) GetCommit(commitHash string) *jpb.Commit {
	data := r.getRawData("commit", commitHash)
	if data == nil {
		return nil
	}
	commit := &jpb.Commit{}
	if err := proto.Unmarshal(data, commit); err != nil {
		log.Printf("failed to unmarshal commit %q: %v", commitHash, err)
		return nil
	}
	return commit
}
func (r *fileRepo) GetReverseDeps(commitHash string) []string {
	data := r.getRawData("rdep", commitHash)
	if data == nil {
		return nil
	}
	b := decodeSliceSliceBytes(data)
	var rdeps []string
	for _, rdep := range b {
		rdeps = append(rdeps, string(rdep))
	}
	return rdeps
}

func (r *fileRepo) getRawData(bucketName, key string) []byte {
	var val []byte
	r.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		val = b.Get([]byte(key))
		return nil
	})
	return val
}

func encodeSliceSliceBytes(b [][]byte) []byte {
	buf := bytes.NewBuffer(nil)
	for _, line := range b {
		binary.Write(buf, binary.LittleEndian, uint32(len(line)))
		binary.Write(buf, binary.LittleEndian, line)
	}
	return buf.Bytes()
}

func decodeSliceSliceBytes(data []byte) [][]byte {
	buf := bytes.NewBuffer(data)
	var b [][]byte
	for buf.Len() > 0 {
		var length uint32
		binary.Read(buf, binary.LittleEndian, &length)
		line := make([]byte, length)
		binary.Read(buf, binary.LittleEndian, &line)
		b = append(b, line)
	}
	return b
}

func (r *fileRepo) ListRefs(start string, refs []string) (n int) {
	return r.listObjs("ref", start, refs)
}
func (r *fileRepo) ListNodes(start string, nodes []string) (n int) {
	return r.listObjs("node", start, nodes)
}
func (r *fileRepo) ListContents(start string, contents []string) (n int) {
	return r.listObjs("content", start, contents)
}
func (r *fileRepo) ListCommits(start string, commits []string) (n int) {
	return r.listObjs("commit", start, commits)
}

func (r *fileRepo) listObjs(bucket, start string, dst []string) (n int) {
	var pos int
	if err := r.db.View(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys
		b := tx.Bucket([]byte(bucket))
		c := b.Cursor()
		for k, _ := c.Seek([]byte(start)); k != nil && pos < len(dst); k, _ = c.Next() {
			dst[pos] = string(k)
			pos++
		}
		return nil
	}); err != nil {
		panic(err)
	}
	return pos
}

func (r *fileRepo) StartTransaction() {
	fmt.Printf("Transaction started...\n")
}
func (r *fileRepo) EndTransaction() error {
	fmt.Printf("Transaction completed\n")
	return nil
}

func (r *fileRepo) PutRef(ptr, val string) {
	if err := r.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("ref"))
		return b.Put([]byte(ptr), []byte(val))
	}); err != nil {
		panic(err)
	}
}
func (r *fileRepo) PutNode(n *jpb.Node) {
	data, err := proto.Marshal(n)
	if err != nil {
		panic(err)
	}
	if err := r.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("node"))
		return b.Put([]byte(n.Head), data)
	}); err != nil {
		panic(err)
	}
}
func (r *fileRepo) DeleteNode(nodeHash string) {
	panic("not implemented")
}
func (r *fileRepo) PutContent(content [][]byte) string {
	hash := graph.HashContent(content)
	enc := encodeSliceSliceBytes(content)
	if err := r.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("content"))
		return b.Put([]byte(hash), enc)
	}); err != nil {
		panic(err)
	}
	return hash
}
func (r *fileRepo) DeleteContent(contentHash string) {
	panic("not implemented")
}
func (r *fileRepo) PutCommit(c *jpb.Commit) {
	data, err := proto.Marshal(c)
	if err != nil {
		panic(err)
	}
	if err := r.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("commit"))
		return b.Put([]byte(graph.HashCommit(c)), data)
	}); err != nil {
		panic(err)
	}
}
func (r *fileRepo) PutReverseDep(newCommit, oldCommit string) {
	if err := r.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("rdep"))
		cur := b.Get([]byte(newCommit))
		var rdeps [][]byte
		if cur != nil {
			rdeps = decodeSliceSliceBytes(cur)
		}
		rdeps = append(rdeps, []byte(oldCommit))
		enc := encodeSliceSliceBytes(rdeps)
		return b.Put([]byte(newCommit), enc)
	}); err != nil {
		panic(err)
	}
}
func (r *fileRepo) DeleteReverseDep(newCommit, oldCommit string) {
	panic("not implemented")
}
