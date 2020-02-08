package filerepo

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/boltdb/bolt"
	"github.com/runningwild/jig/graph"
)

type fileView struct {
	db *bolt.DB
}

func MakeView(dir string) (graph.View, error) {
	os.Mkdir(dir, 0777)
	db, err := bolt.Open(filepath.Join(dir, "view"), 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create view: %w", err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		for _, name := range []string{"frontiers"} {
			if _, err := tx.CreateBucketIfNotExists([]byte(name)); err != nil {
				return fmt.Errorf("create bucket: %s", err)
			}
		}
		b := tx.Bucket([]byte("frontiers"))
		if b.Stats().BucketN == 1 {
			if _, err := b.CreateBucket([]byte("main")); err != nil {
				return fmt.Errorf("failed to create initial branch: %v", err)
			}
		}
		return b.Put([]byte("current"), []byte("main"))
	}); err != nil {
		return nil, fmt.Errorf("failed to initialize database: %w", err)
	}
	return &fileView{
		db: db,
	}, nil
}

func (v *fileView) getRawData(bucketName, key string) ([]byte, error) {
	var val []byte
	if err := v.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		val = b.Get([]byte(key))
		if val == nil {
			return fmt.Errorf("not found")
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return val, nil
}
func (v *fileView) listObjs(bucket, start string, dst []string) (n int, err error) {
	var pos int
	if err := v.db.View(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys
		b := tx.Bucket([]byte(bucket))
		c := b.Cursor()
		for k, _ := c.Seek([]byte(start)); k != nil && pos < len(dst); k, _ = c.Next() {
			dst[pos] = string(k)
			pos++
		}
		return nil
	}); err != nil {
		return 0, err
	}
	return pos, nil
}
func (v *fileView) ListFrontiers(start string, frontiers []string) (n int, err error) {
	return v.listObjs("frontiers", start, frontiers)
}
func (v *fileView) CurrentFrontier() (string, error) {
	data, err := v.getRawData("frontiers", "current")
	if err != nil {
		return "", err
	}
	return string(data), nil
}
func (v *fileView) ChangeFrontiers(frontier string) error {
	if err := v.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("frontiers"))
		if f := b.Bucket([]byte(frontier)); f == nil {
			return fmt.Errorf("frontier not found")
		}
		return b.Put([]byte("current"), []byte(frontier))
	}); err != nil {
		return err
	}
	return nil
}
func (v *fileView) AdvanceFrontier(commit string) error {
	if err := v.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("frontiers"))
		if b == nil {
			return fmt.Errorf("frontiers bucket not found")
		}
		c := b.Get([]byte("current"))
		if c == nil {
			return fmt.Errorf("current frontier unknown")
		}
		f := b.Bucket(c)
		if f == nil {
			return fmt.Errorf("current frontier unspecified")
		}
		return f.Put([]byte(commit), []byte{})
	}); err != nil {
		return err
	}
	return nil
}
func (v *fileView) CreateFrontier(frontier string) error {
	if err := v.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("frontiers"))
		if b == nil {
			return fmt.Errorf("frontiers bucket not found")
		}
		c := b.Get([]byte("current"))
		if c == nil {
			return fmt.Errorf("current frontier unknown")
		}
		from := b.Bucket(c)
		if from == nil {
			return fmt.Errorf("current frontier unspecified")
		}
		to, err := b.CreateBucket([]byte(frontier))
		if err != nil {
			return fmt.Errorf("failed to create frontier %q: %v", frontier, err)
		}
		if err := from.ForEach(func(k, v []byte) error {
			return to.Put(k, v)
		}); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (v *fileView) GetFrontier(frontier string) (graph.Frontier, error) {
	err := v.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("frontiers"))
		if b == nil {
			return fmt.Errorf("frontiers bucket not found")
		}
		if b.Bucket([]byte(frontier)) == nil {
			return fmt.Errorf("frontier unknown")
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &boltFrontier{frontier: frontier, v: v}, nil
}

type boltFrontier struct {
	frontier string
	v        *fileView
}

func (bf *boltFrontier) Observes(commit string) (bool, error) {
	var observes bool
	err := bf.v.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("frontiers"))
		if b == nil {
			return fmt.Errorf("frontiers bucket not found")
		}
		f := b.Bucket([]byte(bf.frontier))
		if f == nil {
			return fmt.Errorf("frontier unknown")
		}
		observes = f.Get([]byte(commit)) != nil
		return nil
	})
	return observes, err
}
