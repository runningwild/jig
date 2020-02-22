package utils_test

import (
	"bytes"
	"fmt"
	"sort"
	"testing"

	"github.com/runningwild/jig/utils"

	. "github.com/smartystreets/goconvey/convey"
)

func TestDiff(t *testing.T) {
	a := bytes.Split([]byte(`
alpha
bravo
charlie
delta
echo
foxtrot
golf
hotel
india
juliet
kangaroo
lemon
nutty
omen
peanut
quasar
rusty`)[1:], []byte{'\n'})
	b := bytes.Split([]byte(`
alpha
foxtrot
golf
hotel
india
delta
echo
juliet
kangaroo
lemon
monkey
nutty
omen
bravo
charlie
peanut
rusty`)[1:], []byte{'\n'})
	Convey("DiffBlocks", t, func() {
		dbs := utils.Diff(a, b)

		Convey("can reconstruct the original version", func() {
			var ra [][]byte
			var importBlocks []utils.ImportBlock
			for _, db := range dbs {
				if ib, ok := db.(utils.ImportBlock); ok {
					importBlocks = append(importBlocks, ib)
				}
			}
			sort.Slice(importBlocks, func(i, j int) bool { return importBlocks[i].BlockID < importBlocks[j].BlockID })
			exportCount := 0
			for _, db := range dbs {
				switch block := db.(type) {
				case utils.InsertionBlock:
				case utils.DeletionBlock:
					ra = append(ra, a[block.Ai:block.Ai+block.Length]...)
				case utils.ImportBlock:
				case utils.ExportBlock:
					ib := importBlocks[exportCount]
					exportCount++
					ra = append(ra, b[ib.Bi:ib.Bi+ib.Length]...)
				case utils.CommonBlock:
					ra = append(ra, b[block.Bi:block.Bi+block.Length]...)
				default:
					t.Fatalf("unknown block type %T", db)
				}
			}
			So(ra, ShouldResemble, a)
		})

		Convey("can reconstruct the new version", func() {
			var rb [][]byte
			for _, db := range dbs {
				switch block := db.(type) {
				case utils.InsertionBlock:
					rb = append(rb, b[block.Bi:block.Bi+block.Length]...)
				case utils.DeletionBlock:
				case utils.ImportBlock:
					rb = append(rb, b[block.Bi:block.Bi+block.Length]...)
				case utils.ExportBlock:
				case utils.CommonBlock:
					rb = append(rb, b[block.Bi:block.Bi+block.Length]...)
				default:
					t.Fatalf("unknown block type %T", db)
				}
			}
			So(rb, ShouldResemble, b)
		})
	})
}

func TestRandomDiffs(t *testing.T) {
	Convey("Diff", t, func() {
		type testcase struct {
			lines, edits, seed int
		}
		for _, tc := range []testcase{
			{10, 0, 0},
			{15, 0, 0},
			{100, 5, 0},
			{100, 5, 1},
			{100, 5, 2},
			{10000, 0, 0},
			{10000, 1, 0},
			{10000, 2, 0},
			{10000, 10, 0},
			{10000, 100, 0},
			{10000, 1000, 0},
		} {
			a := bytes.Split(makeRandomInput(tc.lines, tc.seed), []byte("\n"))
			b := bytes.Split(makeRandomEdits(makeRandomInput(tc.lines, tc.seed), tc.edits, tc.seed), []byte("\n"))
			a = [][]byte{
				[]byte(""),
				[]byte("foo"),
				[]byte(""),
			}
			b = [][]byte{
				[]byte(""),
				[]byte("foo"),
				[]byte(""),
			}
			fmt.Printf("a:\n")
			for _, line := range a {
				fmt.Printf("%q\n", line)
			}
			fmt.Printf("b:\n")
			for _, line := range b {
				fmt.Printf("%q\n", line)
			}
			dbs := utils.Diff(a, b)
			fmt.Printf("%d block\n", len(dbs))
			for _, db := range dbs {
				fmt.Printf("%T: %v\n", db, db)
			}
			var ra [][]byte
			var importBlocks []utils.ImportBlock
			for _, db := range dbs {
				if ib, ok := db.(utils.ImportBlock); ok {
					importBlocks = append(importBlocks, ib)
				}
			}
			sort.Slice(importBlocks, func(i, j int) bool { return importBlocks[i].BlockID < importBlocks[j].BlockID })
			exportCount := 0
			for _, db := range dbs {
				switch block := db.(type) {
				case utils.InsertionBlock:
				case utils.DeletionBlock:
					ra = append(ra, a[block.Ai:block.Ai+block.Length]...)
				case utils.ImportBlock:
				case utils.ExportBlock:
					ib := importBlocks[exportCount]
					exportCount++
					ra = append(ra, b[ib.Bi:ib.Bi+ib.Length]...)
				case utils.CommonBlock:
					ra = append(ra, b[block.Bi:block.Bi+block.Length]...)
				default:
					t.Fatalf("unknown block type %T", db)
				}
			}
			So(string(bytes.Join(ra, []byte("\n"))), ShouldEqual, string(bytes.Join(a, []byte("\n"))))

			var rb [][]byte
			for _, db := range dbs {
				switch block := db.(type) {
				case utils.InsertionBlock:
					rb = append(rb, b[block.Bi:block.Bi+block.Length]...)
				case utils.DeletionBlock:
				case utils.ImportBlock:
					rb = append(rb, b[block.Bi:block.Bi+block.Length]...)
				case utils.ExportBlock:
				case utils.CommonBlock:
					rb = append(rb, b[block.Bi:block.Bi+block.Length]...)
				default:
					t.Fatalf("unknown block type %T", db)
				}
			}
			So(string(bytes.Join(rb, []byte("\n"))), ShouldEqual, string(bytes.Join(b, []byte("\n"))))
		}
	})
}
