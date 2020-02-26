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
	Convey("DiffBlocks", t, func() {
		var a, b [][]byte
		Convey("on empty strings", func() {
			a = nil
			b = nil
			Convey("can reconstruct the before and after versions from the diff blocks", func() {
				So(AssembleDiffBlocksBefore(a, b, utils.Diff(a, b)), ShouldResemble, a)
				So(AssembleDiffBlocksAfter(a, b, utils.Diff(a, b)), ShouldResemble, b)
			})
		})
		Convey("on empty first string", func() {
			a = nil
			b = bytes.Split([]byte(`a.b.c.d.e.f.g`), []byte{'.'})
			Convey("can reconstruct the before and after versions from the diff blocks", func() {
				So(AssembleDiffBlocksBefore(a, b, utils.Diff(a, b)), ShouldResemble, a)
				So(AssembleDiffBlocksAfter(a, b, utils.Diff(a, b)), ShouldResemble, b)
			})
		})
		Convey("on empty second string", func() {
			a = bytes.Split([]byte(`a.b.c.d.e.f.g`), []byte{'.'})
			b = nil
			Convey("can reconstruct the before and after versions from the diff blocks", func() {
				So(AssembleDiffBlocksBefore(a, b, utils.Diff(a, b)), ShouldResemble, a)
				So(AssembleDiffBlocksAfter(a, b, utils.Diff(a, b)), ShouldResemble, b)
			})
		})
		Convey("on equal strings", func() {
			a = bytes.Split([]byte(`a.b.c.d.e.f.g`), []byte{'.'})
			b = bytes.Split([]byte(`a.b.c.d.e.f.g`), []byte{'.'})
			Convey("can reconstruct the before and after versions from the diff blocks", func() {
				So(AssembleDiffBlocksBefore(a, b, utils.Diff(a, b)), ShouldResemble, a)
				So(AssembleDiffBlocksAfter(a, b, utils.Diff(a, b)), ShouldResemble, b)
			})
		})
		Convey("on an insertion at the beginning of the file", func() {
			a = bytes.Split([]byte(`a.b.c.d.e.f.g`), []byte{'.'})
			b = bytes.Split([]byte(`x.y.z.a.b.c.d.e.f.g`), []byte{'.'})
			Convey("can reconstruct the before and after versions from the diff blocks", func() {
				So(AssembleDiffBlocksBefore(a, b, utils.Diff(a, b)), ShouldResemble, a)
				So(AssembleDiffBlocksAfter(a, b, utils.Diff(a, b)), ShouldResemble, b)
			})
		})
		Convey("on an insertion at the end of the file", func() {
			a = bytes.Split([]byte(`a.b.c.d.e.f.g`), []byte{'.'})
			b = bytes.Split([]byte(`a.b.c.d.e.f.g.x.y.z`), []byte{'.'})
			Convey("can reconstruct the before and after versions from the diff blocks", func() {
				So(AssembleDiffBlocksBefore(a, b, utils.Diff(a, b)), ShouldResemble, a)
				So(AssembleDiffBlocksAfter(a, b, utils.Diff(a, b)), ShouldResemble, b)
			})
		})
		Convey("on a deletion at the start of the file", func() {
			a = bytes.Split([]byte(`x.y.z.a.b.c.d.e.f.g`), []byte{'.'})
			b = bytes.Split([]byte(`a.b.c.d.e.f.g`), []byte{'.'})
			Convey("can reconstruct the before and after versions from the diff blocks", func() {
				So(AssembleDiffBlocksBefore(a, b, utils.Diff(a, b)), ShouldResemble, a)
				So(AssembleDiffBlocksAfter(a, b, utils.Diff(a, b)), ShouldResemble, b)
			})
		})
		Convey("on a deletion at the end of the file", func() {
			a = bytes.Split([]byte(`a.b.c.d.e.f.g.x.y.z`), []byte{'.'})
			b = bytes.Split([]byte(`a.b.c.d.e.f.g`), []byte{'.'})
			Convey("can reconstruct the before and after versions from the diff blocks", func() {
				So(AssembleDiffBlocksBefore(a, b, utils.Diff(a, b)), ShouldResemble, a)
				So(AssembleDiffBlocksAfter(a, b, utils.Diff(a, b)), ShouldResemble, b)
			})
		})
		Convey("on an insertion in the middle of the file", func() {
			a = bytes.Split([]byte(`a.b.c.d.e.f.g`), []byte{'.'})
			b = bytes.Split([]byte(`a.b.c.x.y.z.d.e.f.g`), []byte{'.'})
			Convey("can reconstruct the before and after versions from the diff blocks", func() {
				So(AssembleDiffBlocksBefore(a, b, utils.Diff(a, b)), ShouldResemble, a)
				So(AssembleDiffBlocksAfter(a, b, utils.Diff(a, b)), ShouldResemble, b)
			})
		})
		Convey("on a deletion in the middle of the file", func() {
			a = bytes.Split([]byte(`a.b.c.x.y.z.d.e.f.g`), []byte{'.'})
			b = bytes.Split([]byte(`a.b.c.d.e.f.g`), []byte{'.'})
			Convey("can reconstruct the before and after versions from the diff blocks", func() {
				So(AssembleDiffBlocksBefore(a, b, utils.Diff(a, b)), ShouldResemble, a)
				So(AssembleDiffBlocksAfter(a, b, utils.Diff(a, b)), ShouldResemble, b)
			})
		})
		Convey("on duplicating a substring at the beginning", func() {
			a = bytes.Split([]byte(`a.b.c.d.e.f.g`), []byte{'.'})
			b = bytes.Split([]byte(`a.b.c.a.b.c.d.e.f.g`), []byte{'.'})
			Convey("can reconstruct the before and after versions from the diff blocks", func() {
				So(AssembleDiffBlocksBefore(a, b, utils.Diff(a, b)), ShouldResemble, a)
				So(AssembleDiffBlocksAfter(a, b, utils.Diff(a, b)), ShouldResemble, b)
			})
		})
		Convey("on duplicating a substring in the middle", func() {
			a = bytes.Split([]byte(`a.b.c.d.e.f.g`), []byte{'.'})
			b = bytes.Split([]byte(`a.b.c.d.e.a.b.c.f.g`), []byte{'.'})
			Convey("can reconstruct the before and after versions from the diff blocks", func() {
				So(AssembleDiffBlocksBefore(a, b, utils.Diff(a, b)), ShouldResemble, a)
				So(AssembleDiffBlocksAfter(a, b, utils.Diff(a, b)), ShouldResemble, b)
			})
		})
		Convey("on duplicating a substring at the end", func() {
			a = bytes.Split([]byte(`a.b.c.d.e.f.g`), []byte{'.'})
			b = bytes.Split([]byte(`a.b.c.d.e.f.g.a.b.c`), []byte{'.'})
			Convey("can reconstruct the before and after versions from the diff blocks", func() {
				So(AssembleDiffBlocksBefore(a, b, utils.Diff(a, b)), ShouldResemble, a)
				So(AssembleDiffBlocksAfter(a, b, utils.Diff(a, b)), ShouldResemble, b)
			})
		})
		Convey("on deletion and insertion at the end", func() {
			a = bytes.Split([]byte(`a.b.c.d.e.f.g.h.i.j.k`), []byte{'.'})
			b = bytes.Split([]byte(`a.b.c.f.g.h.i.j.l`), []byte{'.'})
			Convey("can reconstruct the before and after versions from the diff blocks", func() {
				So(AssembleDiffBlocksBefore(a, b, utils.Diff(a, b)), ShouldResemble, a)
				So(AssembleDiffBlocksAfter(a, b, utils.Diff(a, b)), ShouldResemble, b)
			})
		})
	})
}

func AssembleDiffBlocksBefore(a, b [][]byte, dbs []utils.DiffBlock) [][]byte {
	var data [][]byte
	var importBlocks []utils.ImportBlock
	for _, db := range dbs {
		if ib, ok := db.(utils.ImportBlock); ok {
			importBlocks = append(importBlocks, ib)
		}
	}
	sort.Slice(importBlocks, func(i, j int) bool { return importBlocks[i].BlockID < importBlocks[j].BlockID })
	var exportCount int
	for _, db := range dbs {
		switch block := db.(type) {
		case utils.InsertionBlock:
		case utils.DeletionBlock:
			data = append(data, a[block.Ai:block.Ai+block.Length]...)
		case utils.ImportBlock:
		case utils.ExportBlock:
			ib := importBlocks[exportCount]
			exportCount++
			data = append(data, b[ib.Bi:ib.Bi+ib.Length]...)
		case utils.CommonBlock:
			data = append(data, b[block.Bi:block.Bi+block.Length]...)
		default:
			panic(fmt.Sprintf("unknown block type %T", db))
		}
	}
	return data
}

func AssembleDiffBlocksAfter(a, b [][]byte, dbs []utils.DiffBlock) [][]byte {
	var data [][]byte
	for _, db := range dbs {
		switch block := db.(type) {
		case utils.InsertionBlock:
			data = append(data, b[block.Bi:block.Bi+block.Length]...)
		case utils.DeletionBlock:
		case utils.ImportBlock:
			data = append(data, b[block.Bi:block.Bi+block.Length]...)
		case utils.ExportBlock:
		case utils.CommonBlock:
			data = append(data, b[block.Bi:block.Bi+block.Length]...)
		default:
			panic(fmt.Sprintf("unknown block type %T", db))
		}
	}
	return data
}
func TestRandomDiffs(t *testing.T) {
	Convey("Diff", t, func() {
		type testcase struct {
			lines, edits, seed int
		}
		for _, tc := range []testcase{
			{10, 0, 0},
			{10, 1, 0},
			{10, 2, 0},
			{10, 3, 0},
			// {15, 0, 0},
			// {100, 5, 0},
			// {100, 5, 1},
			// {100, 5, 2},
			// {10000, 0, 0},
			// {10000, 1, 0},
			// {10000, 2, 0},
			// {10000, 10, 0},
			// {10000, 100, 0},
			// {10000, 1000, 0},
		} {
			a := bytes.Split(makeRandomInput(tc.lines, tc.seed), []byte("\n"))
			b := bytes.Split(makeRandomEdits(makeRandomInput(tc.lines, tc.seed), tc.edits, tc.seed), []byte("\n"))
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
