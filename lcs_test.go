package jig_test

import (
	"testing"

	"github.com/runningwild/jig"

	. "github.com/smartystreets/goconvey/convey"
)

func TestLCS(t *testing.T) {
	Convey("LCS finds the longest common substring", t, func() {
		Convey("at the start of one string and end of the other", func() {
			ai, bi, length := jig.LCS(
				toUint64s("abcdefghijklmnopqrstuvwxyz"),
				toUint64s("012345678abcdefghijklmnopqrs"))
			So(ai, ShouldEqual, 0)
			So(bi, ShouldEqual, 9)
			So(length, ShouldEqual, 19)
		})
		Convey("at the end of one string and start of the other", func() {
			ai, bi, length := jig.LCS(
				toUint64s("012345678abcdefghijklmnopqrs"),
				toUint64s("abcdefghijklmnopqrstuvwxyz"))
			So(ai, ShouldEqual, 9)
			So(bi, ShouldEqual, 0)
			So(length, ShouldEqual, 19)
		})
		Convey("when the strings are equal", func() {
			ai, bi, length := jig.LCS(
				toUint64s("abcdefghijklmnopqrstuvwxyz"),
				toUint64s("abcdefghijklmnopqrstuvwxyz"))
			So(ai, ShouldEqual, 0)
			So(bi, ShouldEqual, 0)
			So(length, ShouldEqual, 26)
		})
		Convey("when the strings have no common values", func() {
			_, _, length := jig.LCS(
				toUint64s("abcdefghijklmnopqrstuvwxyz"),
				toUint64s("0123456789"))
			So(length, ShouldEqual, 0)
		})
	})
}

func toUint64s(s string) []uint64 {
	var v []uint64
	for _, r := range s {
		v = append(v, uint64(r))
	}
	return v
}

func benchmark(b *testing.B, lenA, lenB, overlap int) {
	b.StopTimer()
	ab := make([]uint64, lenA+lenB-overlap)
	for i := range ab {
		ab[i] = uint64(i)
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		jig.LCS(ab[0:lenA], ab[overlap:])
	}
}

func BenchmarkLCS_100_100_halfequal(b *testing.B) {
	benchmark(b, 100, 100, 50)
}

func BenchmarkLCS_1000_1000_halfequal(b *testing.B) {
	benchmark(b, 1000, 1000, 500)
}

func BenchmarkLCS_10000_10000_halfequal(b *testing.B) {
	benchmark(b, 10000, 10000, 5000)
}
