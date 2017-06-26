package jig_test

import (
	"testing"

	"github.com/runningwild/jig"

	. "github.com/smartystreets/goconvey/convey"
)

func TestDequeue(t *testing.T) {
	Convey("Dequeue", t, func() {
		Convey("can push front and pop front in the opposite order", func() {
			var d jig.Dequeue
			d.PushFront(0)
			d.PushFront(1)
			d.PushFront(4)
			d.PushFront(5)
			So(d.PopFront(), ShouldEqual, 5)
			So(d.PopFront(), ShouldEqual, 4)
			So(d.PopFront(), ShouldEqual, 1)
			So(d.PopFront(), ShouldEqual, 0)
		})
		Convey("can push back and pop back in the opposite order", func() {
			var d jig.Dequeue
			d.PushBack(0)
			d.PushBack(1)
			d.PushBack(4)
			d.PushBack(5)
			So(d.PopBack(), ShouldEqual, 5)
			So(d.PopBack(), ShouldEqual, 4)
			So(d.PopBack(), ShouldEqual, 1)
			So(d.PopBack(), ShouldEqual, 0)
		})
		Convey("can push front and pop back in the same order", func() {
			var d jig.Dequeue
			d.PushFront(0)
			d.PushFront(1)
			d.PushFront(4)
			d.PushFront(5)
			So(d.PopBack(), ShouldEqual, 0)
			So(d.PopBack(), ShouldEqual, 1)
			So(d.PopBack(), ShouldEqual, 4)
			So(d.PopBack(), ShouldEqual, 5)
		})
		Convey("can push back and pop front in the same order", func() {
			var d jig.Dequeue
			d.PushBack(0)
			d.PushBack(1)
			d.PushBack(4)
			d.PushBack(5)
			So(d.PopFront(), ShouldEqual, 0)
			So(d.PopFront(), ShouldEqual, 1)
			So(d.PopFront(), ShouldEqual, 4)
			So(d.PopFront(), ShouldEqual, 5)
		})
		Convey("can index properly", func() {
			var d jig.Dequeue
			d.PushFront(4)
			d.PushFront(3)
			d.PushBack(5)
			d.PushBack(6)
			So(d.At(0), ShouldEqual, 3)
			So(d.At(1), ShouldEqual, 4)
			So(d.At(2), ShouldEqual, 5)
			So(d.At(3), ShouldEqual, 6)
		})
		Convey("can index properly after pushing and popping", func() {
			var d jig.Dequeue
			d.PushFront(0)
			d.PushFront(-1)
			d.PushBack(1)
			d.PushBack(2)
			d.PushBack(3)
			d.PushFront(-2)
			d.PushBack(4)
			d.PushBack(5)
			d.PushBack(6)
			d.PopBack()
			d.PopBack()
			d.PopBack()
			d.PopFront()
			d.PopFront()
			d.PopBack()
			So(d.Len(), ShouldEqual, 3)
			So(d.At(0), ShouldEqual, 0)
			So(d.At(1), ShouldEqual, 1)
			So(d.At(2), ShouldEqual, 2)
		})
	})
}
