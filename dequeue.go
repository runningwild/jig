package jig

type Dequeue struct {
	back, front []int
}

func (d *Dequeue) Len() int {
	return len(d.back) + len(d.front)
}

func (d *Dequeue) At(i int) int {
	if i < len(d.front) {
		return d.front[len(d.front)-1-i]
	}
	return d.back[i-len(d.front)]
}

func (d *Dequeue) PushBack(v int) {
	d.back = append(d.back, v)
}

func (d *Dequeue) PushFront(v int) {
	d.front = append(d.front, v)
}

func (d *Dequeue) PopBack() int {
	return popSide(&d.back, &d.front)
}

func (d *Dequeue) PopFront() int {
	return popSide(&d.front, &d.back)
}

// popSide pops from a, using b to rearrange if necessary
func popSide(a, b *[]int) int {
	if len(*a)+len(*b) == 0 {
		panic("cannot Pop an empty Dequeue")
	}
	if len(*a) == 0 {
		amt := (len(*b) + 1) / 2
		*a = make([]int, amt)
		for i := range *a {
			(*a)[i] = (*b)[amt-i-1]
		}
		(*b) = (*b)[amt:]
	}
	v := (*a)[len(*a)-1]
	*a = (*a)[0 : len(*a)-1]
	return v

}
