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
	if len(*b)+len(*a) == 0 {
		panic("cannot Pop and empty Dequeue")
	}
	if len(*a) > 0 {
		v := (*a)[len(*a)-1]
		*a = (*a)[:len(*a)-1]
		return v
	}
	v := (*b)[0]
	*b = (*b)[1:]
	return v
}
