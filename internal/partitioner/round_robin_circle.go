package partitioner

import "sync"

type RRCircle struct {
	v     int
	m     sync.Mutex
	count int
}

func NewRRCircle(count int) *RRCircle {
	return &RRCircle{count: count}
}

func (c *RRCircle) Load() int {
	c.m.Lock()
	defer c.m.Unlock()

	v := c.v

	if c.v == c.count-1 {
		c.v = 0
	} else {
		c.v++
	}

	return v
}
