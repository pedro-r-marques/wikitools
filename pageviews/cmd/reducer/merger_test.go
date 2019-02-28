package main

import (
	"testing"

	"gotest.tools/assert"
)

type TestStreams struct {
	values  [][]int
	cursors []int
	output  *[]int
}

func NewTestStreams(count int, output *[]int) *TestStreams {
	s := new(TestStreams)
	s.values = make([][]int, count)
	s.cursors = make([]int, count)
	s.output = output
	return s
}

func (s TestStreams) Len() int {
	return len(s.values)
}

func (s TestStreams) SetValues(index int, values []int) {
	s.values[index] = values
}

func (s TestStreams) Fill(index int) (bool, int) {
	if index >= len(s.values) {
		return false, -1
	}
	indexValues := s.values[index]
	c := s.cursors[index]
	if c >= len(indexValues) {
		return false, -1
	}
	v := indexValues[c]
	s.cursors[index]++
	return true, v
}

func (s TestStreams) Commit(index int) {
	*s.output = append(*s.output, index)
}

func TestMerger(t *testing.T) {
	output := make([]int, 0, 10)
	streams := NewTestStreams(4, &output)
	streams.SetValues(0, []int{10, 7, 4})
	streams.SetValues(1, []int{6, 3, 2})
	streams.SetValues(2, []int{9, 8})
	streams.SetValues(3, []int{5, 1})
	MergeStreams(streams)
	assert.DeepEqual(t, output, []int{0, 2, 2, 0, 1, 3, 0, 1, 1, 3})
}
