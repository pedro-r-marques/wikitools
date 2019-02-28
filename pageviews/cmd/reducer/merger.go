package main

import (
	"container/heap"
)

// StreamSet is the interface between a set of streams to be merged and the merge code.
type StreamSet interface {
	Len() int
	Fill(index int) (status bool, priority int)
	Commit(index int)
}

// intPair is a tuple with stream index and priority value
type intPair struct {
	index int
	value int
}

// intPairHeap is a heap of stream indexes sorted by priority
type intPairHeap []intPair

func (h intPairHeap) Len() int           { return len(h) }
func (h intPairHeap) Less(i, j int) bool { return h[i].value > h[j].value }
func (h intPairHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *intPairHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(intPair))
}

func (h *intPairHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// MergeStreams performs a sorted merge of a set of data streams.
func MergeStreams(streams StreamSet) {
	heapStore := make(intPairHeap, 0, streams.Len())
	for i := 0; i < streams.Len(); i++ {
		if status, priority := streams.Fill(i); status {
			heapStore = append(heapStore, intPair{i, priority})
		}
	}
	heap.Init(&heapStore)
	for len(heapStore) > 0 {
		kv := heap.Pop(&heapStore).(intPair)
		streams.Commit(kv.index)
		if status, priority := streams.Fill(kv.index); status {
			heap.Push(&heapStore, intPair{kv.index, priority})
		}
	}
}
