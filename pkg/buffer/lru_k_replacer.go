package lruk_replacer

import (
	"container/list"
	"fisi/elenadb/pkg/common"
	"time"
)

const infinity = int64(^uint64(0) >> 1)

// As part of the ElenaDB ® buffer replacement policy we use a LRU-K replacer.
//
// An LRU-K replacer is a generalization of the least recently used (LRU), where
// LRU = LRU-1. The LRU-K policy keeps track of the order in which the K most
// recent page references occurred. When a page needs to be replaced, the LRU-K
//
// See https://www.wikiwand.com/en/Page_replacement_algorithm.
type LRUKReplacer struct {
	// contains filtered or unexported fields
	K          int
	max_frames int16
	len_frames int16
	nodes      map[common.FrameID]*LRUKNode
}

// Records how often a page (frame) is being accessed.
// We, from the LRU-K side, don't know if a page is evictable. It may be in the
// middle of a transaction, for example. The BufferPoolManager is responsible for
// marking a page as evictable calling the SetEvictable method.
type LRUKNode struct {
	K         int
	accesses  list.List
	frame_id  common.FrameID
	evictable bool
}

func New(n_frames int16, k int) *LRUKReplacer {
	return &LRUKReplacer{
		K:          k,
		max_frames: n_frames,
		nodes:      make(map[common.FrameID]*LRUKNode),
	}
}

// When a BufferPool make use of a page, it MUST notify the LRU-K replacer
// to keep track of the page access.
//
// Note for @damaris: accessing a page is equivalent to pinning it.
func (lru *LRUKReplacer) TriggerAccess(frame_id common.FrameID) {
	node, found := lru.nodes[frame_id]

	if found {
		node.registerAccess()
	} else {
		lru.nodes[frame_id] = newNode(lru.K, frame_id)
	}
}

// This method should be called from the BufferPoolManager when a page is deleted.
// @TODO: why deleting in the LRU tho? Wouldn't be better if we maintain the access
// timestamps for future use? @benchmark
func (lru *LRUKReplacer) Remove(frame_id common.FrameID) {
	node, found := lru.nodes[frame_id]
	// We just clean the access list. No need to remove the entry from the map
	// because we are working with a fixed size buffer pool (i.e. fixed num of frames)
	if found {
		node.accesses.Init()
		node.evictable = false
	}
}

// MUST be called when the reference count of a page is 0.
// Other reasons are also ok.
func (lru *LRUKReplacer) SetEvictable(frame_id common.FrameID) {
	node, found := lru.nodes[frame_id]
	if found {
		node.evictable = true
	}
}

// Note that you'll need to check if the returning value is not InvalidFrameID
func (lru *LRUKReplacer) Evict() common.FrameID {
	now := now()
	evicted_frame := common.InvalidFrameID
	max_distance := int64(0)

	for _, node := range lru.nodes {
		if !node.evictable {
			continue
		}

		if d := node.backwardDistance(now); d > max_distance {
			if d == infinity {
				return evicted_frame
			}
			evicted_frame = node.frame_id
			max_distance = d

		}
	}

	delete(lru.nodes, evicted_frame)
	return evicted_frame
}

// LRU-K Node implementations 🐢

// Creates a new frame node, registering the first access.
func newNode(k int, frame_id common.FrameID) *LRUKNode {
	node := &LRUKNode{
		K:        k,
		frame_id: frame_id,
	}
	node.accesses.PushBack(now())
	return node
}

func (node *LRUKNode) registerAccess() {
	if node.accesses.Len() >= node.K {
		node.accesses.Remove(node.accesses.Front())
	}
	node.accesses.PushBack(now())
}

func (node *LRUKNode) backwardDistance(t int64) int64 {
	if node.accesses.Len() < node.K {
		return infinity
	}
	return t - node.accesses.Front().Value.(int64)
}

// Utils!!!

func now() int64 {
	// Note: any running instances of ElenaDB will catastrophically panic after the 2262 year
	// due to an int64 overflow.
	// It will be known as 'The Elena Y2K62 Disaster'
	// Wish I could be there to see it.
	return time.Now().UnixNano()
}
