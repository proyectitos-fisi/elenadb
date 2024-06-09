package lruk_replacer

import (
	"container/list"
	"fisi/elenadb/pkg/common"
	"sync"
	"sync/atomic"
	"time"
)

const infinity = int64(^uint64(0) >> 1)

type FrameID = common.FrameID

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
	max_frames int
	size       atomic.Int32 // number of evictable frames
	nodes      map[FrameID]*LRUKNode
	latch      sync.RWMutex
}

// Records how often a page (frame) is being accessed.
// We, from the LRU-K side, don't know if a page is evictable. It may be in the
// middle of a transaction, for example. The BufferPoolManager is responsible for
// marking a page as evictable calling the SetEvictable method.
type LRUKNode struct {
	K         int
	accesses  list.List
	frame_id  FrameID
	evictable bool
}

func New(n_frames int, k int) *LRUKReplacer {
	return &LRUKReplacer{
		K:          k,
		max_frames: n_frames,
		size:       atomic.Int32{},
		nodes:      make(map[FrameID]*LRUKNode),
		latch:      sync.RWMutex{},
	}
}

// When a BufferPool make use of a page, it MUST notify the LRU-K replacer
// to keep track of the page access.
//
// Note for @damaris: accessing a page is equivalent to pinning it.
func (lru *LRUKReplacer) TriggerAccess(frame_id FrameID) {
	lru.latch.Lock()
	defer lru.latch.Unlock()
	node, found := lru.nodes[frame_id]

	if found {
		node.registerAccess()
	} else {
		// test assertions
		println("--------", len(lru.nodes))

		if len(lru.nodes) >= int(lru.max_frames) {
			panic(
				"LRU-K replacer is full. You may not be removing pages correctly with Remove()." +
					"The LRU-K should map to the same number of frames in the BufferPoolManager",
			)
		}
		lru.nodes[frame_id] = newNode(lru.K, frame_id)
	}
}

// This method should be called from the BufferPoolManager when a page is deleted.
func (lru *LRUKReplacer) Remove(frame_id FrameID) {
	lru.latch.Lock()
	defer lru.latch.Unlock()
	node, found := lru.nodes[frame_id]
	// We just clean the access list. No need to remove the entry from the map
	// because we are working with a fixed size buffer pool (i.e. fixed num of frames)
	if found && node.evictable {
		lru.size.Add(-1)
	}
	delete(lru.nodes, frame_id)
}

// MUST be called when the reference count of a page is 0.
// Other reasons are also ok.
func (lru *LRUKReplacer) SetEvictable(frame_id FrameID, isEvictable bool) {
	lru.latch.Lock()
	defer lru.latch.Unlock()

	node, found := lru.nodes[frame_id]
	if found {
		if node.evictable == isEvictable {
			return
		}

		if isEvictable {
			lru.size.Add(1)
		} else {
			lru.size.Add(-1)
		}
		node.evictable = isEvictable
	}
}

// Note that you'll need to check if the returning value is not InvalidFrameID
func (lru *LRUKReplacer) Evict() FrameID {
	if lru.size.Load() == 0 {
		return common.InvalidFrameID
	}

	now := now()
	evicted_frame := common.InvalidFrameID
	max_distance := int64(0)

	lru.latch.RLock()
	defer lru.latch.RUnlock()
	for _, node := range lru.nodes {
		if !node.evictable {
			continue
		}

		if d := node.backwardDistance(now); d > max_distance {
			if d == infinity {
				return node.frame_id
			}
			evicted_frame = node.frame_id
			max_distance = d

		}
	}
	return evicted_frame
}

func (lru *LRUKReplacer) Size() int {
	return int(lru.size.Load())
}

// LRU-K Node implementations 🐢

// Creates a new frame node, registering the first access.
func newNode(k int, frame_id FrameID) *LRUKNode {
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
