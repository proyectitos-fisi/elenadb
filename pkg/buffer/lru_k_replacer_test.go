package lruk_replacer_test

import (
	lruk_replacer "fisi/elenadb/pkg/buffer"
	"fisi/elenadb/pkg/common"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

type Frame = common.FrameID

const InvalidFrame = common.InvalidFrameID

// So hard to read... This tests whether a LRU-1 (K=1) behaves like a normal LRU.
func TestLRUK1BehavesLikeANormalLRU(t *testing.T) {
	pool_size := 3
	k := 1

	lruk := lruk_replacer.New(pool_size, k)

	lruk.TriggerAccess(Frame(1))
	lruk.TriggerAccess(Frame(2))
	lruk.TriggerAccess(Frame(3))

	if f := lruk.Evict(); f != InvalidFrame {
		t.Errorf("No frames was marked as evicted. Expected to evict nothing, but got %d", f)
	}

	// Note that frame 2 is not evictable. It may be in the middle of a transaction.
	// i.e. reference count is greater than 0.
	lruk.SetEvictable(Frame(1), true)
	lruk.SetEvictable(Frame(2), false)
	lruk.SetEvictable(Frame(3), true)

	assert.Equal(t, lruk.Size(), 2) // two frames are evictable

	if f := lruk.Evict(); f != 1 {
		t.Errorf("Expected to evict frame 1, but got %d", f)
	}
	lruk.Remove(1)
	assert.Equal(t, lruk.Size(), 1)

	if f := lruk.Evict(); f != 3 {
		t.Errorf("Expected to evict frame 3, but got %d", f)
	}
	lruk.Remove(3)
	assert.Equal(t, lruk.Size(), 0)

	if f := lruk.Evict(); f != InvalidFrame {
		t.Errorf("Expected no more evictions, but got %d", f)
	}
}

type set = map[Frame]struct{}

// The whole purpose of a LRU-K is to
// - Avoid sequential flooding (test here)
// - Estimate (predict) the future access patterns
// This also tests concurrent access to the LRU-K replacer.
func TestLRUKPreventsSequentialFlooding(t *testing.T) {
	pool_size := 8
	n_disk_pages := 64
	k := 3

	buffer_pool := set{}
	mutex := sync.RWMutex{}

	lruk := lruk_replacer.New(pool_size, k)

	var wg sync.WaitGroup

	// Simulating three concurrent full scans
	fullScan := func() {
		for i := 1; i <= n_disk_pages; i++ {
			// if int(buffer_pool.Len()) >= pool_size {
			mutex.Lock()
			buffer_len := len(buffer_pool)

			if buffer_len >= pool_size {
				to_remove := lruk.Evict()

				lruk.Remove(Frame(to_remove))
				// buffer_pool.Remove(Frame(to_remove))
				delete(buffer_pool, Frame(to_remove))
			}

			lruk.TriggerAccess(Frame(i))
			buffer_pool[Frame(i)] = struct{}{}
			mutex.Unlock()
			// Do i/o stuff...
			// done!
			lruk.SetEvictable(Frame(i), true)
		}
		wg.Done()
	}

	n_workers := 4

	for i := 0; i < n_workers; i++ {
		wg.Add(1)
		go fullScan()
	}

	// wait for the full scans to finish
	wg.Wait()
}
