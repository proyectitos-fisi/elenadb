package buffer_test

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

	lruk := lruk_replacer.NewLRUK(pool_size, k)

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

	lruk := lruk_replacer.NewLRUK(pool_size, k)

	var wg sync.WaitGroup

	// Simulating three concurrent full scans
	fullScan := func() {
		for i := 1; i <= n_disk_pages; i++ {
			mutex.Lock()
			buffer_len := len(buffer_pool)

			if buffer_len >= pool_size {
				// Halt until there is a frame to evict (i.e. Evict() != InvalidFrame)
				evicted := lruk.Evict()
				for evicted == InvalidFrame {
					evicted = lruk.Evict()
				}
				delete(buffer_pool, Frame(evicted))
				lruk.Remove(Frame(evicted))
			}

			lruk.TriggerAccess(Frame(i))
			buffer_pool[Frame(i)] = struct{}{}
			mutex.Unlock()
			// Do i/o stuff...
			// Done! Release the pin.
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

func TestAccessFurther(t *testing.T) {
	lruk := lruk_replacer.NewLRUK(7, 2)

	// Scenario: add six elements to the replacer. We have [1,2,3,4,5]. Frame 6 is non-evictable.
	lruk.TriggerAccess(1)
	lruk.TriggerAccess(2)
	lruk.TriggerAccess(3)
	lruk.TriggerAccess(4)
	lruk.TriggerAccess(5)
	lruk.TriggerAccess(6)
	lruk.SetEvictable(1, true)
	lruk.SetEvictable(2, true)
	lruk.SetEvictable(3, true)
	lruk.SetEvictable(4, true)
	lruk.SetEvictable(5, true)
	lruk.SetEvictable(6, false)
	assert.Equal(t, lruk.Size(), 5)

	// Scenario: Insert access history for frame 1. Now frame 1 has two access histories.
	// All other frames have max backward k-dist. The order of eviction is [2,3,4,5,1].
	lruk.TriggerAccess(1)
	assert.Equal(t, lruk.Size(), 5)

	assert.Equal(t, lruk.Evict(), Frame(2))
	assert.Equal(t, lruk.Evict(), Frame(3))
	assert.Equal(t, lruk.Evict(), Frame(4))

	assert.Equal(t, lruk.Size(), 2)

	// Scenario: Evict three pages from the replacer. Elements with max k-distance should be popped
	// first based on LRU.
	lruk.TriggerAccess(3)
	lruk.TriggerAccess(4)
	lruk.TriggerAccess(5)
	lruk.TriggerAccess(4)
	lruk.SetEvictable(3, true)
	lruk.SetEvictable(4, true)
	assert.Equal(t, lruk.Size(), 4)

	// Scenario: continue looking for victims. We expect 3 to be evicted next.
	assert.Equal(t, lruk.Evict(), Frame(3))
	assert.Equal(t, lruk.Size(), 3)

	lruk.SetEvictable(6, true)
	assert.Equal(t, lruk.Size(), 4)
	assert.Equal(t, lruk.Evict(), Frame(6))
	assert.Equal(t, lruk.Size(), 3)

	lruk.SetEvictable(1, false)
	assert.Equal(t, lruk.Size(), 2)
	assert.Equal(t, lruk.Evict(), Frame(5))
	assert.Equal(t, lruk.Size(), 1)

	lruk.TriggerAccess(1)
	lruk.TriggerAccess(1)
	lruk.SetEvictable(1, true)
	assert.Equal(t, lruk.Size(), 2)
	assert.Equal(t, lruk.Evict(), Frame(4))

	assert.Equal(t, lruk.Size(), 1)
	assert.Equal(t, lruk.Evict(), Frame(1))
	assert.Equal(t, lruk.Size(), 0)

	assert.Equal(t, lruk.Evict(), InvalidFrame)
	assert.Equal(t, lruk.Size(), 0)
}
