package fluxaorm

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSnowflakeMonotonicAndUnique(t *testing.T) {
	g := newSnowflakeGenerator(1)
	const n = 100000
	seen := make(map[uint64]struct{}, n)
	prev := uint64(0)
	for i := 0; i < n; i++ {
		id := g.next()
		assert.Greater(t, id, prev, "ids must be strictly increasing within a single goroutine")
		_, dup := seen[id]
		assert.False(t, dup, "ids must be unique")
		seen[id] = struct{}{}
		prev = id
	}
}

func TestSnowflakeConcurrentUnique(t *testing.T) {
	g := newSnowflakeGenerator(7)
	const goroutines = 16
	const perG = 20000
	var wg sync.WaitGroup
	results := make([][]uint64, goroutines)
	for w := 0; w < goroutines; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			ids := make([]uint64, perG)
			for i := 0; i < perG; i++ {
				ids[i] = g.next()
			}
			results[w] = ids
		}(w)
	}
	wg.Wait()
	seen := make(map[uint64]struct{}, goroutines*perG)
	for _, ids := range results {
		for _, id := range ids {
			_, dup := seen[id]
			assert.False(t, dup, "ids must be unique across goroutines")
			seen[id] = struct{}{}
		}
	}
	assert.Len(t, seen, goroutines*perG)
}

func TestSnowflakeNodeEncoding(t *testing.T) {
	g := newSnowflakeGenerator(5)
	id := g.next()
	node := int64((id >> idNodeShift) & uint64(idMaxNode))
	assert.Equal(t, int64(5), node)
}

func TestSnowflakeNodeClampedNeverPanics(t *testing.T) {
	g := newSnowflakeGenerator(idMaxNode + 1) // out of range, must clamp not panic
	id := g.next()
	node := int64((id >> idNodeShift) & uint64(idMaxNode))
	assert.LessOrEqual(t, node, idMaxNode)
}

func TestSnowflakeClockBackwardsStaysMonotonic(t *testing.T) {
	g := newSnowflakeGenerator(3)
	// Simulate a far-future lastTime, then ensure subsequent ids do not decrease
	// even though the wall clock is "behind" lastTime.
	first := g.next()
	g.mu.Lock()
	g.lastTime += 100000 // jump lastTime 100s into the future
	g.mu.Unlock()
	prev := first
	for i := 0; i < 5000; i++ {
		id := g.next()
		assert.Greater(t, id, prev)
		prev = id
	}
}
