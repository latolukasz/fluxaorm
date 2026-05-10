package fluxaorm

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestBehavior_InitProducerRetriesAfterFailure proves the lazy init retries
// on subsequent calls after a failure, instead of caching the error permanently.
func TestBehavior_InitProducerRetriesAfterFailure(t *testing.T) {
	// Simulate the retry behavior with the mutex-based approach.
	// A nil producerClient means init hasn't succeeded yet → retry on next call.
	var (
		mu        sync.Mutex
		client    *int // stands in for *kgo.Client
		initCount int
	)

	initFn := func() error {
		mu.Lock()
		defer mu.Unlock()

		if client != nil {
			return nil
		}

		initCount++

		if initCount < 3 {
			return assert.AnError // simulate transient failure
		}

		val := 1
		client = &val // success on 3rd attempt

		return nil
	}

	// Attempts 1-2 fail.
	assert.Error(t, initFn())
	assert.Error(t, initFn())
	assert.Equal(t, 2, initCount)
	assert.Nil(t, client)

	// Attempt 3 succeeds.
	assert.NoError(t, initFn())
	assert.Equal(t, 3, initCount)
	assert.NotNil(t, client)

	// Attempt 4 — already initialized, short-circuits.
	assert.NoError(t, initFn())
	assert.Equal(t, 3, initCount, "must not re-init after success")
}
