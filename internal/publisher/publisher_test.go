package publisher

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPublisher_SendSync(t *testing.T) {
	called := false

	writeFn := func(ctx context.Context, v int) error {
		called = true
		assert.Equal(t, 1, v)
		return nil
	}

	p := NewPublisher[int](t.Context(), writeFn, 1, 1)

	err := p.SendSync(t.Context(), 1)
	assert.NoError(t, err)
	assert.True(t, called)
}

func TestPublisher_SendSync_WaitsForWrite(t *testing.T) {
	writeFn := func(ctx context.Context, v int) error {
		time.Sleep(100 * time.Millisecond)
		return nil
	}

	p := NewPublisher[int](t.Context(), writeFn, 1, 1)

	start := time.Now()
	err := p.SendSync(t.Context(), 1)
	elapsed := time.Since(start)

	assert.NoError(t, err)
	assert.GreaterOrEqual(t, elapsed, 100*time.Millisecond)
}

func TestPublisher_SendAsync(t *testing.T) {
	done := make(chan struct{})

	writeFn := func(ctx context.Context, v int) error {
		return nil
	}

	p := NewPublisher[int](t.Context(), writeFn, 1, 1)

	err := p.SendAsync(t.Context(), 1, func(ctx context.Context, v int, err error) {
		assert.NoError(t, err)
		assert.Equal(t, 1, v)
		close(done)
	})

	assert.NoError(t, err)

	select {
	case <-done:
	case <-time.After(time.Second):
		assert.Fail(t, "callback не был вызван")
	}

	assert.NoError(t, p.Close())
}

func TestPublisher_SendAsync_DoesNotWaitForWrite(t *testing.T) {
	writeStarted := make(chan struct{})
	writeFinished := make(chan struct{})

	writeFn := func(ctx context.Context, v int) error {
		close(writeStarted)
		time.Sleep(100 * time.Millisecond)
		close(writeFinished)
		return nil
	}

	p := NewPublisher[int](t.Context(), writeFn, 1, 1)

	start := time.Now()
	err := p.SendAsync(t.Context(), 1, nil)
	elapsed := time.Since(start)

	assert.NoError(t, err)
	assert.Less(t, elapsed, 50*time.Millisecond)

	<-writeStarted
	<-writeFinished

	assert.NoError(t, p.Close())
}

func TestPublisher_SendAsync_CallbackReceivesError(t *testing.T) {
	expectedErr := errors.New("write failed")
	done := make(chan struct{})

	writeFn := func(ctx context.Context, v int) error {
		return expectedErr
	}

	p := NewPublisher[int](t.Context(), writeFn, 1, 1)

	err := p.SendAsync(t.Context(), 1, func(ctx context.Context, v int, err error) {
		assert.ErrorIs(t, err, expectedErr)
		assert.Equal(t, 1, v)
		close(done)
	})

	assert.NoError(t, err)

	select {
	case <-done:
	case <-time.After(time.Second):
		assert.Fail(t, "callback не был вызван")
	}

	assert.NoError(t, p.Close())
}
