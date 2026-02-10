package consumer

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// TestBatchModeFlush проверяет flush по размеру батча
func TestBatchModeFlush(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var flushed atomic.Int32
	done := make(chan struct{})

	c := NewConsumer[string](ctx, nil, func(ctx context.Context, buf [][]byte) error {
		flushed.Add(int32(len(buf)))
		close(done) // сигнал о завершении flush
		return nil
	})
	_ = c.SetBatchSize(2)
	_ = c.SetMode(t.Context(), BatchMode)

	in := c.In(ctx)

	// отправляем сообщения
	in <- []byte("a")
	in <- []byte("b")

	// ждём завершения flush
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("flush timed out")
	}

	_ = c.Close()

	if flushed.Load() != 2 {
		t.Fatalf("expected 2 flushed messages, got %d", flushed.Load())
	}
}

// TestTimeModeFlush проверяет flush по таймеру
func TestTimeModeFlush(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var flushed atomic.Int32
	done := make(chan struct{})

	c := NewConsumer[string](ctx, nil, func(ctx context.Context, buf [][]byte) error {
		flushed.Add(int32(len(buf)))
		close(done)
		return nil
	})
	c.SetTickerPeriod(20 * time.Millisecond)
	_ = c.SetMode(t.Context(), TimeMode)

	in := c.In(ctx)
	in <- []byte("a")

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("flush timed out")
	}

	_ = c.Close()

	if flushed.Load() != 1 {
		t.Fatalf("expected 1 flushed message, got %d", flushed.Load())
	}
}

// TestHybridModeFlushByBatch проверяет flush по батчу в hybrid режиме
func TestHybridModeFlushByBatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var flushed atomic.Int32
	done := make(chan struct{})

	c := NewConsumer[string](ctx, nil, func(ctx context.Context, buf [][]byte) error {
		flushed.Add(int32(len(buf)))
		close(done)
		return nil
	})
	_ = c.SetBatchSize(2)
	c.SetTickerPeriod(time.Second) // таймер большой, чтобы не мешал батчу
	_ = c.SetMode(t.Context(), HybridMode)

	in := c.In(ctx)
	in <- []byte("a")
	in <- []byte("b")

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("flush timed out")
	}

	_ = c.Close()

	if flushed.Load() != 2 {
		t.Fatalf("expected 2 flushed messages, got %d", flushed.Load())
	}
}

// TestCloseIsIdempotent проверяет, что Close можно вызывать несколько раз
func TestCloseIsIdempotent(t *testing.T) {
	ctx := context.Background()

	c := NewConsumer[string](ctx, nil, func(ctx context.Context, buf [][]byte) error {
		return nil
	})

	if err := c.Close(); err != nil {
		t.Fatal(err)
	}

	// повторный Close() не должен падать
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestNoDeadlockOnCloseWithInFlightSend проверяет, что Close не блокируется
func TestNoDeadlockOnCloseWithInFlightSend(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := NewConsumer[string](ctx, nil, func(ctx context.Context, buf [][]byte) error {
		return nil
	})

	in := c.In(ctx)

	go func() {
		in <- []byte("a")
	}()

	done := make(chan struct{})
	go func() {
		_ = c.Close()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close() deadlocked")
	}
}
