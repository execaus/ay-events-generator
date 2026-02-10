package consumer

import (
	"context"
	"errors"
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

	c := NewConsumer[string](ctx, func(data string) error {
		return nil
	}, func(ctx context.Context, buf []string) error {
		flushed.Add(int32(len(buf)))
		close(done) // сигнал о завершении flush
		return nil
	})
	_ = c.SetBatchSize(2)
	_ = c.SetMode(t.Context(), BatchMode)

	in := c.In(ctx)

	// отправляем сообщения
	in <- "a"
	in <- "b"

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

	c := NewConsumer[string](ctx, func(data string) error {
		return nil
	}, func(ctx context.Context, buf []string) error {
		flushed.Add(int32(len(buf)))
		close(done)
		return nil
	})
	c.SetTickerPeriod(20 * time.Millisecond)
	_ = c.SetMode(t.Context(), TimeMode)

	in := c.In(ctx)
	in <- "a"

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

	c := NewConsumer[string](ctx, func(data string) error {
		return nil
	}, func(ctx context.Context, buf []string) error {
		flushed.Add(int32(len(buf)))
		close(done)
		return nil
	})
	_ = c.SetBatchSize(2)
	c.SetTickerPeriod(time.Second) // таймер большой, чтобы не мешал батчу
	_ = c.SetMode(t.Context(), HybridMode)

	in := c.In(ctx)
	in <- "a"
	in <- "b"

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

	c := NewConsumer[string](ctx, func(data string) error {
		return nil
	}, func(ctx context.Context, buf []string) error {
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

	c := NewConsumer[string](ctx, func(data string) error {
		return nil
	}, func(ctx context.Context, buf []string) error {
		return nil
	})

	in := c.In(ctx)

	go func() {
		in <- "a"
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

func TestInvalidMessagesGoToDLQ(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := NewConsumer[string](ctx, func(data string) error {
		return errors.New("invalid message")
	}, func(ctx context.Context, buf []string) error {
		t.Fatal("flushFn should not be called for invalid messages")
		return nil
	})

	_ = c.SetMode(t.Context(), BatchMode)

	in := c.In(ctx)

	in <- "bad-message"

	select {
	case msg := <-c.DLQ():
		if msg.Message != "bad-message" {
			t.Fatalf("expected message 'bad-message', got %q", msg.Message)
		}
		if msg.Err == nil {
			t.Fatal("expected error in DLQ message, got nil")
		}
	case <-time.After(time.Second):
		t.Fatal("DLQ did not receive message")
	}

	_ = c.Close()
}
