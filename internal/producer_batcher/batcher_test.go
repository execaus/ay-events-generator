package producer_batcher_test

import (
	"ay-events-generator/internal/producer_batcher"
	"sync/atomic"
	"testing"
	"time"
)

// TestSizeModeFlush проверяет, что SizeMode вызывает flushFn при достижении flushSize.
func TestSizeModeFlush(t *testing.T) {
	var called int32
	flushFn := func(batch []int) {
		atomic.AddInt32(&called, 1)
	}

	b, _ := producer_batcher.NewBatcher[int](flushFn)
	b.SetMode(producer_batcher.SizeMode)
	b.SetFlushSize(3)

	// Push три элемента — должно вызвать flushFn один раз
	b.Push(1)
	b.Push(2)
	b.Push(3)

	time.Sleep(50 * time.Millisecond) // ждем асинхронный вызов
	if atomic.LoadInt32(&called) != 1 {
		t.Errorf("expected flushFn to be called once, got %d", called)
	}
}

// TestTimeModeFlush проверяет, что TimeMode вызывает flushFn по таймеру.
func TestTimeModeFlush(t *testing.T) {
	var called int32
	flushFn := func(batch []int) {
		atomic.AddInt32(&called, 1)
	}

	b, _ := producer_batcher.NewBatcher[int](flushFn)
	b.SetFlushTime(50 * time.Millisecond)
	b.SetMode(producer_batcher.TimeMode)

	b.Push(1)
	b.Push(2)

	time.Sleep(120 * time.Millisecond) // ждем таймер
	if atomic.LoadInt32(&called) == 0 {
		t.Errorf("expected flushFn to be called at least once by timer")
	}
}

// TestCloseFlush проверяет, что Close отправляет остаток сообщений.
func TestCloseFlush(t *testing.T) {
	var called int32
	flushFn := func(batch []int) {
		if len(batch) != 2 {
			t.Errorf("expected 2 messages in batch, got %d", len(batch))
		}
		atomic.AddInt32(&called, 1)
	}

	b, _ := producer_batcher.NewBatcher[int](flushFn)
	b.SetMode(producer_batcher.SizeMode)
	b.SetFlushSize(5)

	b.Push(1)
	b.Push(2)

	b.Close() // должен вызвать flushFn
	if atomic.LoadInt32(&called) != 1 {
		t.Errorf("expected flushFn to be called once on Close")
	}
}

// TestPushAfterClose проверяет, что Push после Close игнорируется.
func TestPushAfterClose(t *testing.T) {
	var called int32
	flushFn := func(batch []int) {
		atomic.AddInt32(&called, 1)
	}

	b, _ := producer_batcher.NewBatcher[int](flushFn)
	b.Close()
	b.Push(1)

	time.Sleep(50 * time.Millisecond)
	if atomic.LoadInt32(&called) != 0 {
		t.Errorf("expected flushFn not to be called after Close")
	}
}
