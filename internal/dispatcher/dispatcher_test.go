package dispatcher

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"
)

type mockWriter[T any] struct {
	writeFn func(ctx context.Context, data T) error
	io.Closer
}

func (m *mockWriter[T]) Write(ctx context.Context, data T) error {
	return m.writeFn(ctx, data)
}

func TestDispatcher_Success(t *testing.T) {
	var called int32
	w := &mockWriter[int]{
		writeFn: func(ctx context.Context, data int) error {
			atomic.AddInt32(&called, 1)
			return nil
		},
	}

	d := NewDispatcher[int](w)
	err := d.Write(context.Background(), 42)
	if err != nil {
		t.Fatal(err)
	}

	if atomic.LoadInt32(&called) != 1 {
		t.Errorf("expected writer to be called once, got %d", called)
	}
}

func TestDispatcher_BackoffRetry(t *testing.T) {
	var called int32
	failures := 2
	w := &mockWriter[int]{
		writeFn: func(ctx context.Context, data int) error {
			c := atomic.AddInt32(&called, 1)
			if c <= int32(failures) {
				return errors.New("fail")
			}
			return nil
		},
	}

	d := NewDispatcher[int](w)
	err := d.Write(context.Background(), 100)
	if err != nil {
		t.Fatal(err)
	}

	if atomic.LoadInt32(&called) != int32(failures+1) {
		t.Errorf("expected writer to be called %d times, got %d", failures+1, called)
	}
}

func TestDispatcher_ContextCancel(t *testing.T) {
	var called int32
	w := &mockWriter[int]{
		writeFn: func(ctx context.Context, data int) error {
			atomic.AddInt32(&called, 1)
			// имитируем долгую операцию
			time.Sleep(50 * time.Millisecond)
			return errors.New("fail")
		},
	}

	d := NewDispatcher[int](w)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err := d.Write(ctx, 123)
	if err == nil {
		t.Fatal("expected context cancellation error")
	}

	if atomic.LoadInt32(&called) == 0 {
		t.Errorf("expected at least one call to writer")
	}
}
