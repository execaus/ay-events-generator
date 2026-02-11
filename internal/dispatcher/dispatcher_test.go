package dispatcher

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestDispatcher_Success(t *testing.T) {
	var called int32
	w := func(ctx context.Context) error {
		atomic.AddInt32(&called, 1)
		return nil
	}

	d := NewDispatcher()
	err := d.Write(context.Background(), w)
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
	w := func(ctx context.Context) error {
		c := atomic.AddInt32(&called, 1)
		if c <= int32(failures) {
			return errors.New("fail")
		}
		return nil
	}

	d := NewDispatcher()
	err := d.Write(context.Background(), w)
	if err != nil {
		t.Fatal(err)
	}

	if atomic.LoadInt32(&called) != int32(failures+1) {
		t.Errorf("expected writer to be called %d times, got %d", failures+1, called)
	}
}

func TestDispatcher_ContextCancel(t *testing.T) {
	var called int32
	w := func(ctx context.Context) error {
		atomic.AddInt32(&called, 1)
		time.Sleep(50 * time.Millisecond)
		return errors.New("fail")
	}

	d := NewDispatcher()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err := d.Write(ctx, w)
	if err == nil {
		t.Fatal("expected context cancellation error")
	}

	if atomic.LoadInt32(&called) == 0 {
		t.Errorf("expected at least one call to writer")
	}
}
