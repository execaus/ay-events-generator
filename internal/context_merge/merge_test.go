package context_merge

import (
	"context"
	"testing"
	"time"
)

func TestMerge_CancelWhenAnyContextCanceled(t *testing.T) {
	ctx1, cancel1 := context.WithCancel(context.Background())
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel1()
	defer cancel2()

	merged, _ := Merge(ctx1, ctx2)

	cancel1()

	select {
	case <-merged.Done():
		// expected
	case <-time.After(100 * time.Millisecond):
		t.Fatal("merged context was not canceled when one of contexts was canceled")
	}
}

func TestMerge_NotCanceledIfParentsAlive(t *testing.T) {
	ctx1 := context.Background()
	ctx2 := context.Background()

	merged, cancel := Merge(ctx1, ctx2)
	defer cancel()

	select {
	case <-merged.Done():
		t.Fatal("merged context canceled unexpectedly")
	case <-time.After(50 * time.Millisecond):
		// expected: no cancellation
	}
}

func TestMerge_ManualCancel(t *testing.T) {
	ctx1 := context.Background()

	merged, cancel := Merge(ctx1)
	cancel()

	select {
	case <-merged.Done():
		// expected
	case <-time.After(100 * time.Millisecond):
		t.Fatal("merged context was not canceled by manual cancel")
	}
}

func TestMerge_MultipleContexts(t *testing.T) {
	ctx1, cancel1 := context.WithCancel(context.Background())
	ctx2, cancel2 := context.WithCancel(context.Background())
	ctx3, cancel3 := context.WithCancel(context.Background())
	defer cancel1()
	defer cancel2()
	defer cancel3()

	merged, _ := Merge(ctx1, ctx2, ctx3)

	cancel3()

	select {
	case <-merged.Done():
		// expected
	case <-time.After(100 * time.Millisecond):
		t.Fatal("merged context was not canceled when one of multiple contexts was canceled")
	}
}
