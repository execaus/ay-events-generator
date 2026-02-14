package partitioner

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

// recordingWriter возвращает WritePartitionFn, который записывает
// использованные индексы партиций для последующей проверки в тестах.
func recordingWriter[T any](out *[]int, mu *sync.Mutex) WritePartitionFn[T] {
	return func(ctx context.Context, partition int, message T, callback Callback[T]) error {
		mu.Lock()
		*out = append(*out, partition)
		mu.Unlock()
		return nil
	}
}

func TestPartitioner_RandomMode_Range(t *testing.T) {
	var (
		mu  sync.Mutex
		got []int
		cnt = 5
	)

	p := NewPartitioner[int](recordingWriter[int](&got, &mu))
	err := p.SetRandomMode(cnt)
	assert.NoError(t, err)

	for i := 0; i < 100; i++ {
		err := p.WriteFn(context.Background(), i, nil)
		assert.NoError(t, err)
	}

	for _, idx := range got {
		assert.GreaterOrEqual(t, idx, 0)
		assert.Less(t, idx, cnt)
	}
}

func TestPartitioner_KeyMode_StablePartition(t *testing.T) {
	var (
		mu  sync.Mutex
		got []int
	)

	p := NewPartitioner[string](recordingWriter[string](&got, &mu))
	err := p.SetKeyMode(func(s string) string { return s }, 10)
	assert.NoError(t, err)

	for i := 0; i < 10; i++ {
		err := p.WriteFn(context.Background(), "same-key", nil)
		assert.NoError(t, err)
	}

	assert.NotEmpty(t, got)
	first := got[0]
	for _, idx := range got {
		assert.Equal(t, first, idx, "Ожидалась одна и та же партиция для одного ключа")
	}
}

func TestPartitioner_RoundRobinMode(t *testing.T) {
	var (
		mu  sync.Mutex
		got []int
	)

	p := NewPartitioner[int](recordingWriter[int](&got, &mu))
	err := p.SetRoundRobinMode(3)
	assert.NoError(t, err)

	for i := 0; i < 6; i++ {
		err := p.WriteFn(context.Background(), i, nil)
		assert.NoError(t, err)
	}

	want := []int{0, 1, 2, 0, 1, 2}
	assert.Equal(t, want, got)
}

func TestPartitioner_InvalidArgs(t *testing.T) {
	p := NewPartitioner[int](func(ctx context.Context, partition int, message int, callback Callback[int]) error { return nil })

	assert.Error(t, p.SetRandomMode(0), "Ожидалась ошибка для count <= 0")
	assert.Error(t, p.SetRoundRobinMode(-1), "Ожидалась ошибка для count <= 0")
	assert.Error(t, p.SetKeyMode(nil, 3), "Ожидалась ошибка для nil keyFn")
	assert.Error(t, p.SetKeyMode(func(int) string { return "x" }, 0), "Ожидалась ошибка для count <= 0")
}
