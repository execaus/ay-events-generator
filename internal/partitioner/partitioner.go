package partitioner

import (
	"context"
	"hash/fnv"
	"math/rand"
	"sync/atomic"

	"go.uber.org/zap"
)

// Partitioner отвечает за выбор партиции для сообщения
// в соответствии с текущей стратегией распределения
// (round-robin, по ключу или случайно).
type Partitioner[T any] struct {
	writePartitionFn WritePartitionFn[T]
	config           atomic.Value
}

// NewPartitioner создаёт новый Partitioner с конфигурацией по умолчанию.
// По умолчанию используется одна партиция и стандартный режим распределения.
func NewPartitioner[T any](writeFn WritePartitionFn[T]) *Partitioner[T] {
	p := &Partitioner[T]{
		writePartitionFn: writeFn,
	}

	p.config.Store(&Config[T]{
		mode:  defaultMode,
		count: 1,
		rr:    NewRRCircle(1),
	})

	return p
}

// WriteFn выбирает партицию в соответствии с текущей конфигурацией
// и передает сообщение в ранее переданную функцию для отправки в партицию.
func (p *Partitioner[T]) WriteFn(ctx context.Context, message T, callback Callback[T]) error {
	config := p.config.Load().(*Config[T])

	switch config.mode {
	case roundRobinMode:
		index := config.rr.Load()
		return p.writePartitionFn(ctx, index, message, callback)

	case keyMode:
		key := config.keyFn(message)
		index := p.hashToRange(key, config.count)
		return p.writePartitionFn(ctx, index, message, callback)

	case randomMode:
		index := rand.Intn(config.count)
		return p.writePartitionFn(ctx, index, message, callback)

	default:
		zap.L().Error("invalid mode")
	}

	return ErrInvalidMode
}

// SetRandomMode переключает Partitioner в случайный режим.
// Каждое сообщение направляется в случайную партицию
// в диапазоне [0, count).
// Обновление конфигурации происходит атомарно и потокобезопасно.
func (p *Partitioner[T]) SetRandomMode(count int) error {
	if count <= 0 {
		return ErrInvalidCount
	}

	p.config.Store(&Config[T]{
		mode:  randomMode,
		count: count,
	})

	return nil
}

// SetRoundRobinMode переключает Partitioner в режим round-robin.
// Партиции выбираются последовательно по кругу.
// Обновление конфигурации происходит атомарно и потокобезопасно.
func (p *Partitioner[T]) SetRoundRobinMode(count int) error {
	if count <= 0 {
		return ErrInvalidCount
	}

	p.config.Store(&Config[T]{
		mode:  roundRobinMode,
		count: count,
		rr:    NewRRCircle(count),
	})

	return nil
}

// SetKeyMode переключает Partitioner в режим распределения по ключу.
// Переданная функция keyFn извлекает ключ из сообщения;
// сообщения с одинаковым ключом всегда попадают в одну и ту же партицию.
func (p *Partitioner[T]) SetKeyMode(keyFn func(m T) string, count int) error {
	if count <= 0 {
		return ErrInvalidCount
	}
	if keyFn == nil {
		return ErrInvalidKey
	}

	p.config.Store(&Config[T]{
		mode:  keyMode,
		count: count,
		keyFn: keyFn,
	})

	return nil
}

// hashToRange хэширует строку с помощью FNV-1a
// и отображает результат в диапазон [0, n).
func (p *Partitioner[T]) hashToRange(s string, n int) int {
	h := fnv.New32a()
	_, err := h.Write([]byte(s))
	if err != nil {
		zap.L().Error(err.Error())
		return 0
	}
	return int(h.Sum32() % uint32(n))
}
