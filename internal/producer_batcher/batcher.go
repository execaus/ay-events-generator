package producer_batcher

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

type Batcher[T any] struct {
	mode      BatchMode
	flushTime time.Duration
	flushSize uint
	flushFn   Flush[T]

	buffer []Message[T]
	mutex  sync.Mutex

	stopCh  chan struct{}
	wg      sync.WaitGroup
	stopped atomic.Bool
}

// NewBatcher создает новый батчер с функцией flushFn.
func NewBatcher[T any](flushFn Flush[T]) (*Batcher[T], error) {
	if flushFn == nil {
		return nil, errors.New("flush function not found")
	}

	b := &Batcher[T]{
		mode:      defaultMode,
		flushTime: defaultFlushTime,
		flushSize: defaultFlushSize,
		flushFn:   flushFn,
		buffer:    make([]Message[T], 0, bufferSize),
		stopCh:    make(chan struct{}),
	}

	b.start()
	return b, nil
}

// SetFlushTime устанавливает интервал для TimeMode.
func (b *Batcher[T]) SetFlushTime(duration time.Duration) {
	b.flushTime = duration
}

// SetFlushSize устанавливает размер батча для SizeMode.
func (b *Batcher[T]) SetFlushSize(size uint) {
	b.flushSize = size
}

// SetMode меняет режим батчинга и перезапускает таймер, если нужно.
func (b *Batcher[T]) SetMode(mode BatchMode) {
	if b.mode == mode {
		return
	}
	b.mode = mode
	b.restart()
}

// Push добавляет сообщение в батчер.
func (b *Batcher[T]) Push(ctx context.Context, message T) error {
	if b.stopped.Load() {
		zap.L().Error(ErrBatchStopped.Error())
		return ErrBatchStopped
	}

	b.mutex.Lock()
	b.buffer = append(b.buffer, Message[T]{
		Ctx:  ctx,
		Data: message,
	})

	var messages []Message[T]
	var flushed bool
	if b.mode == SizeMode && len(b.buffer) >= int(b.flushSize) {
		messages = b.flushBuffer()
		flushed = true
	}
	b.mutex.Unlock()

	if flushed {
		go b.flushFn(messages)
	}

	return nil
}

// start запускает таймерную горутину для TimeMode.
func (b *Batcher[T]) start() {
	b.stopped.Swap(false)
	if b.mode == TimeMode {
		b.wg.Add(1)
		go b.timeModeProcess()
	}
}

// restart перезапускает батчер.
func (b *Batcher[T]) restart() {
	b.Close()
	b.start()
}

// timeModeProcess — цикл таймера для TimeMode.
func (b *Batcher[T]) timeModeProcess() {
	defer b.wg.Done()
	ticker := time.NewTicker(b.flushTime)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			b.mutex.Lock()
			messages := b.flushBuffer()
			b.mutex.Unlock()
			if len(messages) > 0 {
				go b.flushFn(messages)
			}
		case <-b.stopCh:
			b.mutex.Lock()
			messages := b.flushBuffer()
			b.mutex.Unlock()
			if len(messages) > 0 {
				go b.flushFn(messages)
			}
			return
		}
	}
}

// flushBuffer копирует и очищает буфер.
func (b *Batcher[T]) flushBuffer() []Message[T] {
	messages := make([]Message[T], len(b.buffer))
	copy(messages, b.buffer)
	b.buffer = b.buffer[:0]
	return messages
}

// Close останавливает батчер и сбрасывает буфер.
func (b *Batcher[T]) Close() {
	if b.stopped.Swap(true) {
		return
	}

	if b.mode == TimeMode {
		close(b.stopCh)
		b.wg.Wait()
	} else if b.mode == SizeMode {
		b.mutex.Lock()
		messages := b.flushBuffer()
		b.mutex.Unlock()
		if len(messages) > 0 {
			b.flushFn(messages)
		}
	}
}
