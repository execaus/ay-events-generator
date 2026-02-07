package producer_batcher

import (
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

type Batcher[T any] struct {
	mode          BatchMode     // Режим батчинга
	flushTime     time.Duration // Время для TimeMode
	flushSize     uint          // Размер батча для SizeMode
	flushFn       Flush[T]      // Функция для отправки батча
	buffer        [bufferMax]T  // Внутренний буфер
	bufferPointer uint          // Индекс следующей записи в буфер
	mutex         sync.Mutex    // Защита буфера
	stopCh        chan struct{} // Канал остановки таймера
	stoppedCh     chan struct{} // Канал уведомления о завершении таймера
	stopped       atomic.Bool   // Флаг остановки батчера
}

// NewBatcher создает новый батчер с заданной функцией flushFn.
// Возвращает ошибку, если flushFn не задан.
func NewBatcher[T any](flushFn Flush[T]) (*Batcher[T], error) {
	if flushFn == nil {
		return nil, errors.New("flush function not found")
	}

	b := &Batcher[T]{
		mode:      defaultMode,
		flushTime: defaultFlushTime,
		flushSize: defaultFlushSize,
		flushFn:   flushFn,
		buffer:    [bufferMax]T{},
		stopCh:    make(chan struct{}),
		stoppedCh: make(chan struct{}),
	}

	b.start()

	return b, nil
}

// SetFlushTime устанавливает интервал времени для TimeMode.
func (b *Batcher[T]) SetFlushTime(duration time.Duration) {
	b.flushTime = duration
}

// SetFlushSize устанавливает размер батча для SizeMode.
func (b *Batcher[T]) SetFlushSize(size uint) {
	b.flushSize = size
}

// SetMode изменяет режим батчинга и перезапускает процесс, если режим меняется.
func (b *Batcher[T]) SetMode(mode BatchMode) {
	if b.mode == mode {
		return
	}
	b.mode = mode
	b.restart()
}

// Push добавляет сообщение в буфер.
// В SizeMode при достижении flushSize вызывается flushFn асинхронно.
// Если батчер остановлен, Push логирует ошибку и игнорирует сообщение.
func (b *Batcher[T]) Push(message T) {
	if b.stopped.Load() {
		zap.L().Error("batcher is stopped")
		return
	}

	b.mutex.Lock()

	b.buffer[b.bufferPointer] = message
	if b.bufferPointer < bufferMax-1 {
		b.bufferPointer++
	}

	var messages []T
	var flushed bool

	if b.mode == SizeMode && b.bufferPointer >= b.flushSize {
		messages = b.flushBuffer()
		flushed = true
	}

	b.mutex.Unlock()

	if flushed {
		go b.flushFn(messages)
	}
}

// start запускает таймерную горутину для TimeMode и сбрасывает флаг stopped.
func (b *Batcher[T]) start() {
	b.stopped.Swap(false)
	if b.mode == TimeMode {
		go b.timeModeProcess()
	}
}

// restart закрывает текущий батчер и запускает его заново.
func (b *Batcher[T]) restart() {
	b.Close()
	b.start()
}

// timeModeProcess — основной цикл таймера для TimeMode.
// Каждые flushTime отправляет накопленные сообщения.
// Обрабатывает остановку через stopCh.
func (b *Batcher[T]) timeModeProcess() {
	t := time.NewTimer(b.flushTime)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			b.mutex.Lock()
			messages := b.flushBuffer()
			b.mutex.Unlock()

			go b.flushFn(messages)

			t.Reset(b.flushTime)
		case <-b.stopCh:
			// Отправляем остаток сообщений при остановке
			messages := b.flushBuffer()
			b.flushFn(messages)
			b.stoppedCh <- struct{}{}
			return
		}
	}
}

// flushBuffer копирует содержимое буфера и сбрасывает указатель.
func (b *Batcher[T]) flushBuffer() []T {
	messages := slices.Clone(b.buffer[:b.bufferPointer])
	b.bufferPointer = 0
	return messages
}

// Close останавливает батчер.
// Для TimeMode останавливает таймер и отправляет остаток сообщений.
// Для SizeMode сразу отправляет остаток сообщений.
// Повторные вызовы игнорируются.
func (b *Batcher[T]) Close() {
	if b.stopped.Swap(true) {
		return
	}
	switch b.mode {
	case TimeMode:
		b.stopCh <- struct{}{}
		<-b.stoppedCh
	case SizeMode:
		messages := b.flushBuffer()
		b.flushFn(messages)
	default:
		zap.L().Error("invalid mode")
	}
}
