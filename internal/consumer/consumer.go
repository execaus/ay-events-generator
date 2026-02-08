package consumer

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// Consumer отвечает за накопление входящих сообщений и их периодический flush
// в зависимости от выбранного режима работы.
type Consumer[T any] struct {
	validMessageFn ValidMessageFn
	readCh         chan []byte
	mode           Mode
	buffer         [][]byte
	batchSize      atomic.Int32
	flushFn        FlushFn
	tickerPeriod   atomic.Value
	closeCh        chan struct{}
	closedWg       sync.WaitGroup
	closed         atomic.Bool
}

// NewConsumer создает новый Consumer и сразу запускает обработку сообщений
// в соответствии с текущим режимом работы.
func NewConsumer[T any](ctx context.Context, validMessageFn ValidMessageFn, flushFn FlushFn) *Consumer[T] {
	c := &Consumer[T]{
		validMessageFn: validMessageFn,
		readCh:         make(chan []byte),
		buffer:         make([][]byte, 0, bufferSize),
		flushFn:        flushFn,
	}

	c.batchSize.Store(defaultBatchSize)
	c.tickerPeriod.Store(defaultPeriodTime)

	c.start(ctx)

	return c
}

// SetMode изменяет режим работы Consumer (Batch / Time / Hybrid).
// Перед переключением останавливает текущие горутины.
func (c *Consumer[T]) SetMode(ctx context.Context, mode Mode) error {
	if err := c.Close(); err != nil {
		zap.L().Error(err.Error())
		return err
	}

	c.mode = mode
	c.start(ctx)

	return nil
}

// SetBatchSize задает максимальный размер батча.
// Возвращает ошибку, если значение выходит за допустимые границы.
func (c *Consumer[T]) SetBatchSize(size int32) error {
	if size < minBatchSize || size > maxBatchSize {
		return ErrInvalidBatchSize
	}

	c.batchSize.Store(size)

	return nil
}

// SetTickerPeriod задает период срабатывания таймера
// для Time и Hybrid режимов.
func (c *Consumer[T]) SetTickerPeriod(period time.Duration) {
	c.tickerPeriod.Store(period)
}

// In возвращает входной канал для отправки сообщений в Consumer.
// Запускает проксирующую горутину, которая пересылает данные во внутренний readCh
// и завершается при закрытии Consumer или контекста.
func (c *Consumer[T]) In(ctx context.Context) <-chan []byte {
	in := make(chan []byte)

	c.closedWg.Add(1)
	go func() {
		defer c.closedWg.Done()
		for v := range in {
			select {
			case <-c.closeCh:
				return
			case <-ctx.Done():
				return
			case c.readCh <- v:
			}
		}
	}()

	return in
}

// batchProcess накапливает сообщения и вызывает flush
// только при достижении batchSize.
func (c *Consumer[T]) batchProcess(ctx context.Context) {
	c.closedWg.Add(1)

	go func() {
		defer c.closedWg.Done()
		for {
			select {
			case v := <-c.readCh:
				c.buffer = append(c.buffer, v)

				if int(c.batchSize.Load()) <= len(c.buffer) {
					c.flush(ctx)
				}
			case <-c.closeCh:
				return
			}
		}
	}()
}

// timeProcess накапливает сообщения и вызывает flush
// по таймеру, независимо от размера буфера.
func (c *Consumer[T]) timeProcess(ctx context.Context) {
	c.closedWg.Add(1)

	go func() {
		defer c.closedWg.Done()

		ticker := time.NewTicker(c.tickerPeriod.Load().(time.Duration))
		defer ticker.Stop()

		for {
			select {
			case <-c.closeCh:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.flush(ctx)
			case v := <-c.readCh:
				c.buffer = append(c.buffer, v)
			}
		}
	}()
}

// hybridProcess комбинирует batch и time подходы.
// Flush вызывается либо по таймеру, либо при достижении batchSize.
func (c *Consumer[T]) hybridProcess(ctx context.Context) {
	c.closedWg.Add(1)

	go func() {
		defer c.closedWg.Done()

		ticker := time.NewTicker(c.tickerPeriod.Load().(time.Duration))
		defer ticker.Stop()

		for {
			select {
			case <-c.closeCh:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.flush(ctx)
			case v := <-c.readCh:
				c.buffer = append(c.buffer, v)
				if int(c.batchSize.Load()) <= len(c.buffer) {
					ticker.Reset(c.tickerPeriod.Load().(time.Duration))
					c.flush(ctx)
				}
			}
		}
	}()
}

// flush отправляет накопленные сообщения в flushFn.
// Буфер копируется, очищается и передается в flush асинхронно.
func (c *Consumer[T]) flush(ctx context.Context) {
	if len(c.buffer) == 0 {
		return
	}

	buf := slices.Clone(c.buffer[:])
	c.buffer = c.buffer[:0]

	go func(ctx context.Context) {
		if err := c.flushFn(ctx, buf); err != nil {
			zap.L().Error(err.Error())
		}
	}(ctx)
}

// start запускает обработку сообщений
// в зависимости от текущего режима Consumer.
func (c *Consumer[T]) start(ctx context.Context) {
	if !c.closed.Swap(false) {
		return
	}

	c.closeCh = make(chan struct{})

	switch c.mode {
	case BatchMode:
		c.batchProcess(ctx)
	case TimeMode:
		c.timeProcess(ctx)
	case HybridMode:
		c.hybridProcess(ctx)
	}
}

// Close сигнализирует всем внутренним горутинам о завершении
// и дожидается их корректной остановки.
func (c *Consumer[T]) Close() error {
	if c.closed.Swap(true) {
		return nil
	}

	close(c.closeCh)
	c.closedWg.Wait()
	return nil
}
