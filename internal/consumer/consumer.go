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
	validMessageFn ValidMessageFn[T]
	readCh         chan T
	mode           Mode
	buffer         []T
	batchSize      atomic.Int32
	flushFn        FlushFn[T]
	tickerPeriod   atomic.Value
	dlq            chan DLQMessage[T]
	closeCh        chan struct{}
	closedWg       sync.WaitGroup
	closed         atomic.Bool
}

// NewConsumer создает новый Consumer и сразу запускает обработку сообщений
// в соответствии с текущим режимом работы.
func NewConsumer[T any](ctx context.Context, validMessageFn ValidMessageFn[T], flushFn FlushFn[T]) *Consumer[T] {
	c := &Consumer[T]{
		validMessageFn: validMessageFn,
		readCh:         make(chan T),
		buffer:         make([]T, 0, bufferSize),
		flushFn:        flushFn,
		dlq:            make(chan DLQMessage[T], dlqBufferSize),
	}

	c.closed.Store(true)
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
func (c *Consumer[T]) In(ctx context.Context) chan<- T {
	in := make(chan T)

	c.closedWg.Add(1)
	go func() {
		defer c.closedWg.Done()

		var err error

		for {
			select {
			case <-c.closeCh:
				return
			case <-ctx.Done():
				return
			case v := <-in:
				err = c.validMessageFn(v)
				if err != nil {
					select {
					case c.dlq <- DLQMessage[T]{
						Message: v,
						Err:     err,
					}:
					default:
						zap.L().Error("dlq is full, dropping message")
					}

					continue
				}

				c.readCh <- v
			}
		}
	}()

	return in
}

func (c *Consumer[T]) DLQ() <-chan DLQMessage[T] {
	return c.dlq
}

// batchProcess накапливает сообщения и вызывает flush
// только при достижении batchSize.
func (c *Consumer[T]) batchProcess(ctx context.Context) {
	c.closedWg.Add(1)

	go func() {
		defer c.closedWg.Done()

		for {
			select {
			case <-ctx.Done():
				return
			case <-c.closeCh:
				return
			case v := <-c.readCh:
				c.buffer = append(c.buffer, v)

				if int(c.batchSize.Load()) <= len(c.buffer) {
					c.flush(ctx)
				}
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
