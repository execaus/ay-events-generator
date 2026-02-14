package publisher

import (
	"context"
	"sync"
	"sync/atomic"

	"go.uber.org/zap"
)

type Publisher[T any] struct {
	write           WriteFn[T]
	asyncMessagesCh chan AsyncMessage[T]
	workersFinished chan struct{}
	closeCh         chan struct{}
	closed          atomic.Bool
}

// NewPublisher создаёт новый Publisher.
// Инициализирует каналы, запускает указанное количество воркеров
// и горутину, отслеживающую их завершение.
func NewPublisher[T any](context context.Context, write WriteFn[T], workerCount int, bufferAsyncMessageSize int) *Publisher[T] {
	s := &Publisher[T]{
		write:           write,
		asyncMessagesCh: make(chan AsyncMessage[T], bufferAsyncMessageSize),
		workersFinished: make(chan struct{}),
		closeCh:         make(chan struct{}),
	}

	wg := &sync.WaitGroup{}
	wg.Add(workerCount)
	for range workerCount {
		go s.worker(context, wg)
	}

	go func() {
		wg.Wait()
		close(s.workersFinished)
	}()

	return s
}

// SendSync отправляет сообщение синхронно.
// Блокируется до завершения операции записи.
// Возвращает ошибку, если Publisher закрыт или запись завершилась неуспешно.
func (w *Publisher[T]) SendSync(ctx context.Context, message T) error {
	if w.closed.Load() {
		return ErrClosed
	}

	err := w.write(ctx, message, nil)
	if err != nil {
		zap.L().Error(err.Error())
		return err
	}

	return nil
}

// SendAsync отправляет сообщение асинхронно.
// Сообщение помещается в очередь и обрабатывается воркером.
// Callback (если задан) будет вызван после попытки записи.
// Возвращает ошибку, если Publisher закрыт.
func (w *Publisher[T]) SendAsync(ctx context.Context, message T, callback AsyncCallback[T]) error {
	if w.closed.Load() {
		return ErrClosed
	}

	w.asyncMessagesCh <- AsyncMessage[T]{
		Ctx:      ctx,
		Message:  message,
		Callback: callback,
	}

	return nil
}

// Close корректно завершает работу Publisher.
// Закрывает канал остановки, ожидает завершения всех воркеров.
// Повторный вызов возвращает ErrClosed.
func (w *Publisher[T]) Close() error {
	if w.closed.Swap(true) {
		return ErrClosed
	}

	close(w.closeCh)
	<-w.workersFinished

	return nil
}

// worker — рабочая горутина, обрабатывающая асинхронные сообщения.
// Завершается при отмене контекста или при закрытии Publisher.
func (w *Publisher[T]) worker(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	var err error

	for {
		select {
		case <-ctx.Done():
			return
		case <-w.closeCh:
			return
		case m := <-w.asyncMessagesCh:
			err = w.write(m.Ctx, m.Message, m.Callback)
			if err != nil {
				zap.L().Error(err.Error())

				if m.Callback == nil {
					continue
				}

				m.Callback(ctx, m.Message, err)
			}
		}
	}
}
