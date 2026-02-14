package context_merge

import (
	"context"
	"sync"
)

// Merge объединяет несколько context.Context в один.
// Возвращаемый контекст будет отменён при отмене любого из переданных контекстов.
// Также возвращается cancel-функция для ручной отмены результирующего контекста.
func Merge(ctxs ...context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())

	doneChannels := make([]<-chan struct{}, len(ctxs)+1)
	for _, c := range ctxs {
		doneChannels = append(doneChannels, c.Done())
	}
	doneChannels = append(doneChannels, ctx.Done())

	doneCh := fanIn[struct{}](doneChannels...)

	go func() {
		<-doneCh
		cancel()
	}()

	return ctx, cancel
}

// fanIn ожидает срабатывания любого из переданных каналов.
// При первом получении сигнала (закрытии или получении значения)
// закрывает результирующий канал result.
// Остальные горутины завершаются после закрытия result.
func fanIn[T any](chs ...<-chan T) chan T {
	var once sync.Once

	result := make(chan T)

	for _, ch := range chs {
		go func(channel <-chan T) {
			select {
			case <-result:
				return
			case <-channel:
				once.Do(func() {
					close(result)
				})
				return
			}
		}(ch)
	}

	return result
}
