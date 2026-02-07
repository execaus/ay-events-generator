package dispatcher

import (
	"context"
	"time"

	"go.uber.org/zap"
)

type Dispatcher[T any] struct {
	writer Writer[T]
}

func NewDispatcher[T any](writer Writer[T]) *Dispatcher[T] {
	return &Dispatcher[T]{writer: writer}
}

func (d *Dispatcher[T]) Write(ctx context.Context, data T) error {
	return d.writeWithBackoff(ctx, data)
}

func (d *Dispatcher[T]) writeWithBackoff(ctx context.Context, data T) error {
	timeout := startBackoffTimeout

	for range backoffAttemptCount {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if err := d.singleWrite(ctx, timeout, data); err != nil {
				zap.L().Error(err.Error())
				timeout = time.Duration(float64(timeout) * backoffMultiply)
				continue
			}
		}

		return nil
	}

	return ErrBackoffTimeout
}

func (d *Dispatcher[T]) singleWrite(ctx context.Context, timeout time.Duration, data T) error {
	ctxT, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if err := d.writer.Write(ctxT, data); err != nil {
		zap.L().Error(err.Error())
		return err
	}

	return nil
}
