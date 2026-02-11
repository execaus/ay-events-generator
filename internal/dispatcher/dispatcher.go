package dispatcher

import (
	"context"
	"time"

	"go.uber.org/zap"
)

type Dispatcher struct{}

// NewDispatcher создает и возвращает новый экземпляр Dispatcher.
// Используется для инициализации диспетчера без дополнительной конфигурации.
func NewDispatcher() *Dispatcher {
	return &Dispatcher{}
}

// Write выполняет запись с использованием механизма повторных попыток (backoff).
// Принимает контекст для управления отменой и функцию записи writeFn.
func (d *Dispatcher) Write(ctx context.Context, writeFn WriteFn) error {
	return d.writeWithBackoff(ctx, writeFn)
}

// writeWithBackoff реализует логику повторных попыток записи с экспоненциальным увеличением таймаута.
// При ошибке выполнения singleWrite таймаут увеличивается согласно коэффициенту backoffMultiply.
// Если контекст отменен — возвращается ошибка контекста.
// Если превышено количество попыток — возвращается ErrBackoffTimeout.
func (d *Dispatcher) writeWithBackoff(ctx context.Context, writeFn WriteFn) error {
	timeout := startBackoffTimeout

	for range backoffAttemptCount {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if err := d.singleWrite(ctx, timeout, writeFn); err != nil {
				zap.L().Error(err.Error())
				timeout = time.Duration(float64(timeout) * backoffMultiply)
				continue
			}
		}

		return nil
	}

	return ErrBackoffTimeout
}

// singleWrite выполняет одну попытку записи с ограничением по времени.
// Создает дочерний контекст с таймаутом и вызывает переданную функцию writeFn.
// В случае ошибки логирует её и возвращает вызывающему коду.
func (d *Dispatcher) singleWrite(ctx context.Context, timeout time.Duration, writeFn WriteFn) error {
	ctxT, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if err := writeFn(ctxT); err != nil {
		zap.L().Error(err.Error())
		return err
	}

	return nil
}
