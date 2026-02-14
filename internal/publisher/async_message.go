package publisher

import (
	"context"
)

type AsyncCallback[T any] = func(ctx context.Context, message T, err error)

type AsyncMessage[T any] struct {
	Ctx      context.Context
	Message  T
	Callback AsyncCallback[T]
}
