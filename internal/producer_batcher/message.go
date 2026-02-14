package producer_batcher

import "context"

type Message[T any] struct {
	Ctx      context.Context
	Data     T
	Callback Callback[T]
}
