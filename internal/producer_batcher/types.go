package producer_batcher

import "context"

type Callback[T any] = func(ctx context.Context, message T, err error)

type Flush[T any] = func(messages []Message[T])
