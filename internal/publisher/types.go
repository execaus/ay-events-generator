package publisher

import "context"

type Callback[T any] = func(ctx context.Context, message T, err error)
type WriteFn[T any] = func(ctx context.Context, message T, callback Callback[T]) error
