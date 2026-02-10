package publisher

import "context"

type WriteFn[T any] = func(ctx context.Context, message T) error
