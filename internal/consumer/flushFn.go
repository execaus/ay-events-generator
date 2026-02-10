package consumer

import "context"

type FlushFn[T any] = func(context.Context, []T) error
