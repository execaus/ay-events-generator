package consumer

import "context"

type ValidMessageFn[T any] = func(data T) error

type FlushFn[T any] = func(context.Context, []T) error
