package consumer

import "context"

type FlushFn = func(context.Context, [][]byte) error
