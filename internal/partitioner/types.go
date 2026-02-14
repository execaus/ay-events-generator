package partitioner

import "context"

type Callback[T any] = func(ctx context.Context, message T, err error)

type WritePartitionFn[T any] = func(ctx context.Context, partition int, message T, callback Callback[T]) error
