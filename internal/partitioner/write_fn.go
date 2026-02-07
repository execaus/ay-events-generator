package partitioner

import "context"

type WriteFn[T any] = func(ctx context.Context, message T) error
type WritePartitionFn[T any] = func(ctx context.Context, partition int, message T) error
