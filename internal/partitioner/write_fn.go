package partitioner

import "context"

type WritePartitionFn[T any] = func(ctx context.Context, partition int, message T) error
