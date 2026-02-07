package producer_batcher

type Flush[T any] = func(messages []T)
