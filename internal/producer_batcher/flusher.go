package producer_batcher

type Flush[T any] = func(messages []Message[T])
