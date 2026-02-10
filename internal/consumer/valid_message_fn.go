package consumer

type ValidMessageFn[T any] = func(data T) error
