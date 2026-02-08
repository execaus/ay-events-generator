package consumer

type ValidMessageFn = func(data []byte) error
