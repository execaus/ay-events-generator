package consumer

type DLQMessage[T any] struct {
	Message T
	Err     error
}
