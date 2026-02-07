package partitioner

type Config[T any] struct {
	mode  Mode
	count int
	keyFn func(T) string
	rr    *RRCircle
}
