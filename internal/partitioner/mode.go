package partitioner

type Mode string

const (
	randomMode     Mode = "random"
	roundRobinMode      = "round_robin"
	keyMode             = "key"

	defaultMode = roundRobinMode
)
