package consumer

type Mode string

const (
	BatchMode  Mode = "batch"
	TimeMode        = "time"
	HybridMode      = "hybrid"
)

const (
	defaultMode = BatchMode
)
