package producer_batcher

type BatchMode string

const (
	TimeMode BatchMode = "time"
	SizeMode           = "size"
)
