package consumer

import "time"

const (
	bufferSize        = 131072
	minBatchSize      = 1
	maxBatchSize      = 10_000
	defaultBatchSize  = minBatchSize
	defaultPeriodTime = 5 * time.Second
)
