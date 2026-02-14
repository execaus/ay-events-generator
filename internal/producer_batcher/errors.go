package producer_batcher

import "errors"

var (
	ErrBatchStopped = errors.New("batch is stopped")
)
