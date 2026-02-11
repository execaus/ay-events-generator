package producer_batcher

import (
	"time"
)

const (
	defaultFlushTime           = 2 * time.Second
	defaultFlushSize           = 30
	defaultMode      BatchMode = SizeMode
	bufferSize                 = 8192
)
