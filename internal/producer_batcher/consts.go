package producer_batcher

import (
	"time"
)

const (
	defaultFlushTime           = 2 * time.Second
	defaultFlushSize           = 300
	defaultMode      BatchMode = SizeMode
	bufferMax                  = 8192
)
