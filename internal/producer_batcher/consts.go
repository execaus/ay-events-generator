package producer_batcher

import (
	"math"
	"time"
)

const (
	defaultFlushTime           = 2 * time.Second
	defaultFlushSize           = 300
	defaultMode      BatchMode = TimeMode
	bufferMax                  = math.MaxUint
)
