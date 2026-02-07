package dispatcher

import (
	"errors"
	"time"
)

const (
	backoffMultiply     = 1.2
	startBackoffTimeout = 1 * time.Second
	backoffAttemptCount = 5
)

var (
	ErrBackoffTimeout = errors.New("backoff timeout")
)
