package partitioner

import "errors"

var (
	ErrInvalidKey   = errors.New("invalid key")
	ErrInvalidCount = errors.New("invalid count")
	ErrInvalidMode  = errors.New("invalid mode")
)
