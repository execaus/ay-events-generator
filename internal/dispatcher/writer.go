package dispatcher

import (
	"context"
	"io"
)

type Writer[T any] interface {
	Write(ctx context.Context, data T) error
	io.Closer
}
