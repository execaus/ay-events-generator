package dispatcher

import "context"

type WriteFn = func(ctx context.Context) error
