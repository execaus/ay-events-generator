package sender

import (
	"ay-events-generator/internal/event"
	"context"
	"time"
)

type PartitionStrategy string

const (
	PartitionByPage PartitionStrategy = "page_id"
	RoundRobin                        = "round-robin"
	Random                            = "random"
)

type Sender interface {
	SendSync(context context.Context, event event.PageViewEvent) error
	SendAsync(context context.Context, event event.PageViewEvent) error
	SetBatchTime(duration time.Time)
	SetBatchEventCount(n uint)
	SetPartitionStrategy(strategy PartitionStrategy)
}
