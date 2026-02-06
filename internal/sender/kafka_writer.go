package sender

import (
	"context"

	"github.com/segmentio/kafka-go"
)

//go:generate mockgen -source=kafka_writer.go -destination=mock/mock_kafka_writer.go -package=mock_sender
type KafkaWriter interface {
	WriteMessages(ctx context.Context, messages ...kafka.Message) error
	Close() error
}
