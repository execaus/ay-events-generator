package sender

import (
	"ay-events-generator/internal/event"
	"context"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

type KafkaSender struct {
	writer KafkaWriter
}

func NewKafkaSender(cfg KafkaConfig) *KafkaSender {
	return &KafkaSender{
		writer: &kafka.Writer{
			Addr:  kafka.TCP(cfg.Broker),
			Topic: cfg.Topic,
		},
	}
}

func (s *KafkaSender) SendSync(ctx context.Context, event event.PageViewEvent) error {
	b, err := event.Bytes()
	if err != nil {
		zap.L().Error(err.Error())
		return err
	}

	err = s.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(event.PageID),
		Value: b,
	})
	if err != nil {
		zap.L().Error(err.Error())
		return err
	}

	return nil
}

func (s *KafkaSender) SendAsync(ctx context.Context, event event.PageViewEvent) error {
	//TODO implement me
	panic("implement me")
}

func (s *KafkaSender) SetBatchTime(duration time.Time) {
	//TODO implement me
	panic("implement me")
}

func (s *KafkaSender) SetBatchEventCount(n uint) {
	//TODO implement me
	panic("implement me")
}

func (s *KafkaSender) SetPartitionStrategy(strategy PartitionStrategy) {
	//TODO implement me
	panic("implement me")
}

func (s *KafkaSender) Close() error {
	return s.writer.Close()
}
