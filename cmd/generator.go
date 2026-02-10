package main

import (
	"ay-events-generator/internal/event"
	"ay-events-generator/internal/generator"
	"ay-events-generator/internal/publisher"
	"context"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

func init() {
	zap.ReplaceGlobals(zap.Must(zap.NewProduction()))
}

const (
	publisherWorkerCount            = 8
	publisherBufferAsyncMessageSize = 4096

	kafkaAddr  = "localhost:9092"
	kafkaTopic = "events"
)

func main() {
	ctx := context.Background()

	gen := generator.NewEventGenerator()
	defer gen.Close()

	kafkaWriter := &kafka.Writer{
		Addr:  kafka.TCP(kafkaAddr),
		Topic: kafkaTopic,
	}
	defer func() {
		if err := kafkaWriter.Close(); err != nil {
			zap.L().Error(err.Error())
		}
	}()

	pub := publisher.NewPublisher[event.PageViewEvent](
		ctx,
		getKafkaWriteFn(kafkaWriter),
		publisherWorkerCount,
		publisherBufferAsyncMessageSize,
	)
	defer func() {
		if err := pub.Close(); err != nil {
			zap.L().Error(err.Error())
		}
	}()

	for ev := range gen.Events() {
		if err := pub.SendAsync(ctx, ev.Event, func(ctx context.Context, message event.PageViewEvent, err error) {
			zap.L().Info(
				"event sent",
				zap.String("user_id", message.UserID),
				zap.Bool("success", err == nil),
			)
		}); err != nil {
			zap.L().Error(err.Error())
		}
	}
}

func getKafkaWriteFn(writer *kafka.Writer) publisher.WriteFn[event.PageViewEvent] {
	return func(ctx context.Context, message event.PageViewEvent) error {
		b, err := message.Bytes()
		if err != nil {
			zap.L().Error(err.Error())
			return err
		}

		err = writer.WriteMessages(ctx,
			kafka.Message{
				Key:   []byte(message.UserID),
				Value: b,
			},
		)
		if err != nil {
			zap.L().Error(err.Error())
			return err
		}

		return nil
	}
}
