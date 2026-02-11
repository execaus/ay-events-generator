package main

import (
	"ay-events-generator/internal/dispatcher"
	"ay-events-generator/internal/event"
	"ay-events-generator/internal/generator"
	"ay-events-generator/internal/partitioner"
	"ay-events-generator/internal/producer_batcher"
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

	kafkaAddr  = "localhost:9093"
	kafkaTopic = "events"

	kafkaPartitionCount = 5
)

/*
Должен ли идти один контекст от генерации события до его отправки в брокер?
Обработка после batch сбора, приходит массив message, партиционирование после сложное, если на основе key.
Делать какой-то буфер для каждой отдельной партиции, то есть сначала select partition
и только потом batch?
*/

func main() {
	ctx := context.Background()

	gen := generator.NewEventGenerator()
	defer gen.Close()

	var kafkaPartitionConnections []*kafka.Conn
	for i := range kafkaPartitionCount {
		conn, err := kafka.DialLeader(ctx, "tcp", kafkaAddr, kafkaTopic, i)
		if err != nil {
			zap.L().Fatal(err.Error())
		}
		kafkaPartitionConnections = append(kafkaPartitionConnections, conn)
	}
	defer func() {
		for _, conn := range kafkaPartitionConnections {
			if err := conn.Close(); err != nil {
				zap.L().Error(err.Error())
			}
		}
	}()

	disp := dispatcher.NewDispatcher()

	part := partitioner.NewPartitioner[event.PageViewEvent](func(ctx context.Context, partition int, message event.PageViewEvent) error {
		b, err := message.Bytes()
		if err != nil {
			zap.L().Error(err.Error())
			return err
		}
		_, err = kafkaPartitionConnections[partition].WriteMessages(kafka.Message{
			Key:   []byte(message.UserID),
			Value: b,
		})
		if err != nil {
			zap.L().Error(err.Error())
			return err
		}

		return nil
	})
	if err := part.SetRoundRobinMode(kafkaPartitionCount); err != nil {
		zap.L().Fatal(err.Error())
	}

	bat, err := producer_batcher.NewBatcher[event.PageViewEvent](func(messages []event.PageViewEvent) {
		// TODO отправить пачкой
		for _, m := range messages {
			write, err := part.WriteFn(m)
			if err != nil {
				zap.L().Error(err.Error())
				continue
			}
			if err = disp.Write(ctx, func(ctx context.Context) error {
				if err := write(ctx, m); err != nil {
					zap.L().Error(err.Error())
					return err
				}

				return nil
			}); err != nil {
				zap.L().Error(err.Error())
				continue
			}
		}
	})
	if err != nil {
		zap.L().Fatal(err.Error())
	}

	pub := publisher.NewPublisher[event.PageViewEvent](
		ctx,
		func(ctx context.Context, message event.PageViewEvent) error {
			bat.Push(message)
			return nil
		},
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
