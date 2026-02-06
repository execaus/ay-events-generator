package sender

import (
	"ay-events-generator/internal/event"
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

type KafkaSender struct {
	writer          KafkaWriter
	asyncMessagesCh chan AsyncMessage
	workersFinished chan struct{}
	closed          atomic.Bool
}

func NewKafkaSender(context context.Context, cfg KafkaConfig, workerCount int, bufferEventCount int) *KafkaSender {
	s := &KafkaSender{
		writer: &kafka.Writer{
			Addr:  kafka.TCP(cfg.Broker),
			Topic: cfg.Topic,
		},
		asyncMessagesCh: make(chan AsyncMessage, bufferEventCount),
		workersFinished: make(chan struct{}),
	}

	wg := &sync.WaitGroup{}
	wg.Add(workerCount)
	for range workerCount {
		go s.worker(context, wg)
	}

	go func() {
		wg.Wait()
		close(s.workersFinished)
	}()

	return s
}

func (s *KafkaSender) SendSync(ctx context.Context, event event.PageViewEvent) error {
	if s.closed.Load() {
		return ErrSenderClosed
	}

	err := s.write(ctx, event)
	if err != nil {
		zap.L().Error(err.Error())
		return err
	}

	return nil
}

func (s *KafkaSender) SendAsync(ctx context.Context, event event.PageViewEvent, callback AsyncCallback) error {
	if s.closed.Load() {
		return ErrSenderClosed
	}

	s.asyncMessagesCh <- AsyncMessage{
		event:    event,
		callback: callback,
	}

	return nil
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
	if s.closed.Swap(true) {
		return nil
	}

	close(s.asyncMessagesCh)
	<-s.workersFinished
	return s.writer.Close()
}

func (s *KafkaSender) worker(ctx context.Context, wg *sync.WaitGroup) {
	var err error

	for m := range s.asyncMessagesCh {
		err = s.write(ctx, m.event)
		if err != nil {
			zap.L().Error(err.Error())
		}

		if m.callback != nil {
			m.callback(m.event, err)
		}
	}

	wg.Done()
}

func (s *KafkaSender) write(ctx context.Context, ev event.PageViewEvent) error {
	b, err := ev.Bytes()
	if err != nil {
		zap.L().Error(err.Error())
		return err
	}
	if err = s.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(ev.PageID),
		Value: b,
	}); err != nil {
		zap.L().Error(err.Error())
		return err
	}

	return nil
}
