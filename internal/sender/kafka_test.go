package sender

import (
	"ay-events-generator/internal/event"
	mock_sender "ay-events-generator/internal/sender/mock"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

// TestKafkaSender_SendSync проверяет, что метод SendSync
// синхронно сериализует событие и отправляет его через KafkaWriter.
func TestKafkaSender_SendSync(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockWriter := mock_sender.NewMockKafkaWriter(ctrl)

	ev := event.PageViewEvent{
		PageID:       "page_1",
		UserID:       "user_1",
		ViewDuration: 1000,
		Timestamp:    time.Now(),
	}

	b, err := ev.Bytes()
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	mockWriter.EXPECT().
		WriteMessages(gomock.Any(), gomock.AssignableToTypeOf([]kafka.Message{})).
		DoAndReturn(func(ctx context.Context, msgs ...kafka.Message) error {
			assert.Len(t, msgs, 1)
			assert.Equal(t, ev.PageID, string(msgs[0].Key))
			assert.Equal(t, b, msgs[0].Value)
			return nil
		})

	ks := &KafkaSender{writer: mockWriter}
	err = ks.SendSync(t.Context(), ev)
	assert.NoError(t, err)
}

// TestKafkaSender_SendSync_WaitsForWrite проверяет, что SendSync
// блокируется до завершения операции записи в Kafka.
func TestKafkaSender_SendSync_WaitsForWrite(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockWriter := mock_sender.NewMockKafkaWriter(ctrl)

	ev := event.PageViewEvent{
		PageID:       "page_1",
		UserID:       "user_1",
		ViewDuration: 1000,
		Timestamp:    time.Now(),
	}

	called := make(chan struct{})

	mockWriter.EXPECT().
		WriteMessages(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, msgs ...kafka.Message) error {
			time.Sleep(100 * time.Millisecond)
			close(called)
			return nil
		})

	ks := &KafkaSender{writer: mockWriter}

	start := time.Now()
	err := ks.SendSync(t.Context(), ev)
	elapsed := time.Since(start)

	<-called

	assert.NoError(t, err)
	assert.GreaterOrEqual(t, elapsed, 100*time.Millisecond)
}

// TestKafkaSender_SendAsync проверяет, что SendAsync
// добавляет событие в очередь асинхронной отправки и
// вызывает callback после завершения записи в Kafka.
func TestKafkaSender_SendAsync(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockWriter := mock_sender.NewMockKafkaWriter(ctrl)

	ev := event.PageViewEvent{
		PageID:       "page_1",
		UserID:       "user_1",
		ViewDuration: 1000,
		Timestamp:    time.Now(),
	}

	b, err := ev.Bytes()
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	done := make(chan struct{})

	mockWriter.EXPECT().
		WriteMessages(gomock.Any(), gomock.AssignableToTypeOf([]kafka.Message{})).
		DoAndReturn(func(ctx context.Context, msgs ...kafka.Message) error {
			assert.Len(t, msgs, 1)
			assert.Equal(t, ev.PageID, string(msgs[0].Key))
			assert.Equal(t, b, msgs[0].Value)
			return nil
		})

	mockWriter.EXPECT().
		Close().
		Return(nil)

	ks := &KafkaSender{
		writer:          mockWriter,
		asyncMessagesCh: make(chan AsyncMessage, 1),
		workersFinished: make(chan struct{}),
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go ks.worker(t.Context(), wg)

	go func() {
		wg.Wait()
		close(ks.workersFinished)
	}()

	err = ks.SendAsync(t.Context(), ev, func(e event.PageViewEvent, err error) {
		assert.NoError(t, err)
		assert.Equal(t, ev.PageID, e.PageID)
		close(done)
	})

	assert.NoError(t, err)

	select {
	case <-done:
	case <-time.After(time.Second):
		assert.Fail(t, "callback не был вызван")
	}

	assert.NoError(t, ks.Close())
}

// TestKafkaSender_SendAsync_DoesNotWaitForWrite проверяет, что SendAsync
// возвращается сразу и не блокируется на операции записи в Kafka.
func TestKafkaSender_SendAsync_DoesNotWaitForWrite(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockWriter := mock_sender.NewMockKafkaWriter(ctrl)

	ev := event.PageViewEvent{
		PageID:       "page_1",
		UserID:       "user_1",
		ViewDuration: 1000,
		Timestamp:    time.Now(),
	}

	writeStarted := make(chan struct{})
	writeFinished := make(chan struct{})

	mockWriter.EXPECT().
		WriteMessages(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, msgs ...kafka.Message) error {
			close(writeStarted)
			time.Sleep(100 * time.Millisecond)
			close(writeFinished)
			return nil
		})

	mockWriter.EXPECT().
		Close().
		Return(nil)

	ks := &KafkaSender{
		writer:          mockWriter,
		asyncMessagesCh: make(chan AsyncMessage, 1),
		workersFinished: make(chan struct{}),
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go ks.worker(t.Context(), wg)

	go func() {
		wg.Wait()
		close(ks.workersFinished)
	}()

	start := time.Now()
	err := ks.SendAsync(t.Context(), ev, nil)
	elapsed := time.Since(start)

	assert.NoError(t, err)
	assert.Less(t, elapsed, 50*time.Millisecond)

	<-writeStarted
	<-writeFinished

	assert.NoError(t, ks.Close())
}

// TestKafkaSender_SendAsync_CallbackReceivesError проверяет, что
// при ошибке записи в Kafka эта ошибка передаётся в callback.
func TestKafkaSender_SendAsync_CallbackReceivesError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockWriter := mock_sender.NewMockKafkaWriter(ctrl)

	ev := event.PageViewEvent{
		PageID: "page_1",
	}

	expectedErr := errors.New("write failed")
	done := make(chan struct{})

	mockWriter.EXPECT().
		WriteMessages(gomock.Any(), gomock.Any()).
		Return(expectedErr)

	mockWriter.EXPECT().
		Close().
		Return(nil)

	ks := &KafkaSender{
		writer:          mockWriter,
		asyncMessagesCh: make(chan AsyncMessage, 1),
		workersFinished: make(chan struct{}),
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go ks.worker(t.Context(), wg)

	go func() {
		wg.Wait()
		close(ks.workersFinished)
	}()

	err := ks.SendAsync(t.Context(), ev, func(e event.PageViewEvent, err error) {
		assert.ErrorIs(t, err, expectedErr)
		close(done)
	})

	assert.NoError(t, err)

	select {
	case <-done:
	case <-time.After(time.Second):
		assert.Fail(t, "callback не был вызван")
	}

	assert.NoError(t, ks.Close())
}
