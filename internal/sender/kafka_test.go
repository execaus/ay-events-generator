package sender

import (
	"ay-events-generator/internal/event"
	mock_sender "ay-events-generator/internal/sender/mock"
	"context"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestKafkaSender_SendSync(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockWriter := mock_sender.NewMockKafkaWriter(ctrl)

	// Пример события
	ev := event.PageViewEvent{
		PageID:       "page_1",
		UserID:       "user_1",
		ViewDuration: 1000,
		Timestamp:    time.Now(),
	}

	// Преобразуем в JSON
	b, err := ev.Bytes()
	require.NoError(t, err)

	// Ожидаем вызов WriteMessages с правильными параметрами
	mockWriter.EXPECT().
		WriteMessages(gomock.Any(), gomock.AssignableToTypeOf([]kafka.Message{})).
		DoAndReturn(func(ctx context.Context, msgs ...kafka.Message) error {
			require.Len(t, msgs, 1)
			require.Equal(t, ev.PageID, string(msgs[0].Key))
			require.Equal(t, b, msgs[0].Value)
			return nil
		})

	ks := &KafkaSender{writer: mockWriter}
	err = ks.SendSync(t.Context(), ev)
	require.NoError(t, err)
}

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
			time.Sleep(100 * time.Millisecond) // имитация задержки
			close(called)
			return nil
		})

	ks := &KafkaSender{writer: mockWriter}

	start := time.Now()
	err := ks.SendSync(t.Context(), ev)
	elapsed := time.Since(start)

	<-called // убедимся, что вызов WriteMessages завершился

	require.NoError(t, err)
	require.GreaterOrEqual(t, elapsed, 100*time.Millisecond)
}
