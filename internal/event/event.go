package event

import (
	"encoding/json"
	"time"

	"go.uber.org/zap"
)

type PageViewEvent struct {
	PageID       string    `json:"page_id"`
	UserID       string    `json:"user_id"`
	ViewDuration int       `json:"view_duration_ms"`
	Timestamp    time.Time `json:"timestamp"`
	UserAgent    string    `json:"user_agent,omitempty"`
	IPAddress    string    `json:"ip_address,omitempty"`
	Region       string    `json:"region,omitempty"`
	IsBounce     bool      `json:"is_bounce"`
}

func (e *PageViewEvent) Bytes() ([]byte, error) {
	b, err := json.Marshal(e)
	if err != nil {
		zap.L().Error(err.Error())
		return nil, err
	}
	return b, nil
}

func (e *PageViewEvent) String() (string, error) {
	b, err := e.Bytes()
	if err != nil {
		zap.L().Error(err.Error())
		return "", nil
	}
	return string(b), nil
}
