package generator

import (
	"ay-events-generator/internal/event"
	"crypto/rand"
	mrand "math/rand"
	"net"
	"time"

	"github.com/google/uuid"
)

const (
	defaultDurationMax = 30_000
	defaultBounceRate  = 0.3

	bounceMax = 5_000
)

var (
	agents = [...]string{
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
		"Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X)",
		"Mozilla/5.0 (Linux; Android 14)",
	}
	regions = [...]string{
		"EU",
		"US",
		"APAC",
		"LATAM",
	}
)

type EventGenerator struct {
	DurationMax int
	BounceRate  float32
}

func NewEventGenerator() *EventGenerator {
	return &EventGenerator{
		DurationMax: defaultDurationMax,
		BounceRate:  defaultBounceRate,
	}
}

func (g *EventGenerator) SetDurationMax(value int) *EventGenerator {
	g.DurationMax = value
	return g
}

func (g *EventGenerator) SetBounceRate(value float32) *EventGenerator {
	g.BounceRate = value
	return g
}

func (g *EventGenerator) Event() event.PageViewEvent {
	var isBounce bool

	duration := mrand.Intn(g.DurationMax) + 1

	if duration < bounceMax {
		isBounce = false
	} else {
		isBounce = mrand.Float32() < g.BounceRate
	}

	return event.PageViewEvent{
		PageID:       uuid.NewString(),
		UserID:       uuid.NewString(),
		ViewDuration: duration,
		Timestamp:    time.Now(),
		UserAgent:    g.randomUserAgent(),
		IPAddress:    g.randomIPv4(),
		Region:       g.randomRegion(),
		IsBounce:     isBounce,
	}
}

func (g *EventGenerator) randomUserAgent() string {
	return agents[mrand.Intn(len(agents))]
}

func (g *EventGenerator) randomRegion() string {
	return regions[mrand.Intn(len(regions))]
}

func (g *EventGenerator) randomIPv4() string {
	ip := make(net.IP, 4)
	_, _ = rand.Read(ip)
	return ip.String()
}
