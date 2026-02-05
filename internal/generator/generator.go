package generator

import (
	"ay-events-generator/internal/event"
	"crypto/rand"
	mrand "math/rand"
	"net"
	"slices"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

// Максимальная длительность просмотра по умолчанию (мс)
const defaultDurationMax = 30_000

// Процент "отскоков" по умолчанию
const defaultBounceRate = 0.3

// Режим генерации по умолчанию
const defaultMode = RegularMode

// Максимальное значение длительности для определения отскока
const bounceMax = 5_000

// Режимы генерации событий
const (
	RegularMode  = "regular" // Постоянный поток событий
	PickLoadMode = "pick"    // Пиковая нагрузка
	NightMode    = "night"   // Ночные редкие события
)

// Вероятности генерации события для разных режимов
const (
	regularModeEventProb = 0.1
	pickLoadMinEvents    = 5
	pickLoadMaxEvents    = 50
	nightModeEventProb   = 0.01
)

// Частота тикера генерации
const tickDuration = 100 * time.Millisecond

var (
	// Список возможных user-agent для событий
	agents = [...]string{
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
		"Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X)",
		"Mozilla/5.0 (Linux; Android 14)",
	}
	// Доступные регионы пользователей
	regions = [...]string{
		"EU",
		"US",
		"APAC",
		"LATAM",
	}
	// Доступные режимы генерации
	mods = [...]string{RegularMode, PickLoadMode, NightMode}
)

// EventGenerator структура генератора событий
type EventGenerator struct {
	DurationMax  int                      // Максимальная длительность события
	BounceRate   float32                  // Вероятность отскока
	Mode         string                   // Режим генерации
	eventChannel chan event.PageViewEvent // Канал для отправки событий
	stopChannel  chan struct{}            // Канал для остановки генерации
}

// NewEventGenerator создает новый экземпляр генератора событий с настройками по умолчанию
func NewEventGenerator() *EventGenerator {
	return &EventGenerator{
		DurationMax:  defaultDurationMax,
		BounceRate:   defaultBounceRate,
		Mode:         defaultMode,
		eventChannel: make(chan event.PageViewEvent),
		stopChannel:  make(chan struct{}),
	}
}

// SetDurationMax задает максимальную длительность события
func (g *EventGenerator) SetDurationMax(value int) *EventGenerator {
	g.DurationMax = value
	return g
}

// SetBounceRate задает вероятность "отскока" для событий
func (g *EventGenerator) SetBounceRate(value float32) *EventGenerator {
	g.BounceRate = value
	return g
}

// SetMode задает режим генерации событий
func (g *EventGenerator) SetMode(mode string) {
	if !slices.Contains(mods[:], mode) {
		zap.L().Error("invalid mode")
	}
	g.Mode = mode
}

// eventTick определяет количество событий, генерируемых за тик, в зависимости от режима
func (g *EventGenerator) eventTick() int {
	switch g.Mode {
	case RegularMode:
		if mrand.Float32() < regularModeEventProb {
			return 0
		}
		return 1
	case PickLoadMode:
		return mrand.Intn(pickLoadMaxEvents-pickLoadMinEvents+1) + pickLoadMinEvents
	case NightMode:
		if mrand.Float32() < nightModeEventProb {
			return 1
		}
		return 0
	default:
		zap.L().Error("invalid mode")
		return 0
	}
}

// Event генерирует одно событие PageViewEvent
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

// Listen возвращает канал событий и запускает генерацию в фоне
func (g *EventGenerator) Listen() <-chan event.PageViewEvent {
	go func() {
		ticker := time.NewTicker(tickDuration)
		defer ticker.Stop()

		for {
			select {
			case <-g.stopChannel:
				close(g.eventChannel)
				return
			case <-ticker.C:
				for range g.eventTick() {
					g.eventChannel <- g.Event()
				}
			}
		}
	}()
	return g.eventChannel
}

func (g *EventGenerator) Close() {
	close(g.stopChannel)
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
