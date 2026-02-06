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
const defaultDurationMax = 600

// Процент "отскоков" по умолчанию
const defaultBounceRate = 0.1

// Процент событий с преднамеренными ошибками
const defaultInvalidRate = 0.05

// Максимальное значение длительности для определения отскока
const bounceMax = 5_000

// Типы дефектов события
const (
	emptyPageIDDefect = iota
	negativeDurationDefect
	invalidJSONDefect
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
	mods = [...]Mode{RegularMode, PickLoadMode, NightMode}
	// Дефекты событий
	defects = [...]int{emptyPageIDDefect, negativeDurationDefect, invalidJSONDefect}
)

// EventGenerator структура генератора событий
type EventGenerator struct {
	DurationMax  int           // Максимальная длительность события
	BounceRate   float32       // Вероятность отскока
	InvalidRate  float32       // Вероятность преднамеренной ошибки
	Mode         Mode          // Режим генерации
	eventChannel chan Event    // Канал для отправки событий
	stopChannel  chan struct{} // Канал для остановки генерации
}

// NewEventGenerator создает новый экземпляр генератора событий с настройками по умолчанию
func NewEventGenerator() *EventGenerator {
	return &EventGenerator{
		DurationMax:  defaultDurationMax,
		BounceRate:   defaultBounceRate,
		InvalidRate:  defaultInvalidRate,
		Mode:         defaultMode,
		eventChannel: make(chan Event),
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
func (g *EventGenerator) SetMode(mode Mode) {
	if !slices.Contains(mods[:], mode) {
		zap.L().Error("invalid mode")
	}
	g.Mode = mode
}

// SetInvalidRate задает вероятность преднамеренной ошибки в событии
func (g *EventGenerator) SetInvalidRate(value float32) {
	g.InvalidRate = value
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
func (g *EventGenerator) Event() Event {
	var isBounce, isInvalid bool

	duration := mrand.Intn(g.DurationMax) + 1

	if duration < bounceMax {
		isBounce = false
	} else {
		isBounce = mrand.Float32() < g.BounceRate
	}

	isInvalid = mrand.Float32() < g.InvalidRate

	if isInvalid {
		return g.getInvalidEvent()
	}

	return g.getValidEvent(duration, isBounce)
}

// Events возвращает канал событий и запускает генерацию в фоне
func (g *EventGenerator) Events() <-chan Event {
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

// getInvalidEvent генерирует случайное "недействительное" событие с одним из предопределённых дефектов
func (g *EventGenerator) getInvalidEvent() Event {
	var e event.PageViewEvent

	defectType := mrand.Intn(len(defects))

	switch defectType {
	case emptyPageIDDefect:
		e = event.PageViewEvent{
			PageID:       "",
			UserID:       uuid.NewString(),
			ViewDuration: mrand.Intn(g.DurationMax) + 1,
			Timestamp:    time.Now(),
			UserAgent:    g.randomUserAgent(),
			IPAddress:    g.randomIPv4(),
			Region:       g.randomRegion(),
			IsBounce:     false,
		}
	case negativeDurationDefect:
		e = event.PageViewEvent{
			PageID:       uuid.NewString(),
			UserID:       uuid.NewString(),
			ViewDuration: -(mrand.Intn(g.DurationMax) + 1),
			Timestamp:    time.Now(),
			UserAgent:    g.randomUserAgent(),
			IPAddress:    g.randomIPv4(),
			Region:       g.randomRegion(),
			IsBounce:     false,
		}
	case invalidJSONDefect:
		e = event.PageViewEvent{
			PageID:       uuid.NewString(),
			UserID:       uuid.NewString(),
			ViewDuration: mrand.Intn(g.DurationMax) + 1,
			Timestamp:    time.Now(),
			UserAgent:    string([]byte{0xff, 0xfe, 0xfd}), // некорректные байты
			IPAddress:    g.randomIPv4(),
			Region:       g.randomRegion(),
			IsBounce:     false,
		}
	default:
		zap.L().Error("invalid defect type")
	}

	return Event{
		Event: e,
		Meta: Meta{
			IsInvalid: true,
		},
	}
}

// getValidEvent возращает корректное событие
func (g *EventGenerator) getValidEvent(duration int, isBounce bool) Event {
	return Event{
		Event: event.PageViewEvent{
			PageID:       uuid.NewString(),
			UserID:       uuid.NewString(),
			ViewDuration: duration,
			Timestamp:    time.Now(),
			UserAgent:    g.randomUserAgent(),
			IPAddress:    g.randomIPv4(),
			Region:       g.randomRegion(),
			IsBounce:     isBounce,
		},
		Meta: Meta{
			IsInvalid: false,
		},
	}
}
