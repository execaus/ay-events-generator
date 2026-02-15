package generator_metrics

import (
	"ay-events-generator/internal/generator"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

type Metrics struct {
	registry *prometheus.Registry
}

func NewMetrics() *Metrics {
	return &Metrics{
		registry: prometheus.NewRegistry(),
	}
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

func (m *Metrics) CollectEventGenerator(gen *generator.EventGenerator) error {
	eventCount := prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "event_generated_count",
		},
	)

	if err := m.registry.Register(eventCount); err != nil {
		zap.L().Error(err.Error())
		return err
	}

	gen.AddPostCreateEventsListener(func(count int) {
		eventCount.Add(float64(count))
	})

	return nil
}
