package generator

import (
	"testing"
	"time"
)

func TestViewDurationMaxBound(t *testing.T) {
	maxDuration := 80_000

	g := NewEventGenerator()
	g.SetDurationMax(maxDuration)

	for range 1000 {
		e := g.Event()
		if !e.Meta.IsInvalid && e.Event.ViewDuration <= 0 || e.Event.ViewDuration > maxDuration {
			t.Fatalf("ViewDuration out of bounds: %d", e.Event.ViewDuration)
		}
	}
}

func TestRandomizationActuallyChangesValues(t *testing.T) {
	g := NewEventGenerator()

	e1 := g.Event()
	e2 := g.Event()

	if e1.Event.PageID == e2.Event.PageID {
		t.Fatal("PageID did not change between events")
	}

	if e1.Event.UserID == e2.Event.UserID {
		t.Fatal("UserID did not change between events")
	}

	if e1.Event.IPAddress == e2.Event.IPAddress {
		t.Fatal("IPAddress did not change between events")
	}
}

func TestRegularModeEventCount(t *testing.T) {
	t.Parallel()

	count := getEventCount(RegularMode, 30*time.Second)

	if count < 30 || count > 300 {
		t.Fatalf("RegularMode: expected 30–300 events, got %d", count)
	}
}

func TestPickLoadModeEventCount(t *testing.T) {
	t.Parallel()

	count := getEventCount(PickLoadMode, 30*time.Second)

	if count < 1500 || count > 15000 {
		t.Fatalf("PickLoadMode: expected 1,500–15,000 events, got %d", count)
	}
}

func TestNightModeEventCount(t *testing.T) {
	t.Parallel()

	count := getEventCount(NightMode, 30*time.Second)

	if count < 1 || count > 6 {
		t.Fatalf("NightMode: expected 1-6 events, got %d", count)
	}
}

func getEventCount(mode string, duration time.Duration) int {
	g := NewEventGenerator()
	g.SetMode(mode)

	result := make(chan int)
	go func() {
		count := 0
		for range g.Listen() {
			count++
		}
		result <- count
	}()

	time.Sleep(duration)
	g.Close()

	return <-result
}

func TestInvalidEventRate(t *testing.T) {
	const totalEvents = 10000
	const expectedRate = 0.05
	const tolerance = 0.01

	g := NewEventGenerator()
	g.SetInvalidRate(expectedRate)

	invalidCount := 0
	for range totalEvents {
		e := g.Event()
		if e.Meta.IsInvalid {
			invalidCount++
		}
	}

	actualRate := float64(invalidCount) / float64(totalEvents)
	if actualRate < expectedRate-tolerance || actualRate > expectedRate+tolerance {
		t.Fatalf("Invalid rate out of expected bounds: got %.4f, expected %.4f ± %.4f", actualRate, expectedRate, tolerance)
	}
}
