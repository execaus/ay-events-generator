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
		if e.ViewDuration <= 0 || e.ViewDuration > maxDuration {
			t.Fatalf("ViewDuration out of bounds: %d", e.ViewDuration)
		}
	}
}

func TestRandomizationActuallyChangesValues(t *testing.T) {
	g := NewEventGenerator()

	e1 := g.Event()
	e2 := g.Event()

	if e1.PageID == e2.PageID {
		t.Fatal("PageID did not change between events")
	}

	if e1.UserID == e2.UserID {
		t.Fatal("UserID did not change between events")
	}

	if e1.IPAddress == e2.IPAddress {
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
