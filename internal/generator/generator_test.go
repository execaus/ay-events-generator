package generator

import (
	"testing"
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
