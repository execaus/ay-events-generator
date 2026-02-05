package generator

import "ay-events-generator/internal/event"

type Event struct {
	Event event.PageViewEvent
	Meta  Meta
}

type Meta struct {
	IsInvalid bool
}
