package sender

import "ay-events-generator/internal/event"

type AsyncCallback = func(event event.PageViewEvent, err error)

type AsyncMessage struct {
	event    event.PageViewEvent
	callback AsyncCallback
}
