package event

type EventType int

const (
	EventMessagePushed EventType = iota
	EventMessagePoped
	EventMessageProceeded
	EventMessageDiscarded
	EventMessageAvailable
)

type Handler interface {
	HandleEvent(EventType, interface{})
}

type HandlerFunc func(EventType, interface{})

func (f HandlerFunc) HandleEvent(e EventType, v interface{}) {
	f(e, v)
}

type event struct {
	e EventType
	v interface{}
}

var (
	handlers    = make(map[EventType][]Handler)
	allHandlers []Handler
	eventc      = make(chan *event, 1000)
)

func Emit(e EventType, v interface{}) {
	eventc <- &event{e, v}
}

func Handle(e EventType, h Handler) {
	handlers[e] = append(handlers[e], h)
}

func HandleAll(h Handler) {
	allHandlers = append(allHandlers, h)
}

func Dispatch() {
	for ev := range eventc {
		for _, h := range allHandlers {
			h.HandleEvent(ev.e, ev.v)
		}
		for _, h := range handlers[ev.e] {
			h.HandleEvent(ev.e, ev.v)
		}
	}
}
