package storage

import "time"

const RetryNoLimit = -100

var (
	DefaultRetry   = 10
	DefaultTimeout = 30 * time.Second
)

type Envelope struct {
	Retry    int
	Timeout  time.Duration
	Messages []*Message
}

func NewEnvelope() *Envelope {
	return &Envelope{
		Retry:   DefaultRetry,
		Timeout: DefaultTimeout,
	}
}

func (e *Envelope) AddMessage(m *Message) {
	e.Messages = append(e.Messages, m)
}

func (e *Envelope) IsComposite() bool {
	return len(e.Messages) > 1
}

func (e *Envelope) IncrRetry() {
	e.Retry = IncrRetry(e.Retry)
}

func (e *Envelope) DecrRetry() {
	e.Retry = DecrRetry(e.Retry)
}

func (e *Envelope) CanRetry() bool {
	return CanRetry(e.Retry)
}

type Message struct {
	ContentType string
	Meta        map[string]interface{}
	Body        []byte
}

func IncrRetry(n int) int {
	if n == RetryNoLimit {
		return n
	}
	return n + 1
}

func DecrRetry(n int) int {
	if n == RetryNoLimit {
		return n
	}
	return n - 1
}

func CanRetry(n int) bool {
	if n == RetryNoLimit {
		return true
	}
	return n > 0
}
