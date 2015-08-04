package storage

import "time"

const RetryNoLimit = -100

var (
	DefaultRetry   = 10
	DefaultTimeout = 30 * time.Second
)

type Message struct {
	Body        []byte
	ContentType string
	Meta        map[string]interface{}
	Retry       int
	Timeout     time.Duration
}

func NewMessage() *Message {
	return &Message{
		Meta:    make(map[string]interface{}),
		Retry:   DefaultRetry,
		Timeout: DefaultTimeout,
	}
}

func (m *Message) IncrRetry() {
	m.Retry = IncrRetry(m.Retry)
}

func (m *Message) DecrRetry() {
	m.Retry = DecrRetry(m.Retry)
}

func (m *Message) CanRetry() bool {
	return CanRetry(m.Retry)
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
