package storage

import (
	"time"

	"github.com/yosisa/pluq/types"
)

var (
	DefaultRetry   = types.Retry(10)
	DefaultTimeout = types.Duration(30 * time.Second)
)

type Envelope struct {
	Queue    string
	Retry    types.Retry
	Timeout  types.Duration
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

type Message struct {
	ContentType string
	Meta        map[string]interface{}
	Body        []byte
}
