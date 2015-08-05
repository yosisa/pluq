package bolt

import (
	"time"

	"github.com/yosisa/pluq/storage"
)

//go:generate msgp
type Message struct {
	Body        []byte                 `msg:"body"`
	ContentType string                 `msg:"ct"`
	Meta        map[string]interface{} `msg:"meta"`
}

func marshal(src *storage.Envelope) ([]byte, error) {
	m := &Message{
		Body:        src.Messages[0].Body,
		ContentType: src.Messages[0].ContentType,
		Meta:        src.Messages[0].Meta,
	}
	return m.MarshalMsg(nil)
}

func unmarshal(b []byte) (*Message, error) {
	var m Message
	_, err := m.UnmarshalMsg(b)
	return &m, err
}

func reconstruct(sd scheduleData, b []byte) (*storage.Envelope, error) {
	m, err := unmarshal(b)
	if err != nil {
		return nil, err
	}
	envelope := &storage.Envelope{
		Retry:   sd.retry(),
		Timeout: time.Duration(sd.timeout()),
		Messages: []*storage.Message{
			{ContentType: m.ContentType, Meta: m.Meta, Body: m.Body},
		},
	}
	return envelope, nil
}
