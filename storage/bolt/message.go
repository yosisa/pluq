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

func marshal(src *storage.Message) ([]byte, error) {
	m := &Message{
		Body:        src.Body,
		ContentType: src.ContentType,
		Meta:        src.Meta,
	}
	return m.MarshalMsg(nil)
}

func unmarshal(b []byte) (*Message, error) {
	var m Message
	_, err := m.UnmarshalMsg(b)
	return &m, err
}

func reconstruct(sd scheduleData, b []byte) (*storage.Message, error) {
	m, err := unmarshal(b)
	if err != nil {
		return nil, err
	}
	msg := &storage.Message{
		Body:        m.Body,
		ContentType: m.ContentType,
		Meta:        m.Meta,
		Retry:       sd.retry(),
		Timeout:     time.Duration(sd.timeout()),
	}
	return msg, nil
}
