package bolt

import (
	"bytes"
	"io"

	"github.com/tinylib/msgp/msgp"
	"github.com/yosisa/pluq/storage"
	"github.com/yosisa/pluq/types"
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

func unmarshal(b []byte) (out []*Message, err error) {
	r := msgp.NewReader(bytes.NewReader(b))
	for {
		var m Message
		if err = m.DecodeMsg(r); err != nil {
			if err == io.EOF {
				err = nil
			}
			return
		}
		out = append(out, &m)
	}
}

func reconstruct(sd scheduleData, b []byte) (*storage.Envelope, error) {
	ms, err := unmarshal(b)
	if err != nil {
		return nil, err
	}
	envelope := &storage.Envelope{
		Retry:   sd.retry(),
		Timeout: types.Duration(sd.timeout()),
	}
	for _, m := range ms {
		envelope.AddMessage(&storage.Message{
			ContentType: m.ContentType,
			Meta:        m.Meta,
			Body:        m.Body,
		})
	}
	return envelope, nil
}
